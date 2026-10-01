/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption

import (
	"context"
	"math"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const (
	defaultScoreBasedMemorySurplus       = 0.2
	defaultScoreBasedMemoryBalanceWeight = 0.0
	scoreBasedMemoryPressureDeadband     = 0.05
	scoreBasedMemoryPressureSaturation   = 2.0
)

type scoreBasedMemoryConfig struct {
	surplus float64
	weight  float64
}

type scoreBasedMemoryShape struct {
	cpu    float64
	memory float64
}

type scoreBasedMemoryPool struct {
	capacityCPU    float64
	capacityMemory float64
	requestCPU     float64
	requestMemory  float64
	capacityRatio  float64
	targetRatio    float64
	pressure       float64
	known          bool
	valid          bool
	shapes         map[string]scoreBasedMemoryShape
}

type scoreBasedMemoryPoolScore struct {
	CapacityMemoryBytesPerCPU float64 `json:"capacityMemoryBytesPerCPU"`
	TargetMemoryBytesPerCPU   float64 `json:"targetMemoryBytesPerCPU"`
	Pressure                  float64 `json:"pressure"`
	NormalizedDelta           float64 `json:"normalizedDelta"`
	Weight                    float64 `json:"weight"`
	SourceNodeCount           int     `json:"sourceNodeCount"`
	Contribution              float64 `json:"contribution"`
}

type scoreBasedMemoryScore struct {
	Enabled         bool                                 `json:"enabled"`
	Correction      float64                              `json:"correction"`
	RemovedCPU      float64                              `json:"removedCPU"`
	SourceNodeCount int                                  `json:"sourceNodeCount"`
	Pools           map[string]scoreBasedMemoryPoolScore `json:"pools,omitempty"`
}

type moveSetScoring struct {
	Score         float64
	BaseScore     float64
	Cost          scoreBasedCostScore
	MemoryBalance scoreBasedMemoryScore
}

type scoreBasedMemorySnapshot struct {
	configs map[string]scoreBasedMemoryConfig
	pools   map[string]*scoreBasedMemoryPool
}

func (snapshot *scoreBasedMemorySnapshot) spanAttributes(nodeCount int) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Int("snapshot_node_count", nodeCount),
		attribute.Int("pool_count", len(snapshot.pools)),
	}
	validPoolCount := 0
	for poolName, pool := range snapshot.pools {
		if !pool.valid {
			continue
		}
		validPoolCount++
		prefix := "pool." + poolName + "."
		attrs = append(attrs,
			attribute.Float64(prefix+"capacity_memory_bytes_per_cpu", pool.capacityRatio),
			attribute.Float64(prefix+"target_memory_bytes_per_cpu", pool.targetRatio),
			attribute.Float64(prefix+"pressure", pool.pressure),
		)
	}
	return append(attrs, attribute.Int("valid_pool_count", validPoolCount))
}

func scoreBasedMemoryConfigs(candidates []*Candidate) map[string]scoreBasedMemoryConfig {
	configs := make(map[string]scoreBasedMemoryConfig)
	for _, candidate := range candidates {
		if candidate == nil || candidate.NodePool == nil {
			continue
		}
		if _, found := configs[candidate.NodePool.Name]; !found {
			configs[candidate.NodePool.Name] = scoreBasedMemoryConfigFor(candidate.NodePool)
		}
	}
	return configs
}

// scoreBasedMemoryConfigFor reads and validates the per-NodePool memory scoring annotations.
func scoreBasedMemoryConfigFor(nodePool *v1.NodePool) scoreBasedMemoryConfig {
	config := scoreBasedMemoryConfig{surplus: defaultScoreBasedMemorySurplus, weight: defaultScoreBasedMemoryBalanceWeight}
	if nodePool == nil || nodePool.Annotations == nil {
		return config
	}
	if raw, found := nodePool.Annotations[v1.ScoreBasedMemorySurplusAnnotationKey]; found {
		config.surplus = parseScoreBasedMemoryAnnotation(nodePool, v1.ScoreBasedMemorySurplusAnnotationKey, raw, defaultScoreBasedMemorySurplus)
	}
	if raw, found := nodePool.Annotations[v1.ScoreBasedMemoryBalanceWeightAnnotationKey]; found {
		config.weight = parseScoreBasedMemoryAnnotation(nodePool, v1.ScoreBasedMemoryBalanceWeightAnnotationKey, raw, defaultScoreBasedMemoryBalanceWeight)
	}
	return config
}

func parseScoreBasedMemoryAnnotation(nodePool *v1.NodePool, key, raw string, fallback float64) float64 {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		log.FromContext(context.Background()).V(1).Info("using default score-based memory setting for invalid value",
			"NodePool", nodePool.Name,
			"annotation", key,
			"value", raw,
			"default", fallback,
		)
		return fallback
	}
	return value
}

func hasScoreBasedMemoryBalanceWeight(configs map[string]scoreBasedMemoryConfig) bool {
	for _, config := range configs {
		if config.weight > 0 {
			return true
		}
	}
	return false
}

func scoreBasedMemoryConfigChanged(previous map[string]string, candidates []*Candidate) (map[string]string, bool) {
	current := make(map[string]string)
	for _, candidate := range candidates {
		if candidate == nil || candidate.NodePool == nil {
			continue
		}
		name := candidate.NodePool.Name
		if _, found := current[name]; found {
			continue
		}
		annotations := candidate.NodePool.Annotations
		surplus, hasSurplus := annotations[v1.ScoreBasedMemorySurplusAnnotationKey]
		weight, hasWeight := annotations[v1.ScoreBasedMemoryBalanceWeightAnnotationKey]
		if !hasSurplus && !hasWeight {
			continue
		}
		current[name] = surplus + "\x00" + weight
	}
	if len(current) != len(previous) {
		return current, true
	}
	for name, value := range current {
		if previous[name] != value {
			return current, true
		}
	}
	return current, false
}

// newScoreBasedMemorySnapshot aggregates pool ratios once from the node snapshot owned by the scheduler search.
//
//nolint:gocyclo // Keep the per-pool aggregation and invalid-profile handling in one snapshot pass.
func newScoreBasedMemorySnapshot(nodes state.StateNodes, configs map[string]scoreBasedMemoryConfig) *scoreBasedMemorySnapshot {
	enabled := false
	for _, config := range configs {
		if config.weight > 0 {
			enabled = true
			break
		}
	}
	if !enabled {
		return nil
	}
	snapshot := &scoreBasedMemorySnapshot{
		configs: configs,
		pools:   make(map[string]*scoreBasedMemoryPool),
	}
	for name := range configs {
		snapshot.pools[name] = &scoreBasedMemoryPool{known: true, shapes: map[string]scoreBasedMemoryShape{}}
	}
	for _, node := range nodes {
		if node == nil || !node.Managed() {
			continue
		}
		poolName := node.Labels()[v1.NodePoolLabelKey]
		pool, tracked := snapshot.pools[poolName]
		if !tracked {
			continue
		}
		shape := scoreBasedMemoryNodeShape(node)
		if shape.cpu > 0 && shape.memory > 0 && finiteScoreValue(shape.cpu) && finiteScoreValue(shape.memory) {
			pool.shapes[node.Name()] = shape
		}
		if node.MarkedForDeletion() || node.Node == nil || !node.Initialized() || standby.IsNodeClaimStandby(node.NodeClaim) || standby.IsNodeClaimActivating(node.NodeClaim) || standby.HasNodeTaint(node.Node) {
			continue
		}
		if shape.cpu <= 0 || shape.memory <= 0 || !finiteScoreValue(shape.cpu) || !finiteScoreValue(shape.memory) {
			pool.known = false
			continue
		}
		requests := scoreBasedNonDaemonRequests(node)
		pool.capacityCPU += shape.cpu
		pool.capacityMemory += shape.memory
		pool.requestCPU += requests.cpu
		pool.requestMemory += requests.memory
	}
	for poolName, pool := range snapshot.pools {
		config := snapshot.configs[poolName]
		if !pool.known || pool.capacityCPU <= 0 || pool.capacityMemory <= 0 || pool.requestCPU <= 0 || pool.requestMemory <= 0 {
			continue
		}
		capacityRatio := pool.capacityMemory / pool.capacityCPU
		targetRatio := (1 + config.surplus) * pool.requestMemory / pool.requestCPU
		if !finiteScoreValue(capacityRatio) || !finiteScoreValue(targetRatio) || capacityRatio <= 0 || targetRatio <= 0 {
			continue
		}
		pool.capacityRatio = capacityRatio
		pool.targetRatio = targetRatio
		pool.pressure = scoreBasedMemoryPressure(capacityRatio, targetRatio)
		pool.valid = true
	}
	return snapshot
}

// score evaluates one complete command so replacement and standby additions are accounted once per joint move.
//
//nolint:gocyclo // Keep source, replacement, and standby deltas in one joint command correction.
func (snapshot *scoreBasedMemorySnapshot) score(command Command) scoreBasedMemoryScore {
	details := scoreBasedMemoryScore{}
	if snapshot == nil {
		return details
	}
	memoryActive := false
	for _, candidate := range command.Candidates {
		if candidate == nil || candidate.NodePool == nil {
			continue
		}
		if snapshot.configs[candidate.NodePool.Name].weight > 0 {
			memoryActive = true
		}
	}
	for _, replacement := range command.Replacements {
		if replacement == nil || replacement.NodeClaim == nil {
			continue
		}
		if snapshot.configs[replacement.NodeClaim.Labels[v1.NodePoolLabelKey]].weight > 0 {
			memoryActive = true
		}
	}
	for _, existing := range command.Results.ExistingNodes {
		if existing == nil || existing.NodeClaim == nil || !standby.IsNodeClaimStandby(existing.NodeClaim) || len(existing.Pods) == 0 {
			continue
		}
		if snapshot.configs[existing.Labels()[v1.NodePoolLabelKey]].weight > 0 {
			memoryActive = true
		}
	}
	if !memoryActive {
		return details
	}
	details.Enabled = true
	details.SourceNodeCount = len(command.Candidates)
	details.Pools = map[string]scoreBasedMemoryPoolScore{}
	localDelta := map[string]float64{}
	poolNames := map[string]struct{}{}
	for _, candidate := range command.Candidates {
		if candidate == nil || candidate.NodePool == nil {
			return scoreBasedMemoryScore{Enabled: true}
		}
		poolName := candidate.NodePool.Name
		pool := snapshot.pools[poolName]
		if pool == nil {
			return scoreBasedMemoryScore{Enabled: true}
		}
		shape, found := pool.shapes[candidate.Name()]
		if !found || shape.cpu <= 0 || shape.memory <= 0 {
			return scoreBasedMemoryScore{Enabled: true}
		}
		details.RemovedCPU += shape.cpu
		if snapshot.configs[poolName].weight > 0 {
			if !pool.valid {
				return scoreBasedMemoryScore{Enabled: true}
			}
			localDelta[poolName] += memoryShapeDelta(shape, pool.capacityRatio)
			poolNames[poolName] = struct{}{}
		}
	}
	if details.RemovedCPU <= 0 || !finiteScoreValue(details.RemovedCPU) {
		return scoreBasedMemoryScore{Enabled: true}
	}
	for _, replacement := range command.Replacements {
		if replacement == nil || replacement.NodeClaim == nil {
			return scoreBasedMemoryScore{Enabled: true}
		}
		poolName := replacement.Labels[v1.NodePoolLabelKey]
		if _, configured := snapshot.configs[poolName]; !configured {
			return scoreBasedMemoryScore{Enabled: true}
		}
		pool := snapshot.pools[poolName]
		if pool == nil {
			return scoreBasedMemoryScore{Enabled: true}
		}
		config := snapshot.configs[poolName]
		if config.weight <= 0 {
			continue
		}
		if !pool.valid {
			return scoreBasedMemoryScore{Enabled: true}
		}
		shapeOptions := replacement.ScoreBasedMemoryAllocatableOptions()
		if len(shapeOptions) == 0 {
			return scoreBasedMemoryScore{Enabled: true}
		}
		contribution, found := conservativeAddedMemoryDelta(shapeOptions, pool.capacityRatio, pool.pressure)
		if !found {
			return scoreBasedMemoryScore{Enabled: true}
		}
		localDelta[poolName] += contribution
		poolNames[poolName] = struct{}{}
	}
	for _, existing := range command.Results.ExistingNodes {
		if existing == nil || existing.NodeClaim == nil || !standby.IsNodeClaimStandby(existing.NodeClaim) || len(existing.Pods) == 0 {
			continue
		}
		poolName := existing.Labels()[v1.NodePoolLabelKey]
		if _, configured := snapshot.configs[poolName]; !configured {
			return scoreBasedMemoryScore{Enabled: true}
		}
		pool := snapshot.pools[poolName]
		if pool == nil {
			return scoreBasedMemoryScore{Enabled: true}
		}
		if snapshot.configs[poolName].weight <= 0 {
			continue
		}
		shape, found := pool.shapes[existing.Name()]
		if !found || !pool.valid {
			return scoreBasedMemoryScore{Enabled: true}
		}
		localDelta[poolName] -= memoryShapeDelta(shape, pool.capacityRatio)
		poolNames[poolName] = struct{}{}
	}
	if len(poolNames) == 0 {
		return details
	}
	denominator := details.RemovedCPU
	for poolName := range poolNames {
		pool := snapshot.pools[poolName]
		config := snapshot.configs[poolName]
		normalizedDelta := clampUnit(localDelta[poolName] / denominator)
		contribution := config.weight * pool.pressure * normalizedDelta * float64(details.SourceNodeCount)
		details.Pools[poolName] = scoreBasedMemoryPoolScore{
			CapacityMemoryBytesPerCPU: pool.capacityRatio,
			TargetMemoryBytesPerCPU:   pool.targetRatio,
			Pressure:                  pool.pressure,
			NormalizedDelta:           normalizedDelta,
			Weight:                    config.weight,
			SourceNodeCount:           details.SourceNodeCount,
			Contribution:              contribution,
		}
		details.Correction += contribution
	}
	if !finiteScoreValue(details.Correction) {
		return scoreBasedMemoryScore{Enabled: true}
	}
	return details
}

func scoreBasedMemoryNodeShape(node *state.StateNode) scoreBasedMemoryShape {
	allocatable := node.Allocatable()
	daemonRequests := node.DaemonSetRequests()
	return scoreBasedMemoryShape{
		cpu:    nonNegativeResourceValue(allocatable, daemonRequests, corev1.ResourceCPU),
		memory: nonNegativeResourceValue(allocatable, daemonRequests, corev1.ResourceMemory),
	}
}

func scoreBasedNonDaemonRequests(node *state.StateNode) scoreBasedMemoryShape {
	requests := node.PodRequests()
	daemonRequests := node.DaemonSetRequests()
	return scoreBasedMemoryShape{
		cpu:    nonNegativeResourceValue(requests, daemonRequests, corev1.ResourceCPU),
		memory: nonNegativeResourceValue(requests, daemonRequests, corev1.ResourceMemory),
	}
}

func nonNegativeResourceValue(total, subtract corev1.ResourceList, name corev1.ResourceName) float64 {
	quantity := total[name].DeepCopy()
	if value, found := subtract[name]; found {
		quantity.Sub(value)
	}
	if quantity.Sign() < 0 {
		return 0
	}
	return quantity.AsApproximateFloat64()
}

func scoreBasedMemoryPressure(capacityRatio, targetRatio float64) float64 {
	ratio := capacityRatio / targetRatio
	if ratio >= 1-scoreBasedMemoryPressureDeadband && ratio <= 1+scoreBasedMemoryPressureDeadband {
		return 0
	}
	pressure := math.Log(ratio) / math.Log(scoreBasedMemoryPressureSaturation)
	return math.Max(-1, math.Min(1, pressure))
}

func finiteScoreValue(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}

func conservativeAddedMemoryDelta(options []corev1.ResourceList, capacityRatio, pressure float64) (float64, bool) {
	var best, bestDelta float64
	found := false
	for _, option := range options {
		shape := scoreBasedMemoryShape{
			cpu:    scoreBasedResourceValue(option, corev1.ResourceCPU),
			memory: scoreBasedResourceValue(option, corev1.ResourceMemory),
		}
		if shape.cpu <= 0 || shape.memory <= 0 || !finiteScoreValue(shape.cpu) || !finiteScoreValue(shape.memory) {
			return 0, false
		}
		shapeDelta := memoryShapeDelta(shape, capacityRatio)
		scoreDelta := -pressure * shapeDelta
		if !finiteScoreValue(scoreDelta) {
			return 0, false
		}
		if !found || scoreDelta < best {
			best = scoreDelta
			bestDelta = shapeDelta
			found = true
		}
	}
	if !found {
		return 0, false
	}
	return -bestDelta, true
}

func memoryShapeDelta(shape scoreBasedMemoryShape, capacityRatio float64) float64 {
	return shape.memory/capacityRatio - shape.cpu
}

func scoreBasedResourceValue(resources corev1.ResourceList, name corev1.ResourceName) float64 {
	quantity := resources[name]
	return quantity.AsApproximateFloat64()
}

func clampUnit(value float64) float64 {
	return math.Max(-1, math.Min(1, value))
}
