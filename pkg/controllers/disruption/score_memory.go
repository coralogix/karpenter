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

type scoreBasedMemoryLoggedShape struct {
	CPUCores    float64 `json:"cpuCores"`
	MemoryBytes float64 `json:"memoryBytes"`
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
	MemorySurplus             float64                     `json:"memorySurplus"`
	Status                    scoreFactorStatus           `json:"status"`
	Reason                    string                      `json:"reason,omitempty"`
	CapacityMemoryBytesPerCPU float64                     `json:"capacityMemoryBytesPerCPU"`
	TargetMemoryBytesPerCPU   float64                     `json:"targetMemoryBytesPerCPU"`
	Pressure                  float64                     `json:"pressure"`
	NormalizedDelta           float64                     `json:"normalizedDelta"`
	Weight                    float64                     `json:"weight"`
	SourceNodeCount           int                         `json:"sourceNodeCount"`
	Contribution              float64                     `json:"contribution"`
	RemovedShape              scoreBasedMemoryLoggedShape `json:"removedShape"`
	AddedShape                scoreBasedMemoryLoggedShape `json:"addedShape"`
	AddedShapeAssessment      string                      `json:"addedShapeAssessment,omitempty"`
}

type scoreBasedMemoryScore struct {
	Enabled         bool                                 `json:"enabled"`
	Status          scoreFactorStatus                    `json:"status"`
	Reason          string                               `json:"reason,omitempty"`
	Contribution    float64                              `json:"contribution"`
	RemovedCPU      float64                              `json:"removedCPU"`
	SourceNodeCount int                                  `json:"sourceNodeCount"`
	Pools           map[string]scoreBasedMemoryPoolScore `json:"pools,omitempty"`
}

type scoreBasedMemoryCommandSummary struct {
	score               scoreBasedMemoryScore
	sources             []scoreBasedMemorySourceCapacity
	replacements        []scoreBasedMemoryReplacementCapacity
	standbyDestinations []scoreBasedMemoryStandbyCapacity
}

type scoreBasedMemorySourceCapacity struct {
	poolName string
	hasPool  bool
	shape    scoreBasedMemoryShape
	hasShape bool
}

type scoreBasedMemoryReplacementCapacity struct {
	poolName       string
	nodeClaimValid bool
	configured     bool
	options        []corev1.ResourceList
}

type scoreBasedMemoryStandbyCapacity struct {
	poolName string
	shape    scoreBasedMemoryShape
	hasShape bool
}

type scoreBasedMemorySnapshot struct {
	pools map[string]*scoreBasedMemoryPool
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

func hasScoreBasedMemoryBalanceWeight(configs scoreBasedScoringConfig) bool {
	for _, config := range configs {
		if config.memory.weight > 0 {
			return true
		}
	}
	return false
}

// newScoreBasedMemorySnapshot aggregates pool ratios once from the node snapshot owned by the scheduler search.
//
//nolint:gocyclo // Keep the per-pool aggregation and invalid-profile handling in one snapshot pass.
func newScoreBasedMemorySnapshot(nodes state.StateNodes, configs scoreBasedScoringConfig) *scoreBasedMemorySnapshot {
	enabled := false
	for _, config := range configs {
		if config.memory.weight > 0 {
			enabled = true
			break
		}
	}
	if !enabled {
		return nil
	}
	snapshot := &scoreBasedMemorySnapshot{
		pools: make(map[string]*scoreBasedMemoryPool),
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
		config := configs[poolName].memory
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

func scoreBasedMemoryScoreForCommand(command Command, snapshot *scoreBasedMemorySnapshot, configs scoreBasedScoringConfig) scoreBasedMemoryScore {
	summary := newScoreBasedMemoryCommandSummary(command, snapshot, configs)
	if !summary.score.Enabled {
		return summary.score
	}
	if snapshot == nil {
		return unavailableScoreBasedMemory(summary.score, "memory_baseline_snapshot_missing")
	}
	return snapshot.scoreCorrection(summary, configs)
}

//nolint:gocyclo // Sources and destinations share one summary so scoring and diagnostics cannot diverge.
func (snapshot *scoreBasedMemorySnapshot) scoreCorrection(summary *scoreBasedMemoryCommandSummary, configs scoreBasedScoringConfig) scoreBasedMemoryScore {
	localDelta := map[string]float64{}
	poolNames := map[string]struct{}{}
	removedCPU := 0.0

	for _, source := range summary.sources {
		if !source.hasPool {
			return unavailableScoreBasedMemory(summary.score, "source_node_pool_missing")
		}
		pool := snapshot.pools[source.poolName]
		if pool == nil {
			return unavailableScoreBasedMemory(summary.score, "source_pool_snapshot_missing")
		}
		if !source.hasShape || source.shape.cpu <= 0 || source.shape.memory <= 0 {
			return unavailableScoreBasedMemory(summary.score, "source_node_shape_unavailable")
		}
		removedCPU += source.shape.cpu
		if configs[source.poolName].memory.weight > 0 {
			if !pool.valid {
				return unavailableScoreBasedMemory(summary.score, "source_pool_baseline_unavailable")
			}
			localDelta[source.poolName] += memoryShapeDelta(source.shape, pool.capacityRatio)
			poolNames[source.poolName] = struct{}{}
		}
	}
	if removedCPU <= 0 || !finiteScoreValue(removedCPU) {
		return unavailableScoreBasedMemory(summary.score, "removed_cpu_unavailable")
	}

	for _, replacement := range summary.replacements {
		if !replacement.nodeClaimValid {
			return unavailableScoreBasedMemory(summary.score, "replacement_node_claim_missing")
		}
		if !replacement.configured {
			return unavailableScoreBasedMemory(summary.score, "replacement_pool_configuration_missing")
		}
		pool := snapshot.pools[replacement.poolName]
		if pool == nil {
			return unavailableScoreBasedMemory(summary.score, "replacement_pool_snapshot_missing")
		}
		config := configs[replacement.poolName].memory
		if config.weight <= 0 {
			continue
		}
		if !pool.valid {
			return unavailableScoreBasedMemory(summary.score, "replacement_pool_baseline_unavailable")
		}
		if len(replacement.options) == 0 {
			return unavailableScoreBasedMemory(summary.score, "replacement_shape_unavailable")
		}
		shape, contribution, found := conservativeAddedMemoryShape(replacement.options, pool.capacityRatio, pool.pressure)
		if !found {
			return unavailableScoreBasedMemory(summary.score, "replacement_shape_unavailable")
		}
		localDelta[replacement.poolName] += contribution
		poolScore := summary.score.Pools[replacement.poolName]
		poolScore.AddedShape = addScoreBasedMemoryShape(poolScore.AddedShape, shape)
		poolScore.AddedShapeAssessment = "conservatively_assessed_worst_case_compatible_offering"
		summary.score.Pools[replacement.poolName] = poolScore
		poolNames[replacement.poolName] = struct{}{}
	}

	for _, destination := range summary.standbyDestinations {
		if _, configured := configs[destination.poolName]; !configured {
			return unavailableScoreBasedMemory(summary.score, "standby_pool_configuration_missing")
		}
		pool := snapshot.pools[destination.poolName]
		if pool == nil {
			return unavailableScoreBasedMemory(summary.score, "standby_pool_snapshot_missing")
		}
		if configs[destination.poolName].memory.weight <= 0 {
			continue
		}
		if !destination.hasShape || !pool.valid {
			return unavailableScoreBasedMemory(summary.score, "standby_shape_or_pool_baseline_unavailable")
		}
		localDelta[destination.poolName] -= memoryShapeDelta(destination.shape, pool.capacityRatio)
		poolScore := summary.score.Pools[destination.poolName]
		poolScore.AddedShape = addScoreBasedMemoryShape(poolScore.AddedShape, destination.shape)
		if poolScore.AddedShapeAssessment == "conservatively_assessed_worst_case_compatible_offering" || poolScore.AddedShapeAssessment == "conservative_replacement_and_measured_standby_shapes" {
			poolScore.AddedShapeAssessment = "conservative_replacement_and_measured_standby_shapes"
		} else {
			poolScore.AddedShapeAssessment = "measured_standby_allocatable_shape"
		}
		summary.score.Pools[destination.poolName] = poolScore
		poolNames[destination.poolName] = struct{}{}
	}
	if len(poolNames) == 0 {
		return unavailableScoreBasedMemory(summary.score, "memory_balance_inputs_unavailable")
	}

	summary.score.Status = scoreFactorApplied
	summary.score.Reason = ""
	summary.score.RemovedCPU = removedCPU
	summary.score.SourceNodeCount = len(summary.sources)
	for poolName := range poolNames {
		pool := snapshot.pools[poolName]
		config := configs[poolName].memory
		normalizedDelta := clampUnit(localDelta[poolName] / removedCPU)
		contribution := config.weight * pool.pressure * normalizedDelta * float64(summary.score.SourceNodeCount)
		poolScore := summary.score.Pools[poolName]
		poolScore.Status = scoreFactorApplied
		poolScore.Reason = ""
		poolScore.CapacityMemoryBytesPerCPU = pool.capacityRatio
		poolScore.TargetMemoryBytesPerCPU = pool.targetRatio
		poolScore.Pressure = pool.pressure
		poolScore.NormalizedDelta = normalizedDelta
		poolScore.Weight = config.weight
		poolScore.SourceNodeCount = summary.score.SourceNodeCount
		poolScore.Contribution = contribution
		summary.score.Pools[poolName] = poolScore
		summary.score.Contribution += contribution
	}
	if !finiteScoreValue(summary.score.Contribution) {
		return unavailableScoreBasedMemory(summary.score, "non_finite_memory_correction")
	}
	return summary.score
}

//nolint:gocyclo // Gather each command capacity input once and retain its diagnostics.
func newScoreBasedMemoryCommandSummary(command Command, snapshot *scoreBasedMemorySnapshot, configs scoreBasedScoringConfig) *scoreBasedMemoryCommandSummary {
	summary := &scoreBasedMemoryCommandSummary{
		score: scoreBasedMemoryScore{
			Status: scoreFactorDisabled,
			Reason: "memory_balance_weight_zero",
			Pools:  map[string]scoreBasedMemoryPoolScore{},
		},
		sources:             make([]scoreBasedMemorySourceCapacity, 0, len(command.Candidates)),
		replacements:        make([]scoreBasedMemoryReplacementCapacity, 0, len(command.Replacements)),
		standbyDestinations: make([]scoreBasedMemoryStandbyCapacity, 0, len(command.Results.ExistingNodes)),
	}
	addPool := func(poolName string) {
		if _, found := summary.score.Pools[poolName]; found {
			return
		}
		config := scoreBasedMemoryConfigForPool(poolName, configs)
		poolScore := scoreBasedMemoryPoolScore{MemorySurplus: config.surplus, Weight: config.weight}
		if config.weight > 0 {
			summary.score.Enabled = true
			summary.score.Status = scoreFactorUnavailable
			summary.score.Reason = "memory_balance_inputs_unavailable"
			poolScore.Status = scoreFactorUnavailable
			poolScore.Reason = "memory_balance_inputs_unavailable"
		} else {
			poolScore.Status = scoreFactorDisabled
			poolScore.Reason = "memory_balance_weight_zero"
		}
		summary.score.Pools[poolName] = poolScore
	}
	for _, candidate := range command.Candidates {
		source := scoreBasedMemorySourceCapacity{}
		if candidate != nil && candidate.NodePool != nil {
			source.hasPool = true
			source.poolName = candidate.NodePool.Name
			addPool(source.poolName)
			if snapshot != nil {
				if pool := snapshot.pools[source.poolName]; pool != nil {
					if shape, found := pool.shapes[candidate.Name()]; found {
						source.shape = shape
						source.hasShape = true
						poolScore := summary.score.Pools[source.poolName]
						poolScore.RemovedShape = addScoreBasedMemoryShape(poolScore.RemovedShape, shape)
						summary.score.Pools[source.poolName] = poolScore
					}
				}
			}
		}
		summary.sources = append(summary.sources, source)
	}
	for _, replacement := range command.Replacements {
		replacementCapacity := scoreBasedMemoryReplacementCapacity{}
		if replacement != nil && replacement.NodeClaim != nil {
			replacementCapacity.nodeClaimValid = true
			replacementCapacity.poolName = replacement.Labels[v1.NodePoolLabelKey]
			_, replacementCapacity.configured = configs[replacementCapacity.poolName]
			addPool(replacementCapacity.poolName)
			if snapshot != nil && configs[replacementCapacity.poolName].memory.weight > 0 {
				replacementCapacity.options = replacement.ScoreBasedMemoryAllocatableOptions()
			}
		}
		summary.replacements = append(summary.replacements, replacementCapacity)
	}
	for _, existing := range command.Results.ExistingNodes {
		if existing == nil || !scoreBasedMemoryPoolHasStandbyAddition(existing.NodeClaim, len(existing.Pods)) {
			continue
		}
		destination := scoreBasedMemoryStandbyCapacity{poolName: existing.Labels()[v1.NodePoolLabelKey]}
		addPool(destination.poolName)
		if snapshot != nil {
			if pool := snapshot.pools[destination.poolName]; pool != nil {
				if shape, found := pool.shapes[existing.Name()]; found {
					destination.shape = shape
					destination.hasShape = true
				}
			}
		}
		summary.standbyDestinations = append(summary.standbyDestinations, destination)
	}
	return summary
}

func scoreBasedMemoryConfigForPool(poolName string, configs scoreBasedScoringConfig) scoreBasedMemoryConfig {
	if config, found := configs[poolName]; found {
		return config.memory
	}
	return scoreBasedMemoryConfig{surplus: defaultScoreBasedMemorySurplus, weight: defaultScoreBasedMemoryBalanceWeight}
}

func scoreBasedMemoryPoolHasStandbyAddition(nodeClaim *v1.NodeClaim, podCount int) bool {
	return nodeClaim != nil && standby.IsNodeClaimStandby(nodeClaim) && podCount > 0
}

func unavailableScoreBasedMemory(score scoreBasedMemoryScore, reason string) scoreBasedMemoryScore {
	score.Status = scoreFactorUnavailable
	score.Reason = reason
	score.Contribution = 0
	score.RemovedCPU = 0
	score.SourceNodeCount = 0
	for poolName, poolScore := range score.Pools {
		poolScore.CapacityMemoryBytesPerCPU = 0
		poolScore.TargetMemoryBytesPerCPU = 0
		poolScore.Pressure = 0
		poolScore.NormalizedDelta = 0
		poolScore.SourceNodeCount = 0
		poolScore.Contribution = 0
		poolScore.AddedShape = scoreBasedMemoryLoggedShape{}
		poolScore.AddedShapeAssessment = ""
		if poolScore.Weight > 0 {
			poolScore.Status = scoreFactorUnavailable
			poolScore.Reason = reason
		}
		score.Pools[poolName] = poolScore
	}
	return score
}

func addScoreBasedMemoryShape(total scoreBasedMemoryLoggedShape, shape scoreBasedMemoryShape) scoreBasedMemoryLoggedShape {
	total.CPUCores += shape.cpu
	total.MemoryBytes += shape.memory
	return total
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

func conservativeAddedMemoryShape(options []corev1.ResourceList, capacityRatio, pressure float64) (scoreBasedMemoryShape, float64, bool) {
	var best, bestDelta float64
	var bestShape scoreBasedMemoryShape
	found := false
	for _, option := range options {
		shape := scoreBasedMemoryShape{
			cpu:    scoreBasedResourceValue(option, corev1.ResourceCPU),
			memory: scoreBasedResourceValue(option, corev1.ResourceMemory),
		}
		if shape.cpu <= 0 || shape.memory <= 0 || !finiteScoreValue(shape.cpu) || !finiteScoreValue(shape.memory) {
			return scoreBasedMemoryShape{}, 0, false
		}
		shapeDelta := memoryShapeDelta(shape, capacityRatio)
		scoreDelta := -pressure * shapeDelta
		if !finiteScoreValue(scoreDelta) {
			return scoreBasedMemoryShape{}, 0, false
		}
		if !found || scoreDelta < best {
			best = scoreDelta
			bestDelta = shapeDelta
			bestShape = shape
			found = true
		}
	}
	if !found {
		return scoreBasedMemoryShape{}, 0, false
	}
	return bestShape, -bestDelta, true
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
