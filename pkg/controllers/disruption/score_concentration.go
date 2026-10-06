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

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

const (
	defaultScoreBasedInstanceTypeConcentrationThreshold = 0.5
	defaultScoreBasedInstanceTypeConcentrationWeight    = 0.0
)

type scoreBasedInstanceTypeConcentrationConfig struct {
	threshold float64
	weight    float64
}

type scoreBasedInstanceTypeConcentrationSourceScore struct {
	NodeName              string  `json:"nodeName"`
	NodePool              string  `json:"nodePool"`
	InstanceType          string  `json:"instanceType"`
	PoolNodeCount         int     `json:"poolNodeCount"`
	InstanceTypeNodeCount int     `json:"instanceTypeNodeCount"`
	Share                 float64 `json:"share"`
	Threshold             float64 `json:"threshold"`
	Weight                float64 `json:"weight"`
	Qualifies             bool    `json:"qualifies"`
	Contribution          float64 `json:"contribution"`
}

type scoreBasedInstanceTypeConcentrationTypeScore struct {
	PoolNodeCount         int     `json:"poolNodeCount"`
	InstanceTypeNodeCount int     `json:"instanceTypeNodeCount"`
	Share                 float64 `json:"share"`
	Threshold             float64 `json:"threshold"`
	Weight                float64 `json:"weight"`
	Qualifies             bool    `json:"qualifies"`
	SourceNodeCount       int     `json:"sourceNodeCount"`
	Contribution          float64 `json:"contribution"`
}

type scoreBasedInstanceTypeConcentrationPoolScore struct {
	PoolNodeCount   int                                                     `json:"poolNodeCount"`
	SourceNodeCount int                                                     `json:"sourceNodeCount"`
	Contribution    float64                                                 `json:"contribution"`
	InstanceTypes   map[string]scoreBasedInstanceTypeConcentrationTypeScore `json:"instanceTypes"`
}

type scoreBasedInstanceTypeConcentrationScore struct {
	Enabled      bool                                                    `json:"enabled"`
	Status       scoreFactorStatus                                       `json:"status"`
	Reason       string                                                  `json:"reason,omitempty"`
	Contribution float64                                                 `json:"contribution"`
	SourceNodes  []scoreBasedInstanceTypeConcentrationSourceScore        `json:"-"`
	Pools        map[string]scoreBasedInstanceTypeConcentrationPoolScore `json:"pools"`
}

type scoreBasedInstanceTypeConcentrationPool struct {
	nodeCount      int
	instanceCounts map[string]int
}

type scoreBasedInstanceTypeConcentrationSnapshot struct {
	pools map[string]*scoreBasedInstanceTypeConcentrationPool
}

func scoreBasedInstanceTypeConcentrationConfigFor(nodePool *v1.NodePool) scoreBasedInstanceTypeConcentrationConfig {
	config := scoreBasedInstanceTypeConcentrationConfig{
		threshold: defaultScoreBasedInstanceTypeConcentrationThreshold,
		weight:    defaultScoreBasedInstanceTypeConcentrationWeight,
	}
	if nodePool == nil || nodePool.Annotations == nil {
		return config
	}
	if raw, found := nodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey]; found {
		config.threshold = parseScoreBasedInstanceTypeConcentrationAnnotation(nodePool, v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey, raw, defaultScoreBasedInstanceTypeConcentrationThreshold, func(value float64) bool {
			return value >= 0 && value < 1
		})
	}
	if raw, found := nodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey]; found {
		config.weight = parseScoreBasedInstanceTypeConcentrationAnnotation(nodePool, v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey, raw, defaultScoreBasedInstanceTypeConcentrationWeight, func(value float64) bool {
			return value >= 0
		})
	}
	return config
}

func parseScoreBasedInstanceTypeConcentrationAnnotation(nodePool *v1.NodePool, key, raw string, fallback float64, valid func(float64) bool) float64 {
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || !valid(value) {
		log.FromContext(context.Background()).V(1).Info("using default score-based instance-type concentration setting for invalid value",
			"NodePool", nodePool.Name,
			"annotation", key,
			"value", raw,
			"default", fallback,
		)
		return fallback
	}
	return value
}

func scoreBasedInstanceTypeConcentrationConfigChanged(previous map[string]string, candidates []*Candidate) (map[string]string, bool) {
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
		threshold, hasThreshold := annotations[v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey]
		weight, hasWeight := annotations[v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey]
		if !hasThreshold && !hasWeight {
			continue
		}
		current[name] = threshold + "\x00" + weight
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

// newScoreBasedInstanceTypeConcentrationSnapshot counts the full immutable node inventory for
// each configured pool. Standby, activating, and otherwise ineligible initialized nodes remain
// part of the inventory; nodes without an instance-type label remain in the denominator.
func newScoreBasedInstanceTypeConcentrationSnapshot(nodes state.StateNodes, configs scoreBasedScoringConfig) *scoreBasedInstanceTypeConcentrationSnapshot {
	snapshot := &scoreBasedInstanceTypeConcentrationSnapshot{
		pools: make(map[string]*scoreBasedInstanceTypeConcentrationPool, len(configs)),
	}
	for poolName := range configs {
		snapshot.pools[poolName] = &scoreBasedInstanceTypeConcentrationPool{instanceCounts: map[string]int{}}
	}
	for _, node := range nodes {
		if !scoreBasedConcentrationInventoryNode(node) {
			continue
		}
		labels := node.Labels()
		poolName := labels[v1.NodePoolLabelKey]
		pool := snapshot.pools[poolName]
		if pool == nil {
			continue
		}
		pool.nodeCount++
		if instanceType := labels[corev1.LabelInstanceTypeStable]; instanceType != "" {
			pool.instanceCounts[instanceType]++
		}
	}
	return snapshot
}

func (snapshot *scoreBasedInstanceTypeConcentrationSnapshot) score(command Command, configs scoreBasedScoringConfig) scoreBasedInstanceTypeConcentrationScore {
	details := scoreBasedInstanceTypeConcentrationScore{
		Status:      "disabled",
		Reason:      "instance_type_concentration_weight_zero",
		SourceNodes: make([]scoreBasedInstanceTypeConcentrationSourceScore, 0, len(command.Candidates)),
		Pools:       map[string]scoreBasedInstanceTypeConcentrationPoolScore{},
	}
	for _, candidate := range command.Candidates {
		source, ok := snapshot.sourceScore(candidate, configs)
		if !ok {
			continue
		}
		details.SourceNodes = append(details.SourceNodes, source)
		details.addSource(source)
	}
	return details
}

func scoreBasedConcentrationInventoryNode(node *state.StateNode) bool {
	return node != nil && node.Managed() && node.Node != nil && node.Initialized() && !node.MarkedForDeletion() &&
		(node.Node.DeletionTimestamp == nil || node.Node.DeletionTimestamp.IsZero())
}

func (snapshot *scoreBasedInstanceTypeConcentrationSnapshot) sourceScore(candidate *Candidate, configs scoreBasedScoringConfig) (scoreBasedInstanceTypeConcentrationSourceScore, bool) {
	if candidate == nil || candidate.NodePool == nil {
		return scoreBasedInstanceTypeConcentrationSourceScore{}, false
	}
	poolName := candidate.NodePool.Name
	config := configs[poolName].concentration
	pool := snapshot.pools[poolName]
	instanceType := candidate.Labels()[corev1.LabelInstanceTypeStable]
	source := scoreBasedInstanceTypeConcentrationSourceScore{
		NodeName:     candidate.Name(),
		NodePool:     poolName,
		InstanceType: instanceType,
		Threshold:    config.threshold,
		Weight:       config.weight,
	}
	if pool != nil {
		source.PoolNodeCount = pool.nodeCount
		source.InstanceTypeNodeCount = pool.instanceCounts[instanceType]
	}
	if source.PoolNodeCount > 0 && instanceType != "" {
		source.Share = float64(source.InstanceTypeNodeCount) / float64(source.PoolNodeCount)
	}
	source.Qualifies = instanceType != "" && source.Share > config.threshold
	if source.Qualifies {
		source.Contribution = config.weight
	}
	return source, true
}

func (score *scoreBasedInstanceTypeConcentrationScore) addSource(source scoreBasedInstanceTypeConcentrationSourceScore) {
	if source.Weight > 0 {
		score.Enabled = true
		score.Status = "applied"
		score.Reason = ""
	}
	score.Contribution += source.Contribution
	poolScore := score.Pools[source.NodePool]
	if poolScore.InstanceTypes == nil {
		poolScore = scoreBasedInstanceTypeConcentrationPoolScore{
			PoolNodeCount: source.PoolNodeCount,
			InstanceTypes: map[string]scoreBasedInstanceTypeConcentrationTypeScore{},
		}
	}
	poolScore.SourceNodeCount++
	poolScore.Contribution += source.Contribution
	if source.InstanceType != "" {
		typeScore, found := poolScore.InstanceTypes[source.InstanceType]
		if !found {
			typeScore = scoreBasedInstanceTypeConcentrationTypeScore{
				PoolNodeCount:         source.PoolNodeCount,
				InstanceTypeNodeCount: source.InstanceTypeNodeCount,
				Share:                 source.Share,
				Threshold:             source.Threshold,
				Weight:                source.Weight,
				Qualifies:             source.Qualifies,
			}
		}
		typeScore.SourceNodeCount++
		typeScore.Contribution += source.Contribution
		poolScore.InstanceTypes[source.InstanceType] = typeScore
	}
	score.Pools[source.NodePool] = poolScore
}
