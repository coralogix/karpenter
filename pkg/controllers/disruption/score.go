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
)

// defaultScoreBasedUtilisationWeight is a dimensionless weight for each
// source node's CPU slack ratio.
const defaultScoreBasedUtilisationWeight = 1.0

type scoreFactorStatus string

const (
	scoreFactorApplied     scoreFactorStatus = "applied"
	scoreFactorDisabled    scoreFactorStatus = "disabled"
	scoreFactorUnavailable scoreFactorStatus = "unavailable"
)

// scoreBasedSourceScore captures the exact source-node values consumed by the
// factors. The identity and values are copied during evaluation so log output
// does not depend on mutable Candidate objects.
type scoreBasedSourceScore struct {
	NodeName                  string                                         `json:"nodeName"`
	NodeClaimName             string                                         `json:"nodeClaimName"`
	NodePoolName              string                                         `json:"nodePoolName"`
	InstanceType              string                                         `json:"instanceType"`
	CapacityType              string                                         `json:"capacityType"`
	PriceUSDPerHour           float64                                        `json:"priceUSDPerHour"`
	AllocatableCPU            float64                                        `json:"allocatableCPU"`
	RequestedCPU              float64                                        `json:"requestedCPU"`
	BaselineUSDPerCPUHour     float64                                        `json:"baselineUSDPerCPUHour"`
	NormalizedSourceCost      float64                                        `json:"normalizedSourceCost"`
	CostContribution          float64                                        `json:"costContribution"`
	CPUSlackFraction          float64                                        `json:"cpuSlackFraction"`
	UtilisationWeight         float64                                        `json:"utilisationWeight"`
	UtilisationContribution   float64                                        `json:"utilisationContribution"`
	InstanceTypeConcentration scoreBasedInstanceTypeConcentrationSourceScore `json:"instanceTypeConcentration"`
}

type scoreBasedUtilisationScore struct {
	Contribution float64                 `json:"contribution"`
	Status       scoreFactorStatus       `json:"status"`
	Reason       string                  `json:"reason,omitempty"`
	SourceNodes  []scoreBasedSourceScore `json:"-"`
}

type scoreBasedNodePoolScoringConfig struct {
	utilisationWeight float64
	memory            scoreBasedMemoryConfig
	concentration     scoreBasedInstanceTypeConcentrationConfig
}

type scoreBasedScoringConfig map[string]scoreBasedNodePoolScoringConfig

func scoreBasedScoringConfigForCandidates(candidates []*Candidate) scoreBasedScoringConfig {
	configs := scoreBasedScoringConfig{}
	for _, candidate := range candidates {
		if candidate == nil || candidate.NodePool == nil {
			continue
		}
		if _, found := configs[candidate.NodePool.Name]; found {
			continue
		}
		configs[candidate.NodePool.Name] = scoreBasedNodePoolScoringConfig{
			utilisationWeight: scoreBasedUtilisationWeight(candidate.NodePool),
			memory:            scoreBasedMemoryConfigFor(candidate.NodePool),
			concentration:     scoreBasedInstanceTypeConcentrationConfigFor(candidate.NodePool),
		}
	}
	return configs
}

// moveSetScore is the single canonical breakdown used for ranking, selection,
// and logs. The total preserves the existing addition order: cost plus CPU
// utilisation, then the joint memory and concentration terms.
type moveSetScore struct {
	Total   float64                 `json:"total"`
	Factors moveSetScoreFactors     `json:"factors"`
	Sources []scoreBasedSourceScore `json:"-"`
}

type moveSetScoreFactors struct {
	Cost                      scoreBasedCostScore                      `json:"cost"`
	Utilisation               scoreBasedUtilisationScore               `json:"utilisation"`
	MemoryBalance             scoreBasedMemoryScore                    `json:"memoryBalance"`
	InstanceTypeConcentration scoreBasedInstanceTypeConcentrationScore `json:"instanceTypeConcentration"`
}

// moveSetScorer owns the snapshots and parsed configuration used by every
// single, pair, and batch evaluation in one search.
type moveSetScorer struct {
	prices        *scoreBasedPriceSnapshot
	configs       scoreBasedScoringConfig
	memory        *scoreBasedMemorySnapshot
	concentration *scoreBasedInstanceTypeConcentrationSnapshot
}

func newMoveSetScorer(
	prices *scoreBasedPriceSnapshot,
	configs scoreBasedScoringConfig,
	memory *scoreBasedMemorySnapshot,
	concentration *scoreBasedInstanceTypeConcentrationSnapshot,
) *moveSetScorer {
	return &moveSetScorer{
		prices:        prices,
		configs:       configs,
		memory:        memory,
		concentration: concentration,
	}
}

func (scorer *moveSetScorer) Score(command Command) moveSetScore {
	cost := scorer.prices.score(command)
	utilisation := scoreMoveSetUtilisation(command, scorer.configs)
	memory := scoreBasedMemoryScoreForCommand(command, scorer.memory, scorer.configs)
	concentration := scorer.concentration.score(command, scorer.configs)
	baseScore := cost.Contribution + utilisation.Contribution
	total := baseScore + memory.Contribution + concentration.Contribution

	sources := mergeScoreBasedSourceScores(command, cost, utilisation, concentration)
	return moveSetScore{
		Total: total,
		Factors: moveSetScoreFactors{
			Cost:                      cost,
			Utilisation:               utilisation,
			MemoryBalance:             memory,
			InstanceTypeConcentration: concentration,
		},
		Sources: sources,
	}
}

func scoreBasedMoveSetScoreIsValid(scoring moveSetScore) bool {
	if scoring.Factors.Cost.Status != scoreFactorApplied || !scoring.Factors.Cost.Valid {
		return false
	}
	return finiteScoreValue(scoring.Total) && scoring.Total > 0
}

func mergeScoreBasedSourceScores(
	command Command,
	cost scoreBasedCostScore,
	utilisation scoreBasedUtilisationScore,
	concentration scoreBasedInstanceTypeConcentrationScore,
) []scoreBasedSourceScore {
	byNode := scoreBasedSourceIdentities(command)
	mergeScoreBasedCostSources(byNode, cost.SourceNodes)
	mergeScoreBasedUtilisationSources(byNode, utilisation.SourceNodes)
	mergeScoreBasedConcentrationSources(byNode, concentration.SourceNodes)
	ordered := make([]scoreBasedSourceScore, 0, len(command.Candidates))
	for _, candidate := range command.Candidates {
		if candidate != nil {
			ordered = append(ordered, byNode[candidate.Name()])
		}
	}
	return ordered
}

func scoreBasedSourceIdentities(command Command) map[string]scoreBasedSourceScore {
	byNode := make(map[string]scoreBasedSourceScore, len(command.Candidates))
	for _, candidate := range command.Candidates {
		if candidate != nil {
			byNode[candidate.Name()] = scoreBasedSourceIdentity(candidate)
		}
	}
	return byNode
}

func scoreBasedSourceIdentity(candidate *Candidate) scoreBasedSourceScore {
	source := scoreBasedSourceScore{
		NodeName:     candidate.Name(),
		InstanceType: candidate.Labels()[corev1.LabelInstanceTypeStable],
		CapacityType: candidate.Labels()[v1.CapacityTypeLabelKey],
	}
	if candidate.NodePool != nil {
		source.NodePoolName = candidate.NodePool.Name
	}
	if candidate.NodeClaim != nil {
		source.NodeClaimName = candidate.NodeClaim.Name
	}
	return source
}

func mergeScoreBasedCostSources(byNode map[string]scoreBasedSourceScore, sources []scoreBasedSourceScore) {
	for _, source := range sources {
		merged := byNode[source.NodeName]
		if merged.NodeName != "" {
			merged.PriceUSDPerHour = source.PriceUSDPerHour
			merged.AllocatableCPU = source.AllocatableCPU
			merged.BaselineUSDPerCPUHour = source.BaselineUSDPerCPUHour
			merged.NormalizedSourceCost = source.NormalizedSourceCost
			merged.CostContribution = source.CostContribution
			byNode[source.NodeName] = merged
		}
	}
}

func mergeScoreBasedUtilisationSources(byNode map[string]scoreBasedSourceScore, sources []scoreBasedSourceScore) {
	for _, source := range sources {
		merged, found := byNode[source.NodeName]
		if found {
			merged.AllocatableCPU = source.AllocatableCPU
			merged.RequestedCPU = source.RequestedCPU
			merged.CPUSlackFraction = source.CPUSlackFraction
			merged.UtilisationWeight = source.UtilisationWeight
			merged.UtilisationContribution = source.UtilisationContribution
			byNode[source.NodeName] = merged
		}
	}
}

func mergeScoreBasedConcentrationSources(byNode map[string]scoreBasedSourceScore, sources []scoreBasedInstanceTypeConcentrationSourceScore) {
	for _, source := range sources {
		merged, found := byNode[source.NodeName]
		if found {
			merged.InstanceTypeConcentration = source
			byNode[source.NodeName] = merged
		}
	}
}

func scoreMoveSetUtilisation(command Command, configs scoreBasedScoringConfig) scoreBasedUtilisationScore {
	score := scoreBasedUtilisationScore{
		Status:      scoreFactorApplied,
		SourceNodes: make([]scoreBasedSourceScore, 0, len(command.Candidates)),
	}
	for _, candidate := range command.Candidates {
		allocatableCPU := candidateAllocatableCPU(candidate)
		if allocatableCPU <= 0 {
			source := scoreBasedSourceScore{}
			if candidate != nil {
				source.NodeName = candidate.Name()
				if candidate.NodePool != nil {
					source.NodePoolName = candidate.NodePool.Name
				}
			}
			score.SourceNodes = append(score.SourceNodes, source)
			continue
		}
		requestedCPU := candidateRequestedCPU(candidate)
		unallocatedCPU := allocatableCPU - requestedCPU
		if unallocatedCPU < 0 {
			unallocatedCPU = 0
		}
		weight := defaultScoreBasedUtilisationWeight
		if candidate != nil && candidate.NodePool != nil {
			if configured, found := configs[candidate.NodePool.Name]; found {
				weight = configured.utilisationWeight
			}
		}
		slackFraction := unallocatedCPU / allocatableCPU
		contribution := weight * slackFraction
		score.Contribution += contribution
		source := scoreBasedSourceScore{
			AllocatableCPU:          allocatableCPU,
			RequestedCPU:            requestedCPU,
			CPUSlackFraction:        slackFraction,
			UtilisationWeight:       weight,
			UtilisationContribution: contribution,
		}
		if candidate != nil {
			source.NodeName = candidate.Name()
			if candidate.NodePool != nil {
				source.NodePoolName = candidate.NodePool.Name
			}
		}
		score.SourceNodes = append(score.SourceNodes, source)
	}
	return score
}

// consolidationSavingsUSDPerHour mirrors EstimatedSavings for commands produced by scheduling simulation,
// without treating evacuation as zero savings.
func consolidationSavingsUSDPerHour(cmd Command) float64 {
	sourcePrice := sumCandidatePrices(cmd.Candidates)
	if len(cmd.Replacements) == 0 {
		return sourcePrice
	}
	destPrice := 0.0
	for _, nodeClaim := range cmd.Results.NewNodeClaims {
		if len(nodeClaim.InstanceTypeOptions) > 0 {
			offerings := nodeClaim.InstanceTypeOptions[0].Offerings
			if len(offerings) > 0 {
				destPrice += offerings.Cheapest().Price
			}
		}
	}
	return sourcePrice - destPrice
}

func candidateAllocatableCPU(candidate *Candidate) float64 {
	if candidate == nil {
		return 0
	}
	if candidate.Node != nil && candidate.Node.Status.Allocatable != nil {
		cpuCapacity := candidate.Node.Status.Allocatable[corev1.ResourceCPU]
		cpu := cpuCapacity.AsApproximateFloat64()
		if cpu > 0 {
			return cpu
		}
	}
	if candidate.instanceType != nil && candidate.instanceType.Capacity != nil {
		cpuCapacity := candidate.instanceType.Capacity[corev1.ResourceCPU]
		return cpuCapacity.AsApproximateFloat64()
	}
	return 0
}

func candidateRequestedCPU(candidate *Candidate) float64 {
	if candidate == nil || candidate.StateNode == nil {
		return 0
	}
	requests := candidate.PodRequests()
	requestedCPU := requests[corev1.ResourceCPU]
	return requestedCPU.AsApproximateFloat64()
}

func scoreBasedUtilisationWeight(nodePool *v1.NodePool) float64 {
	if nodePool == nil || nodePool.Annotations == nil {
		return defaultScoreBasedUtilisationWeight
	}
	value := nodePool.Annotations[v1.ScoreBasedUtilisationWeightAnnotationKey]
	if value == "" {
		return defaultScoreBasedUtilisationWeight
	}
	weight, err := strconv.ParseFloat(value, 64)
	if err != nil || weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
		log.FromContext(context.Background()).V(1).Info("using default score-based utilization weight for invalid value",
			"NodePool", nodePool.Name,
			"annotation", v1.ScoreBasedUtilisationWeightAnnotationKey,
			"value", value,
		)
		return defaultScoreBasedUtilisationWeight
	}
	return weight
}
