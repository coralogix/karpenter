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

// memoryGiBWeight weights memory relative to CPU cores in the workload-size denominator.
const memoryGiBWeight = 0.125

const gibibyte = 1024 * 1024 * 1024

// defaultScoreBasedUtilisationWeight is a dimensionless weight for each
// source node's CPU slack ratio.
const defaultScoreBasedUtilisationWeight = 1.0

// scoreBasedSourceScore records the exact per-source values consumed by the
// cost and CPU-slack terms while a move set is evaluated.
type scoreBasedSourceScore struct {
	PriceUSDPerHour         float64 `json:"priceUSDPerHour"`
	AllocatableCPU          float64 `json:"allocatableCPU"`
	RequestedCPU            float64 `json:"requestedCPU"`
	BaselineUSDPerCPUHour   float64 `json:"baselineUSDPerCPUHour"`
	NormalizedSourceCost    float64 `json:"normalizedSourceCost"`
	CostContribution        float64 `json:"costContribution"`
	CPUSlackFraction        float64 `json:"cpuSlackFraction"`
	UtilisationWeight       float64 `json:"utilisationWeight"`
	UtilisationContribution float64 `json:"utilisationContribution"`
}

// nodePriorityScore is a search-guidance heuristic.
// Uses non-daemon pod CPU/memory requests from cluster state via PodRequests().
func nodePriorityScore(c *Candidate) float64 {
	if c == nil || c.StateNode == nil {
		return 0
	}
	nodePrice := c.Price
	workloadSize := nonDaemonWorkloadSize(c.PodRequests())
	if workloadSize <= 0 {
		return nodePrice
	}
	return nodePrice / workloadSize
}

func nonDaemonWorkloadSize(requests corev1.ResourceList) float64 {
	cores := requests.Cpu().AsApproximateFloat64()
	gibibytes := requests.Memory().AsApproximateFloat64() / gibibyte
	return cores + gibibytes*memoryGiBWeight
}

// moveSetPriorityScore combines the normalized cost contribution with the
// additive, dimensionless CPU-slack contribution.
func moveSetPriorityScore(cmd Command, prices *scoreBasedPriceSnapshot) float64 {
	score, _ := scoreMoveSetPriority(cmd, prices)
	return score
}

func scoreMoveSetPriority(cmd Command, prices *scoreBasedPriceSnapshot) (float64, scoreBasedCostScore) {
	score, cost, _, _ := scoreMoveSetPriorityDetails(cmd, prices)
	return score, cost
}

func scoreMoveSetPriorityDetails(cmd Command, prices *scoreBasedPriceSnapshot) (float64, scoreBasedCostScore, float64, []scoreBasedSourceScore) {
	cost := prices.score(cmd)
	if !cost.Valid {
		return math.NaN(), cost, 0, cost.SourceNodes
	}
	utilisation, utilisationScores := moveSetUtilisationScoreDetails(cmd)
	for i := range cost.SourceNodes {
		if i >= len(utilisationScores) {
			break
		}
		cost.SourceNodes[i].RequestedCPU = utilisationScores[i].RequestedCPU
		cost.SourceNodes[i].AllocatableCPU = utilisationScores[i].AllocatableCPU
		cost.SourceNodes[i].CPUSlackFraction = utilisationScores[i].CPUSlackFraction
		cost.SourceNodes[i].UtilisationWeight = utilisationScores[i].UtilisationWeight
		cost.SourceNodes[i].UtilisationContribution = utilisationScores[i].UtilisationContribution
	}
	return cost.Contribution + utilisation, cost, utilisation, cost.SourceNodes
}

func moveSetUtilisationScore(cmd Command) float64 {
	score, _ := moveSetUtilisationScoreDetails(cmd)
	return score
}

func moveSetUtilisationScoreDetails(cmd Command) (float64, []scoreBasedSourceScore) {
	var score float64
	details := make([]scoreBasedSourceScore, 0, len(cmd.Candidates))
	for _, candidate := range cmd.Candidates {
		allocatableCPU := candidateAllocatableCPU(candidate)
		if allocatableCPU <= 0 {
			details = append(details, scoreBasedSourceScore{})
			continue
		}
		requestedCPU := candidateRequestedCPU(candidate)
		unallocatedCPU := allocatableCPU - requestedCPU
		if unallocatedCPU < 0 {
			unallocatedCPU = 0
		}
		weight := defaultScoreBasedUtilisationWeight
		if candidate.NodePool != nil {
			weight = scoreBasedUtilisationWeight(candidate.NodePool)
		}
		slackFraction := unallocatedCPU / allocatableCPU
		contribution := weight * slackFraction
		score += contribution
		details = append(details, scoreBasedSourceScore{
			AllocatableCPU:          allocatableCPU,
			RequestedCPU:            requestedCPU,
			CPUSlackFraction:        slackFraction,
			UtilisationWeight:       weight,
			UtilisationContribution: contribution,
		})
	}
	return score, details
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
