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
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// memoryGiBWeight weights memory relative to CPU cores in the workload-size denominator.
const memoryGiBWeight = 0.125

const gibibyte = 1024 * 1024 * 1024

// millidollarsPerDollar converts hourly prices from USD/h to m$/h.
const millidollarsPerDollar = 1000

// defaultScoreBasedUtilisationWeightMilliPerVCPUHour is the default utilization-weight (m$/vCPU/h).
const defaultScoreBasedUtilisationWeightMilliPerVCPUHour = 1.0

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

// moveSetPriorityScore ranks a simulated consolidation command. Hourly costs use m$/h; the cost-efficiency
// term is in m$/vCPU/h and the utilization term is utilization-weight (m$/vCPU/h) times a unitless CPU slack ratio.
func moveSetPriorityScore(cmd Command) float64 {
	return moveSetCostEfficiencyMilliPerVCPUHour(cmd) + moveSetUtilisationScore(cmd)
}

func moveSetCostEfficiencyMilliPerVCPUHour(cmd Command) float64 {
	savingsMilliPerHour := consolidationSavingsUSDPerHour(cmd) * millidollarsPerDollar
	allocatableCPU := removedCandidatesAllocatableCPU(cmd.Candidates)
	if allocatableCPU <= 0 {
		return 0
	}
	return savingsMilliPerHour / allocatableCPU
}

func moveSetUtilisationScore(cmd Command) float64 {
	var score float64
	for _, candidate := range cmd.Candidates {
		allocatableCPU := candidateAllocatableCPU(candidate)
		if allocatableCPU <= 0 {
			continue
		}
		requestedCPU := candidateRequestedCPU(candidate)
		unallocatedCPU := allocatableCPU - requestedCPU
		if unallocatedCPU < 0 {
			unallocatedCPU = 0
		}
		weight := defaultScoreBasedUtilisationWeightMilliPerVCPUHour
		if candidate.NodePool != nil {
			weight = scoreBasedUtilisationWeight(candidate.NodePool)
		}
		score += weight * (unallocatedCPU / allocatableCPU)
	}
	return score
}

// consolidationSavingsUSDPerHour mirrors EstimatedSavings for commands produced by scheduling simulation,
// without treating evacuation as zero savings.
func consolidationSavingsUSDPerHour(cmd Command) float64 {
	sourcePrice := getCandidatePrices(cmd.Candidates)
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

func removedCandidatesAllocatableCPU(candidates []*Candidate) float64 {
	var total float64
	for _, candidate := range candidates {
		total += candidateAllocatableCPU(candidate)
	}
	return total
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
		return defaultScoreBasedUtilisationWeightMilliPerVCPUHour
	}
	value := nodePool.Annotations[v1.ScoreBasedUtilisationWeightAnnotationKey]
	if value == "" {
		return defaultScoreBasedUtilisationWeightMilliPerVCPUHour
	}
	weight, err := strconv.ParseFloat(value, 64)
	if err != nil || weight <= 0 {
		log.FromContext(context.Background()).V(1).Info("using default score-based utilization weight for invalid value",
			"NodePool", nodePool.Name,
			"annotation", v1.ScoreBasedUtilisationWeightAnnotationKey,
			"value", value,
		)
		return defaultScoreBasedUtilisationWeightMilliPerVCPUHour
	}
	return weight
}
