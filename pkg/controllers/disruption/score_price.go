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
	"math"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

type scoreBasedPriceGroupKey struct {
	nodePool     string
	capacityType string
}

type scoreBasedPriceGroup struct {
	totalPrice float64
	totalCPU   float64
	nodeCount  int
	known      bool
	baseline   float64
}

type scoreBasedNodePrice struct {
	group scoreBasedPriceGroupKey
	cpu   float64
	price float64
	known bool
}

type scoreBasedPriceGroupScore struct {
	BaselineUSDPerCPUHour float64 `json:"baselineUSDPerCPUHour"`
	BaselineNodeCount     int     `json:"baselineNodeCount"`
	SourceNodeCount       int     `json:"sourceNodeCount"`
	NormalizedSourceCost  float64 `json:"normalizedSourceCost"`
	BaselineKnown         bool    `json:"baselineKnown"`
}

type scoreBasedCostScore struct {
	Enabled              bool                                 `json:"enabled"`
	Valid                bool                                 `json:"valid"`
	SavingsFraction      float64                              `json:"savingsFraction"`
	NormalizedSourceCost float64                              `json:"normalizedSourceCost"`
	Contribution         float64                              `json:"contribution"`
	Groups               map[string]scoreBasedPriceGroupScore `json:"groups,omitempty"`
}

type scoreBasedPriceSnapshot struct {
	groups map[scoreBasedPriceGroupKey]*scoreBasedPriceGroup
	nodes  map[string]scoreBasedNodePrice
}

// newScoreBasedPriceSnapshot builds price baselines from the immutable scheduling snapshot,
// including initialized active managed nodes that are not disruption candidates.
func newScoreBasedPriceSnapshot(nodes state.StateNodes, candidates []*Candidate) *scoreBasedPriceSnapshot {
	snapshot := &scoreBasedPriceSnapshot{
		groups: map[scoreBasedPriceGroupKey]*scoreBasedPriceGroup{},
		nodes:  map[string]scoreBasedNodePrice{},
	}
	instanceTypes := scoreBasedInstanceTypeMaps(candidates)
	for _, node := range nodes {
		addScoreBasedBaselineNode(snapshot, node, instanceTypes)
	}
	finalizeScoreBasedPriceGroups(snapshot)
	return snapshot
}

func scoreBasedInstanceTypeMaps(candidates []*Candidate) map[string]map[string]*cloudprovider.InstanceType {
	instanceTypes := map[string]map[string]*cloudprovider.InstanceType{}
	for _, candidate := range candidates {
		if candidate != nil && candidate.NodePool != nil && candidate.nodePoolInstanceTypes != nil {
			instanceTypes[candidate.NodePool.Name] = candidate.nodePoolInstanceTypes
		}
	}
	return instanceTypes
}

func addScoreBasedBaselineNode(snapshot *scoreBasedPriceSnapshot, node *state.StateNode, instanceTypes map[string]map[string]*cloudprovider.InstanceType) {
	if !scoreBasedPriceBaselineNode(node) {
		return
	}
	labels := node.Labels()
	key := scoreBasedPriceGroupKey{nodePool: labels[v1.NodePoolLabelKey], capacityType: labels[v1.CapacityTypeLabelKey]}
	if key.nodePool == "" || key.capacityType == "" {
		return
	}
	group := snapshot.groups[key]
	if group == nil {
		group = &scoreBasedPriceGroup{known: true}
		snapshot.groups[key] = group
	}
	group.nodeCount++
	cpuQuantity := node.Allocatable()[corev1.ResourceCPU]
	cpu := cpuQuantity.AsApproximateFloat64()
	if cpu <= 0 || !finiteScoreValue(cpu) {
		group.known = false
		snapshot.nodes[node.Name()] = scoreBasedNodePrice{group: key}
		return
	}
	group.totalCPU += cpu
	instanceType := instanceTypes[key.nodePool][labels[corev1.LabelInstanceTypeStable]]
	price, known := scoreBasedOfferingPrice(instanceType, labels)
	snapshot.nodes[node.Name()] = scoreBasedNodePrice{group: key, cpu: cpu, price: price, known: known}
	if !known {
		group.known = false
		return
	}
	group.totalPrice += price
}

func finalizeScoreBasedPriceGroups(snapshot *scoreBasedPriceSnapshot) {
	for _, group := range snapshot.groups {
		if !group.known || group.totalCPU <= 0 || !finiteScoreValue(group.totalPrice) || !finiteScoreValue(group.totalCPU) {
			group.known = false
			continue
		}
		group.baseline = group.totalPrice / group.totalCPU
		if !finiteScoreValue(group.baseline) || group.baseline < 0 {
			group.known = false
		}
	}
}

func scoreBasedPriceBaselineNode(node *state.StateNode) bool {
	return node != nil && node.Managed() && !node.MarkedForDeletion() && node.Node != nil && node.Initialized() &&
		!standby.IsNodeClaimStandby(node.NodeClaim) && !standby.IsNodeClaimActivating(node.NodeClaim) && !standby.HasNodeTaint(node.Node)
}

func scoreBasedOfferingPrice(instanceType *cloudprovider.InstanceType, labels map[string]string) (float64, bool) {
	if instanceType == nil {
		return 0, false
	}
	price, found := instanceType.OfferingPrice(labels[corev1.LabelTopologyZone], labels[v1.CapacityTypeLabelKey])
	if !found || !finiteScoreValue(price) || price < 0 {
		return 0, false
	}
	return price, true
}

// score computes the normalized cost term as
// (source USD/h - replacement USD/h) / source USD/h, multiplied by the sum over
// source nodes of price / (allocatable CPU * that NodePool/capacity-type baseline).
// A known zero-price source adds zero to the normalized source sum. Unknown
// source prices and non-zero prices without a valid positive baseline invalidate the move.
func (snapshot *scoreBasedPriceSnapshot) score(command Command) scoreBasedCostScore {
	details := scoreBasedCostScore{Enabled: true, Groups: map[string]scoreBasedPriceGroupScore{}}
	if snapshot == nil || len(command.Candidates) == 0 {
		return details
	}
	sourcePrice := sumCandidatePrices(command.Candidates)
	if sourcePrice <= 0 || !finiteScoreValue(sourcePrice) {
		return details
	}
	savingsFraction := consolidationSavingsUSDPerHour(command) / sourcePrice
	if !finiteScoreValue(savingsFraction) {
		return details
	}
	for _, candidate := range command.Candidates {
		normalizedPrice, nodePrice, group, valid := snapshot.normalizedCandidatePrice(candidate)
		if !valid {
			return scoreBasedCostScore{Enabled: true}
		}
		addScoreBasedCostGroup(details.Groups, nodePrice.group, group, normalizedPrice)
		details.NormalizedSourceCost += normalizedPrice
	}
	contribution := savingsFraction * details.NormalizedSourceCost
	if !finiteScoreValue(contribution) {
		return scoreBasedCostScore{Enabled: true}
	}
	details.SavingsFraction = savingsFraction
	details.Contribution = contribution
	details.Valid = true
	return details
}

func (snapshot *scoreBasedPriceSnapshot) normalizedCandidatePrice(candidate *Candidate) (float64, scoreBasedNodePrice, *scoreBasedPriceGroup, bool) {
	nodePrice, cpu, valid := snapshot.sourceNodePrice(candidate)
	if !valid {
		return 0, scoreBasedNodePrice{}, nil, false
	}
	group := snapshot.groups[nodePrice.group]
	if group == nil {
		return 0, scoreBasedNodePrice{}, nil, false
	}
	normalizedPrice := 0.0
	if nodePrice.price > 0 {
		if !group.known || group.baseline <= 0 {
			return 0, scoreBasedNodePrice{}, nil, false
		}
		normalizedPrice = nodePrice.price / (cpu * group.baseline)
	}
	return normalizedPrice, nodePrice, group, finiteScoreValue(normalizedPrice)
}

func (snapshot *scoreBasedPriceSnapshot) sourceNodePrice(candidate *Candidate) (scoreBasedNodePrice, float64, bool) {
	if candidate == nil || candidate.NodePool == nil || candidate.Node == nil {
		return scoreBasedNodePrice{}, 0, false
	}
	nodePrice, found := snapshot.nodes[candidate.Name()]
	if !found || !nodePrice.known || candidate.Price != nodePrice.price {
		return scoreBasedNodePrice{}, 0, false
	}
	cpu := candidateAllocatableCPU(candidate)
	if cpu <= 0 || !finiteScoreValue(cpu) || math.Abs(cpu-nodePrice.cpu) > math.Max(1, cpu)*1e-9 {
		return scoreBasedNodePrice{}, 0, false
	}
	return nodePrice, cpu, true
}

func addScoreBasedCostGroup(groups map[string]scoreBasedPriceGroupScore, key scoreBasedPriceGroupKey, group *scoreBasedPriceGroup, normalizedPrice float64) {
	groupKey := scoreBasedPriceGroupName(key)
	groupScore := groups[groupKey]
	groupScore.BaselineUSDPerCPUHour = group.baseline
	groupScore.BaselineNodeCount = group.nodeCount
	groupScore.SourceNodeCount++
	groupScore.NormalizedSourceCost += normalizedPrice
	groupScore.BaselineKnown = group.known
	groups[groupKey] = groupScore
}

func scoreBasedPriceGroupName(key scoreBasedPriceGroupKey) string {
	return key.nodePool + "/" + key.capacityType
}
