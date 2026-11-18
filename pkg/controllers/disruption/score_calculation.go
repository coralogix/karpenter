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
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const defaultUtilizationWeight = 1.0

type moveScore struct {
	Total           float64            `json:"total"`
	Savings         float64            `json:"savings"`
	SourcePrice     float64            `json:"sourcePrice"`
	NewNodePrice    float64            `json:"newNodePrice"`
	StandbyPrices   map[string]float64 `json:"standbyPrices"`
	SavingsFraction float64            `json:"savingsFraction"`
	Cost            float64            `json:"cost"`
	CostReason      string             `json:"costReason,omitempty"`
	Utilization     float64            `json:"utilization"`
	Sources         []sourceScore      `json:"sources"`
}

type sourceScore struct {
	Node              string  `json:"node"`
	NodePool          string  `json:"nodePool"`
	CapacityType      string  `json:"capacityType"`
	Price             float64 `json:"price"`
	AllocatableCPU    float64 `json:"allocatableCPU"`
	RequestedCPU      float64 `json:"requestedCPU"`
	Baseline          float64 `json:"baseline"`
	UtilizationWeight float64 `json:"utilizationWeight"`
	NormalizedPrice   float64 `json:"normalizedPrice"`
	Slack             float64 `json:"slack"`
	CostReason        string  `json:"costReason,omitempty"`
	UtilizationReason string  `json:"utilizationReason,omitempty"`
}

type capacityKey struct {
	nodePool     string
	capacityType string
}

type instanceTypesByNodePool map[string]map[string]*cloudprovider.InstanceType

type scoreInputs struct {
	pricePerCPUByCapacityKey    map[capacityKey]float64
	utilizationWeightByNodePool map[string]float64
}

func newScoreInputs(nodes state.StateNodes, instanceTypes instanceTypesByNodePool, candidates []*Candidate, recorder events.Recorder) scoreInputs {
	priceByKey := map[capacityKey]float64{}
	cpuByKey := map[capacityKey]float64{}
	for _, n := range nodes {
		if !n.Managed() || !n.Initialized() || n.MarkedForDeletion() || !n.Node.DeletionTimestamp.IsZero() || standby.HasStandbyTaint(n.Node) {
			continue
		}
		key := capacityKeyOf(n)
		priceByKey[key] += pscheduling.NodePrice(n, instanceTypes.instanceTypeOf(n))
		cpuByKey[key] += cpuCores(n.Allocatable())
	}
	inputs := scoreInputs{
		pricePerCPUByCapacityKey:    map[capacityKey]float64{},
		utilizationWeightByNodePool: map[string]float64{},
	}
	for key, cpu := range cpuByKey {
		if cpu > 0 {
			inputs.pricePerCPUByCapacityKey[key] = priceByKey[key] / cpu
		}
	}
	for _, c := range candidates {
		if _, ok := inputs.utilizationWeightByNodePool[c.NodePool.Name]; !ok {
			inputs.utilizationWeightByNodePool[c.NodePool.Name] = utilizationWeight(c.NodePool, recorder)
		}
	}
	return inputs
}

func (m instanceTypesByNodePool) instanceTypeOf(n *state.StateNode) *cloudprovider.InstanceType {
	return m[n.Labels()[v1.NodePoolLabelKey]][n.Labels()[corev1.LabelInstanceTypeStable]]
}

func utilizationWeight(np *v1.NodePool, recorder events.Recorder) float64 {
	raw, ok := np.Annotations[v1.UtilizationWeightAnnotationKey]
	if !ok {
		return defaultUtilizationWeight
	}
	weight, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(weight) || math.IsInf(weight, 0) || weight <= 0 {
		recorder.Publish(events.Event{InvolvedObject: np, Type: corev1.EventTypeWarning, Reason: events.InvalidUtilizationWeight, Message: fmt.Sprintf("Invalid value %q for annotation %q; using %g", raw, v1.UtilizationWeightAnnotationKey, defaultUtilizationWeight), DedupeValues: []string{string(np.UID), v1.UtilizationWeightAnnotationKey, raw}, DedupeTimeout: 15 * time.Minute})
		return defaultUtilizationWeight
	}
	return weight
}

// scoreMove reads the move's inputs from the command and computeScore derives the terms from them, so the formulas can be
// tested with plain numbers.
func scoreMove(cmd Command, inputs scoreInputs) moveScore {
	standbyPrices := map[string]float64{}
	for _, n := range cmd.Results.ExistingNodes {
		if len(n.Pods) > 0 && standby.HasStandbyTaint(n.Node) {
			standbyPrices[n.Name()] = n.Price
		}
	}
	var sources []sourceScore
	for _, c := range cmd.Candidates {
		key := capacityKeyOf(c.StateNode)
		sources = append(sources, sourceScore{
			Node:              c.Name(),
			NodePool:          key.nodePool,
			CapacityType:      key.capacityType,
			Price:             c.Price,
			AllocatableCPU:    cpuCores(c.Allocatable()),
			RequestedCPU:      cpuCores(c.PodRequests()),
			Baseline:          inputs.pricePerCPUByCapacityKey[key],
			UtilizationWeight: inputs.utilizationWeightByNodePool[c.NodePool.Name],
		})
	}
	return computeScore(cmd.ReplacementCost(), standbyPrices, sources)
}

func computeScore(newNodePrice float64, standbyPrices map[string]float64, sources []sourceScore) moveScore {
	score := moveScore{
		SourcePrice:   lo.SumBy(sources, func(s sourceScore) float64 { return s.Price }),
		NewNodePrice:  newNodePrice,
		StandbyPrices: standbyPrices,
		Sources:       sources,
	}
	score.Savings = score.SourcePrice - newNodePrice - lo.Sum(lo.Values(standbyPrices))
	var normalized float64
	for i := range score.Sources {
		s := &score.Sources[i]
		switch {
		case s.Baseline <= 0:
			s.CostReason = "zero baseline"
			score.CostReason = s.CostReason
		case s.Price == 0:
			s.CostReason = "zero price"
		case s.AllocatableCPU <= 0:
			s.CostReason = "zero allocatable CPU"
		default:
			s.NormalizedPrice = s.Price / (s.AllocatableCPU * s.Baseline)
			normalized += s.NormalizedPrice
		}
		if s.AllocatableCPU <= 0 {
			s.UtilizationReason = "zero allocatable CPU"
		} else {
			s.Slack = (s.AllocatableCPU - math.Max(0, math.Min(s.RequestedCPU, s.AllocatableCPU))) / s.AllocatableCPU
			score.Utilization += s.UtilizationWeight * s.Slack
		}
	}
	if score.SourcePrice <= 0 {
		score.CostReason = "zero source price sum"
	} else {
		score.SavingsFraction = score.Savings / score.SourcePrice
		if score.CostReason == "" {
			score.Cost = score.SavingsFraction * normalized
		}
	}
	score.Total = score.Cost + score.Utilization
	return score
}

func capacityKeyOf(n *state.StateNode) capacityKey {
	return capacityKey{nodePool: n.Labels()[v1.NodePoolLabelKey], capacityType: n.Labels()[v1.CapacityTypeLabelKey]}
}

func cpuCores(resources corev1.ResourceList) float64 {
	return float64(resources.Cpu().MilliValue()) / 1000
}
