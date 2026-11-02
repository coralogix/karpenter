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

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

type scoreSource struct {
	Node, NodePool, CapacityType                          string
	Price, AllocatableCPU, RequestedCPU, Baseline, Weight float64
	NormalizedPrice, Slack                                float64
	CostReason, UtilizationReason                         string
}

type scoreBreakdown struct {
	Savings, SavingsFraction, Cost, Utilization, Total float64
	CostReason                                         string
	Sources                                            []scoreSource
}

type scorePoolCapacity struct{ pool, capacity string }

type scoreInputs struct {
	prices  map[string]float64
	sources map[*Candidate]scoreSource
}

func (inputs scoreInputs) score(cmd Command, results pscheduling.Results) (scoreBreakdown, map[string]float64) {
	prices := map[string]float64{}
	for _, name := range standbyDestinations(results) {
		prices[name] = inputs.prices[name]
	}
	sources := make([]scoreSource, 0, len(cmd.Candidates))
	savings := -(cmd.SourceCost() - cmd.EstimatedSavings())
	for _, c := range cmd.Candidates {
		source := inputs.sources[c]
		sources = append(sources, source)
		savings += source.Price
	}
	for _, price := range prices {
		savings -= price
	}
	return calculateScore(sources, savings), prices
}

//nolint:gocyclo // The snapshot filters, aggregation, and per-candidate inputs are kept together.
func newScoreInputs(nodes state.StateNodes, candidates []*Candidate, prices map[string]float64, recorder events.Recorder) scoreInputs {
	type aggregate struct{ price, cpu float64 }
	totals := map[scorePoolCapacity]aggregate{}
	for _, n := range nodes {
		if !n.Managed() || !n.Initialized() || n.MarkedForDeletion() || !n.Node.DeletionTimestamp.IsZero() || standby.HasStandbyTaint(n.Node) {
			continue
		}
		key := scorePoolCapacity{n.Labels()[v1.NodePoolLabelKey], n.Labels()[v1.CapacityTypeLabelKey]}
		total := totals[key]
		total.price += prices[n.Name()]
		allocatable := n.Allocatable()
		total.cpu += float64(allocatable.Cpu().MilliValue()) / 1000
		totals[key] = total
	}
	weights := map[string]float64{}
	for _, c := range candidates {
		if _, ok := weights[c.NodePool.Name]; !ok {
			weights[c.NodePool.Name] = utilizationWeight(c.NodePool, recorder)
		}
	}
	inputs := scoreInputs{prices: prices, sources: map[*Candidate]scoreSource{}}
	nodesByName := map[string]*state.StateNode{}
	for _, n := range nodes {
		nodesByName[n.Name()] = n
	}
	for _, c := range candidates {
		n := nodesByName[c.Name()]
		if n == nil {
			continue
		}
		key := scorePoolCapacity{n.Labels()[v1.NodePoolLabelKey], n.Labels()[v1.CapacityTypeLabelKey]}
		total := totals[key]
		baseline := 0.0
		if total.cpu > 0 {
			baseline = total.price / total.cpu
		}
		allocatable, requested := n.Allocatable(), n.PodRequests()
		inputs.sources[c] = scoreSource{Node: n.Name(), NodePool: key.pool, CapacityType: key.capacity, Price: prices[n.Name()], AllocatableCPU: float64(allocatable.Cpu().MilliValue()) / 1000, RequestedCPU: float64(requested.Cpu().MilliValue()) / 1000, Baseline: baseline, Weight: weights[key.pool]}
	}
	return inputs
}

func utilizationWeight(np *v1.NodePool, recorder events.Recorder) float64 {
	raw, ok := np.Annotations[v1.UtilizationWeightAnnotationKey]
	if !ok {
		return 1
	}
	weight, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(weight) || math.IsInf(weight, 0) || weight <= 0 {
		recorder.Publish(events.Event{InvolvedObject: np, Type: corev1.EventTypeWarning, Reason: events.InvalidUtilizationWeight, Message: fmt.Sprintf("Invalid value %q for annotation %q; using 1", raw, v1.UtilizationWeightAnnotationKey), DedupeValues: []string{string(np.UID), v1.UtilizationWeightAnnotationKey, raw}, DedupeTimeout: 15 * time.Minute})
		return 1
	}
	return weight
}

func calculateScore(sources []scoreSource, savings float64) scoreBreakdown {
	result := scoreBreakdown{Savings: savings, Sources: append([]scoreSource(nil), sources...)}
	var sourcePrice, normalized float64
	for i := range result.Sources {
		s := &result.Sources[i]
		sourcePrice += s.Price
		if s.Baseline <= 0 {
			result.CostReason = "zero baseline"
		}
		switch {
		case s.Price == 0:
			s.CostReason = "zero price"
		case s.Baseline <= 0:
			s.CostReason = "zero baseline"
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
			result.Utilization += s.Weight * s.Slack
		}
	}
	if sourcePrice <= 0 {
		result.CostReason = "zero source price sum"
	} else {
		result.SavingsFraction = savings / sourcePrice
		if result.CostReason == "" {
			result.Cost = result.SavingsFraction * normalized
		}
	}
	result.Total = result.Cost + result.Utilization
	return result
}
