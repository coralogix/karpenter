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

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

const (
	defaultMemoryBalanceWeight = 0
	defaultMemorySurplus       = 0.2
	memoryPressureDeadband     = 0.05
)

// memoryBalancePool is a source NodePool with a positive memory-balance-weight. Zero memory per CPU means unknown.
type memoryBalancePool struct {
	MemoryPerCPU       float64
	TargetMemoryPerCPU float64
	Weight             float64
}

// nodeShape is a node's CPU and memory without DaemonSet requests.
type nodeShape struct {
	NodePool string
	CPU      float64
	Memory   float64
}

type memoryBalanceScore struct {
	Contribution float64                    `json:"contribution"`
	RemovedCPU   float64                    `json:"removedCPU,omitempty"`
	Pools        map[string]memoryPoolScore `json:"pools,omitempty"`
}

type memoryPoolScore struct {
	MemoryPerCPU       float64 `json:"q"`
	TargetMemoryPerCPU float64 `json:"t"`
	Pressure           float64 `json:"pressure"`
	Delta              float64 `json:"delta"`
	Weight             float64 `json:"weight"`
	Contribution       float64 `json:"contribution"`
	Reason             string  `json:"reason,omitempty"`
}

func newMemoryBalancePools(baseline state.StateNodes, candidates []*Candidate, recorder events.Recorder) map[string]memoryBalancePool {
	shapes := map[string]nodeShape{}
	requests := map[string]nodeShape{}
	for _, n := range baseline {
		name := n.Labels()[v1.NodePoolLabelKey]
		shapes[name] = shapes[name].add(shapeOf(name, n.Allocatable(), n.DaemonSetRequests()))
		requests[name] = requests[name].add(shapeOf(name, n.PodRequests(), n.DaemonSetRequests()))
	}
	pools := map[string]memoryBalancePool{}
	for _, np := range lo.UniqBy(lo.Map(candidates, func(c *Candidate, _ int) *v1.NodePool { return c.NodePool }), func(np *v1.NodePool) string { return np.Name }) {
		if weight, surplus := readMemoryBalanceSettings(np, recorder); weight > 0 {
			pools[np.Name] = memoryBalancePool{
				MemoryPerCPU:       shapes[np.Name].memoryPerCPU(),
				TargetMemoryPerCPU: (1 + surplus) * requests[np.Name].memoryPerCPU(),
				Weight:             weight,
			}
		}
	}
	return pools
}

// shapeOf subtracts DaemonSet requests from total, never going below zero.
func shapeOf(nodePool string, total, daemonSetRequests corev1.ResourceList) nodeShape {
	r := resources.Subtract(total, daemonSetRequests)
	return nodeShape{NodePool: nodePool, CPU: math.Max(0, cpuCores(r)), Memory: math.Max(0, float64(r.Memory().Value()))}
}

func (s nodeShape) add(o nodeShape) nodeShape {
	return nodeShape{NodePool: o.NodePool, CPU: s.CPU + o.CPU, Memory: s.Memory + o.Memory}
}

func readMemoryBalanceSettings(np *v1.NodePool, recorder events.Recorder) (weight, surplus float64) {
	nonNegative := func(v float64) bool { return v >= 0 }
	weight = readFloatAnnotation(np, recorder, v1.MemoryBalanceWeightAnnotationKey, defaultMemoryBalanceWeight, events.InvalidMemoryBalanceWeight, nonNegative)
	surplus = readFloatAnnotation(np, recorder, v1.MemorySurplusAnnotationKey, defaultMemorySurplus, events.InvalidMemorySurplus, nonNegative)
	return weight, surplus
}

func (s nodeShape) memoryPerCPU() float64 {
	if s.CPU <= 0 {
		return 0
	}
	return s.Memory / s.CPU
}

// newNodeShapes uses the instance type of the new node's price estimate (Command.ReplacementCost).
func newNodeShapes(cmd Command) []nodeShape {
	var shapes []nodeShape
	for _, n := range cmd.Results.NewNodeClaims {
		if len(n.InstanceTypeOptions) > 0 {
			it := n.InstanceTypeOptions[0]
			shapes = append(shapes, shapeOf(n.NodePoolName, it.Allocatable(), n.DaemonOverhead(it)))
		}
	}
	return shapes
}

// scoreMemoryBalance scores how a move shifts each affected pool's memory per CPU toward its target. The affected pools
// are the move's source pools with memory balance on; a pool that only receives an added node is not affected.
func scoreMemoryBalance(pools map[string]memoryBalancePool, sources, added []nodeShape) memoryBalanceScore {
	deltas := map[string]float64{}
	var removedCPU float64
	for _, s := range sources {
		removedCPU += s.CPU
		if p, ok := pools[s.NodePool]; ok {
			deltas[s.NodePool] += computeShapeDelta(s, p.MemoryPerCPU)
		}
	}
	for _, a := range added {
		if _, ok := deltas[a.NodePool]; ok {
			deltas[a.NodePool] -= computeShapeDelta(a, pools[a.NodePool].MemoryPerCPU)
		}
	}
	if len(deltas) == 0 {
		return memoryBalanceScore{}
	}
	score := memoryBalanceScore{RemovedCPU: removedCPU, Pools: map[string]memoryPoolScore{}}
	for name, delta := range deltas {
		p := pools[name]
		s := memoryPoolScore{MemoryPerCPU: p.MemoryPerCPU, TargetMemoryPerCPU: p.TargetMemoryPerCPU, Weight: p.Weight}
		switch {
		case p.MemoryPerCPU <= 0 || p.TargetMemoryPerCPU <= 0:
			s.Reason = "unknown memory per CPU"
		case removedCPU <= 0:
			s.Reason = "zero removed CPU"
		default:
			s.Pressure = computeMemoryPressure(p.MemoryPerCPU / p.TargetMemoryPerCPU)
			s.Delta = delta
			s.Contribution = p.Weight * s.Pressure * clampUnit(delta/removedCPU) * float64(len(sources))
		}
		score.Pools[name] = s
		score.Contribution += s.Contribution
	}
	return score
}

func computeShapeDelta(s nodeShape, memoryPerCPU float64) float64 {
	return s.Memory/memoryPerCPU - s.CPU
}

func computeMemoryPressure(ratio float64) float64 {
	if ratio >= 1-memoryPressureDeadband && ratio <= 1+memoryPressureDeadband {
		return 0
	}
	return clampUnit(math.Log2(ratio))
}

func clampUnit(v float64) float64 {
	return math.Max(-1, math.Min(1, v))
}
