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
	"testing"
)

func TestCostAndUtilizationScore(t *testing.T) {
	cases := []struct {
		name          string
		sources       []sourceScore
		newNodePrice  float64
		standbyPrices map[string]float64
		cost          float64
		utilization   float64
	}{
		{"one capacity type", []sourceScore{{Price: 4, AllocatableCPU: 4, RequestedCPU: 2, Baseline: 0.5, UtilizationWeight: 1}}, 2, nil, 1, 0.5},
		{"two capacity types", []sourceScore{{Price: 4, AllocatableCPU: 4, RequestedCPU: 4, Baseline: 0.5, UtilizationWeight: 1}, {Price: 2, AllocatableCPU: 4, RequestedCPU: 2, Baseline: 0.25, UtilizationWeight: 2}}, 3, nil, 2, 1},
		{"zero price source", []sourceScore{{Price: 0, AllocatableCPU: 4, RequestedCPU: 0, Baseline: 1, UtilizationWeight: 1}, {Price: 4, AllocatableCPU: 4, RequestedCPU: 4, Baseline: 1, UtilizationWeight: 1}}, 0, nil, 1, 1},
		{"zero baseline", []sourceScore{{Price: 4, AllocatableCPU: 4, RequestedCPU: 4, UtilizationWeight: 1}, {Price: 4, AllocatableCPU: 4, RequestedCPU: 2, Baseline: 1, UtilizationWeight: 1}}, 4, nil, 0, 0.5},
		{"reactivated standby price reduces f", []sourceScore{{Price: 4, AllocatableCPU: 4, RequestedCPU: 2, Baseline: 0.5, UtilizationWeight: 1}}, 0, map[string]float64{"standby": 1}, 1.5, 0.5},
		{"clamped requests and zero allocatable", []sourceScore{{Price: 4, AllocatableCPU: 4, RequestedCPU: -1, Baseline: 1, UtilizationWeight: 2}, {Price: 0, Baseline: 1, UtilizationWeight: 1}}, 0, nil, 1, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := computeScore(tc.newNodePrice, tc.standbyPrices, tc.sources)
			if math.Abs(got.Cost-tc.cost) > 1e-9 || math.Abs(got.Utilization-tc.utilization) > 1e-9 {
				t.Fatalf("score = cost %v utilization %v, want %v and %v", got.Cost, got.Utilization, tc.cost, tc.utilization)
			}
		})
	}
}
