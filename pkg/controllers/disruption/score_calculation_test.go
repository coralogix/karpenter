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

	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

func TestCostAndUtilizationScore(t *testing.T) {
	cases := []struct {
		name                     string
		sources                  []scoreSource
		savings, cost, slackTerm float64
	}{
		{"one capacity type", []scoreSource{{Price: 4, AllocatableCPU: 4, RequestedCPU: 2, Baseline: 0.5, Weight: 1}}, 2, 1, 0.5},
		{"two capacity types", []scoreSource{{Price: 4, AllocatableCPU: 4, RequestedCPU: 4, Baseline: 0.5, Weight: 1}, {Price: 2, AllocatableCPU: 4, RequestedCPU: 2, Baseline: 0.25, Weight: 2}}, 3, 2, 1},
		{"zero price source", []scoreSource{{Price: 0, AllocatableCPU: 4, RequestedCPU: 0, Baseline: 1, Weight: 1}, {Price: 4, AllocatableCPU: 4, RequestedCPU: 4, Baseline: 1, Weight: 1}}, 4, 1, 1},
		{"zero baseline", []scoreSource{{Price: 0, AllocatableCPU: 4, RequestedCPU: 4, Weight: 1}, {Price: 4, AllocatableCPU: 4, RequestedCPU: 2, Baseline: 1, Weight: 1}}, 4, 0, 0.5},
		{"reactivated standby reduces savings", []scoreSource{{Price: 4, AllocatableCPU: 4, RequestedCPU: 2, Baseline: 1, Weight: 1}}, 3, 0.75, 0.5},
		{"clamped requests and zero allocatable", []scoreSource{{Price: 4, AllocatableCPU: 4, RequestedCPU: -1, Baseline: 1, Weight: 2}, {Price: 0, Baseline: 1, Weight: 1}}, 4, 1, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := calculateScore(tc.sources, tc.savings)
			if tc.name == "reactivated standby reduces savings" {
				candidate := candidateWithPrice(t, 4)
				cmd := Command{Candidates: []*Candidate{candidate}, Results: pscheduling.Results{ExistingNodes: standbyReactivationResults("standby")}}
				inputs := scoreInputs{prices: map[string]float64{"standby": 1}, sources: map[*Candidate]scoreSource{candidate: tc.sources[0]}}
				got, _ = inputs.score(cmd, cmd.Results)
				if got.SavingsFraction != 0.75 {
					t.Fatalf("savings fraction = %v, want 0.75", got.SavingsFraction)
				}
			}
			if math.Abs(got.Cost-tc.cost) > 1e-9 || math.Abs(got.Utilization-tc.slackTerm) > 1e-9 {
				t.Fatalf("score = cost %v slack %v, want %v and %v", got.Cost, got.Utilization, tc.cost, tc.slackTerm)
			}
		})
	}
}
