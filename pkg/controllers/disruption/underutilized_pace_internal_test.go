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
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func paceTestNodePool(rate, maxNodes string) *v1.NodePool {
	annotations := map[string]string{
		v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey: rate,
	}
	if maxNodes != "" {
		annotations[v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey] = maxNodes
	}
	return &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "default",
			Annotations: annotations,
		},
	}
}

func paceCommand(np *v1.NodePool, count int) *Command {
	candidates := make([]*Candidate, count)
	for i := range candidates {
		candidates[i] = &Candidate{NodePool: np}
	}
	return &Command{Candidates: candidates}
}

func TestCandidateAllowedBatchCap(t *testing.T) {
	cases := []struct {
		name           string
		rate, maxNodes string
		selectedCount  int
		allowed        bool
	}{
		{name: "below cap", rate: "100", maxNodes: "2", selectedCount: 1, allowed: true},
		{name: "at cap", rate: "100", maxNodes: "2", selectedCount: 2, allowed: false},
		{name: "single-node cap allows first", rate: "1", maxNodes: "1", selectedCount: 0, allowed: true},
		{name: "single-node cap blocks second", rate: "1", maxNodes: "1", selectedCount: 1, allowed: false},
		{name: "no cap when unset", rate: "1", maxNodes: "", selectedCount: 99, allowed: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
			np := paceTestNodePool(tc.rate, tc.maxNodes)
			if got := pace.candidateAllowed(np, tc.selectedCount); got != tc.allowed {
				t.Fatalf("candidateAllowed(selectedCount=%d) = %v, want %v", tc.selectedCount, got, tc.allowed)
			}
		})
	}
}

func TestCandidateAllowedRateCooldown(t *testing.T) {
	cases := []struct {
		name              string
		rate, maxNodes    string
		chargeCount       int
		stillBlockedAfter time.Duration
		allowedAfter      time.Duration
	}{
		{name: "unit rate", rate: "1", maxNodes: "", chargeCount: 1, stillBlockedAfter: 30 * time.Second, allowedAfter: 61 * time.Second},
		{name: "fractional rate", rate: "0.5", maxNodes: "100", chargeCount: 1, stillBlockedAfter: 1 * time.Minute, allowedAfter: 2 * time.Minute},
		{name: "weighted by node count", rate: "1", maxNodes: "100", chargeCount: 2, stillBlockedAfter: 1 * time.Minute, allowedAfter: 2 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testClock := clocktesting.NewFakeClock(time.Now())
			pace := NewUnderutilizedConsolidationPace(testClock)
			np := paceTestNodePool(tc.rate, tc.maxNodes)

			if !pace.candidateAllowed(np, 0) {
				t.Fatal("expected first admission before any charge")
			}
			pace.Charge(paceCommand(np, tc.chargeCount))
			if pace.candidateAllowed(np, 0) {
				t.Fatal("expected admission to be blocked immediately after charge")
			}

			testClock.Step(tc.stillBlockedAfter)
			if pace.candidateAllowed(np, 0) {
				t.Fatalf("expected admission to remain blocked after %s", tc.stillBlockedAfter)
			}
			testClock.Step(tc.allowedAfter - tc.stillBlockedAfter)
			if !pace.candidateAllowed(np, 0) {
				t.Fatalf("expected admission after %s elapsed", tc.allowedAfter)
			}
		})
	}
}

func TestCandidateAllowedUnconfigured(t *testing.T) {
	cases := []struct {
		name string
		np   *v1.NodePool
	}{
		{name: "unset", np: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}},
		{name: "invalid rate fails open", np: paceTestNodePool("0", "1")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
			if !pace.candidateAllowed(tc.np, 100) {
				t.Fatal("expected unconfigured pace to ignore batch cap and allow candidates")
			}
			pace.Charge(paceCommand(tc.np, 1))
			if !pace.candidateAllowed(tc.np, 100) {
				t.Fatal("expected charge to be a no-op for unconfigured pace")
			}
		})
	}
}

func TestCandidateAllowedNilPace(t *testing.T) {
	var pace *UnderutilizedConsolidationPace
	if !pace.candidateAllowed(paceTestNodePool("0", "1"), 100) {
		t.Fatal("expected nil pace to disable planning-time checks")
	}
}
