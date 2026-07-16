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

func TestCandidateAllowedUnsetConfiguration(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected unset configuration to allow candidates")
	}
}

func TestCandidateAllowedInvalidConfigurationFailsOpen(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := paceTestNodePool("0", "1")
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected invalid configuration to fail open and allow candidates")
	}
	pace.Charge(paceCommand(np, 1))
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected charge with invalid configuration to be a no-op")
	}
}

func TestCandidateAllowedRateOnlyConfiguration(t *testing.T) {
	testClock := clocktesting.NewFakeClock(time.Now())
	pace := NewUnderutilizedConsolidationPace(testClock)
	np := paceTestNodePool("1", "")
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected rate-only configuration to allow candidates")
	}
	pace.Charge(paceCommand(np, 1))
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected rate-only configuration to pace after charge")
	}
}

func TestCandidateAllowedBatchCapBoundary(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := paceTestNodePool("100", "2")
	if !pace.candidateAllowed(np, 1) {
		t.Fatal("expected selectedCount == max-1 to allow another candidate")
	}
	if pace.candidateAllowed(np, 2) {
		t.Fatal("expected selectedCount == max to block candidates")
	}
}

func TestCandidateAllowedRateCooldown(t *testing.T) {
	testClock := clocktesting.NewFakeClock(time.Now())
	pace := NewUnderutilizedConsolidationPace(testClock)
	np := paceTestNodePool("1", "100")
	pace.Charge(&Command{Candidates: []*Candidate{{NodePool: np}}})
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected rate cooldown to block candidates")
	}
	testClock.Step(61 * time.Second)
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected candidates after cooldown elapsed")
	}
}

func TestCandidateAllowedNilPaceDisablesChecks(t *testing.T) {
	var pace *UnderutilizedConsolidationPace
	np := paceTestNodePool("0", "1")
	if !pace.candidateAllowed(np, 100) {
		t.Fatal("expected nil pace to disable planning-time checks")
	}
}

func TestUnderutilizedConsolidationPaceFractional(t *testing.T) {
	testClock := clocktesting.NewFakeClock(time.Now())
	pace := NewUnderutilizedConsolidationPace(testClock)
	np := paceTestNodePool("0.5", "100")
	cmd := paceCommand(np, 1)

	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected first admission")
	}
	pace.Charge(cmd)
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected second admission to be blocked")
	}

	testClock.Step(2 * time.Minute)
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected admission after interval elapsed")
	}
}

func TestUnderutilizedConsolidationPaceWeightedCooldown(t *testing.T) {
	testClock := clocktesting.NewFakeClock(time.Now())
	pace := NewUnderutilizedConsolidationPace(testClock)
	np := paceTestNodePool("1", "100")
	cmd := paceCommand(np, 2)

	pace.Charge(cmd)
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected admission to be blocked before weighted cooldown elapsed")
	}

	testClock.Step(1 * time.Minute)
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected admission to remain blocked after one minute for two-node charge")
	}

	testClock.Step(1 * time.Minute)
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected admission after two-minute weighted cooldown")
	}
}

func TestUnderutilizedConsolidationPaceUnset(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	cmd := paceCommand(np, 1)

	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected first admission with unset pace configuration")
	}
	pace.Charge(cmd)
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected repeated admission with unset pace configuration")
	}
}

func TestUnderutilizedConsolidationPaceCommandSizeLimit(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := paceTestNodePool("1", "1")

	if pace.candidateAllowed(np, 1) {
		t.Fatal("expected selectedCount at per-consolidation cap to block another candidate")
	}
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected single candidate within cap to be allowed")
	}
}

func TestUnderutilizedConsolidationPaceRateOnlyNoBatchCap(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := paceTestNodePool("1", "")

	if !pace.candidateAllowed(np, 99) {
		t.Fatal("expected rate-only configuration to impose no per-command batch cap")
	}
}
