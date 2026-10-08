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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func namedPacingNodePool(name, rate, batch string) *v1.NodePool {
	np := paceTestNodePool(rate, batch)
	np.Name = name
	return np
}

func TestDisruptionPacingPerPoolCooldownAtExactBoundary(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	pacing := NewDisruptionPacing(clock)
	paced := namedPacingNodePool("paced", "0.2", "1")
	other := namedPacingNodePool("other", "0.2", "1")
	pacing.Refresh([]*v1.NodePool{paced, other}, nil)

	if !completePacingCommand(pacing, paceCommandWithPods(paced, 1)) {
		t.Fatal("expected paced pool command to be admitted")
	}
	if pacing.CandidateAllowed(paced, 0) {
		t.Fatal("expected paced pool to be blocked after a successful start")
	}
	if !pacing.CandidateAllowed(other, 0) {
		t.Fatal("expected independent pool to remain eligible")
	}

	clock.Step(5*time.Minute - time.Nanosecond)
	if pacing.CandidateAllowed(paced, 0) {
		t.Fatal("expected paced pool to remain blocked immediately before five minutes")
	}
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(paced, 0) {
		t.Fatal("expected paced pool to be eligible at the exact five-minute boundary")
	}
}

func TestDisruptionPacingBatchAppliesCapAndChargesOnlySuccessfulNodes(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clock)
	np := namedPacingNodePool("paced", "0.5", "2")
	pacing.Refresh([]*v1.NodePool{np}, nil)
	commands := []Command{
		*paceCommandWithPods(np, 1),
		*paceCommandWithPods(np, 1),
		*paceCommandWithPods(np, 1),
	}

	accepted := pacing.AdmitCommands(commands)
	if len(accepted) != 2 || accepted[0] != 0 || accepted[1] != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want the first two whole commands under cap 2", accepted)
	}
	start := clock.Now()
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{{Command: &commands[0], StartTime: start}})
	clock.Step(2*time.Minute - time.Nanosecond)
	if pacing.CandidateAllowed(np, 0) {
		t.Fatal("expected a one-node success to remain blocked until two minutes")
	}
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(np, 0) {
		t.Fatal("expected failed and unused reservations to be released at the cooldown boundary")
	}
}

func TestDisruptionPacingBatchUsesSuccessfulCountAndLatestStart(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clock)
	np := namedPacingNodePool("paced", "0.5", "")
	pacing.Refresh([]*v1.NodePool{np}, nil)

	commands := []Command{
		*paceCommandWithPods(np, 1),
		*paceCommandWithPods(np, 1),
	}
	accepted := pacing.AdmitCommands(commands)
	if len(accepted) != 2 || accepted[0] != 0 || accepted[1] != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want both whole commands", accepted)
	}
	firstStart := clock.Now()
	clock.Step(30 * time.Second)
	latestStart := clock.Now()
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{
		{Command: &commands[0], StartTime: firstStart},
		{Command: &commands[1], StartTime: latestStart},
	})

	clock.Step(4*time.Minute - time.Nanosecond)
	if pacing.CandidateAllowed(np, 0) {
		t.Fatal("expected the aggregate two-node cooldown to remain blocked until four minutes after the latest start")
	}
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(np, 0) {
		t.Fatal("expected the pool to become eligible at the aggregate cooldown boundary")
	}
}

func TestDisruptionPacingRateChangesDoNotShortenCooldown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		initial string
		updated string
		wait    time.Duration
	}{
		{name: "faster", initial: "0.5", updated: "1", wait: 2 * time.Minute},
		{name: "slower", initial: "1", updated: "0.5", wait: time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := clocktesting.NewFakeClock(time.Now())
			pacing := NewDisruptionPacing(clock)
			np := namedPacingNodePool("stable", tc.initial, "1")
			pacing.Refresh([]*v1.NodePool{np}, nil)
			cmd := paceCommandWithPods(np, 1)
			if !completePacingCommand(pacing, cmd) {
				t.Fatalf("expected command to be admitted")
			}

			elapsed := time.Duration(0)
			if tc.name == "slower" {
				clock.Step(30 * time.Second)
				elapsed = 30 * time.Second
			}
			np = namedPacingNodePool("stable", tc.updated, "1")
			pacing.Refresh([]*v1.NodePool{np}, nil)
			clock.Step(tc.wait - elapsed - time.Nanosecond)
			if pacing.CandidateAllowed(np, 0) {
				t.Fatal("rate change shortened an outstanding cooldown")
			}
			clock.Step(time.Nanosecond)
			if !pacing.CandidateAllowed(np, 0) {
				t.Fatal("expected cooldown to expire at the original or extended boundary")
			}
		})
	}
}

func TestDisruptionPacingRateChangeDoesNotReviveExpiredCooldown(t *testing.T) {
	for _, tc := range []struct {
		name    string
		elapsed time.Duration
	}{
		{name: "at expiry", elapsed: time.Minute},
		{name: "after expiry", elapsed: 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := clocktesting.NewFakeClock(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
			pacing := NewDisruptionPacing(clock)
			np := namedPacingNodePool("stable", "1", "1")
			pacing.Refresh([]*v1.NodePool{np}, nil)
			if !completePacingCommand(pacing, paceCommandWithPods(np, 1)) {
				t.Fatal("expected initial command to be admitted")
			}

			clock.Step(tc.elapsed)
			np = namedPacingNodePool("stable", "0.1", "1")
			pacing.Refresh([]*v1.NodePool{np}, nil)
			if !pacing.CandidateAllowed(np, 0) {
				t.Fatal("slower rate revived an already expired cooldown")
			}
		})
	}
}

func TestDisruptionPacingAdmissionRejectsUnknownManagedPools(t *testing.T) {
	pacing := NewDisruptionPacing(clocktesting.NewFakeClock(time.Now()))
	known := paceTestNodePool("1", "1")
	known.Name = "known"
	pacing.Refresh([]*v1.NodePool{known}, nil)

	unknown := paceTestNodePool("1", "1")
	unknown.Name = "unknown"
	if len(pacing.AdmitCommands([]Command{*paceCommandWithPods(unknown, 1)})) != 0 {
		t.Fatal("expected admission to reject candidates from pools absent from the managed snapshot")
	}
}

func TestDisruptionPacingAdmissionValidatesMembershipForEmptyCandidates(t *testing.T) {
	pacing := NewDisruptionPacing(clocktesting.NewFakeClock(time.Now()))
	known := paceTestNodePool("1", "1")
	known.Name = "known"
	unknown := paceTestNodePool("1", "1")
	unknown.Name = "unknown"
	pacing.Refresh([]*v1.NodePool{known}, nil)

	commands := []Command{
		*paceCommand(unknown),
		{Candidates: []*Candidate{
			{NodePool: known, reschedulablePods: []*corev1.Pod{{}}},
			{NodePool: unknown},
		}},
	}
	if accepted := pacing.AdmitCommands(commands); len(accepted) != 0 {
		t.Fatalf("AdmitCommands() accepted %v, want empty-only and mixed commands with an unmanaged pool rejected", accepted)
	}
}

func TestDisruptionPacingAdmissionSkipsPolicyChecksForEmptyCandidates(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clock)
	known := paceTestNodePool("1", "1")
	known.Name = "known"
	pacing.Refresh([]*v1.NodePool{known}, nil)

	commands := []Command{*paceCommandWithPods(known, 1), *paceCommand(known)}
	if accepted := pacing.AdmitCommands(commands); len(accepted) != 2 || accepted[0] != 0 || accepted[1] != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want non-empty and empty commands under a one-node pass cap", accepted)
	}
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{{Command: &commands[0], StartTime: clock.Now()}})
	if accepted := pacing.AdmitCommands([]Command{*paceCommand(known)}); len(accepted) != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want empty command admitted during cooldown", accepted)
	}

	invalid := paceTestNodePool("0", "0")
	invalid.Name = "invalid"
	pacing.Refresh([]*v1.NodePool{known, invalid}, nil)
	if accepted := pacing.AdmitCommands([]Command{*paceCommand(invalid)}); len(accepted) != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want empty candidate admitted for a managed pool with an invalid policy", accepted)
	}
}

func TestDisruptionPacingRejectsOversizedMultiCandidateCommands(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clock)
	np := namedPacingNodePool("paced", "1", "1")
	pacing.Refresh([]*v1.NodePool{np}, nil)

	tooLarge := Command{Candidates: []*Candidate{
		{NodePool: np, reschedulablePods: []*corev1.Pod{{}}},
		{NodePool: np, reschedulablePods: []*corev1.Pod{{}}},
	}}
	commands := []Command{tooLarge, *paceCommandWithPods(np, 1)}
	accepted := pacing.AdmitCommands(commands)
	if len(accepted) != 1 || accepted[0] != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want only the single-node command", accepted)
	}
	start := clock.Now()
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{{Command: &commands[1], StartTime: start}})
	if pacing.CandidateAllowed(np, 0) {
		t.Fatal("expected the accepted command to charge its pool scope")
	}
}

func TestDisruptionPacingCanonicalAnnotationsAndFiniteRateValidation(t *testing.T) {
	annotations := map[string]string{
		v1.DisruptionPacingPerMinuteAnnotationKey: "0.20",
		v1.DisruptionPacingPerBatchAnnotationKey:  "01",
	}
	config := disruptionPacingConfigFor(&v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "canonical", Annotations: annotations}})
	if !config.configured || config.blocked || config.rate != 0.2 || config.maxNodesPerBatch != 1 {
		t.Fatalf("canonical annotations produced config %+v", config)
	}

	malformed := paceTestNodePool("NaN", "1")
	pacing := NewDisruptionPacing(clocktesting.NewFakeClock(time.Now()))
	if !pacing.CandidateAllowed(malformed, 0) {
		t.Fatal("expected a malformed rate to preserve fail-open behavior")
	}
}
