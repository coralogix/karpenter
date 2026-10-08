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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

func groupedPacingNodePool(name, group, rate, batch string) *v1.NodePool {
	annotations := map[string]string{
		v1.DisruptionPacingGroupAnnotationKey:     group,
		v1.DisruptionPacingPerMinuteAnnotationKey: rate,
	}
	if batch != "" {
		annotations[v1.DisruptionPacingPerBatchAnnotationKey] = batch
	}
	return &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}}
}

func TestDisruptionPacingGroupSharesCooldownAtExactBoundary(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	pacing := NewDisruptionPacing(clock)
	first := groupedPacingNodePool("first", "stable-workloads", "0.2", "1")
	second := groupedPacingNodePool("second", "stable-workloads", "0.20", "1")
	independentGroup := groupedPacingNodePool("independent", "batch-workloads", "0.2", "1")
	independentPool := paceTestNodePool("0.2", "1")
	independentPool.Name = "unshared"
	pacing.Refresh([]*v1.NodePool{first, second, independentGroup, independentPool}, nil)

	accepted := pacing.AdmitCommands([]Command{*paceCommandWithPods(first, 1)})
	if len(accepted) != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want one command", accepted)
	}
	cmd := paceCommandWithPods(first, 1)
	start := clock.Now()
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{{Command: cmd, StartTime: start}})
	if pacing.CandidateAllowed(second, 0) {
		t.Fatal("expected the shared group to be blocked after a successful start")
	}
	if !pacing.CandidateAllowed(independentGroup, 0) || !pacing.CandidateAllowed(independentPool, 0) {
		t.Fatal("expected independent groups and ungrouped pools to remain eligible")
	}

	clock.Step(5*time.Minute - time.Nanosecond)
	if pacing.CandidateAllowed(second, 0) {
		t.Fatal("expected the shared group to remain blocked immediately before five minutes")
	}
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(second, 0) {
		t.Fatal("expected the shared group to be eligible at the exact five-minute boundary")
	}
}

func TestDisruptionPacingBatchAppliesCapAndChargesOnlySuccessfulNodes(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clock)
	first := groupedPacingNodePool("first", "stable-workloads", "0.5", "2")
	second := groupedPacingNodePool("second", "stable-workloads", "0.50", "2")
	pacing.Refresh([]*v1.NodePool{first, second}, nil)
	commands := []Command{
		*paceCommandWithPods(first, 1),
		*paceCommandWithPods(second, 1),
		*paceCommandWithPods(first, 1),
	}

	accepted := pacing.AdmitCommands(commands)
	if len(accepted) != 2 || accepted[0] != 0 || accepted[1] != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want the first two whole commands under cap 2", accepted)
	}
	start := clock.Now()
	// The second command failed to start. Only one of the two reserved nodes
	// should contribute to the two-minute cooldown for a 0.5/minute rate.
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{{Command: &commands[0], StartTime: start}})
	clock.Step(2*time.Minute - time.Nanosecond)
	if pacing.CandidateAllowed(second, 0) {
		t.Fatal("expected a one-node success to remain blocked until two minutes")
	}
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(second, 0) {
		t.Fatal("expected failed and unused reservations to be released at the cooldown boundary")
	}
}

func TestDisruptionPacingBatchUsesSuccessfulCountAndLatestStart(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clock)
	first := groupedPacingNodePool("first", "stable-workloads", "0.5", "")
	second := groupedPacingNodePool("second", "stable-workloads", "0.50", "")
	pacing.Refresh([]*v1.NodePool{first, second}, nil)

	commands := []Command{
		*paceCommandWithPods(first, 1),
		*paceCommandWithPods(second, 1),
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
	if pacing.CandidateAllowed(first, 0) {
		t.Fatal("expected the aggregate two-node cooldown to remain blocked until four minutes after the latest start")
	}
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(second, 0) {
		t.Fatal("expected the group to become eligible at the aggregate cooldown boundary")
	}
}

func TestDisruptionPacingMembershipChangesCarryCooldownBetweenPasses(t *testing.T) {
	for _, movedPool := range []string{"first", "second", "both"} {
		t.Run("move-"+movedPool, func(t *testing.T) { testDisruptionPacingMembershipChange(t, movedPool) })
	}
}

func testDisruptionPacingMembershipChange(t *testing.T, movedPool string) {
	clock := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clock)
	first := groupedPacingNodePool("first", "group-g", "0.2", "1")
	second := groupedPacingNodePool("second", "group-g", "0.2", "1")
	pacing.Refresh([]*v1.NodePool{first, second}, nil)
	accepted := pacing.AdmitCommands([]Command{*paceCommandWithPods(first, 1)})
	if len(accepted) != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want one command", accepted)
	}
	cmd := paceCommandWithPods(first, 1)
	start := clock.Now()
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{{Command: cmd, StartTime: start}})

	if shouldMovePacingPool(movedPool, first.Name) {
		first = groupedPacingNodePool("first", "group-h", "0.2", "1")
	}
	if shouldMovePacingPool(movedPool, second.Name) {
		second = groupedPacingNodePool("second", "group-h", "0.2", "1")
	}
	pacing.Refresh([]*v1.NodePool{first, second}, nil)
	assertPacingPoolsBlocked(t, pacing, first, second, "expected completed group cooldown to carry to both current scopes")
	clock.Step(5*time.Minute - time.Nanosecond)
	assertPacingPoolsBlocked(t, pacing, first, second, "expected completed group admission to carry its cooldown to both scopes")
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(first, 0) {
		t.Fatal("expected first scope to become eligible at the exact five-minute boundary")
	}
	if !pacing.CandidateAllowed(second, 0) {
		t.Fatal("expected second scope to become eligible at the exact five-minute boundary")
	}
}

func shouldMovePacingPool(movedPool, poolName string) bool {
	return movedPool == poolName || movedPool == "both"
}

func assertPacingPoolsBlocked(t *testing.T, pacing *DisruptionPacing, first, second *v1.NodePool, message string) {
	t.Helper()
	if pacing.CandidateAllowed(first, 0) || pacing.CandidateAllowed(second, 0) {
		t.Fatal(message)
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
			np := groupedPacingNodePool("stable", "stable-workloads", tc.initial, "1")
			pacing.Refresh([]*v1.NodePool{np}, nil)
			cmd := paceCommandWithPods(np, 1)
			accepted := pacing.AdmitCommands([]Command{*cmd})
			if len(accepted) != 1 {
				t.Fatalf("AdmitCommands() accepted %v, want one command", accepted)
			}
			pacing.ChargePass([]DisruptionPacingSuccessfulStart{{Command: cmd, StartTime: clock.Now()}})

			elapsed := time.Duration(0)
			if tc.name == "slower" {
				clock.Step(30 * time.Second)
				elapsed = 30 * time.Second
			}
			np = groupedPacingNodePool("stable", "stable-workloads", tc.updated, "1")
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
			np := groupedPacingNodePool("stable", "stable-workloads", "1", "1")
			pacing.Refresh([]*v1.NodePool{np}, nil)
			if !completePacingCommand(pacing, paceCommandWithPods(np, 1)) {
				t.Fatal("expected initial command to be admitted")
			}

			clock.Step(tc.elapsed)
			np = groupedPacingNodePool("stable", "stable-workloads", "0.1", "1")
			pacing.Refresh([]*v1.NodePool{np}, nil)
			if !pacing.CandidateAllowed(np, 0) {
				t.Fatal("slower rate revived an already expired cooldown")
			}
		})
	}
}

func TestDisruptionPacingGroupJoinPreservesLatestScopeDeadline(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	pacing := NewDisruptionPacing(clock)
	a := paceTestNodePool("0.1", "")
	a.Name = "a"
	b := paceTestNodePool("1", "")
	b.Name = "b"
	pacing.Refresh([]*v1.NodePool{a, b}, nil)
	if !completePacingCommand(pacing, paceCommandWithPods(a, 1)) {
		t.Fatal("expected pool a command to be admitted")
	}

	clock.Step(time.Minute)
	if !completePacingCommand(pacing, paceCommandWithPods(b, 2)) {
		t.Fatal("expected pool b command to be admitted")
	}
	clock.Step(time.Minute)

	a = groupedPacingNodePool("a", "stable-workloads", "0.1", "2")
	b = groupedPacingNodePool("b", "stable-workloads", "0.1", "2")
	pacing.Refresh([]*v1.NodePool{a, b}, nil)
	if pacing.CandidateAllowed(a, 0) {
		t.Fatal("expected joined group to remain blocked after migration")
	}

	clock.Step(8*time.Minute - time.Nanosecond)
	if pacing.CandidateAllowed(a, 0) || pacing.CandidateAllowed(b, 0) {
		t.Fatal("expected the merged group deadline to follow pool a's longer cooldown")
	}
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(a, 0) || !pacing.CandidateAllowed(b, 0) {
		t.Fatal("expected every group member to become eligible at the preserved ten-minute boundary")
	}
}

func TestDisruptionPacingSlowerRateAfterJoinDoesNotExtendDeadline(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	pacing := NewDisruptionPacing(clock)
	a := paceTestNodePool("1", "")
	a.Name = "a"
	pacing.Refresh([]*v1.NodePool{a}, nil)
	if !completePacingCommand(pacing, paceCommandWithPods(a, 1)) {
		t.Fatal("expected pool a command to be admitted")
	}

	clock.Step(30 * time.Second)
	a = groupedPacingNodePool("a", "stable-workloads", "0.1", "2")
	pacing.Refresh([]*v1.NodePool{a}, nil)

	clock.Step(30*time.Second - time.Nanosecond)
	if pacing.CandidateAllowed(a, 0) {
		t.Fatal("slower rate after join extended the outstanding deadline")
	}
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(a, 0) {
		t.Fatal("expected the group to become eligible at the original one-minute boundary")
	}
}

func TestDisruptionPacingMigrationMergesSimultaneousScopeMoves(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	pacing := NewDisruptionPacing(clock)
	first := groupedPacingNodePool("first", "group-g", "0.2", "1")
	second := groupedPacingNodePool("second", "group-g", "0.2", "1")
	pacing.Refresh([]*v1.NodePool{first, second}, nil)
	if !completePacingCommand(pacing, paceCommandWithPods(first, 1)) {
		t.Fatal("expected initial group command to be admitted")
	}

	first = groupedPacingNodePool("first", "group-h", "0.2", "1")
	second = groupedPacingNodePool("second", "group-h", "0.2", "1")
	pacing.Refresh([]*v1.NodePool{first, second}, nil)
	if pacing.CandidateAllowed(first, 0) || pacing.CandidateAllowed(second, 0) {
		t.Fatal("expected simultaneous scope moves to carry the shared deadline once")
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

	invalid := groupedPacingNodePool("invalid", "invalid group", "0", "0")
	pacing.Refresh([]*v1.NodePool{known, invalid}, nil)
	if accepted := pacing.AdmitCommands([]Command{*paceCommand(invalid)}); len(accepted) != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want empty candidate admitted for a managed pool with an invalid policy", accepted)
	}
}

func TestDisruptionPacingInvalidGroupWarningsDeduplicatePerNodePool(t *testing.T) {
	first := groupedPacingNodePool("first", "stable-workloads", "1", "1")
	first.UID = types.UID("first-uid")
	second := groupedPacingNodePool("second", "stable-workloads", "2", "1")
	second.UID = types.UID("second-uid")
	fakeRecorder := record.NewFakeRecorder(10)
	pacing := NewDisruptionPacing(clocktesting.NewFakeClock(time.Now()))
	poolEvents := events.NewRecorder(fakeRecorder)

	pacing.Refresh([]*v1.NodePool{first, second}, poolEvents)
	pacing.Refresh([]*v1.NodePool{first, second}, poolEvents)

	var emitted []string
	for {
		select {
		case event := <-fakeRecorder.Events:
			emitted = append(emitted, event)
		default:
			if len(emitted) != 2 {
				t.Fatalf("warning events = %v, want one event for each NodePool", emitted)
			}
			return
		}
	}
}

func TestDisruptionPacingConflictRecoveryPreservesCooldown(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clock)
	active := groupedPacingNodePool("active", "stable-workloads", "0.2", "1")
	inactive := groupedPacingNodePool("inactive", "stable-workloads", "0.20", "1")
	pacing.Refresh([]*v1.NodePool{active, inactive}, nil)
	cmd := paceCommandWithPods(active, 1)
	accepted := pacing.AdmitCommands([]Command{*cmd})
	if len(accepted) != 1 {
		t.Fatalf("AdmitCommands() accepted %v, want one command", accepted)
	}
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{{Command: cmd, StartTime: clock.Now()}})

	inactive = groupedPacingNodePool("inactive", "stable-workloads", "0.3", "1")
	pacing.Refresh([]*v1.NodePool{active, inactive}, nil)
	if pacing.CandidateAllowed(active, 0) {
		t.Fatal("expected a mismatched inactive member to block the whole group")
	}
	inactive = groupedPacingNodePool("inactive", "stable-workloads", "0.20", "1")
	pacing.Refresh([]*v1.NodePool{active, inactive}, nil)
	clock.Step(5*time.Minute - time.Nanosecond)
	if pacing.CandidateAllowed(active, 0) {
		t.Fatal("expected conflict recovery to preserve the outstanding cooldown")
	}
	clock.Step(time.Nanosecond)
	if !pacing.CandidateAllowed(active, 0) {
		t.Fatal("expected the group to recover automatically at its original cooldown boundary")
	}
}

func TestDisruptionPacingRejectsWholeMultiScopeCommands(t *testing.T) {
	clock := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clock)
	first := groupedPacingNodePool("first", "stable-workloads", "1", "1")
	second := groupedPacingNodePool("second", "stable-workloads", "1", "1")
	independent := groupedPacingNodePool("independent", "batch-workloads", "1", "1")
	pacing.Refresh([]*v1.NodePool{first, second, independent}, nil)

	// An oversized group command is rejected whole. A later command touching an
	// independent scope in the same batch can still be admitted.
	tooLarge := Command{Candidates: []*Candidate{
		{NodePool: first, reschedulablePods: []*corev1.Pod{{}}},
		{NodePool: second, reschedulablePods: []*corev1.Pod{{}}},
	}}
	crossScope := Command{Candidates: []*Candidate{
		{NodePool: second, reschedulablePods: []*corev1.Pod{{}}},
		{NodePool: independent, reschedulablePods: []*corev1.Pod{{}}},
	}}
	commands := []Command{tooLarge, *paceCommandWithPods(first, 1), crossScope, *paceCommandWithPods(independent, 1)}
	accepted := pacing.AdmitCommands(commands)
	if len(accepted) != 2 || accepted[0] != 1 || accepted[1] != 3 {
		t.Fatalf("AdmitCommands() accepted %v, want second and fourth commands only", accepted)
	}
	start := clock.Now()
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{
		{Command: &commands[1], StartTime: start},
		{Command: &commands[3], StartTime: start},
	})
	if pacing.CandidateAllowed(first, 0) || pacing.CandidateAllowed(second, 0) {
		t.Fatal("expected the accepted group command to charge its shared scope")
	}
	if pacing.CandidateAllowed(independent, 0) {
		t.Fatal("expected the independent command to be charged after admission")
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
		t.Fatal("expected a malformed standalone rate to preserve the ungrouped fail-open behavior")
	}
	malformed.Annotations[v1.DisruptionPacingGroupAnnotationKey] = "stable-workloads"
	pacing.Refresh([]*v1.NodePool{malformed}, nil)
	if pacing.CandidateAllowed(malformed, 0) {
		t.Fatal("expected a malformed grouped rate to block non-empty pacing")
	}
}
