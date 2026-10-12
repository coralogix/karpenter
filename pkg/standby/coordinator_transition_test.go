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

package standby

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"

	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestStandbyTransitionPostEvacuationRepairsPartialMarkerWrite(t *testing.T) {
	ctx := context.Background()
	standbySince := time.Date(2026, time.October, 1, 9, 30, 0, 0, time.UTC)
	h := newCoordinatorTestFixture(t, "evac-node", "evac-claim", time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC))
	failTaintOnce := true
	h.client.beforeNodePatch = func(node *corev1.Node) error {
		if !utilstandby.IsNodeClaimStandby(h.readClaim(h.ctx)) || !utilstandby.HasNodeTaint(node) {
			return nil
		}
		if failTaintOnce {
			failTaintOnce = false
			return errors.New("injected transient node taint failure")
		}
		return nil
	}

	result, err := h.coordinator.EnterStandby(ctx, h.enterStandbyRequest(), EnterStandbyOptions{BarrierTaintPresent: true, Since: standbySince})
	if err != nil {
		t.Fatalf("EnterStandby() error = %v", err)
	}
	if !StandbyEntrySucceeded(result) {
		t.Fatalf("EnterStandby() outcome = %q, want completed standby entry after retry", result.Outcome)
	}
	h.assertStandbyComplete(t, standbySince)

	result, err = h.coordinator.EnterStandby(ctx, h.enterStandbyRequest(), EnterStandbyOptions{BarrierTaintPresent: true, Since: standbySince})
	if err != nil {
		t.Fatalf("repeat EnterStandby() error = %v", err)
	}
	if result.Outcome != OutcomeUnchanged || !StandbyEntryComplete(result.Node, result.NodeClaim) {
		t.Fatalf("repeat EnterStandby() = %+v, want unchanged complete entry", result)
	}
}

func TestStandbyTransitionPostEvacuationPreservesStandbySinceAfterRestart(t *testing.T) {
	ctx := context.Background()
	standbySince := time.Date(2026, time.October, 2, 14, 0, 0, 0, time.UTC)
	h := newCoordinatorTestFixture(t, "evac-node", "evac-claim", time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC))
	claim := h.readClaim(ctx)
	utilstandby.SetNodeClaimStandby(claim, true, standbySince)
	if err := h.client.Update(ctx, claim); err != nil {
		t.Fatalf("seeding partial standby marker: %v", err)
	}
	restarted := NewCoordinator(h.client, h.client, h.coordinator.clock)
	result, err := restarted.EnterStandby(ctx, h.enterStandbyRequest(), EnterStandbyOptions{BarrierTaintPresent: true, Since: standbySince})
	if err != nil {
		t.Fatalf("EnterStandby() error = %v", err)
	}
	if !StandbyEntrySucceeded(result) {
		t.Fatalf("EnterStandby() outcome = %q, want completed standby entry", result.Outcome)
	}
	h.assertStandbyComplete(t, standbySince)
}
