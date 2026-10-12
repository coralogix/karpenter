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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestStandbyTransitionPostEvacuationRepairsPartialMarkerWrite(t *testing.T) {
	ctx := context.Background()
	standbySince := time.Date(2026, time.October, 1, 9, 30, 0, 0, time.UTC)
	h := newPostEvacuationHarness(t, standbySince)
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

	result, err := h.coordinator.EnterStandby(ctx, h.request(), h.postEvacOptions(standbySince))
	if err != nil {
		t.Fatalf("EnterStandby() error = %v", err)
	}
	if !StandbyEntrySucceeded(result) {
		t.Fatalf("EnterStandby() outcome = %q, want completed standby entry after retry", result.Outcome)
	}
	h.assertStandbyComplete(t, standbySince)
}

func TestStandbyTransitionPostEvacuationPreservesStandbySinceAfterRestart(t *testing.T) {
	ctx := context.Background()
	standbySince := time.Date(2026, time.October, 2, 14, 0, 0, 0, time.UTC)
	h := newPostEvacuationHarness(t, standbySince)
	claim := h.readClaim(ctx)
	utilstandby.SetNodeClaimStandby(claim, true, standbySince)
	if err := h.client.Update(ctx, claim); err != nil {
		t.Fatalf("seeding partial standby marker: %v", err)
	}
	restarted := NewCoordinator(h.client, h.client, h.coordinator.clock)
	result, err := restarted.EnterStandby(ctx, h.request(), h.postEvacOptions(standbySince))
	if err != nil {
		t.Fatalf("EnterStandby() error = %v", err)
	}
	if !StandbyEntrySucceeded(result) {
		t.Fatalf("EnterStandby() outcome = %q, want completed standby entry", result.Outcome)
	}
	h.assertStandbyComplete(t, standbySince)
}

func TestStandbyTransitionPostEvacuationUnchangedWhenAlreadyComplete(t *testing.T) {
	ctx := context.Background()
	standbySince := time.Date(2026, time.October, 3, 8, 0, 0, 0, time.UTC)
	h := newPostEvacuationHarness(t, standbySince)
	claim := h.readClaim(ctx)
	utilstandby.SetNodeClaimStandby(claim, true, standbySince)
	if err := h.client.Update(ctx, claim); err != nil {
		t.Fatalf("marking NodeClaim standby: %v", err)
	}
	node := h.readNode(ctx)
	utilstandby.SetNodeTaint(node, true)
	if err := h.client.Update(ctx, node); err != nil {
		t.Fatalf("tainting Node: %v", err)
	}
	result, err := h.coordinator.EnterStandby(ctx, h.request(), h.postEvacOptions(standbySince))
	if err != nil {
		t.Fatalf("EnterStandby() error = %v", err)
	}
	if result.Outcome != OutcomeUnchanged || !StandbyEntryComplete(result.Node, result.NodeClaim) {
		t.Fatalf("EnterStandby() = %+v, want unchanged complete entry", result)
	}
}

type postEvacuationHarness struct {
	ctx         context.Context
	client      *patchHookClient
	coordinator *Coordinator
	node        *corev1.Node
	nodeClaim   *karpv1.NodeClaim
}

func newPostEvacuationHarness(t *testing.T, _ time.Time) *postEvacuationHarness {
	t.Helper()
	ctx := context.Background()
	nodeClaim := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "evac-claim",
		UID:  types.UID("evac-claim-uid"),
		Labels: map[string]string{
			karpv1.NodePoolLabelKey:        "standby-pool",
			karpv1.CapacityTypeLabelKey:    karpv1.CapacityTypeOnDemand,
			corev1.LabelInstanceTypeStable: "m5.large",
		},
	}}
	nodeClaim.Status.ProviderID = "provider-evac"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "evac-node",
		UID:    types.UID("evac-node-uid"),
		Labels: map[string]string{karpv1.NodePoolLabelKey: "standby-pool"},
	}, Spec: corev1.NodeSpec{ProviderID: nodeClaim.Status.ProviderID}}
	base := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(nodeClaim, node).Build()
	wrapped := &patchHookClient{Client: base}
	clk := clocktesting.NewFakeClock(time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC))
	return &postEvacuationHarness{
		ctx: ctx, client: wrapped, coordinator: NewCoordinator(base, wrapped, clk),
		node: node.DeepCopy(), nodeClaim: nodeClaim.DeepCopy(),
	}
}

func (h *postEvacuationHarness) request() EnterStandbyRequest {
	return EnterStandbyRequest{NodeRef: h.node, NodeClaimRef: h.nodeClaim, ProviderID: h.nodeClaim.Status.ProviderID}
}

func (h *postEvacuationHarness) postEvacOptions(since time.Time) EnterStandbyOptions {
	return EnterStandbyOptions{BarrierTaintPresent: true, Since: since}
}

func (h *postEvacuationHarness) readClaim(ctx context.Context) *karpv1.NodeClaim {
	claim := &karpv1.NodeClaim{}
	if err := h.client.Get(ctx, client.ObjectKeyFromObject(h.nodeClaim), claim); err != nil {
		panic(err)
	}
	return claim
}

func (h *postEvacuationHarness) readNode(ctx context.Context) *corev1.Node {
	node := &corev1.Node{}
	if err := h.client.Get(ctx, client.ObjectKeyFromObject(h.node), node); err != nil {
		panic(err)
	}
	return node
}

func (h *postEvacuationHarness) assertStandbyComplete(t *testing.T, since time.Time) {
	t.Helper()
	node := h.readNode(h.ctx)
	claim := h.readClaim(h.ctx)
	if !StandbyEntryComplete(node, claim) {
		t.Fatalf("incomplete standby state: nodeTaints=%v claim=%v", node.Spec.Taints, claim.Annotations)
	}
	gotSince, ok := utilstandby.NodeClaimStandbySince(claim)
	if !ok || !gotSince.Equal(since) {
		t.Fatalf("standby since = %v (ok=%v), want %v", gotSince, ok, since)
	}
}
