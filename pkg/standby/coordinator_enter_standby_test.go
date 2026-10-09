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
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

type patchHookClient struct {
	client.Client
	afterPatch func(context.Context, client.Object) error
}

func (c *patchHookClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if err := c.Client.Patch(ctx, object, patch, options...); err != nil {
		return err
	}
	if c.afterPatch != nil {
		return c.afterPatch(ctx, object)
	}
	return nil
}

type enterStandbyTestHarness struct {
	ctx         context.Context
	client      *patchHookClient
	coordinator *Coordinator
	node        *corev1.Node
	nodeClaim   *karpv1.NodeClaim
}

func newEnterStandbyTestHarness(t *testing.T) *enterStandbyTestHarness {
	t.Helper()
	ctx := context.Background()
	nodeClaim := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-claim",
		UID:  types.UID("standby-claim-uid"),
		Labels: map[string]string{
			karpv1.NodePoolLabelKey:        "standby-pool",
			karpv1.CapacityTypeLabelKey:    karpv1.CapacityTypeOnDemand,
			corev1.LabelInstanceTypeStable: "m5.large",
		},
	}}
	nodeClaim.Status.ProviderID = "provider-id"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-node",
		UID:  types.UID("standby-node-uid"),
		Labels: map[string]string{
			karpv1.NodePoolLabelKey:        "standby-pool",
			karpv1.NodeInitializedLabelKey: "true",
		},
	}, Spec: corev1.NodeSpec{ProviderID: nodeClaim.Status.ProviderID}}
	base := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(nodeClaim, node).
		WithIndex(&corev1.Pod{}, "spec.nodeName", podNodeNameIndex).
		Build()
	wrapped := &patchHookClient{Client: base}
	clk := clocktesting.NewFakeClock(time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC))
	coordinator := NewCoordinator(base, wrapped, clk)
	return &enterStandbyTestHarness{
		ctx: ctx, client: wrapped, coordinator: coordinator,
		node: node.DeepCopy(), nodeClaim: nodeClaim.DeepCopy(),
	}
}

func (h *enterStandbyTestHarness) request() EnterStandbyRequest {
	return EnterStandbyRequest{
		NodeRef:      h.node,
		NodeClaimRef: h.nodeClaim,
		ProviderID:   h.nodeClaim.Status.ProviderID,
	}
}

func (h *enterStandbyTestHarness) options() EnterStandbyOptions {
	return EnterStandbyOptions{
		Since: h.coordinator.clock.Now(),
		Eligibility: func(context.Context, *corev1.Node, *karpv1.NodeClaim) (bool, error) {
			return true, nil
		},
	}
}

func (h *enterStandbyTestHarness) createWorkload(ctx context.Context, name string, annotations map[string]string) error {
	return h.client.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: annotations},
		Spec:       corev1.PodSpec{NodeName: h.node.Name},
	})
}

func (h *enterStandbyTestHarness) assertNodeActive(t *testing.T) {
	t.Helper()
	node := &corev1.Node{}
	if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(h.node), node); err != nil {
		t.Fatalf("getting Node: %v", err)
	}
	claim := &karpv1.NodeClaim{}
	if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(h.nodeClaim), claim); err != nil {
		t.Fatalf("getting NodeClaim: %v", err)
	}
	assertNoStandbyAPIState(t, node, claim)
}

func (h *enterStandbyTestHarness) assertWorkloadPresent(t *testing.T, name string) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := h.client.Get(h.ctx, client.ObjectKey{Namespace: "default", Name: name}, pod); err != nil {
		t.Fatalf("getting workload Pod: %v", err)
	}
	if pod.Spec.NodeName != h.node.Name {
		t.Fatalf("workload bound to Node %q, want %q", pod.Spec.NodeName, h.node.Name)
	}
}

func assertNoStandbyAPIState(t *testing.T, node *corev1.Node, claim *karpv1.NodeClaim) {
	t.Helper()
	if utilstandby.IsNodeClaimStandby(claim) || utilstandby.IsNodeClaimActivating(claim) || utilstandby.HasNodeTaint(node) {
		t.Fatalf("active Node still has standby state: claimAnnotations=%v nodeTaints=%v", claim.Annotations, node.Spec.Taints)
	}
	if hasDisruptionNoScheduleTaint(node) {
		t.Fatal("late workload left the temporary disruption taint on the active node")
	}
}

func hasDisruptionNoScheduleTaint(node *corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.MatchTaint(&karpv1.DisruptedNoScheduleTaint) {
			return true
		}
	}
	return false
}

func podNodeNameIndex(object client.Object) []string {
	pod := object.(*corev1.Pod)
	if pod.Spec.NodeName == "" {
		return nil
	}
	return []string{pod.Spec.NodeName}
}

func TestEnterStandbyEmptyMarkingRecoversWhenWorkloadAppears(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		afterPatch func(context.Context, *enterStandbyTestHarness, client.Object) (bool, error)
	}{
		{
			name: "after disruption barrier taint",
			afterPatch: func(ctx context.Context, h *enterStandbyTestHarness, object client.Object) (bool, error) {
				node, ok := object.(*corev1.Node)
				if !ok || !hasDisruptionNoScheduleTaint(node) {
					return false, nil
				}
				return true, h.createWorkload(ctx, "late-workload", nil)
			},
		},
		{
			name: "after standby marker",
			afterPatch: func(ctx context.Context, h *enterStandbyTestHarness, object client.Object) (bool, error) {
				claim, ok := object.(*karpv1.NodeClaim)
				if !ok || !utilstandby.IsNodeClaimStandby(claim) {
					return false, nil
				}
				return true, h.createWorkload(ctx, "late-workload", nil)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newEnterStandbyTestHarness(t)
			injected := false
			h.client.afterPatch = func(ctx context.Context, object client.Object) error {
				if injected {
					return nil
				}
				didInject, err := tc.afterPatch(ctx, h, object)
				if err != nil {
					return err
				}
				if didInject {
					injected = true
				}
				return nil
			}
			result, err := h.coordinator.EnterStandby(ctx, h.request(), h.options())
			if err != nil {
				t.Fatalf("EnterStandby() error = %v", err)
			}
			if result.Outcome != OutcomeRecovered {
				t.Fatalf("EnterStandby() outcome = %q, want %q", result.Outcome, OutcomeRecovered)
			}
			h.assertNodeActive(t)
			h.assertWorkloadPresent(t, "late-workload")
		})
	}
}

func TestEnterStandbyEmptyMarkingDoesNotRetaintAfterConcurrentActivation(t *testing.T) {
	ctx := context.Background()
	h := newEnterStandbyTestHarness(t)
	h.client.afterPatch = func(ctx context.Context, object client.Object) error {
		claim, ok := object.(*karpv1.NodeClaim)
		if !ok || !utilstandby.IsNodeClaimStandby(claim) {
			return nil
		}
		node := &corev1.Node{}
		if err := h.client.Get(ctx, types.NamespacedName{Name: h.node.Name}, node); err != nil {
			return err
		}
		stateNode := &state.StateNode{Node: node, NodeClaim: claim.DeepCopy()}
		return h.coordinator.Activate(ctx, utilstandby.ActivationSourceRecovery, stateNode)
	}
	result, err := h.coordinator.EnterStandby(ctx, h.request(), h.options())
	if err != nil {
		t.Fatalf("EnterStandby() error = %v", err)
	}
	if result.Outcome != OutcomeSkipped && result.Outcome != OutcomeRecovered {
		t.Fatalf("EnterStandby() outcome = %q, want skipped or recovered active capacity", result.Outcome)
	}
	h.assertNodeActive(t)
}
