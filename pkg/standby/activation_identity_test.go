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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

type activationSurfaceState struct {
	claimAnnotations map[string]string
	nodeAnnotations  map[string]string
	nodeTaints       []corev1.Taint
}

func captureActivationSurface(node *corev1.Node, claim *karpv1.NodeClaim) activationSurfaceState {
	return activationSurfaceState{
		claimAnnotations: copyStringMap(claim.Annotations),
		nodeAnnotations:  copyStringMap(node.Annotations),
		nodeTaints:       append([]corev1.Taint(nil), node.Spec.Taints...),
	}
}

func captureNodeSurface(node *corev1.Node) activationSurfaceState {
	return activationSurfaceState{
		nodeAnnotations: copyStringMap(node.Annotations),
		nodeTaints:      append([]corev1.Taint(nil), node.Spec.Taints...),
	}
}

func activationSurfaceEqual(a, b activationSurfaceState) bool {
	return mapsEqual(a.claimAnnotations, b.claimAnnotations) &&
		mapsEqual(a.nodeAnnotations, b.nodeAnnotations) &&
		taintsEqual(a.nodeTaints, b.nodeTaints)
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := map[string]string{}
	for key, value := range in {
		out[key] = value
	}
	return out
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func taintsEqual(a, b []corev1.Taint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || a[i].Value != b[i].Value || a[i].Effect != b[i].Effect {
			return false
		}
	}
	return true
}

func newStandbyActivationFixture(t *testing.T, nodeName string) (*patchHookClient, *Coordinator, *state.StateNode, *corev1.Node, *karpv1.NodeClaim) {
	t.Helper()
	ctx := context.Background()
	providerID := "provider://" + nodeName
	claim := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: nodeName + "-claim",
		UID:  types.UID(nodeName + "-claim-uid"),
		Labels: map[string]string{
			karpv1.NodePoolLabelKey:        "standby-pool",
			corev1.LabelInstanceTypeStable: "m5.large",
		},
	}}
	utilstandby.SetNodeClaimStandby(claim, true, time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC))
	claim.Status.ProviderID = providerID
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: nodeName,
		UID:  types.UID(nodeName + "-node-uid"),
		Labels: map[string]string{
			karpv1.NodePoolLabelKey: "standby-pool",
		},
	}, Spec: corev1.NodeSpec{ProviderID: providerID, Taints: []corev1.Taint{utilstandby.NodeTaint()}}}
	base := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(claim, node).Build()
	wrapped := &patchHookClient{Client: base}
	coordinator := NewCoordinator(base, wrapped, clocktesting.NewFakeClock(time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC)))
	stateNode := &state.StateNode{Node: node.DeepCopy(), NodeClaim: claim.DeepCopy()}
	_ = ctx
	return wrapped, coordinator, stateNode, node, claim
}

func replaceNodeSameName(ctx context.Context, kubeClient client.Client, node *corev1.Node, newNodeUID types.UID) error {
	if err := kubeClient.Delete(ctx, node); err != nil {
		return err
	}
	replacementNode := node.DeepCopy()
	replacementNode.UID = newNodeUID
	replacementNode.ResourceVersion = ""
	replacementNode.Spec.Taints = []corev1.Taint{utilstandby.NodeTaint()}
	delete(replacementNode.Annotations, utilstandby.NodeClaimActivatingAnnotationKey)
	return kubeClient.Create(ctx, replacementNode)
}

func replaceActiveNodeSameName(ctx context.Context, kubeClient client.Client, node *corev1.Node, newNodeUID types.UID) error {
	if err := kubeClient.Delete(ctx, node); err != nil {
		return err
	}
	replacementNode := node.DeepCopy()
	replacementNode.UID = newNodeUID
	replacementNode.ResourceVersion = ""
	replacementNode.Spec.Taints = nil
	delete(replacementNode.Annotations, utilstandby.NodeClaimActivatingAnnotationKey)
	return kubeClient.Create(ctx, replacementNode)
}

func replaceSameName(ctx context.Context, kubeClient client.Client, node *corev1.Node, claim *karpv1.NodeClaim, newNodeUID, newClaimUID types.UID) error {
	if err := kubeClient.Delete(ctx, claim); err != nil {
		return err
	}
	if err := kubeClient.Delete(ctx, node); err != nil {
		return err
	}
	replacementClaim := claim.DeepCopy()
	replacementClaim.UID = newClaimUID
	replacementClaim.ResourceVersion = ""
	utilstandby.SetNodeClaimStandby(replacementClaim, true, time.Date(2026, time.October, 12, 13, 0, 0, 0, time.UTC))
	if err := kubeClient.Create(ctx, replacementClaim); err != nil {
		return err
	}
	replacementNode := node.DeepCopy()
	replacementNode.UID = newNodeUID
	replacementNode.ResourceVersion = ""
	replacementNode.Spec.Taints = []corev1.Taint{utilstandby.NodeTaint()}
	delete(replacementNode.Annotations, utilstandby.NodeClaimActivatingAnnotationKey)
	if err := kubeClient.Create(ctx, replacementNode); err != nil {
		return err
	}
	return nil
}

func readActivationObjects(ctx context.Context, kubeClient client.Client, nodeName, claimName string) (*corev1.Node, *karpv1.NodeClaim) {
	node := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		panic(err)
	}
	claim := &karpv1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: claimName}, claim); err != nil {
		panic(err)
	}
	return node, claim
}

func TestActivateRejectsReplacedObjectsBeforeMutation(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		replace func(context.Context, client.Client, *corev1.Node, *karpv1.NodeClaim) error
	}{
		{
			name: "Node",
			replace: func(ctx context.Context, kubeClient client.Client, node *corev1.Node, claim *karpv1.NodeClaim) error {
				return replaceSameName(ctx, kubeClient, node, claim, types.UID("replacement-node-uid"), claim.UID)
			},
		},
		{
			name: "NodeClaim",
			replace: func(ctx context.Context, kubeClient client.Client, node *corev1.Node, claim *karpv1.NodeClaim) error {
				return replaceSameName(ctx, kubeClient, node, claim, node.UID, types.UID("replacement-claim-uid"))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kubeClient, coordinator, stateNode, node, claim := newStandbyActivationFixture(t, "pre-"+strings.ToLower(tc.name))
			if err := tc.replace(ctx, kubeClient, node, claim); err != nil {
				t.Fatalf("replacing object: %v", err)
			}
			liveNode, liveClaim := readActivationObjects(ctx, kubeClient, node.Name, claim.Name)
			before := captureActivationSurface(liveNode, liveClaim)

			_, err := coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
			if err == nil {
				t.Fatal("expected activation to fail for a replaced object")
			}
			afterNode, afterClaim := readActivationObjects(ctx, kubeClient, node.Name, claim.Name)
			after := captureActivationSurface(afterNode, afterClaim)
			if !activationSurfaceEqual(before, after) {
				t.Fatalf("replacement object changed during failed activation:\nbefore=%+v\nafter=%+v", before, after)
			}
		})
	}
}

func TestActivateRejectsProviderIDMismatchBeforeMutation(t *testing.T) {
	ctx := context.Background()
	kubeClient, coordinator, stateNode, node, claim := newStandbyActivationFixture(t, "provider-mismatch")
	liveNode, liveClaim := readActivationObjects(ctx, kubeClient, node.Name, claim.Name)
	liveNode.Spec.ProviderID = "provider://other"
	if err := kubeClient.Update(ctx, liveNode); err != nil {
		t.Fatalf("updating node provider ID: %v", err)
	}
	before := captureActivationSurface(liveNode, liveClaim)

	_, err := coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
	if err == nil {
		t.Fatal("expected activation to fail for provider ID mismatch")
	}
	afterNode, afterClaim := readActivationObjects(ctx, kubeClient, node.Name, claim.Name)
	if !activationSurfaceEqual(before, captureActivationSurface(afterNode, afterClaim)) {
		t.Fatal("live objects changed during provider ID mismatch rejection")
	}
}

func TestActivateRejectsReplacementAfterTaintRemoval(t *testing.T) {
	ctx := context.Background()
	kubeClient, coordinator, stateNode, node, claim := newStandbyActivationFixture(t, "mid-replace")
	swapped := false
	var replacementBefore activationSurfaceState
	kubeClient.afterPatch = func(patchCtx context.Context, object client.Object) error {
		patchedNode, ok := object.(*corev1.Node)
		if !ok || swapped || utilstandby.HasNodeTaint(patchedNode) {
			return nil
		}
		swapped = true
		if err := replaceNodeSameName(patchCtx, kubeClient, node, types.UID("replacement-node-uid")); err != nil {
			return err
		}
		replacementNode, _ := readActivationObjects(patchCtx, kubeClient, node.Name, claim.Name)
		replacementBefore = captureNodeSurface(replacementNode)
		return nil
	}

	_, err := coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
	if err == nil {
		t.Fatal("expected activation to fail after the node was replaced mid-protocol")
	}
	replacementNode, _ := readActivationObjects(ctx, kubeClient, node.Name, claim.Name)
	if replacementNode.UID == node.UID {
		t.Fatal("expected the live node to be a same-name replacement")
	}
	if !activationSurfaceEqual(replacementBefore, captureNodeSurface(replacementNode)) {
		t.Fatal("replacement node was mutated after mid-protocol identity rejection")
	}
}

func TestActivateRetryAfterConflictPreservesExpectedIdentity(t *testing.T) {
	ctx := context.Background()
	kubeClient, coordinator, stateNode, node, claim := newStandbyActivationFixture(t, "conflict-replace")
	conflicts := 0
	swapped := false
	var replacementBefore activationSurfaceState
	kubeClient.beforeNodePatch = func(patchedNode *corev1.Node) error {
		if !utilstandby.HasNodeActivationMarker(patchedNode) || utilstandby.HasNodeTaint(patchedNode) {
			return nil
		}
		conflicts++
		if conflicts == 1 {
			return apierrors.NewConflict(corev1.Resource("nodes"), patchedNode.Name, nil)
		}
		if !swapped {
			swapped = true
			if err := replaceSameName(ctx, kubeClient, node, claim, types.UID("replacement-node-uid"), claim.UID); err != nil {
				t.Fatalf("replacing node after conflict: %v", err)
			}
			replacementNode, replacementClaim := readActivationObjects(ctx, kubeClient, node.Name, claim.Name)
			replacementBefore = captureActivationSurface(replacementNode, replacementClaim)
		}
		return nil
	}

	_, err := coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
	if err == nil {
		t.Fatal("expected activation to fail once the replacement is observed on retry")
	}
	if conflicts < 1 {
		t.Fatal("expected at least one optimistic-lock conflict before identity rejection")
	}
	afterNode, afterClaim := readActivationObjects(ctx, kubeClient, node.Name, claim.Name)
	if !activationSurfaceEqual(replacementBefore, captureActivationSurface(afterNode, afterClaim)) {
		t.Fatal("replacement object changed after conflict retry with identity rejection")
	}
}

func TestActivateRetriesSameObjectAfterConflict(t *testing.T) {
	ctx := context.Background()
	kubeClient, coordinator, stateNode, _, _ := newStandbyActivationFixture(t, "conflict-retry")
	conflicts := 0
	kubeClient.beforeNodePatch = func(node *corev1.Node) error {
		if conflicts > 0 || !utilstandby.HasNodeTaint(node) {
			return nil
		}
		conflicts++
		return apierrors.NewConflict(corev1.Resource("nodes"), node.Name, nil)
	}

	result, err := coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
	if err != nil {
		t.Fatalf("activating after retriable conflict: %v", err)
	}
	if len(result.Activated) != 1 || !ActivationComplete(result.Activated[0].Node, result.Activated[0].NodeClaim) {
		t.Fatalf("unexpected activation result: %+v", result)
	}
	if conflicts != 1 {
		t.Fatalf("conflict count = %d, want 1", conflicts)
	}
}
