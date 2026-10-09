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

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

func newStandbyActivationFixture(t *testing.T, nodeName string) *coordinatorTestFixture {
	t.Helper()
	return newCoordinatorTestFixture(t, nodeName, nodeName+"-claim", func(node *corev1.Node, claim *karpv1.NodeClaim) {
		utilstandby.SetNodeClaimStandby(claim, true, time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC))
		utilstandby.SetNodeTaint(node, true)
	})
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
			h := newStandbyActivationFixture(t, "pre-"+strings.ToLower(tc.name))
			stateNode := &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()}
			if err := tc.replace(ctx, h.client, h.node, h.nodeClaim); err != nil {
				t.Fatalf("replacing object: %v", err)
			}
			liveNode, liveClaim := readActivationObjects(ctx, h.client, h.node.Name, h.nodeClaim.Name)
			beforeNode, beforeClaim := liveNode.DeepCopy(), liveClaim.DeepCopy()

			_, err := h.coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
			if err == nil {
				t.Fatal("expected activation to fail for a replaced object")
			}
			afterNode, afterClaim := readActivationObjects(ctx, h.client, h.node.Name, h.nodeClaim.Name)
			require.Equal(t, beforeNode, afterNode)
			require.Equal(t, beforeClaim, afterClaim)
		})
	}
}

func TestActivateRejectsProviderIDMismatchBeforeMutation(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name                      string
		mutateNodeProviderID      bool
		mutateNodeClaimProviderID bool
		mutateSnapshotProviderID  bool
	}{
		{name: "Node provider ID", mutateNodeProviderID: true},
		{name: "NodeClaim provider ID", mutateNodeClaimProviderID: true},
		{name: "requested provider ID", mutateSnapshotProviderID: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newStandbyActivationFixture(t, "provider-mismatch")
			stateNode := &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()}
			liveNode, liveClaim := readActivationObjects(ctx, h.client, h.node.Name, h.nodeClaim.Name)
			if tc.mutateNodeProviderID {
				liveNode.Spec.ProviderID = "provider://other"
				require.NoError(t, h.client.Update(ctx, liveNode))
			}
			if tc.mutateNodeClaimProviderID {
				liveClaim.Status.ProviderID = "provider://other"
				require.NoError(t, h.client.Status().Update(ctx, liveClaim))
			}
			if tc.mutateSnapshotProviderID {
				stateNode.Node.Spec.ProviderID = "provider://other"
			}
			beforeNode, beforeClaim := liveNode.DeepCopy(), liveClaim.DeepCopy()

			_, err := h.coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
			require.Error(t, err)
			afterNode, afterClaim := readActivationObjects(ctx, h.client, h.node.Name, h.nodeClaim.Name)
			require.Equal(t, beforeNode, afterNode)
			require.Equal(t, beforeClaim, afterClaim)
		})
	}
}

func TestActivateRejectsReplacementAfterTaintRemoval(t *testing.T) {
	ctx := context.Background()
	h := newStandbyActivationFixture(t, "mid-replace")
	stateNode := &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()}
	swapped := false
	var replacementBefore *corev1.Node
	h.client.afterPatch = func(patchCtx context.Context, object client.Object) error {
		patchedNode, ok := object.(*corev1.Node)
		if !ok || swapped || utilstandby.HasNodeTaint(patchedNode) {
			return nil
		}
		swapped = true
		if err := replaceNodeSameName(patchCtx, h.client, h.node, types.UID("replacement-node-uid")); err != nil {
			return err
		}
		replacementNode, _ := readActivationObjects(patchCtx, h.client, h.node.Name, h.nodeClaim.Name)
		replacementBefore = replacementNode.DeepCopy()
		return nil
	}

	_, err := h.coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
	if err == nil {
		t.Fatal("expected activation to fail after the node was replaced mid-protocol")
	}
	replacementNode, _ := readActivationObjects(ctx, h.client, h.node.Name, h.nodeClaim.Name)
	if replacementNode.UID == h.node.UID {
		t.Fatal("expected the live node to be a same-name replacement")
	}
	require.Equal(t, replacementBefore, replacementNode)
}

func TestActivateRetryAfterConflictPreservesExpectedIdentity(t *testing.T) {
	ctx := context.Background()
	h := newStandbyActivationFixture(t, "conflict-replace")
	stateNode := &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()}
	conflicts := 0
	swapped := false
	var replacementNodeBefore *corev1.Node
	var replacementClaimBefore *karpv1.NodeClaim
	h.client.beforeNodePatch = func(patchedNode *corev1.Node) error {
		if !utilstandby.HasNodeActivationMarker(patchedNode) || utilstandby.HasNodeTaint(patchedNode) {
			return nil
		}
		conflicts++
		if conflicts == 1 {
			return apierrors.NewConflict(corev1.Resource("nodes"), patchedNode.Name, nil)
		}
		if !swapped {
			swapped = true
			if err := replaceSameName(ctx, h.client, h.node, h.nodeClaim, types.UID("replacement-node-uid"), h.nodeClaim.UID); err != nil {
				t.Fatalf("replacing node after conflict: %v", err)
			}
			replacementNode, replacementClaim := readActivationObjects(ctx, h.client, h.node.Name, h.nodeClaim.Name)
			replacementNodeBefore = replacementNode.DeepCopy()
			replacementClaimBefore = replacementClaim.DeepCopy()
		}
		return nil
	}

	_, err := h.coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
	if err == nil {
		t.Fatal("expected activation to fail once the replacement is observed on retry")
	}
	if conflicts < 1 {
		t.Fatal("expected at least one optimistic-lock conflict before identity rejection")
	}
	afterNode, afterClaim := readActivationObjects(ctx, h.client, h.node.Name, h.nodeClaim.Name)
	require.Equal(t, replacementNodeBefore, afterNode)
	require.Equal(t, replacementClaimBefore, afterClaim)
}

func TestActivateRetriesSameObjectAfterConflict(t *testing.T) {
	ctx := context.Background()
	h := newStandbyActivationFixture(t, "conflict-retry")
	stateNode := &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()}
	conflicts := 0
	h.client.beforeNodePatch = func(node *corev1.Node) error {
		if conflicts > 0 || !utilstandby.HasNodeTaint(node) {
			return nil
		}
		conflicts++
		return apierrors.NewConflict(corev1.Resource("nodes"), node.Name, nil)
	}

	result, err := h.coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
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
