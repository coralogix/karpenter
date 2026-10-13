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

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"

	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func TestActivateStandbyNode(t *testing.T) {
	ctx := context.Background()
	h := newCoordinatorTestFixture(t, "standby-node", "standby-node-claim", func(node *corev1.Node, nodeClaim *karpv1.NodeClaim) {
		nodeClaim.Labels[karpv1.NodePoolLabelKey] = "standby-observability-test"
		nodeClaim.Labels[corev1.LabelInstanceTypeStable] = "m6i.large"
		utilstandby.SetNodeClaimStandby(nodeClaim, true, time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC))
		utilstandby.SetNodeTaint(node, true)
	})
	stateNode := &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()}

	actResult, err := h.coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode)
	require.NoError(t, err)
	require.Len(t, actResult.Activated, 1)
	pair := actResult.Activated[0]
	require.Equal(t, h.nodeClaim.Status.ProviderID, pair.ProviderID)
	require.Equal(t, h.nodeClaim.Name, pair.NodeClaim.Name)
	require.True(t, ActivationComplete(pair.Node, pair.NodeClaim))

	assertActivatedStandbyNode(t, ctx, h.client, h.node, h.nodeClaim)
	if got := standbyActivationCount(t, "standby-observability-test", "m6i.large", string(utilstandby.ActivationSourceProvisioning)); got != 1 {
		t.Fatalf("expected one standby activation, got %v", got)
	}
	if _, err := h.coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateNode); err != nil {
		t.Fatalf("retrying activation: %v", err)
	}
	if got := standbyActivationCount(t, "standby-observability-test", "m6i.large", string(utilstandby.ActivationSourceProvisioning)); got != 1 {
		t.Fatalf("expected idempotent retry not to increment activations, got %v", got)
	}
}

func assertActivatedStandbyNode(t *testing.T, ctx context.Context, kubeClient client.Client, node *corev1.Node, nodeClaim *karpv1.NodeClaim) {
	t.Helper()
	storedNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: node.Name}, storedNode); err != nil {
		t.Fatalf("getting activated node: %v", err)
	}
	if utilstandby.HasNodeTaint(storedNode) {
		t.Fatal("standby taint remained after activation")
	}
	storedNodeClaim := &karpv1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), storedNodeClaim); err != nil {
		t.Fatalf("getting activated nodeclaim: %v", err)
	}
	if utilstandby.IsNodeClaimStandby(storedNodeClaim) || utilstandby.IsNodeClaimActivating(storedNodeClaim) {
		t.Fatal("standby or activation marker remained after activation")
	}
}

func TestActivatePartialSuccessSurvivesLaterFailure(t *testing.T) {
	ctx := context.Background()
	since := time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC)
	claimA := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-a", UID: types.UID("claim-a-uid"), Labels: map[string]string{karpv1.NodePoolLabelKey: "pool"}}}
	claimB := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-b", UID: types.UID("claim-b-uid"), Labels: map[string]string{karpv1.NodePoolLabelKey: "pool"}}}
	utilstandby.SetNodeClaimStandby(claimA, true, since)
	utilstandby.SetNodeClaimStandby(claimB, true, since)
	claimA.Status.ProviderID = "provider-a"
	claimB.Status.ProviderID = "provider-b"
	nodeA := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: types.UID("node-a-uid")}, Spec: corev1.NodeSpec{ProviderID: "provider-a", Taints: []corev1.Taint{utilstandby.NodeTaint()}}}
	nodeB := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", UID: types.UID("node-b-uid")}, Spec: corev1.NodeSpec{ProviderID: "provider-b", Taints: []corev1.Taint{utilstandby.NodeTaint()}}}
	base := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(claimA, nodeA, claimB, nodeB).Build()
	wrapped := &patchHookClient{Client: base}
	failSecond := true
	wrapped.beforeNodePatch = func(node *corev1.Node) error {
		if node.Name == nodeB.Name && failSecond {
			failSecond = false
			return errors.New("injected activation failure")
		}
		return nil
	}
	coordinator := NewCoordinator(base, wrapped)
	stateA := &state.StateNode{Node: nodeA.DeepCopy(), NodeClaim: claimA.DeepCopy()}
	stateB := &state.StateNode{Node: nodeB.DeepCopy(), NodeClaim: claimB.DeepCopy()}
	actResult, err := coordinator.Activate(ctx, utilstandby.ActivationSourceProvisioning, stateA, stateB)
	if err == nil {
		t.Fatal("expected activation to fail after partial success")
	}
	if len(actResult.Activated) != 1 || actResult.Activated[0].NodeClaim.Name != claimA.Name {
		t.Fatalf("partial activation result = %+v, want only the first node activated", actResult)
	}
	storedA := &karpv1.NodeClaim{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(claimA), storedA); err != nil {
		t.Fatalf("getting first NodeClaim: %v", err)
	}
	if utilstandby.IsNodeClaimStandby(storedA) {
		t.Fatal("successful activation should clear standby markers on the first NodeClaim")
	}
	storedB := &karpv1.NodeClaim{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(claimB), storedB); err != nil {
		t.Fatalf("getting second NodeClaim: %v", err)
	}
	if !utilstandby.IsNodeClaimStandby(storedB) {
		t.Fatal("failed activation should leave the second NodeClaim in standby")
	}
}

func TestActivateRecordsStandbyMetricsForRecoveryPaths(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name         string
		nodeName     string
		nodePool     string
		annotations  map[string]string
		activateWith utilstandby.ActivationSource
		wantSource   string
	}{
		{
			name:     "preserves compaction source",
			nodeName: "standby-recovery-node",
			nodePool: "standby-recovery-observability-test",
			annotations: map[string]string{
				utilstandby.NodeClaimAnnotationKey:                 "true",
				utilstandby.NodeClaimActivatingAnnotationKey:       "true",
				utilstandby.NodeClaimActivationSourceAnnotationKey: string(utilstandby.ActivationSourceCompaction),
			},
			activateWith: utilstandby.ActivationSourceRecovery,
			wantSource:   string(utilstandby.ActivationSourceCompaction),
		},
		{
			name:     "legacy activation without source",
			nodeName: "standby-legacy-recovery-node",
			nodePool: "standby-legacy-observability-test",
			annotations: map[string]string{
				utilstandby.NodeClaimAnnotationKey:           "true",
				utilstandby.NodeClaimActivatingAnnotationKey: "true",
			},
			activateWith: utilstandby.ActivationSourceRecovery,
			wantSource:   string(utilstandby.ActivationSourceRecovery),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCoordinatorTestFixture(t, tc.nodeName, tc.nodeName+"-claim", func(node *corev1.Node, nodeClaim *karpv1.NodeClaim) {
				nodeClaim.Labels[karpv1.NodePoolLabelKey] = tc.nodePool
				delete(nodeClaim.Labels, corev1.LabelInstanceTypeStable)
				nodeClaim.Annotations = tc.annotations
				utilstandby.SetNodeTaint(node, true)
			})
			stateNode := &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()}
			if _, err := h.coordinator.Activate(ctx, tc.activateWith, stateNode); err != nil {
				t.Fatalf("activating standby node: %v", err)
			}
			if got := standbyActivationCount(t, tc.nodePool, "unknown", tc.wantSource); got != 1 {
				t.Fatalf("expected one activation with source %q, got %v", tc.wantSource, got)
			}
		})
	}
}

func TestRepairStandbyNodeResumesEmptyInterruptedActivation(t *testing.T) {
	ctx := context.Background()
	h := newCoordinatorTestFixture(t, "standby-node", "standby-node-claim", func(node *corev1.Node, nodeClaim *karpv1.NodeClaim) {
		node.Annotations = map[string]string{utilstandby.NodeClaimActivatingAnnotationKey: "true"}
		nodeClaim.Labels[corev1.LabelInstanceTypeStable] = "m6i.large"
		nodeClaim.Annotations = map[string]string{
			utilstandby.NodeClaimAnnotationKey:           "true",
			utilstandby.NodeClaimActivatingAnnotationKey: "true",
		}
		utilstandby.SetNodeTaint(node, true)
	})
	result, err := h.coordinator.RepairStandbyNode(ctx, RepairRequest{
		NodeRef:      h.node,
		NodeClaimRef: h.nodeClaim,
		ProviderID:   h.nodeClaim.Status.ProviderID,
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeRecovered, result.Outcome)
	assertActivatedStandbyNode(t, ctx, h.client, h.node, h.nodeClaim)
}

func TestRepairStandbyNodeRestoresMissingStandbyTaint(t *testing.T) {
	ctx := context.Background()
	h := newCoordinatorTestFixture(t, "standby-node", "standby-node-claim", func(_ *corev1.Node, nodeClaim *karpv1.NodeClaim) {
		nodeClaim.Annotations = map[string]string{utilstandby.NodeClaimAnnotationKey: "true"}
	})
	result, err := h.coordinator.RepairStandbyNode(ctx, RepairRequest{
		NodeRef:      h.node,
		NodeClaimRef: h.nodeClaim,
		ProviderID:   h.nodeClaim.Status.ProviderID,
	})
	require.NoError(t, err)
	require.Equal(t, OutcomeRecovered, result.Outcome)
	storedNode := &corev1.Node{}
	require.NoError(t, h.client.Get(ctx, client.ObjectKeyFromObject(h.node), storedNode))
	require.True(t, utilstandby.HasNodeTaint(storedNode))
}

func standbyActivationCount(t *testing.T, nodePool, instanceType, source string) float64 {
	t.Helper()
	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != "karpenter_nodes_standby_activated_total" {
			continue
		}
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["nodepool"] == nodePool && labels["instance_type"] == instanceType && labels["source"] == source {
				return metric.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("standby activation metric for nodepool %q, instance type %q, source %q not found", nodePool, instanceType, source)
	return 0
}
