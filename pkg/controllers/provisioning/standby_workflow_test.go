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

package provisioning

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

type activationPatchHookClient struct {
	client.Client
	failNode string
}

func (c *activationPatchHookClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if node, ok := object.(*corev1.Node); ok && node.Name == c.failNode {
		return errors.New("injected activation failure")
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

func TestActivateScheduledStandbyNodesPreservesSuccessfulActivations(t *testing.T) {
	ctx := context.Background()
	clk := clocktesting.NewFakeClock(time.Date(2026, time.October, 12, 12, 0, 0, 0, time.UTC))
	since := clk.Now()
	claimA := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "claim-a", UID: types.UID("claim-a-uid"),
		Labels:      map[string]string{v1.NodePoolLabelKey: "pool"},
		Annotations: map[string]string{standby.NodeClaimAnnotationKey: since.Format(time.RFC3339Nano)},
	}, Status: v1.NodeClaimStatus{ProviderID: "provider-a"}}
	nodeA := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", UID: types.UID("node-a-uid")}, Spec: corev1.NodeSpec{ProviderID: "provider-a", Taints: []corev1.Taint{standby.NodeTaint()}}}
	claimB := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "claim-b", UID: types.UID("claim-b-uid"),
		Labels:      map[string]string{v1.NodePoolLabelKey: "pool"},
		Annotations: map[string]string{standby.NodeClaimAnnotationKey: since.Format(time.RFC3339Nano)},
	}, Status: v1.NodeClaimStatus{ProviderID: "provider-b"}}
	nodeB := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", UID: types.UID("node-b-uid")}, Spec: corev1.NodeSpec{ProviderID: "provider-b", Taints: []corev1.Taint{standby.NodeTaint()}}}
	base := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(claimA, nodeA, claimB, nodeB).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(object client.Object) []string {
			pod := object.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).Build()
	wrapped := &activationPatchHookClient{Client: base, failNode: "node-b"}
	cluster := state.NewCluster(clk, wrapped, nil)
	cluster.UpdateNodeClaim(claimA.DeepCopy())
	cluster.UpdateNodeClaim(claimB.DeepCopy())
	if err := cluster.UpdateNode(ctx, nodeA.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if err := cluster.UpdateNode(ctx, nodeB.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	coordinator := standbypkg.NewCoordinator(base, wrapped, clk)
	prov := &Provisioner{standbyCoordinator: coordinator, cluster: cluster, kubeClient: wrapped, clock: clk, recorder: events.NewRecorder(record.NewFakeRecorder(1))}
	stateA := &state.StateNode{Node: nodeA.DeepCopy(), NodeClaim: claimA.DeepCopy()}
	stateB := &state.StateNode{Node: nodeB.DeepCopy(), NodeClaim: claimB.DeepCopy()}

	_, err := prov.activateScheduledStandbyNodes(ctx, []*state.StateNode{stateA, stateB})
	if err == nil {
		t.Fatal("expected activation to fail on the second standby node")
	}
	storedA := &v1.NodeClaim{}
	if err := wrapped.Get(ctx, client.ObjectKeyFromObject(claimA), storedA); err != nil {
		t.Fatal(err)
	}
	if standby.IsNodeClaimStandby(storedA) {
		t.Fatal("first standby activation should remain committed in the API")
	}
	storedB := &v1.NodeClaim{}
	if err := wrapped.Get(ctx, client.ObjectKeyFromObject(claimB), storedB); err != nil {
		t.Fatal(err)
	}
	if !standby.IsNodeClaimStandby(storedB) {
		t.Fatal("failed activation should leave the second NodeClaim in standby for a later retry")
	}
}
