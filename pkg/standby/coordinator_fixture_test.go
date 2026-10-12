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
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

type coordinatorTestFixture struct {
	ctx         context.Context
	client      *patchHookClient
	coordinator *Coordinator
	node        *corev1.Node
	nodeClaim   *karpv1.NodeClaim
}

func newCoordinatorTestFixture(t *testing.T, nodeName, claimName string, clk time.Time) *coordinatorTestFixture {
	t.Helper()
	ctx := context.Background()
	nodeClaim := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: claimName,
		UID:  types.UID(claimName + "-uid"),
		Labels: map[string]string{
			karpv1.NodePoolLabelKey:        "standby-pool",
			karpv1.CapacityTypeLabelKey:    karpv1.CapacityTypeOnDemand,
			corev1.LabelInstanceTypeStable: "m5.large",
		},
	}}
	nodeClaim.Status.ProviderID = "provider://" + claimName
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: nodeName,
		UID:  types.UID(nodeName + "-uid"),
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
	fakeClock := clocktesting.NewFakeClock(clk)
	return &coordinatorTestFixture{
		ctx: ctx, client: wrapped, coordinator: NewCoordinator(base, wrapped, fakeClock),
		node: node.DeepCopy(), nodeClaim: nodeClaim.DeepCopy(),
	}
}

func (f *coordinatorTestFixture) enterStandbyRequest() EnterStandbyRequest {
	return EnterStandbyRequest{NodeRef: f.node, NodeClaimRef: f.nodeClaim, ProviderID: f.nodeClaim.Status.ProviderID}
}

func (f *coordinatorTestFixture) readClaim(ctx context.Context) *karpv1.NodeClaim {
	claim := &karpv1.NodeClaim{}
	if err := f.client.Get(ctx, client.ObjectKeyFromObject(f.nodeClaim), claim); err != nil {
		panic(err)
	}
	return claim
}

func (f *coordinatorTestFixture) readNode(ctx context.Context) *corev1.Node {
	node := &corev1.Node{}
	if err := f.client.Get(ctx, client.ObjectKeyFromObject(f.node), node); err != nil {
		panic(err)
	}
	return node
}

func (f *coordinatorTestFixture) assertStandbyComplete(t *testing.T, since time.Time) {
	t.Helper()
	node := f.readNode(f.ctx)
	claim := f.readClaim(f.ctx)
	if !StandbyEntryComplete(node, claim) {
		t.Fatalf("incomplete standby state: nodeTaints=%v claim=%v", node.Spec.Taints, claim.Annotations)
	}
	gotSince, ok := utilstandby.NodeClaimStandbySince(claim)
	if !ok || !gotSince.Equal(since) {
		t.Fatalf("standby since = %v (ok=%v), want %v", gotSince, ok, since)
	}
}
