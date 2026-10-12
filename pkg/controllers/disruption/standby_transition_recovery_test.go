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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestStandbyTransitionRecoveryEvacuationRepairsBeforeBarrierRelease(t *testing.T) {
	ctx := context.Background()
	standbySince := time.Date(2026, time.October, 5, 10, 0, 0, 0, time.UTC)
	clk := clocktesting.NewFakeClock(standbySince)
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "evac-claim",
		UID:  types.UID("evac-claim-uid"),
		Labels: map[string]string{
			v1.NodePoolLabelKey:            "standby-pool",
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			corev1.LabelInstanceTypeStable: "m5.large",
		},
	}, Status: v1.NodeClaimStatus{ProviderID: "provider-evac"}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "evac-node",
		UID:  types.UID("evac-node-uid"),
		Labels: map[string]string{
			v1.NodePoolLabelKey:        "standby-pool",
			v1.NodeInitializedLabelKey: "true",
		},
	}, Spec: corev1.NodeSpec{
		ProviderID: nodeClaim.Status.ProviderID,
		Taints:     []corev1.Taint{v1.DisruptedNoScheduleTaint},
	}}
	base := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(nodeClaim, node).Build()
	wrapped := &standbyTransitionClient{Client: base}
	blockTaintWrites := true
	wrapped.beforeNodePatch = func(node *corev1.Node) error {
		if !blockTaintWrites {
			return nil
		}
		claim := &v1.NodeClaim{}
		if err := wrapped.Get(ctx, client.ObjectKeyFromObject(nodeClaim), claim); err != nil {
			return err
		}
		if standby.IsNodeClaimStandby(claim) && standby.HasNodeTaint(node) {
			return errors.New("injected transient node taint failure")
		}
		return nil
	}
	coordinator := standbypkg.NewCoordinator(base, wrapped, clk)
	candidate := &Candidate{StateNode: &state.StateNode{Node: node.DeepCopy(), NodeClaim: nodeClaim.DeepCopy()}}
	queue := &Queue{standbyCoordinator: coordinator, clock: clk, kubeClient: wrapped, apiReader: base}

	if err := queue.enterStandbyAfterEvacuation(ctx, candidate); err == nil {
		t.Fatal("enterStandbyAfterEvacuation() should fail while standby entry is incomplete")
	}
	blockTaintWrites = false
	if err := queue.enterStandbyAfterEvacuation(ctx, candidate); err != nil {
		t.Fatalf("retry enterStandbyAfterEvacuation() error = %v", err)
	}
	liveNode := &corev1.Node{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(node), liveNode); err != nil {
		t.Fatalf("getting Node: %v", err)
	}
	liveClaim := &v1.NodeClaim{}
	if err := base.Get(ctx, client.ObjectKeyFromObject(nodeClaim), liveClaim); err != nil {
		t.Fatalf("getting NodeClaim: %v", err)
	}
	if !standbypkg.StandbyEntryComplete(liveNode, liveClaim) {
		t.Fatalf("evacuation standby entry incomplete: node=%v claim=%v", liveNode.Spec.Taints, liveClaim.Annotations)
	}
}
