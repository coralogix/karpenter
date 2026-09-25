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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/utils/standby"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNodeControllerRequeuesStandbyActivation(t *testing.T) {
	ctx := context.Background()
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:        "standby-node",
		UID:         types.UID("nodeclaim-uid"),
		Annotations: map[string]string{standby.NodeClaimActivatingAnnotationKey: "true"},
	}}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "standby-node",
			Annotations: map[string]string{standby.NodeClaimActivatingAnnotationKey: "true"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: apis.Group + "/v1",
				Kind:       "NodeClaim",
				Name:       nodeClaim.Name,
				UID:        types.UID("nodeclaim-uid"),
			}},
		},
		// Simulate a restart after untainting the Node but before clearing the
		// Node and NodeClaim activation markers.
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(node, nodeClaim).Build()
	provisioner := &Provisioner{batcher: NewBatcher[types.UID](clocktesting.NewFakeClock(time.Now()))}
	controller := NewNodeController(kubeClient, provisioner)

	result, err := controller.Reconcile(ctx, node)
	if err != nil {
		t.Fatalf("reconciling activating node: %v", err)
	}
	if result.RequeueAfter != 10*time.Second {
		t.Fatalf("requeue delay = %s, want 10s", result.RequeueAfter)
	}

	storedNodeClaim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), storedNodeClaim); err != nil {
		t.Fatalf("getting nodeclaim: %v", err)
	}
	standby.SetNodeClaimActivating(storedNodeClaim, false)
	if err := kubeClient.Update(ctx, storedNodeClaim); err != nil {
		t.Fatalf("clearing activation marker: %v", err)
	}
	storedNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(node), storedNode); err != nil {
		t.Fatalf("getting node: %v", err)
	}
	delete(storedNode.Annotations, standby.NodeClaimActivatingAnnotationKey)
	if err := kubeClient.Update(ctx, storedNode); err != nil {
		t.Fatalf("clearing node activation marker: %v", err)
	}
	result, err = controller.Reconcile(ctx, storedNode)
	if err != nil {
		t.Fatalf("reconciling completed activation: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("completed activation requeue delay = %s, want zero", result.RequeueAfter)
	}
}

func TestNodeControllerFindsClaimMarkerBeforeNodeMarker(t *testing.T) {
	ctx := context.Background()
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:        "standby-node",
		UID:         types.UID("nodeclaim-uid"),
		Annotations: map[string]string{standby.NodeClaimActivatingAnnotationKey: "true"},
	}}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "standby-node",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: apis.Group + "/v1",
				Kind:       "NodeClaim",
				Name:       nodeClaim.Name,
				UID:        nodeClaim.UID,
			}},
		},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{standby.NodeTaint()}},
	}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(node, nodeClaim).Build()
	provisioner := &Provisioner{batcher: NewBatcher[types.UID](clocktesting.NewFakeClock(time.Now()))}
	controller := NewNodeController(kubeClient, provisioner)

	result, err := controller.Reconcile(ctx, node)
	if err != nil {
		t.Fatalf("reconciling standby node: %v", err)
	}
	if result.RequeueAfter != 10*time.Second {
		t.Fatalf("requeue delay = %s, want 10s", result.RequeueAfter)
	}
}
