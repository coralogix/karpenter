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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestLifecycleReadLivePairRejectsStaleIdentity(t *testing.T) {
	ctx := context.Background()
	currentNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: types.UID("current-node")}, Spec: corev1.NodeSpec{ProviderID: "provider"}}
	currentClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim", UID: types.UID("current-claim")}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(currentNode, currentClaim).Build()
	lifecycle := NewLifecycle(kubeClient, kubeClient)

	tests := []struct {
		name       string
		nodeRef    *corev1.Node
		claimRef   *v1.NodeClaim
		providerID string
	}{
		{name: "node UID", nodeRef: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: currentNode.Name, UID: "old-node"}}, claimRef: currentClaim, providerID: "provider"},
		{name: "NodeClaim UID", nodeRef: currentNode, claimRef: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: currentClaim.Name, UID: "old-claim"}}, providerID: "provider"},
		{name: "provider ID", nodeRef: currentNode, claimRef: currentClaim, providerID: "old-provider"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, matches, err := lifecycle.ReadLivePair(ctx, test.nodeRef, test.claimRef, test.providerID)
			if err != nil {
				t.Fatalf("reading live pair: %v", err)
			}
			if matches {
				t.Fatal("stale Node or NodeClaim identity matched the live pair")
			}
		})
	}
}

func TestEnsureStandbyTaintForClaimConflictsWithConcurrentActivation(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:        "claim",
		Annotations: map[string]string{NodeClaimAnnotationKey: "true"},
	}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(node, nodeClaim).Build()
	lifecycle := NewLifecycle(activationRaceReader{Reader: kubeClient, writer: kubeClient}, kubeClient)

	err := lifecycle.EnsureStandbyTaintForClaim(ctx, node, nodeClaim)
	if !apierrors.IsConflict(err) {
		t.Fatalf("ensuring standby taint error = %v, want optimistic-lock conflict", err)
	}
	storedNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(node), storedNode); err != nil {
		t.Fatalf("getting live Node: %v", err)
	}
	if !HasNodeActivationMarker(storedNode) || HasNodeTaint(storedNode) {
		t.Fatalf("concurrent activation was not preserved: annotations=%v taints=%v", storedNode.Annotations, storedNode.Spec.Taints)
	}
}

type activationRaceReader struct {
	client.Reader
	writer client.Client
}

func (r activationRaceReader) Get(ctx context.Context, key client.ObjectKey, object client.Object, options ...client.GetOption) error {
	if _, ok := object.(*v1.NodeClaim); ok {
		node := &corev1.Node{}
		if err := r.writer.Get(ctx, types.NamespacedName{Name: "node"}, node); err != nil {
			return err
		}
		if node.Annotations == nil {
			node.Annotations = map[string]string{}
		}
		node.Annotations[NodeClaimActivatingAnnotationKey] = "true"
		if err := r.writer.Update(ctx, node); err != nil {
			return err
		}
	}
	return r.Reader.Get(ctx, key, object, options...)
}
