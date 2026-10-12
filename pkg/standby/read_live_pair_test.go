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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestReadLivePairRejectsStaleIdentity(t *testing.T) {
	ctx := context.Background()
	currentNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: types.UID("current-node")}, Spec: corev1.NodeSpec{ProviderID: "provider"}}
	currentClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim", UID: types.UID("current-claim")}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(currentNode, currentClaim).Build()
	coordinator := TestCoordinator(kubeClient, nil)

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
			_, _, matches, err := coordinator.readLivePair(ctx, test.nodeRef, test.claimRef, test.providerID)
			if err != nil {
				t.Fatalf("reading live pair: %v", err)
			}
			if matches {
				t.Fatal("stale Node or NodeClaim identity matched the live pair")
			}
		})
	}
}
