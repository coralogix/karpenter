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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestEnsureStandbyTaintsDoesNotRetaintActivatedNodeFromStaleState(t *testing.T) {
	ctx := context.Background()
	liveNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	liveNodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim-1"}}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(liveNode, liveNodeClaim).
		Build()

	staleNode := liveNode.DeepCopy()
	standby.SetNodeTaint(staleNode, true)
	staleNodeClaim := liveNodeClaim.DeepCopy()
	standby.SetNodeClaimStandby(staleNodeClaim, true)

	controller := &Controller{kubeClient: kubeClient, apiReader: kubeClient}
	if err := controller.ensureStandbyTaints(ctx, &state.StateNode{Node: staleNode, NodeClaim: staleNodeClaim}); err != nil {
		t.Fatalf("ensureStandbyTaints() error = %v", err)
	}

	storedNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(liveNode), storedNode); err != nil {
		t.Fatalf("getting live Node: %v", err)
	}
	if standby.HasNodeTaint(storedNode) {
		t.Fatal("ensureStandbyTaints() restored the taint using stale standby state after activation")
	}
}
