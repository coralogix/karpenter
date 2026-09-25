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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/standby"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func TestActivateStandbyNode(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "standby-node"},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{standby.NodeTaint()}},
	}
	nodeClaim := &karpv1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:        "standby-node",
		Labels:      map[string]string{karpv1.NodePoolLabelKey: "standby-observability-test"},
		Annotations: map[string]string{standby.NodeClaimAnnotationKey: "true"},
	}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(node, nodeClaim).Build()
	provisioner := &Provisioner{kubeClient: kubeClient}
	stateNode := &state.StateNode{Node: node.DeepCopy(), NodeClaim: nodeClaim.DeepCopy()}

	if err := provisioner.ActivateStandbyNodes(ctx, stateNode); err != nil {
		t.Fatalf("activating standby node: %v", err)
	}

	storedNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: node.Name}, storedNode); err != nil {
		t.Fatalf("getting activated node: %v", err)
	}
	if standby.HasNodeTaint(storedNode) {
		t.Fatal("standby taint remained after activation")
	}
	storedNodeClaim := &karpv1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), storedNodeClaim); err != nil {
		t.Fatalf("getting activated nodeclaim: %v", err)
	}
	if standby.IsNodeClaimStandby(storedNodeClaim) || standby.IsNodeClaimActivating(storedNodeClaim) {
		t.Fatal("standby or activation marker remained after activation")
	}
	if got := standbyActivationCount(t, "standby-observability-test"); got != 1 {
		t.Fatalf("expected one standby activation, got %v", got)
	}
	if err := provisioner.ActivateStandbyNodes(ctx, stateNode); err != nil {
		t.Fatalf("retrying activation: %v", err)
	}
	if got := standbyActivationCount(t, "standby-observability-test"); got != 1 {
		t.Fatalf("expected idempotent retry not to increment activations, got %v", got)
	}
}

func standbyActivationCount(t *testing.T, nodePool string) float64 {
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
			for _, label := range metric.Label {
				if label.GetName() == "nodepool" && label.GetValue() == nodePool {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	t.Fatalf("standby activation metric for nodepool %q not found", nodePool)
	return 0
}
