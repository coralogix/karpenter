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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestNonDaemonWorkloadSize(t *testing.T) {
	requests := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("4"),
		corev1.ResourceMemory: resource.MustParse("8Gi"),
	}
	got := nonDaemonWorkloadSize(requests)
	want := 4.0 + 8.0*memoryGiBWeight
	if got != want {
		t.Fatalf("nonDaemonWorkloadSize() = %v, want %v", got, want)
	}
}

func TestNodePriorityScore(t *testing.T) {
	offering := &cloudprovider.Offering{
		Price: 0.40,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
			corev1.LabelTopologyZone: "us-east-1a",
		}),
	}
	instanceType := &cloudprovider.InstanceType{
		Name:      "m5.large",
		Offerings: cloudprovider.Offerings{offering},
	}

	node := state.NewNode()
	node.Node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				corev1.LabelInstanceTypeStable: instanceType.Name,
				v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			},
		},
		Spec: corev1.NodeSpec{
			ProviderID: "provider-1",
		},
	}
	candidate := &Candidate{
		StateNode:    node,
		instanceType: instanceType,
	}

	if nodePriorityScore(nil) != 0 {
		t.Fatal("expected zero score for nil candidate")
	}
	if nodePriorityScore(candidate) != 0.40 {
		t.Fatalf("empty workload: nodePriorityScore() = %v, want 0.40", nodePriorityScore(candidate))
	}

	workloadRequests := corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("2"),
		corev1.ResourceMemory: resource.MustParse("4Gi"),
	}
	workload := nonDaemonWorkloadSize(workloadRequests)
	want := 0.40 / workload

	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: node.Node.Name,
			Containers: []corev1.Container{{
				Resources: corev1.ResourceRequirements{
					Requests: workloadRequests,
				},
			}},
		},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(node.Node, pod).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
			indexedPod := obj.(*corev1.Pod)
			if indexedPod.Spec.NodeName == "" {
				return nil
			}
			return []string{indexedPod.Spec.NodeName}
		}).
		Build()
	cluster := state.NewCluster(clocktesting.NewFakeClock(time.Now()), kubeClient, nil)
	if err := cluster.UpdateNode(ctx, node.Node); err != nil {
		t.Fatalf("UpdateNode() = %v", err)
	}
	if err := cluster.UpdatePod(ctx, pod); err != nil {
		t.Fatalf("UpdatePod() = %v", err)
	}

	var loadedNode *state.StateNode
	for n := range cluster.Nodes() {
		loadedNode = n
		break
	}
	if loadedNode == nil {
		t.Fatal("expected node in cluster state")
	}

	loadedCandidate := &Candidate{
		StateNode:    loadedNode,
		instanceType: instanceType,
	}
	if got := nodePriorityScore(loadedCandidate); got != want {
		t.Fatalf("loaded workload: nodePriorityScore() = %v, want %v", got, want)
	}
}
