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

package scheduling_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	"sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	karpscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

func TestSchedulerPrecomputeMatchesLegacyDaemonResources(t *testing.T) {
	ctx := context.Background()
	inputs := testNodePoolInputs(ctx)

	nodeA := state.NewNode()
	nodeA.Node = test.Node(test.NodeOptions{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"env": "a"}},
	})
	nodeB := state.NewNode()
	nodeB.Node = test.Node(test.NodeOptions{
		ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"env": "b"}},
		Taints:     []corev1.Taint{{Key: "special", Effect: corev1.TaintEffectNoSchedule}},
	})
	stateNodes := []*state.StateNode{nodeA, nodeB}

	daemonA := test.Pod(test.PodOptions{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "daemon-a"},
		NodeRequirements: []corev1.NodeSelectorRequirement{{
			Key:      "env",
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{"a"},
		}},
		ResourceRequirements: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
	})
	daemonB := test.Pod(test.PodOptions{
		ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "daemon-b"},
		NodeRequirements: []corev1.NodeSelectorRequirement{{
			Key:      "env",
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{"b"},
		}},
		ResourceRequirements: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")},
		},
		Tolerations: []corev1.Toleration{{
			Key:      "special",
			Operator: corev1.TolerationOpExists,
			Effect:   corev1.TaintEffectNoSchedule,
		}},
	})
	daemonSetPods := []*corev1.Pod{daemonA, daemonB}

	precompute := scheduling.NewSchedulerPrecompute(ctx, inputs, daemonSetPods, stateNodes)
	for _, node := range stateNodes {
		legacy := legacyNodeDaemonResources(node, daemonSetPods)
		if !equality.Semantic.DeepEqual(precompute.NodeDaemonResources[node.Name()], legacy) {
			t.Fatalf("precompute mismatch for node %s: got %v, want %v", node.Name(), precompute.NodeDaemonResources[node.Name()], legacy)
		}
	}
}

func TestSchedulerPrecomputeKeepsAnonymousDaemonPodsDistinct(t *testing.T) {
	ctx := context.Background()
	inputs := testNodePoolInputs(ctx)
	node := state.NewNode()
	node.Node = test.Node(test.NodeOptions{
		ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"env": "a"}},
	})

	daemonSetPods := []*corev1.Pod{
		anonymousDaemonPod(resource.MustParse("100m"), "a"),
		anonymousDaemonPod(resource.MustParse("200m"), "a"),
	}
	precompute := scheduling.NewSchedulerPrecompute(ctx, inputs, daemonSetPods, []*state.StateNode{node})

	gotCPU := precompute.NodeDaemonResources[node.Name()][corev1.ResourceCPU]
	wantCPU := resource.MustParse("300m")
	if gotCPU.Cmp(wantCPU) != 0 {
		t.Fatalf("expected node daemon CPU %s, got %s", wantCPU.String(), gotCPU.String())
	}
}

func testNodePoolInputs(ctx context.Context) *scheduling.NodePoolInputs {
	nodePool := test.NodePool()
	instanceTypes := []*cloudprovider.InstanceType{
		fake.NewInstanceType(fake.InstanceTypeOptions{
			Name: "default",
			Resources: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("8Gi"),
				corev1.ResourcePods:   resource.MustParse("110"),
			},
		}),
	}
	return scheduling.NewNodePoolInputs(ctx, events.NewRecorder(&record.FakeRecorder{}), []*v1.NodePool{nodePool}, map[string][]*cloudprovider.InstanceType{
		nodePool.Name: instanceTypes,
	})
}

func anonymousDaemonPod(cpu resource.Quantity, envValue string) *corev1.Pod {
	return &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeSelector: map[string]string{"env": envValue},
			Containers: []corev1.Container{{
				Name: "daemon",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: cpu},
				},
			}},
		},
	}
}

func legacyNodeDaemonResources(node *state.StateNode, daemonSetPods []*corev1.Pod) corev1.ResourceList {
	var compatible []*corev1.Pod
	for _, p := range daemonSetPods {
		if err := karpscheduling.Taints(node.Taints()).ToleratesPod(p); err != nil {
			continue
		}
		if err := karpscheduling.NewLabelRequirements(node.Labels()).Compatible(karpscheduling.NewStrictPodRequirements(p)); err != nil {
			continue
		}
		compatible = append(compatible, p)
	}
	return resources.RequestsForPods(compatible...)
}
