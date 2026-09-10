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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
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

func TestSchedulerPrecomputeKeysCachesByProviderID(t *testing.T) {
	ctx := context.Background()
	inputs := testNodePoolInputs(ctx)

	node := state.NewNode()
	node.Node = test.Node(test.NodeOptions{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "same-name",
			Labels: map[string]string{"env": "node"},
		},
		ProviderID: "provider-node",
	})
	nodeClaim := state.NewNode()
	nodeClaim.NodeClaim = test.NodeClaim(v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "same-name",
			Labels: map[string]string{"env": "nodeclaim"},
		},
		Status: v1.NodeClaimStatus{ProviderID: "provider-nodeclaim"},
	})

	daemonSetPods := []*corev1.Pod{
		test.Pod(test.PodOptions{
			NodeRequirements: []corev1.NodeSelectorRequirement{{
				Key:      "env",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"node"},
			}},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
			},
		}),
		test.Pod(test.PodOptions{
			NodeRequirements: []corev1.NodeSelectorRequirement{{
				Key:      "env",
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{"nodeclaim"},
			}},
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m")},
			},
		}),
	}

	precompute := scheduling.NewSchedulerPrecompute(ctx, inputs, daemonSetPods, []*state.StateNode{node, nodeClaim})
	if len(precompute.NodeLabelRequirements) != 2 {
		t.Fatalf("expected two label requirement cache entries, got %d", len(precompute.NodeLabelRequirements))
	}
	if len(precompute.NodeDaemonResources) != 2 {
		t.Fatalf("expected two daemon resource cache entries, got %d", len(precompute.NodeDaemonResources))
	}
	for providerID, expectedEnv := range map[string]string{
		"provider-node":      "node",
		"provider-nodeclaim": "nodeclaim",
	} {
		if err := precompute.NodeLabelRequirements[providerID].Compatible(karpscheduling.NewLabelRequirements(map[string]string{"env": expectedEnv})); err != nil {
			t.Fatalf("expected label requirements for %s to preserve env=%s, got %v", providerID, expectedEnv, err)
		}
	}
	if got := precompute.NodeDaemonResources["provider-node"][corev1.ResourceCPU]; got.Cmp(resource.MustParse("100m")) != 0 {
		t.Fatalf("expected node daemon CPU to be 100m, got %s", got.String())
	}
	if got := precompute.NodeDaemonResources["provider-nodeclaim"][corev1.ResourceCPU]; got.Cmp(resource.MustParse("200m")) != 0 {
		t.Fatalf("expected nodeclaim daemon CPU to be 200m, got %s", got.String())
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

func TestSchedulerPrecomputeEvaluatesAllDaemonAffinityTermsWithoutMutation(t *testing.T) {
	ctx := context.Background()
	nodePool := test.NodePool()
	nodePool.Spec.Template.Spec.Requirements = []v1.NodeSelectorRequirementWithMinValues{{
		Key:      corev1.LabelTopologyZone,
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{"test-zone-1"},
	}}
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
	inputs := scheduling.NewNodePoolInputs(ctx, events.NewRecorder(&record.FakeRecorder{}), []*v1.NodePool{nodePool}, map[string][]*cloudprovider.InstanceType{
		nodePool.Name: instanceTypes,
	})

	node := state.NewNode()
	node.Node = test.Node(test.NodeOptions{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-a",
			Labels: map[string]string{corev1.LabelTopologyZone: "test-zone-1"},
		},
	})
	daemon := test.Pod(test.PodOptions{
		ResourceRequirements: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		},
	})
	daemon.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      corev1.LabelTopologyZone,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"test-zone-2"},
				}}},
				{MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      corev1.LabelTopologyZone,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"test-zone-1"},
				}}},
			},
		},
	}}
	original := daemon.DeepCopy()

	precompute := scheduling.NewSchedulerPrecompute(ctx, inputs, []*corev1.Pod{daemon}, []*state.StateNode{node})
	if len(precompute.DaemonOverhead) != 1 {
		t.Fatalf("expected daemon overhead for one NodeClaimTemplate, got %d", len(precompute.DaemonOverhead))
	}
	var daemonOverhead corev1.ResourceList
	for _, overhead := range precompute.DaemonOverhead {
		daemonOverhead = overhead
	}
	if gotCPU := daemonOverhead[corev1.ResourceCPU]; gotCPU.Cmp(resource.MustParse("100m")) != 0 {
		t.Fatalf("expected daemon overhead to include the compatible affinity term, got %s", gotCPU.String())
	}
	if gotCPU := precompute.NodeDaemonResources[node.Name()][corev1.ResourceCPU]; gotCPU.Cmp(resource.MustParse("100m")) != 0 {
		t.Fatalf("expected existing-node daemon resources to include the compatible affinity term, got %s", gotCPU.String())
	}
	if !equality.Semantic.DeepEqual(daemon, original) {
		t.Fatalf("expected daemon pod to remain unchanged after precompute, got %#v, want %#v", daemon.Spec, original.Spec)
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
