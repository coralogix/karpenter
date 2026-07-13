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

package disruption_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

func TestUnderutilizedConsolidationPaceFractional(t *testing.T) {
	testClock := clocktesting.NewFakeClock(time.Now())
	pace := disruption.NewUnderutilizedConsolidationPace(testClock)
	np := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
			Annotations: map[string]string{
				v1.MaxUnderutilizedConsolidationsPerMinuteAnnotationKey: "0.5",
			},
		},
	}

	if !pace.TryAdmit(np) {
		t.Fatal("expected first admission")
	}
	if pace.TryAdmit(np) {
		t.Fatal("expected second admission to be blocked")
	}

	testClock.Step(2 * time.Minute)
	if !pace.TryAdmit(np) {
		t.Fatal("expected admission after interval elapsed")
	}
}

func TestUnderutilizedConsolidationPaceInvalid(t *testing.T) {
	pace := disruption.NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
			Annotations: map[string]string{
				v1.MaxUnderutilizedConsolidationsPerMinuteAnnotationKey: "0",
			},
		},
	}

	if pace.CanAdmit(np) {
		t.Fatal("expected invalid pace configuration to block admission")
	}
}

func TestUnderutilizedConsolidationPaceUnset(t *testing.T) {
	pace := disruption.NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}

	if !pace.TryAdmit(np) {
		t.Fatal("expected first admission with unset pace configuration")
	}
	if !pace.TryAdmit(np) {
		t.Fatal("expected repeated admission with unset pace configuration")
	}
}

var _ = Describe("Underutilized consolidation pace", func() {
	var nodePool *v1.NodePool
	labels := map[string]string{"app": "test"}

	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					Budgets: []v1.Budget{{
						Nodes: "100%",
					}},
					ConsolidateAfter: v1.MustParseNillableDuration("0s"),
				},
			},
		})
	})

	It("should pace single-node consolidation", func() {
		nodePool.Annotations = map[string]string{
			v1.MaxUnderutilizedConsolidationsPerMinuteAnnotationKey: "1",
		}
		nodeClaims, nodes := test.NodeClaimsAndNodes(2, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		for _, nc := range nodeClaims {
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}

		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)
		pods := test.Pods(3, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: labels,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         "apps/v1",
						Kind:               "ReplicaSet",
						Name:               rs.Name,
						UID:                rs.UID,
						Controller:         lo.ToPtr(true),
						BlockOwnerDeletion: lo.ToPtr(true),
					},
				}}})
		ExpectApplied(ctx, env.Client, rs, pods[0], pods[1], pods[2], nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[1])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(1))

		*queue = lo.FromPtr(disruption.NewQueue(env.Client, recorder, cluster, fakeClock, prov))
		nodeClaims, nodes = test.NodeClaimsAndNodes(2, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("32"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		for _, nc := range nodeClaims {
			nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		pods = test.Pods(3, test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Labels: labels,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         "apps/v1",
						Kind:               "ReplicaSet",
						Name:               rs.Name,
						UID:                rs.UID,
						Controller:         lo.ToPtr(true),
						BlockOwnerDeletion: lo.ToPtr(true),
					},
				}}})
		ExpectApplied(ctx, env.Client, pods[0], pods[1], pods[2], nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[1])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*corev1.Node{nodes[0], nodes[1]}, []*v1.NodeClaim{nodeClaims[0], nodeClaims[1]})

		cluster.MarkUnconsolidated()
		fakeClock.Step(30 * time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(0))

		cluster.MarkUnconsolidated()
		fakeClock.Step(31 * time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	It("should pace multi-node consolidation as a single command", func() {
		nodePool.Annotations = map[string]string{
			v1.MaxUnderutilizedConsolidationsPerMinuteAnnotationKey: "1",
		}
		currentInstanceType := fake.NewInstanceType(fake.InstanceTypeOptions{
			Name: "current-on-demand",
			Offerings: []*cloudprovider.Offering{
				{
					Available:    true,
					Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"}),
					Price:        0.5,
				},
			},
		})
		otherInstanceType := fake.NewInstanceType(fake.InstanceTypeOptions{
			Name: "other-on-demand",
			Offerings: []*cloudprovider.Offering{
				{
					Available:    true,
					Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"}),
					Price:        0.4,
				},
			},
		})
		cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{currentInstanceType, otherInstanceType}

		nodeClaims, nodes := test.NodeClaimsAndNodes(3, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: currentInstanceType.Name,
					v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
					corev1.LabelTopologyZone:       "test-zone-1a",
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("4"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		for i := range nodeClaims {
			nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		ExpectApplied(ctx, env.Client, nodePool)
		ExpectApplied(ctx, env.Client, lo.Map(nodeClaims, func(o *v1.NodeClaim, _ int) client.Object { return o })...)
		ExpectApplied(ctx, env.Client, lo.Map(nodes, func(o *corev1.Node, _ int) client.Object { return o })...)
		pods := test.Pods(4, test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			},
		})
		ExpectApplied(ctx, env.Client, lo.Map(pods, func(o *corev1.Pod, _ int) client.Object { return o })...)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[1])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[2])
		ExpectManualBinding(ctx, env.Client, pods[3], nodes[2])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		ExpectSingletonReconciled(ctx, disruptionController)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Candidates).To(HaveLen(2))

		*queue = lo.FromPtr(disruption.NewQueue(env.Client, recorder, cluster, fakeClock, prov))
		nodeClaims, nodes = test.NodeClaimsAndNodes(3, v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: currentInstanceType.Name,
					v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
					corev1.LabelTopologyZone:       "test-zone-1a",
				},
			},
			Status: v1.NodeClaimStatus{
				Allocatable: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceCPU:  resource.MustParse("4"),
					corev1.ResourcePods: resource.MustParse("100"),
				},
			},
		})
		for i := range nodeClaims {
			nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		}
		ExpectApplied(ctx, env.Client, lo.Map(nodeClaims, func(o *v1.NodeClaim, _ int) client.Object { return o })...)
		ExpectApplied(ctx, env.Client, lo.Map(nodes, func(o *corev1.Node, _ int) client.Object { return o })...)
		pods = test.Pods(4, test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			},
		})
		ExpectApplied(ctx, env.Client, lo.Map(pods, func(o *corev1.Pod, _ int) client.Object { return o })...)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[1])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[2])
		ExpectManualBinding(ctx, env.Client, pods[3], nodes[2])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		cluster.MarkUnconsolidated()
		fakeClock.Step(30 * time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(0))
	})
})
