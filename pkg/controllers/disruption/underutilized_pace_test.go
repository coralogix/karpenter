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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

func paceAnnotations(rate string, maxNodes string) map[string]string {
	annotations := map[string]string{
		v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey: rate,
	}
	if maxNodes != "" {
		annotations[v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey] = maxNodes
	}
	return annotations
}

// paceConsolidatableNodes returns count consolidatable NodeClaims/Nodes attached to nodePool.
func paceConsolidatableNodes(nodePool *v1.NodePool, count int) ([]*v1.NodeClaim, []*corev1.Node) {
	nodeClaims, nodes := test.NodeClaimsAndNodes(count, v1.NodeClaim{
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
	return nodeClaims, nodes
}

// paceReplicaSetPods returns count pods owned by rs.
func paceReplicaSetPods(rs *appsv1.ReplicaSet, count int) []*corev1.Pod {
	return test.Pods(count, test.PodOptions{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"app": "test"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         "apps/v1",
				Kind:               "ReplicaSet",
				Name:               rs.Name,
				UID:                rs.UID,
				Controller:         lo.ToPtr(true),
				BlockOwnerDeletion: lo.ToPtr(true),
			}},
		},
	})
}

func paceOnDemandInstanceType(name string, price float64) *cloudprovider.InstanceType {
	return fake.NewInstanceType(fake.InstanceTypeOptions{
		Name: name,
		Offerings: []*cloudprovider.Offering{{
			Available:    true,
			Requirements: scheduling.NewLabelRequirements(map[string]string{v1.CapacityTypeLabelKey: v1.CapacityTypeOnDemand, corev1.LabelTopologyZone: "test-zone-1a"}),
			Price:        price,
		}},
	})
}

func asObjects[T client.Object](in []T) []client.Object {
	return lo.Map(in, func(o T, _ int) client.Object { return o })
}

var _ = Describe("Underutilized consolidation pace", func() {
	var nodePool *v1.NodePool

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
		nodePool.Annotations = paceAnnotations("1", "100")
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)

		nodeClaims, nodes := paceConsolidatableNodes(nodePool, 2)
		pods := paceReplicaSetPods(rs, 3)
		ExpectApplied(ctx, env.Client, rs, pods[0], pods[1], pods[2], nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[1])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(1))

		*queue = lo.FromPtr(disruption.NewQueue(env.Client, recorder, cluster, fakeClock, prov))
		nodeClaims, nodes = paceConsolidatableNodes(nodePool, 2)
		pods = paceReplicaSetPods(rs, 3)
		ExpectApplied(ctx, env.Client, pods[0], pods[1], pods[2], nodeClaims[0], nodes[0], nodeClaims[1], nodes[1], nodePool)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[1])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		cluster.MarkUnconsolidated()
		fakeClock.Step(30 * time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(0))

		cluster.MarkUnconsolidated()
		fakeClock.Step(31 * time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	It("should pace multi-node consolidation by disrupted node count", func() {
		nodePool.Annotations = paceAnnotations("1", "100")
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)

		nodeClaims, nodes := paceConsolidatableNodes(nodePool, 3)
		pods := paceReplicaSetPods(rs, 4)
		ExpectApplied(ctx, env.Client, nodePool, rs, pods[0], pods[1], pods[2], pods[3])
		ExpectApplied(ctx, env.Client, asObjects(nodeClaims)...)
		ExpectApplied(ctx, env.Client, asObjects(nodes)...)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[1])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[2])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		ExpectSingletonReconciled(ctx, disruptionController)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].ConsolidationType()).To(Equal(disruption.MultiNodeConsolidationType))
		Expect(cmds[0].Candidates).To(HaveLen(2))

		*queue = lo.FromPtr(disruption.NewQueue(env.Client, recorder, cluster, fakeClock, prov))
		nodeClaims, nodes = paceConsolidatableNodes(nodePool, 3)
		pods = paceReplicaSetPods(rs, 4)
		ExpectApplied(ctx, env.Client, pods[0], pods[1], pods[2], pods[3])
		ExpectApplied(ctx, env.Client, asObjects(nodeClaims)...)
		ExpectApplied(ctx, env.Client, asObjects(nodes)...)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[1])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[2])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		cluster.MarkUnconsolidated()
		fakeClock.Step(90 * time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(0))

		cluster.MarkUnconsolidated()
		fakeClock.Step(31 * time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})

	It("should cap the batch and fall back to single-node consolidation", func() {
		nodePool.Annotations = paceAnnotations("100", "1")
		currentInstanceType := paceOnDemandInstanceType("current-on-demand", 0.5)
		otherInstanceType := paceOnDemandInstanceType("other-on-demand", 0.4)
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
		ExpectApplied(ctx, env.Client, asObjects(nodeClaims)...)
		ExpectApplied(ctx, env.Client, asObjects(nodes)...)
		pods := test.Pods(4, test.PodOptions{
			ResourceRequirements: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
			},
		})
		ExpectApplied(ctx, env.Client, asObjects(pods)...)
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectManualBinding(ctx, env.Client, pods[1], nodes[1])
		ExpectManualBinding(ctx, env.Client, pods[2], nodes[2])
		ExpectManualBinding(ctx, env.Client, pods[3], nodes[2])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
		ExpectSingletonReconciled(ctx, disruptionController)

		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Candidates).To(HaveLen(1))
		Expect(cmds[0].ConsolidationType()).To(Equal(disruption.SingleNodeConsolidationType))
	})
})
