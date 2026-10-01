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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

var _ = Describe("Simulation max capacity", func() {
	It("caps replacement fit checks while retaining larger launch options", func() {
		nodePool := test.NodePool()
		nodePool.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmptyOrUnderutilized
		nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("0s")

		instanceTypes := make([]*cloudprovider.InstanceType, 0, 3)
		for _, cpu := range []string{"48", "64", "96"} {
			instanceType := fake.NewInstanceType("cpu-"+cpu,
				fake.WithResources(corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(cpu),
					corev1.ResourceMemory: resource.MustParse("64Gi"),
					corev1.ResourcePods:   resource.MustParse("100"),
				}),
			)
			// Keep the CPU capacities exact so two 48-CPU requests fit an uncapped 96-CPU type.
			instanceType.Overhead = &cloudprovider.InstanceTypeOverhead{
				KubeReserved: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("10Mi")},
			}
			instanceTypes = append(instanceTypes, instanceType)
		}
		cloudProvider.InstanceTypes = instanceTypes

		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, nodePool, rs)
		nodeClaims := make([]*v1.NodeClaim, 0, 2)
		nodes := make([]*corev1.Node, 0, 2)
		pods := make([]*corev1.Pod, 0, 2)
		for range 2 {
			capacity := corev1.ResourceList{
				corev1.ResourceCPU:  resource.MustParse("48"),
				corev1.ResourcePods: resource.MustParse("100"),
			}
			nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: "cpu-48",
					v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
					corev1.LabelTopologyZone:       "test-zone-1",
				}},
				Status: v1.NodeClaimStatus{Capacity: capacity, Allocatable: capacity},
			})
			nodeClaims = append(nodeClaims, nodeClaim)
			nodes = append(nodes, node)
			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
					APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID,
					Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true),
				}}},
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("48"),
				}},
			})
			pods = append(pods, pod)
		}
		for i := range nodes {
			ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i], pods[i])
			ExpectManualBinding(ctx, env.Client, pods[i], nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		simulateConsolidation := func() pscheduling.Results {
			GinkgoHelper()
			pdbs, err := pdb.NewLimits(ctx, env.Client)
			Expect(err).To(Succeed())
			nodePoolMap, instanceTypesByNodePool, err := disruption.BuildNodePoolMap(ctx, env.Client, cloudProvider)
			Expect(err).To(Succeed())
			candidates := make([]*disruption.Candidate, 0, len(nodes))
			for _, node := range nodes {
				stateNode := ExpectStateNodeExists(cluster, node)
				candidate, err := disruption.NewCandidate(ctx, env.Client, recorder, env.Clock, stateNode, pdbs, nodePoolMap, instanceTypesByNodePool, queue, disruption.GracefulDisruptionClass)
				Expect(err).To(Succeed())
				candidates = append(candidates, candidate)
			}
			results, err := disruption.SimulateScheduling(ctx, env.Client, cluster, prov, env.Clock, recorder, []pscheduling.Options{pscheduling.IsConsolidationSimulation}, candidates...)
			Expect(err).To(Succeed())
			return results
		}

		uncapped := simulateConsolidation()
		Expect(uncapped.AllNonPendingPodsScheduled()).To(BeTrue())
		Expect(uncapped.NewNodeClaims).To(HaveLen(1))
		Expect(uncapped.NewNodeClaims[0].InstanceTypeOptions).To(ConsistOf(HaveField("Name", "cpu-96")))

		nodePool.Annotations = map[string]string{pscheduling.SimulationMaxCapacityAnnotationKey: `{"cpu":"64"}`}
		ExpectApplied(ctx, env.Client, nodePool)

		capped := simulateConsolidation()
		Expect(capped.AllNonPendingPodsScheduled()).To(BeTrue())
		Expect(capped.NewNodeClaims).To(HaveLen(2))
		for _, nodeClaim := range capped.NewNodeClaims {
			Expect(nodeClaim.InstanceTypeOptions).To(ContainElement(HaveField("Name", "cpu-96")))
		}
	})
})
