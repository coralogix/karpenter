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
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Simulation max capacity", func() {
	var nodePool *v1.NodePool

	BeforeEach(func() {
		nodePool = test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{
				Spec: v1.NodeClaimTemplateSpec{
					Requirements: []v1.NodeSelectorRequirementWithMinValues{{
						Key:      v1.CapacityTypeLabelKey,
						Operator: corev1.NodeSelectorOpIn,
						Values:   []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand, v1.CapacityTypeReserved},
					}},
				},
			}},
		})
	})

	It("splits new claims at the configured cap while keeping larger launch types", func() {
		nodePool.Annotations = map[string]string{scheduling.SimulationMaxCapacityAnnotationKey: `{"cpu":"64"}`}
		cloudProvider.InstanceTypes = simulationMaxCapacityInstanceTypes()
		ExpectApplied(ctx, env.Client, nodePool)

		pods := simulationMaxCapacityPods(6)
		for _, pod := range pods {
			ExpectApplied(ctx, env.Client, pod)
		}
		results, err := prov.Schedule(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(results.NewNodeClaims).To(HaveLen(6))
		for _, nodeClaim := range results.NewNodeClaims {
			Expect(nodeClaim.Pods).To(HaveLen(1))
			cpuRequest := nodeClaim.Spec.Resources.Requests[corev1.ResourceCPU]
			Expect(cpuRequest.Cmp(resource.MustParse("47"))).To(Equal(0))

			launchClaim := nodeClaim.ToNodeClaim()
			instanceTypeRequirement, found := lo.Find(launchClaim.Spec.Requirements, func(requirement v1.NodeSelectorRequirementWithMinValues) bool {
				return requirement.Key == corev1.LabelInstanceTypeStable
			})
			Expect(found).To(BeTrue())
			Expect(instanceTypeRequirement.Values).To(ConsistOf("cpu48", "cpu64", "cpu96"))
			launchCPURequest := launchClaim.Spec.Resources.Requests[corev1.ResourceCPU]
			Expect(launchCPURequest.Cmp(resource.MustParse("47"))).To(Equal(0))
			Expect(launchClaim.Annotations).ToNot(HaveKey(scheduling.SimulationMaxCapacityAnnotationKey))
		}
	})

	It("packs two pods on cpu96 when no simulation cap is configured", func() {
		cloudProvider.InstanceTypes = simulationMaxCapacityInstanceTypes()
		ExpectApplied(ctx, env.Client, nodePool)

		pods := simulationMaxCapacityPods(6)
		for _, pod := range pods {
			ExpectApplied(ctx, env.Client, pod)
		}
		results, err := prov.Schedule(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(results.NewNodeClaims).To(HaveLen(3))
		for _, nodeClaim := range results.NewNodeClaims {
			Expect(nodeClaim.Pods).To(HaveLen(2))
			cpuRequest := nodeClaim.Spec.Resources.Requests[corev1.ResourceCPU]
			Expect(cpuRequest.Cmp(resource.MustParse("94"))).To(Equal(0))
			Expect(nodeClaim.InstanceTypeOptions).To(HaveLen(1))
			Expect(nodeClaim.InstanceTypeOptions[0].Name).To(Equal("cpu96"))
		}
	})

	It("uses full actual capacity on an existing cpu96 node", func() {
		nodePool.Annotations = map[string]string{scheduling.SimulationMaxCapacityAnnotationKey: `{"cpu":"64"}`}
		cloudProvider.InstanceTypes = simulationMaxCapacityInstanceTypes()
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            nodePool.Name,
				corev1.LabelInstanceTypeStable: "cpu96",
				v1.CapacityTypeLabelKey:        v1.CapacityTypeSpot,
				corev1.LabelTopologyZone:       "test-zone-1",
			}},
			Status: v1.NodeClaimStatus{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("96"),
					corev1.ResourceMemory:           resource.MustParse("256Gi"),
					corev1.ResourcePods:             resource.MustParse("110"),
					corev1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
				},
				Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:              resource.MustParse("96"),
					corev1.ResourceMemory:           resource.MustParse("256Gi"),
					corev1.ResourcePods:             resource.MustParse("110"),
					corev1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
				},
			},
		})
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)
		ExpectMakeNodesInitialized(ctx, env.Client, env.Clock, node)
		ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))
		ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))

		pods := simulationMaxCapacityPods(2)
		for _, pod := range pods {
			ExpectApplied(ctx, env.Client, pod)
		}
		results, err := prov.Schedule(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(results.NewNodeClaims).To(BeEmpty())
		Expect(results.ExistingNodeToPodMapping()[nodeClaim.Name]).To(ConsistOf(pods))
	})
})

func simulationMaxCapacityInstanceTypes() []*cloudprovider.InstanceType {
	var instanceTypes []*cloudprovider.InstanceType
	for _, cpu := range []string{"48", "64", "96"} {
		name := "cpu" + cpu
		instanceTypes = append(instanceTypes, fake.NewInstanceType(name, fake.WithResources(corev1.ResourceList{
			corev1.ResourceCPU:              resource.MustParse(cpu),
			corev1.ResourceMemory:           resource.MustParse("256Gi"),
			corev1.ResourcePods:             resource.MustParse("110"),
			corev1.ResourceEphemeralStorage: resource.MustParse("100Gi"),
		})))
	}
	return instanceTypes
}

func simulationMaxCapacityPods(count int) []*corev1.Pod {
	pods := make([]*corev1.Pod, count)
	for i := range pods {
		pods[i] = test.UnschedulablePod(test.PodOptions{ResourceRequirements: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("47"),
				corev1.ResourceMemory: resource.MustParse("1Gi"),
			},
		}})
		pods[i].Name = fmt.Sprintf("simulation-capacity-pod-%d", i)
	}
	return pods
}
