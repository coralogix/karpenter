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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Pair move evacuation", func() {
	It("launches one new node, then drains both sources into standby", func() {
		givenPairMoveSources()

		reconcileThroughValidationDelay(disruptionController)

		commands := queue.GetCommands()
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates).To(HaveLen(2))
		Expect(commands[0].Replacements).To(HaveLen(1))
		sources := commands[0].Candidates
		pods := []*corev1.Pod{podOnNode(sources[0].Node), podOnNode(sources[1].Node)}
		ExpectObjectReconciled(ctx, env.Client, queue, sources[0].NodeClaim)
		expectNotEvicted(pods[0])
		expectNotEvicted(pods[1])

		replacement := &v1.NodeClaim{}
		Expect(env.Client.Get(ctx, client.ObjectKey{Name: commands[0].Replacements[0].Name}, replacement)).To(Succeed())
		replacement, replacementNode := ExpectNodeClaimDeployedAndStateUpdated(ctx, env.Client, cluster, cloudProvider, replacement)
		refreshStandbyState(replacementNode, replacement)
		ExpectObjectReconciled(ctx, env.Client, queue, sources[0].NodeClaim)
		for _, pod := range pods {
			EventuallyExpectTerminating(ctx, env.Client, pod)
			ExpectDeleted(ctx, env.Client, pod)
		}
		ExpectObjectReconciled(ctx, env.Client, queue, sources[0].NodeClaim)

		expectMarkedStandby(sources[0].Node)
		expectMarkedStandby(sources[1].Node)
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(3))
		Expect(queue.IsEmpty()).To(BeTrue())
	})
})

// givenPairMoveSources gives two nodes of the cheapest instance type, each too full to take the other's pod. Neither can
// move alone, but both pods fit on one larger node that costs less than the two sources.
func givenPairMoveSources() {
	GinkgoHelper()
	small := pairMoveInstanceType("pair-source", "2", 1)
	large := pairMoveInstanceType("pair-destination", "4", 1.5)
	cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{small, large}
	clusterCost.Reset()
	ExpectSingletonReconciled(ctx, pricingController)
	pool := scoreBasedStandbyNodePool(v1.ConsolidationPolicyWhenEmptyOrUnderutilized, map[string]string{v1.ReclamationDelayAnnotationKey: "1h"}, []v1.Budget{{Nodes: "100%"}})
	pool.Spec.Template.Spec.Requirements = append(pool.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{
		Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand},
	})
	claims, nodes := standbyTestNodeClaims(pool, 2)
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, pool, rs)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
	for i, pod := range paceReplicaSetPods(rs, 2) {
		allocatable := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourcePods: resource.MustParse("10")}
		claims[i].Labels[corev1.LabelInstanceTypeStable], claims[i].Labels[corev1.LabelTopologyZone] = small.Name, "test-zone-1a"
		claims[i].Status.Allocatable, nodes[i].Status.Allocatable = allocatable, allocatable
		nodes[i].Labels = claims[i].Labels
		pod.Labels[moveSourceLabel] = nodes[i].Name
		pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1500m")}
		ExpectApplied(ctx, env.Client, claims[i], nodes[i], pod)
		ExpectManualBinding(ctx, env.Client, pod, nodes[i])
	}
	refreshStandbyStates(nodes, claims)
}

func pairMoveInstanceType(name, cpu string, price float64) *cloudprovider.InstanceType {
	instanceType := paceOnDemandInstanceType(name, price)
	return fake.NewInstanceType(name, fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)}), fake.WithOfferings(*instanceType.Offerings[0]))
}
