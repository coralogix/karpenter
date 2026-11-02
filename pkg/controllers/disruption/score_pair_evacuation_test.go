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

var _ = Describe("Pair evacuation", func() {
	It("launches one replacement and leaves both sources standby", func() {
		claims, nodes, pods := givenPairSources()
		evacuatePair(claims[0], pods)
		for _, node := range nodes {
			expectMarkedStandby(node)
		}
	})
})

func evacuatePair(claim *v1.NodeClaim, pods []*corev1.Pod) {
	GinkgoHelper()
	reconcileThroughValidationDelay(disruptionController)
	commands := queue.GetCommands()
	Expect(commands).To(HaveLen(1))
	Expect(commands[0].Candidates).To(HaveLen(2))
	Expect(commands[0].Replacements).To(HaveLen(1))
	replacement := &v1.NodeClaim{}
	Expect(env.Client.Get(ctx, client.ObjectKey{Name: commands[0].Replacements[0].Name}, replacement)).To(Succeed())
	ExpectObjectReconciled(ctx, env.Client, queue, claim)
	for _, pod := range pods {
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
	}
	replacement, replacementNode := ExpectNodeClaimDeployedAndStateUpdated(ctx, env.Client, cluster, cloudProvider, replacement)
	refreshStandbyState(replacementNode, replacement)
	ExpectObjectReconciled(ctx, env.Client, queue, claim)
	for _, pod := range pods {
		ExpectDeleted(ctx, env.Client, pod)
	}
	ExpectObjectReconciled(ctx, env.Client, queue, claim)
}

func givenPairSources() ([]*v1.NodeClaim, []*corev1.Node, []*corev1.Pod) {
	GinkgoHelper()
	sourceType := paceOnDemandInstanceType("pair-source", 1)
	replacementType := fake.NewInstanceType("pair-replacement", fake.WithResources(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8")}), fake.WithOfferings(*paceOnDemandInstanceType("offering", 1.5).Offerings[0]))
	cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{sourceType, replacementType}
	clusterCost.Reset()
	ExpectSingletonReconciled(ctx, pricingController)
	pool := scoreBasedStandbyNodePool(v1.ConsolidationPolicyWhenEmptyOrUnderutilized, map[string]string{v1.ReclamationDelayAnnotationKey: "1h"}, []v1.Budget{{Nodes: "100%"}})
	pool.Spec.Template.Spec.Requirements = append(pool.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand}})
	claims, nodes := standbyTestNodeClaims(pool, 2)
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, pool, rs)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
	pods := paceReplicaSetPods(rs, 2)
	for i := range nodes {
		claims[i].Labels[corev1.LabelInstanceTypeStable] = sourceType.Name
		claims[i].Labels[corev1.LabelTopologyZone] = "test-zone-1a"
		claims[i].Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeOnDemand
		nodes[i].Labels = claims[i].Labels
		claims[i].Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("4")
		nodes[i].Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("4")
		pods[i].Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3")}
		ExpectApplied(ctx, env.Client, claims[i], nodes[i], pods[i])
		ExpectManualBinding(ctx, env.Client, pods[i], nodes[i])
	}
	refreshStandbyStates(nodes, claims)
	return claims, nodes, pods
}
