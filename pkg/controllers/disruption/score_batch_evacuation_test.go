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
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Batch evacuation", func() {
	It("evicts from both sources together and marks both standby", func() {
		claims, nodes, pods := givenBatchSourcesAndStandbyDestination()

		reconcileThroughValidationDelay(disruptionController)
		commands := queue.GetCommands()
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates).To(HaveLen(2))
		ExpectObjectReconciled(ctx, env.Client, queue, claims[1])
		for _, pod := range pods {
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
			Expect(pod.DeletionTimestamp.IsZero()).To(BeFalse())
		}
		ExpectDeleted(ctx, env.Client, pods[0])
		ExpectObjectReconciled(ctx, env.Client, queue, claims[1])
		expectMarkedStandby(nodes[1])
		standbySince := nodes[1].Annotations[v1.StandbyNodeAnnotationKey]
		env.Clock.Step(time.Second)
		ExpectObjectReconciled(ctx, env.Client, queue, claims[1])
		expectMarkedStandby(nodes[1])
		Expect(nodes[1].Annotations[v1.StandbyNodeAnnotationKey]).To(Equal(standbySince))
		ExpectDeleted(ctx, env.Client, pods[1])
		ExpectObjectReconciled(ctx, env.Client, queue, claims[1])
		expectMarkedStandby(nodes[1])
		expectMarkedStandby(nodes[2])
	})
})

func givenBatchSourcesAndStandbyDestination() ([]*v1.NodeClaim, []*corev1.Node, []*corev1.Pod) {
	GinkgoHelper()
	pool := scoreBasedStandbyNodePool(v1.ConsolidationPolicyWhenEmptyOrUnderutilized, map[string]string{v1.ReclamationDelayAnnotationKey: "1h"}, []v1.Budget{{Nodes: "100%"}})
	claims, nodes := standbyTestNodeClaims(pool, 3)
	expensive := leastExpensiveInstance.DeepCopy()
	expensive.Name = "batch-source"
	expensive.Offerings = make(cloudprovider.Offerings, len(leastExpensiveInstance.Offerings))
	for i, offering := range leastExpensiveInstance.Offerings {
		copy := *offering
		copy.Price += 1
		expensive.Offerings[i] = &copy
	}
	cloudProvider.InstanceTypes = append(cloudProvider.InstanceTypes, expensive)
	for _, i := range []int{1, 2} {
		claims[i].Labels[corev1.LabelInstanceTypeStable] = expensive.Name
		nodes[i].Labels[corev1.LabelInstanceTypeStable] = expensive.Name
	}
	clusterCost.Reset()
	ExpectSingletonReconciled(ctx, pricingController)
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, rs, pool)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
	for i := range nodes {
		ExpectApplied(ctx, env.Client, claims[i], nodes[i])
	}
	pods := paceReplicaSetPods(rs, 2)
	for i, pod := range pods {
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, nodes[i+1])
	}
	refreshStandbyStates(nodes, claims)
	reconcileDisruption()
	expectMarkedStandby(nodes[0])
	refreshStandbyStates(nodes, claims)
	return claims, nodes, pods
}
