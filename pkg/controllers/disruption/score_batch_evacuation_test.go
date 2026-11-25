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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

const batchSourceLabel = "batch-move-source"

var _ = Describe("Batch move evacuation", func() {
	It("drains both sources together and marks each standby as soon as it is empty", func() {
		givenBatchMoveSourcesAndStandbyDestination()

		reconcileThroughValidationDelay(disruptionController)

		commands := queue.GetCommands()
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates).To(HaveLen(2))
		// The PDB holds the command's first source, so draining sources one after another would never reach the second.
		held, other := commands[0].Candidates[0], commands[0].Candidates[1]
		heldPod, otherPod := podOnNode(held.Node), podOnNode(other.Node)
		pdb := test.PodDisruptionBudget(test.PDBOptions{
			Labels: map[string]string{batchSourceLabel: held.Node.Name}, MaxUnavailable: lo.ToPtr(intstr.FromInt(0)),
		})
		ExpectApplied(ctx, env.Client, pdb)

		ExpectObjectReconciled(ctx, env.Client, queue, held.NodeClaim)
		EventuallyExpectTerminating(ctx, env.Client, otherPod)
		expectNotEvicted(heldPod)
		ExpectDeleted(ctx, env.Client, otherPod)
		ExpectObjectReconciled(ctx, env.Client, queue, held.NodeClaim)
		otherNode := &corev1.Node{}
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(other.Node), otherNode)).To(Succeed())
		expectMarkedStandby(otherNode)
		expectNoDisruptedTaint(otherNode)
		standbySince := otherNode.Annotations[v1.StandbyNodeAnnotationKey]
		Expect(queue.IsEmpty()).To(BeFalse())

		env.Clock.Step(time.Second)
		ExpectDeleted(ctx, env.Client, pdb)
		ExpectObjectReconciled(ctx, env.Client, queue, held.NodeClaim)
		EventuallyExpectTerminating(ctx, env.Client, heldPod)
		ExpectDeleted(ctx, env.Client, heldPod)
		ExpectObjectReconciled(ctx, env.Client, queue, held.NodeClaim)

		heldNode := &corev1.Node{}
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(held.Node), heldNode)).To(Succeed())
		expectMarkedStandby(heldNode)
		expectMarkedStandby(otherNode)
		Expect(otherNode.Annotations[v1.StandbyNodeAnnotationKey]).To(Equal(standbySince))
		Expect(queue.IsEmpty()).To(BeTrue())
	})
})

// givenBatchMoveSourcesAndStandbyDestination gives two expensive sources with one pod each and a cheaper standby node that
// can host both pods, so moving both sources at once outscores moving either.
func givenBatchMoveSourcesAndStandbyDestination() {
	GinkgoHelper()
	pool := scoreBasedStandbyNodePool(v1.ConsolidationPolicyWhenEmptyOrUnderutilized, map[string]string{v1.ReclamationDelayAnnotationKey: "1h"}, []v1.Budget{{Nodes: "100%"}})
	claims, nodes := standbyTestNodeClaims(pool, 3)
	expensive := leastExpensiveInstance.DeepCopy()
	expensive.Name = "batch-source"
	expensive.Offerings = make(cloudprovider.Offerings, len(leastExpensiveInstance.Offerings))
	for i, offering := range leastExpensiveInstance.Offerings {
		offeringCopy := *offering
		offeringCopy.Price += 1
		expensive.Offerings[i] = &offeringCopy
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
	for i, pod := range paceReplicaSetPods(rs, 2) {
		source := nodes[i+1]
		pod.Labels[batchSourceLabel] = source.Name
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, source)
	}
	refreshStandbyStates(nodes, claims)
	reconcileDisruption()
	expectMarkedStandby(nodes[0])
	refreshStandbyStates(nodes, claims)
}

func podOnNode(node *corev1.Node) *corev1.Pod {
	GinkgoHelper()
	pods := &corev1.PodList{}
	Expect(env.Client.List(ctx, pods, client.MatchingLabels{batchSourceLabel: node.Name})).To(Succeed())
	Expect(pods.Items).To(HaveLen(1))
	return &pods.Items[0]
}

func expectNotEvicted(pod *corev1.Pod) {
	GinkgoHelper()
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
	Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue(), "expected pod %s not to be evicted", pod.Name)
}
