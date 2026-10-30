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
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Standby reactivation", func() {
	It("clears standby on a consolidation destination and still deletes the source", func() {
		_, destinationNode, destination, source := givenStandbyDestinationAndSource()

		reconcileThroughValidationDelay(disruptionController)

		Expect(queue.GetCommands()).To(HaveLen(1))
		expectNotStandby(destinationNode, destination)
		ExpectObjectReconciled(ctx, env.Client, queue, source)
		ExpectNotFound(ctx, env.Client, source)
	})

	It("leaves standby unchanged when validation rejects the consolidation", func() {
		nodePool, destinationNode, destination, _ := givenStandbyDestinationAndSource()
		validator := NewTestScoreBasedConsolidationValidator(nodePool, WithUnderutilizedBlockingBudget())

		reconcileThroughValidationDelay(scoreBasedDisruptionController(validator))

		Expect(queue.GetCommands()).To(BeEmpty())
		expectMarkedStandby(destinationNode, destination)
	})
})

// givenStandbyDestinationAndSource returns a standby node and a score-based source node running one pod.
// The pod is owned by a ReplicaSet so consolidation may move it.
func givenStandbyDestinationAndSource() (nodePool *v1.NodePool, destinationNode *corev1.Node, destination, source *v1.NodeClaim) {
	nodePool = scoreBasedStandbyNodePool(v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
		map[string]string{v1.ReclamationDelayAnnotationKey: "1h"}, []v1.Budget{{Nodes: "100%"}})
	nodeClaims, nodes := standbyTestNodeClaims(nodePool, 2)
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, rs)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
	ExpectApplied(ctx, env.Client, nodePool, nodeClaims[0], nodes[0], nodeClaims[1], nodes[1])
	pod := paceReplicaSetPods(rs, 1)[0]
	ExpectApplied(ctx, env.Client, pod)
	ExpectManualBinding(ctx, env.Client, pod, nodes[1])
	refreshStandbyStates(nodes, nodeClaims)
	reconcileDisruption()
	expectMarkedStandby(nodes[0], nodeClaims[0])
	refreshStandbyStates(nodes, nodeClaims)
	return nodePool, nodes[0], nodeClaims[0], nodeClaims[1]
}

// scoreBasedDisruptionController runs only score-based consolidation, validated by the given validator.
func scoreBasedDisruptionController(validator disruption.Validator) *disruption.Controller {
	c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil)
	method := disruption.NewScoreBasedConsolidation(c, disruption.WithValidator(validator))
	return disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost, disruption.WithMethods(method))
}

// reconcileThroughValidationDelay runs a disruption pass that waits out the score-based validation delay.
func reconcileThroughValidationDelay(controller *disruption.Controller) {
	ExpectParallelized(func() { ExpectSingletonReconciled(ctx, controller) }, func() {
		Eventually(env.Clock.HasWaiters, 10*time.Second).Should(BeTrue())
		env.Clock.Step(15 * time.Second)
	})
}

func expectNotStandby(node *corev1.Node, nodeClaim *v1.NodeClaim) {
	GinkgoHelper()
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
	expectNodeWithoutStandbyTaint(node)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), nodeClaim)).To(Succeed())
	Expect(nodeClaim.Annotations).NotTo(HaveKey(v1.StandbyNodeClaimAnnotationKey))
}
