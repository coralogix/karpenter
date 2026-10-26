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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

const (
	standbyDefaultReclamationDelay      = 15 * time.Second
	standbyBeforeReclamationDelay       = 14 * time.Second
	standbyAfterDefaultReclamationDelay = standbyDefaultReclamationDelay + time.Second
)

var _ = Describe("Standby and reclamation", func() {
	DescribeTable("marks an empty score-based node standby instead of deleting it",
		func(policy v1.ConsolidationPolicy) {
			_, nodeClaim, node := givenStandbyNode(policy, nil)
			reconcileDisruption()

			expectMarkedStandby(node, nodeClaim)
			Expect(queue.GetCommands()).To(BeEmpty())
		},
		Entry("WhenEmptyOrUnderutilized", v1.ConsolidationPolicyWhenEmptyOrUnderutilized),
		Entry("Balanced", v1.ConsolidationPolicyBalanced),
	)

	It("reclaims a standby node after the default reclamation delay", func() {
		_, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmpty, nil)
		reconcileDisruption()

		reconcileAfter(standbyBeforeReclamationDelay, node, nodeClaim)
		Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(1))
		Expect(queue.GetCommands()).To(BeEmpty())

		reconcileAfter(standbyAfterDefaultReclamationDelay-standbyBeforeReclamationDelay, node, nodeClaim)
		expectNodeReclaimed()
	})

	It("clears standby when a reschedulable pod is on the node", func() {
		_, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmptyOrUnderutilized, nil)
		pod := test.Pod()
		reconcileDisruption()

		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
		reconcileAfter(0, node, nodeClaim)

		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
		expectNodeWithoutStandbyTaint(node)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), nodeClaim)).To(Succeed())
		Expect(nodeClaim.Annotations).NotTo(HaveKey(v1.StandbyNodeClaimAnnotationKey))
	})

	It("still deletes empty nodes on pools without score-based consolidation", func() {
		nodePool := test.NodePool(v1.NodePool{
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmpty,
					Budgets:             []v1.Budget{{Nodes: "100%"}},
				},
			},
		})
		nodeClaim, node := standbyTestNodeClaim(nodePool)
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		refreshStandbyState(node, nodeClaim)
		reconcileDisruption()

		expectNodeReclaimed()
	})

	It("emits a NodePool event when the reclamation delay annotation is unparseable", func() {
		nodePool, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmpty, map[string]string{
			v1.ReclamationDelayAnnotationKey: "not-a-duration",
		})
		reconcileDisruption()

		reconcileAfter(standbyAfterDefaultReclamationDelay, node, nodeClaim)

		_, found := lo.Find(recorder.Events(), func(e events.Event) bool {
			obj, ok := e.InvolvedObject.(metav1.Object)
			return e.Reason == events.InvalidReclamationDelay && ok && obj.GetName() == nodePool.Name
		})
		Expect(found).To(BeTrue())
		expectNodeReclaimed()
	})

	It("restarts the reclamation delay when a node is marked again with a stale timestamp", func() {
		_, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmpty, nil)
		nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, map[string]string{
			v1.StandbyNodeClaimAnnotationKey: env.Clock.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano),
		})
		ExpectApplied(ctx, env.Client, nodeClaim)
		refreshStandbyState(node, nodeClaim)
		reconcileDisruption()

		reconcileAfter(standbyBeforeReclamationDelay, node, nodeClaim)
		Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(1))
		Expect(queue.GetCommands()).To(BeEmpty())

		reconcileAfter(standbyAfterDefaultReclamationDelay-standbyBeforeReclamationDelay, node, nodeClaim)
		expectNodeReclaimed()
	})

	It("clears standby when the pool stops qualifying", func() {
		nodePool, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmpty, nil)
		reconcileDisruption()
		expectMarkedStandby(node, nodeClaim)

		nodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("Never")
		ExpectApplied(ctx, env.Client, nodePool)
		reconcileAfter(0, node, nodeClaim)

		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
		expectNodeWithoutStandbyTaint(node)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), nodeClaim)).To(Succeed())
		Expect(nodeClaim.Annotations).NotTo(HaveKey(v1.StandbyNodeClaimAnnotationKey))
	})

	It("does not evacuate a non-empty WhenEmpty score-based node", func() {
		_, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmpty, nil)
		pod := test.Pod()
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
		refreshStandbyState(node, nodeClaim)
		reconcileDisruption()

		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
		expectNodeWithoutStandbyTaint(node)
		Expect(queue.GetCommands()).To(BeEmpty())
	})

	It("honors a custom positive reclamation delay", func() {
		_, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmpty, map[string]string{
			v1.ReclamationDelayAnnotationKey: "1m",
		})
		reconcileDisruption()

		reconcileAfter(standbyAfterDefaultReclamationDelay, node, nodeClaim)
		Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(1))
		Expect(queue.GetCommands()).To(BeEmpty())

		reconcileAfter(time.Minute-standbyAfterDefaultReclamationDelay, node, nodeClaim)
		expectNodeReclaimed()
	})

	It("backfills a missing standby timestamp and does not reclaim on that pass", func() {
		_, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmpty, nil)
		node.Spec.Taints = append(node.Spec.Taints, v1.StandbyNoScheduleTaint)
		ExpectApplied(ctx, env.Client, node)
		refreshStandbyState(node, nodeClaim)

		reconcileDisruption()

		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), nodeClaim)).To(Succeed())
		Expect(nodeClaim.Annotations[v1.StandbyNodeClaimAnnotationKey]).NotTo(BeEmpty())
		Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(1))
		Expect(queue.GetCommands()).To(BeEmpty())
	})

	It("does not reclaim on the pass that writes the standby timestamp when delay is zero", func() {
		_, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmpty, map[string]string{
			v1.ReclamationDelayAnnotationKey: "0s",
		})
		reconcileDisruption()

		expectMarkedStandby(node, nodeClaim)
		Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(1))
		Expect(queue.GetCommands()).To(BeEmpty())

		refreshStandbyState(node, nodeClaim)
		reconcileDisruption()
		expectNodeReclaimed()
	})

	It("reclaims at most one standby node per pass when the Empty budget is one", func() {
		nodePool := scoreBasedStandbyNodePool(v1.ConsolidationPolicyWhenEmpty, nil, []v1.Budget{{Nodes: "1", Reasons: []v1.DisruptionReason{v1.DisruptionReasonEmpty}}})
		nodeClaims, nodes := standbyTestNodeClaims(nodePool, 2)
		ExpectApplied(ctx, env.Client, nodePool, nodeClaims[0], nodes[0], nodeClaims[1], nodes[1])
		refreshStandbyStates(nodes, nodeClaims)
		reconcileDisruption()

		reconcileAfterAll(standbyAfterDefaultReclamationDelay, nodes, nodeClaims)
		cmds := queue.GetCommands()
		Expect(cmds).To(HaveLen(1))
		Expect(cmds[0].Candidates).To(HaveLen(1))
		Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(2))
	})

	It("marks standby without consuming an Empty disruption budget of zero", func() {
		nodePool := scoreBasedStandbyNodePool(v1.ConsolidationPolicyWhenEmptyOrUnderutilized, nil, []v1.Budget{{Nodes: "0", Reasons: []v1.DisruptionReason{v1.DisruptionReasonEmpty}}})
		nodeClaim, node := standbyTestNodeClaim(nodePool)
		ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
		refreshStandbyState(node, nodeClaim)
		reconcileDisruption()

		expectMarkedStandby(node, nodeClaim)
	})

	DescribeTable("does not mark standby when the node is protected",
		func(mutate standbyNodeMutator) {
			_, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmptyOrUnderutilized, nil)
			mutate(node, nodeClaim)
			refreshStandbyState(node, nodeClaim)
			reconcileDisruption()

			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
			expectNodeWithoutStandbyTaint(node)
		},
		Entry("do-not-disrupt annotation", withDoNotDisrupt),
		Entry("nominated for a pending pod", withNominatedNode),
		Entry("buffer pods", withBufferPods),
	)
})

type standbyNodeMutator func(node *corev1.Node, nodeClaim *v1.NodeClaim)

func withDoNotDisrupt(node *corev1.Node, _ *v1.NodeClaim) {
	node.Annotations = lo.Assign(node.Annotations, map[string]string{v1.DoNotDisruptAnnotationKey: "true"})
	ExpectApplied(ctx, env.Client, node)
}

func withNominatedNode(node *corev1.Node, _ *v1.NodeClaim) {
	cluster.NominateNodeForPod(ctx, node.Spec.ProviderID)
	Expect(cluster.UpdateNode(ctx, node)).To(Succeed())
}

func withBufferPods(node *corev1.Node, _ *v1.NodeClaim) {
	cluster.UpdateBufferPodCounts(map[string]int{
		node.Spec.ProviderID: 2,
	})
}

func givenStandbyNode(policy v1.ConsolidationPolicy, extraAnnotations map[string]string) (*v1.NodePool, *v1.NodeClaim, *corev1.Node) {
	nodePool := scoreBasedStandbyNodePool(policy, extraAnnotations, nil)
	nodeClaim, node := standbyTestNodeClaim(nodePool)
	ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
	refreshStandbyState(node, nodeClaim)
	return nodePool, nodeClaim, node
}

func refreshStandbyState(node *corev1.Node, nodeClaim *v1.NodeClaim) {
	var nodes []*corev1.Node
	var nodeClaims []*v1.NodeClaim
	if node != nil {
		nodes = []*corev1.Node{node}
	}
	if nodeClaim != nil {
		nodeClaims = []*v1.NodeClaim{nodeClaim}
	}
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
}

func refreshStandbyStates(nodes []*corev1.Node, nodeClaims []*v1.NodeClaim) {
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
}

func reconcileDisruption() {
	ExpectSingletonReconciled(ctx, disruptionController)
}

func reconcileAfter(d time.Duration, node *corev1.Node, nodeClaim *v1.NodeClaim) {
	if d != 0 {
		env.Clock.Step(d)
	}
	refreshStandbyState(node, nodeClaim)
	reconcileDisruption()
}

func reconcileAfterAll(d time.Duration, nodes []*corev1.Node, nodeClaims []*v1.NodeClaim) {
	if d != 0 {
		env.Clock.Step(d)
	}
	refreshStandbyStates(nodes, nodeClaims)
	reconcileDisruption()
}

func expectMarkedStandby(node *corev1.Node, nodeClaim *v1.NodeClaim) {
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
	expectNodeHasStandbyTaint(node)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), nodeClaim)).To(Succeed())
	Expect(nodeClaim.Annotations[v1.StandbyNodeClaimAnnotationKey]).NotTo(BeEmpty())
}

func expectNodeReclaimed() {
	cmds := queue.GetCommands()
	Expect(cmds).To(HaveLen(1))
	ExpectObjectReconciled(ctx, env.Client, queue, cmds[0].Candidates[0].NodeClaim)
	Expect(len(ExpectNodeClaims(ctx, env.Client))).To(Equal(0))
}

func scoreBasedStandbyNodePool(policy v1.ConsolidationPolicy, extraAnnotations map[string]string, budgets []v1.Budget) *v1.NodePool {
	annotations := map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""}
	for k, v := range extraAnnotations {
		annotations[k] = v
	}
	if budgets == nil {
		budgets = []v1.Budget{{Nodes: "100%", Reasons: []v1.DisruptionReason{v1.DisruptionReasonEmpty}}}
	}
	return test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Annotations: annotations},
		Spec: v1.NodePoolSpec{
			Disruption: v1.Disruption{
				ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				ConsolidationPolicy: policy,
				Budgets:             budgets,
			},
		},
	})
}

func standbyTestNodeClaim(nodePool *v1.NodePool) (*v1.NodeClaim, *corev1.Node) {
	claims, nodes := standbyTestNodeClaims(nodePool, 1)
	return claims[0], nodes[0]
}

func standbyTestNodeClaims(nodePool *v1.NodePool, count int) ([]*v1.NodeClaim, []*corev1.Node) {
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
	for i := range nodeClaims {
		nodeClaims[i].StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
	}
	return nodeClaims, nodes
}

func expectNodeHasStandbyTaint(node *corev1.Node) {
	_, found := lo.Find(node.Spec.Taints, func(taint corev1.Taint) bool {
		return taint.MatchTaint(&v1.StandbyNoScheduleTaint)
	})
	Expect(found).To(BeTrue(), "expected standby taint on node %s", node.Name)
}

func expectNodeWithoutStandbyTaint(node *corev1.Node) {
	_, found := lo.Find(node.Spec.Taints, func(taint corev1.Taint) bool {
		return taint.MatchTaint(&v1.StandbyNoScheduleTaint)
	})
	Expect(found).To(BeFalse(), "expected no standby taint on node %s", node.Name)
}
