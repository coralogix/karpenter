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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/insufficientcapacity"
)

var _ = Describe("Capacity decay", func() {
	BeforeEach(insufficientcapacity.Reset)

	It("reclaims about half the excess standby cores per half-life, oldest first", func() {
		_, nodeClaims, nodes := givenStandbyNodesOldestFirst(4, map[string]string{
			disruption.CapacityDecayHalfLifeAnnotationKey: "2m",
		})
		reconcileStandby()

		reconcileAfterAll(2*time.Minute, nodes, nodeClaims)

		expectReclaimedInOrder(nodes[0], nodes[1])
	})

	It("decays more slowly after a recent insufficient-capacity launch failure", func() {
		nodePool, nodeClaims, nodes := givenStandbyNodesOldestFirst(4, map[string]string{
			disruption.CapacityDecayHalfLifeAnnotationKey:                     "2m",
			disruption.CapacityDecayInsufficientCapacityHalfLifeAnnotationKey: "10m",
		})
		insufficientcapacity.Record(nodePool.Name, env.Clock.Now())
		reconcileStandby()

		reconcileAfterAll(2*time.Minute, nodes, nodeClaims)

		expectReclaimedInOrder(nodes[0])
	})

	It("keeps standby to cover the active capacity of the pools it lists", func() {
		fallbackPool := test.NodePool(v1.NodePool{Spec: v1.NodePoolSpec{Disruption: v1.Disruption{Budgets: []v1.Budget{{Nodes: "0"}}}}})
		ExpectApplied(ctx, env.Client, fallbackPool)
		givenActiveNodes(fallbackPool, 2)
		_, nodeClaims, nodes := givenStandbyNodesOldestFirst(3, map[string]string{
			disruption.CapacityDecayHalfLifeAnnotationKey: "2m",
			disruption.CapacityDecayPoolsAnnotationKey:    " , " + fallbackPool.Name + " ,",
		})
		reconcileStandby()

		reconcileAfterAll(20*time.Minute, nodes, nodeClaims)

		expectReclaimedInOrder(nodes[0])
	})

	It("keeps the reclamation delay and warns when the half-life is invalid", func() {
		nodePool, _, nodes := givenStandbyNodesOldestFirst(1, map[string]string{
			disruption.CapacityDecayHalfLifeAnnotationKey: "not-a-duration",
			v1.ReclamationDelayAnnotationKey:              "0s",
		})

		reconcileStandby()

		expectReclaimedInOrder(nodes[0])
		expectWarning(nodePool, disruption.InvalidCapacityDecayHalfLife)
	})
})

// givenStandbyNodesOldestFirst creates a score-based pool with count standby nodes of 32 cores and no active nodes. The
// nodes entered standby a minute apart, oldest first.
func givenStandbyNodesOldestFirst(count int, annotations map[string]string) (*v1.NodePool, []*v1.NodeClaim, []*corev1.Node) {
	nodePool := scoreBasedStandbyNodePool(v1.ConsolidationPolicyWhenEmpty, annotations, nil)
	nodeClaims, nodes := standbyTestNodeClaims(nodePool, count)
	ExpectApplied(ctx, env.Client, nodePool)
	for i, node := range nodes {
		node.Spec.Taints = append(node.Spec.Taints, v1.StandbyNoScheduleTaint)
		node.Annotations = lo.Assign(node.Annotations, map[string]string{
			v1.StandbyNodeAnnotationKey: env.Clock.Now().Add(time.Duration(i-count) * time.Minute).UTC().Format(time.RFC3339Nano),
		})
		ExpectApplied(ctx, env.Client, nodeClaims[i], node)
	}
	refreshStandbyStates(nodes, nodeClaims)
	return nodePool, nodeClaims, nodes
}

func expectReclaimedInOrder(nodes ...*corev1.Node) {
	cmds := queue.GetCommands()
	Expect(cmds).To(HaveLen(1))
	Expect(lo.Map(cmds[0].Candidates, func(c *disruption.Candidate, _ int) string { return c.Name() })).
		To(Equal(lo.Map(nodes, func(n *corev1.Node, _ int) string { return n.Name })))
}

// givenActiveNodes creates count nodes of 32 cores in nodePool that are not standby.
func givenActiveNodes(nodePool *v1.NodePool, count int) {
	nodeClaims, nodes := standbyTestNodeClaims(nodePool, count)
	for i := range nodes {
		ExpectApplied(ctx, env.Client, nodeClaims[i], nodes[i])
	}
	refreshStandbyStates(nodes, nodeClaims)
}

func expectWarning(nodePool *v1.NodePool, reason string) {
	_, found := lo.Find(recorder.Events(), func(e events.Event) bool {
		obj, ok := e.InvolvedObject.(metav1.Object)
		return e.Type == corev1.EventTypeWarning && e.Reason == reason && ok && obj.GetName() == nodePool.Name
	})
	Expect(found).To(BeTrue(), "expected a %s warning on NodePool %s", reason, nodePool.Name)
}
