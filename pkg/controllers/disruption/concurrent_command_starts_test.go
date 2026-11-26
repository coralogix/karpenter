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

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

// Another loop may act between a command's snapshot and its start (ADR-0012).
var _ = Describe("Concurrent command starts", func() {
	It("reactivates a move's destination that went standby after the snapshot", func() {
		destination := givenActiveDestinationAndSource()

		reconcileThroughValidationDelay(disruptionController, func() { markStandbyAfterSnapshot(destination) })

		Expect(queue.GetCommands()).To(HaveLen(1))
		expectNotStandby(destination)
	})

	It("keeps the standby timestamp of an evacuation source that went standby after the snapshot", func() {
		source, sourceNode, markedAt := givenEvictionStartedAfterSourceWentStandby()

		finishEvacuation(source)

		expectMarkedStandby(sourceNode)
		Expect(sourceNode.Annotations[v1.StandbyNodeAnnotationKey]).To(Equal(markedAt))
	})

	It("lets only one command hold a node", func() {
		nodePool, nodeClaim, node := givenStandbyNode(v1.ConsolidationPolicyWhenEmptyOrUnderutilized, nil)

		errs := startConcurrentlyAndFinish(nodeClaim, driftCommand(nodePool, node), driftCommand(nodePool, node))

		Expect(errs).To(ConsistOf(Succeed(), HaveOccurred()))
		ExpectNotFound(ctx, env.Client, nodeClaim)
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(1))
		Expect(queue.IsEmpty()).To(BeTrue())
	})
})

// givenActiveDestinationAndSource sets up a source a move would empty onto a destination that is not standby, and
// returns the destination. The destination runs a pod, so standby marking leaves it alone.
func givenActiveDestinationAndSource() *corev1.Node {
	GinkgoHelper()
	_, destination, _ := givenStandbyDestinationAndSource()
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, rs)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
	pod := paceReplicaSetPods(rs, 1)[0]
	ExpectApplied(ctx, env.Client, pod)
	ExpectManualBinding(ctx, env.Client, pod, destination)
	_, err := standby.Clear(ctx, env.Client, destination)
	Expect(err).NotTo(HaveOccurred())
	refreshStandbyStates([]*corev1.Node{destination}, nil)
	return destination
}

// markStandbyAfterSnapshot marks the stored Node only. Cluster state, which the command's snapshot came from, still
// sees the node as active. It returns the stored standby timestamp.
func markStandbyAfterSnapshot(node *corev1.Node) string {
	GinkgoHelper()
	Expect(standby.Mark(ctx, env.Client, node, env.Clock.Now())).To(Succeed())
	stored := &corev1.Node{}
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), stored)).To(Succeed())
	return stored.Annotations[v1.StandbyNodeAnnotationKey]
}

// givenEvictionStartedAfterSourceWentStandby queues an evacuation, marks its source standby after the snapshot, steps
// the clock a minute so a fresh mark would differ, and starts the eviction. It returns the source's NodeClaim and Node
// and the source's standby timestamp.
func givenEvictionStartedAfterSourceWentStandby() (*v1.NodeClaim, *corev1.Node, string) {
	GinkgoHelper()
	_, _, source := givenStandbyDestinationAndSource()
	reconcileThroughValidationDelay(disruptionController)
	sourceNode := queuedSourceNode()
	markedAt := markStandbyAfterSnapshot(sourceNode)
	env.Clock.Step(time.Minute)
	ExpectObjectReconciled(ctx, env.Client, queue, source)
	return source, sourceNode, markedAt
}

func driftCommand(nodePool *v1.NodePool, node *corev1.Node) *disruption.Command {
	GinkgoHelper()
	nct := scheduling.NewNodeClaimTemplate(nodePool)
	nct.InstanceTypeOptions = cloudProvider.InstanceTypes
	return &disruption.Command{
		Method:            disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock),
		CreationTimestamp: env.Clock.Now(),
		ID:                uuid.New(),
		Candidates:        []*disruption.Candidate{{StateNode: ExpectStateNodeExists(cluster, node), NodePool: nodePool}},
		Replacements:      []*disruption.Replacement{{NodeClaim: &scheduling.NodeClaim{NodeClaimTemplate: *nct}}},
	}
}

// startConcurrentlyAndFinish starts the commands at once, readies the replacements of the queued commands and lets the
// queue delete the node's NodeClaim. It returns each start's error.
func startConcurrentlyAndFinish(nodeClaim *v1.NodeClaim, cmds ...*disruption.Command) []error {
	GinkgoHelper()
	errs := make([]error, len(cmds))
	ExpectParallelized(lo.Map(cmds, func(cmd *disruption.Command, i int) func() {
		return func() { errs[i] = queue.StartCommand(ctx, cmd) }
	})...)
	for _, cmd := range queue.GetCommands() {
		replacement := &v1.NodeClaim{}
		Expect(env.Client.Get(ctx, client.ObjectKey{Name: cmd.Replacements[0].Name}, replacement)).To(Succeed())
		replacement, replacementNode := ExpectNodeClaimDeployedAndStateUpdated(ctx, env.Client, cluster, cloudProvider, replacement)
		refreshStandbyState(replacementNode, replacement)
	}
	ExpectObjectReconciled(ctx, env.Client, queue, nodeClaim)
	ExpectNodeClaimsCascadeDeletion(ctx, env.Client, nodeClaim)
	return errs
}
