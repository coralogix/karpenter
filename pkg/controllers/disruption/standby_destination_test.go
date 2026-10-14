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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/testsupport"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var _ = Describe("Disruption", func() {
	Context("Standby destinations", func() {
		It("uses new capacity instead of a standby node", func() {
			queue = testsupport.NewTestQueue(env.Client, recorder, cluster, env.Clock, prov, terminator.NewQueue(env.Client, recorder))
			nodePool := test.NodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{
					Name: "standby-destination-pool",
					Annotations: map[string]string{
						v1.ScoreBasedConsolidationAnnotationKey: "",
					},
				},
				Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					Budgets:             []v1.Budget{{Nodes: "100%"}},
				}},
			})
			ExpectApplied(ctx, env.Client, nodePool)

			zone := mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any()
			capacityType := mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any()
			standbyClaim, standbyNode := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        capacityType,
					corev1.LabelTopologyZone:       zone,
				}},
				Status: v1.NodeClaimStatus{Capacity: leastExpensiveInstance.Capacity, Allocatable: leastExpensiveInstance.Capacity},
			})
			standby.SetNodeClaimStandby(standbyClaim, true, env.Clock.Now())
			standby.SetNodeTaint(standbyNode, true)

			sourceCandidate, err := createScoreBasedCandidateForPool(nodePool, mostExpensiveInstance, test.Pod())
			Expect(err).ToNot(HaveOccurred())
			ExpectApplied(ctx, env.Client, standbyClaim, standbyNode)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController,
				[]*corev1.Node{standbyNode, sourceCandidate.Node}, []*v1.NodeClaim{standbyClaim, sourceCandidate.NodeClaim})

			c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil)
			scoreBased := disruption.NewScoreBasedConsolidation(c)
			disruption.ScoreBasedConsolidationTimeoutDuration = 20 * time.Second
			var commands []disruption.Command
			var computeErr error
			ExpectParallelized(
				func() {
					commands, computeErr = scoreBased.ComputeCommands(ctx, map[string]int{nodePool.Name: 1}, sourceCandidate)
				},
				func() {
					Eventually(env.Clock.HasWaiters, 10*time.Second).Should(BeTrue())
					env.Clock.Step(15 * time.Second)
				},
			)
			Expect(computeErr).ToNot(HaveOccurred())
			Expect(commands).To(HaveLen(1))
			Expect(commands[0].Results.ExistingNodeToPodMapping()[standbyClaim.Name]).To(BeEmpty())
			Expect(commands[0].Replacements).ToNot(BeEmpty())
			commands[0].Method = scoreBased

			Expect(queue.StartCommand(ctx, &commands[0])).To(Succeed())

			liveStandbyClaim := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(standbyClaim), liveStandbyClaim)).To(Succeed())
			Expect(standby.IsNodeClaimStandby(liveStandbyClaim)).To(BeTrue())
			liveStandbyNode := &corev1.Node{}
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(standbyNode), liveStandbyNode)).To(Succeed())
			Expect(standby.HasNodeTaint(liveStandbyNode)).To(BeTrue())
		})
	})
})
