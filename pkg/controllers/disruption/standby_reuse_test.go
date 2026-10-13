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
	Context("Standby reuse", func() {
		computeScoreBasedEvacuation := func(nodePool *v1.NodePool, source *disruption.Candidate) disruption.Command {
			c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil)
			scoreBased := disruption.NewScoreBasedConsolidation(c)
			disruption.ScoreBasedConsolidationTimeoutDuration = 20 * time.Second

			var cmds []disruption.Command
			var err error
			ExpectParallelized(
				func() {
					cmds, err = scoreBased.ComputeCommands(ctx, map[string]int{nodePool.Name: 1}, source)
				},
				func() {
					Eventually(env.Clock.HasWaiters, 10*time.Second).Should(BeTrue())
					env.Clock.Step(15 * time.Second)
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(cmds).To(HaveLen(1))
			cmds[0].Method = scoreBased
			return cmds[0]
		}

		It("activates a standby destination before evacuating the source", func() {
			queue = testsupport.NewTestQueue(env.Client, recorder, cluster, env.Clock, prov, terminator.NewQueue(env.Client, recorder))
			scoreBasedNodePool := test.NodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{
					Name: "standby-reuse-pool",
					Annotations: map[string]string{
						v1.ScoreBasedConsolidationAnnotationKey: "",
					},
				},
				Spec: v1.NodePoolSpec{
					Disruption: v1.Disruption{
						ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
						ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
						Budgets:             []v1.Budget{{Nodes: "100%"}},
					},
				},
			})
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)

			zone := mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any()
			capacityType := mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any()
			standbyClaim, standbyNode := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					v1.NodePoolLabelKey:            scoreBasedNodePool.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        capacityType,
					corev1.LabelTopologyZone:       zone,
				}},
				Status: v1.NodeClaimStatus{
					Capacity:    leastExpensiveInstance.Capacity,
					Allocatable: leastExpensiveInstance.Capacity,
				},
			})
			standby.SetNodeClaimStandby(standbyClaim, true, env.Clock.Now())
			standby.SetNodeTaint(standbyNode, true)

			sourcePod := test.Pod()
			sourceCandidate, err := createScoreBasedCandidateForPool(scoreBasedNodePool, mostExpensiveInstance, sourcePod)
			Expect(err).ToNot(HaveOccurred())

			ExpectApplied(ctx, env.Client, standbyClaim, standbyNode)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{standbyNode, sourceCandidate.Node}, []*v1.NodeClaim{standbyClaim, sourceCandidate.NodeClaim})

			cmd := computeScoreBasedEvacuation(scoreBasedNodePool, sourceCandidate)
			Expect(cmd.Action).To(Equal(disruption.EvacuateAction))
			Expect(cmd.Replacements).To(BeEmpty())
			Expect(cmd.Results.ExistingNodeToPodMapping()[standbyClaim.Name]).To(ConsistOf(sourcePod))

			Expect(standby.HasNodeTaint(standbyNode)).To(BeTrue())
			Expect(queue.StartCommand(ctx, &cmd)).To(Succeed())

			activatedNode := ExpectNodeExists(ctx, env.Client, standbyNode.Name)
			Expect(standby.HasNodeTaint(activatedNode)).To(BeFalse())
			activatedClaim := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(standbyClaim), activatedClaim)).To(Succeed())
			Expect(standby.IsNodeClaimStandby(activatedClaim)).To(BeFalse())

			sourceNode := ExpectNodeExists(ctx, env.Client, sourceCandidate.Name())
			Expect(sourceNode.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
		})

		It("rejects a standby destination that disappeared before execution", func() {
			scoreBasedNodePool := test.NodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{
					Name: "standby-missing-pool",
					Annotations: map[string]string{
						v1.ScoreBasedConsolidationAnnotationKey: "",
					},
				},
				Spec: v1.NodePoolSpec{
					Disruption: v1.Disruption{
						ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
						ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
						Budgets:             []v1.Budget{{Nodes: "100%"}},
					},
				},
			})
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)

			zone := mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any()
			capacityType := mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any()
			standbyClaim, standbyNode := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					v1.NodePoolLabelKey:            scoreBasedNodePool.Name,
					corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
					v1.CapacityTypeLabelKey:        capacityType,
					corev1.LabelTopologyZone:       zone,
				}},
				Status: v1.NodeClaimStatus{
					Capacity:    leastExpensiveInstance.Capacity,
					Allocatable: leastExpensiveInstance.Capacity,
				},
			})
			standby.SetNodeClaimStandby(standbyClaim, true, env.Clock.Now())
			standby.SetNodeTaint(standbyNode, true)

			sourcePod := test.Pod()
			sourceCandidate, err := createScoreBasedCandidateForPool(scoreBasedNodePool, mostExpensiveInstance, sourcePod)
			Expect(err).ToNot(HaveOccurred())
			ExpectApplied(ctx, env.Client, standbyClaim, standbyNode)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{standbyNode, sourceCandidate.Node}, []*v1.NodeClaim{standbyClaim, sourceCandidate.NodeClaim})

			cmd := computeScoreBasedEvacuation(scoreBasedNodePool, sourceCandidate)
			Expect(cmd.Replacements).To(BeEmpty())

			Expect(env.Client.Delete(ctx, standbyNode)).To(Succeed())
			Expect(env.Client.Delete(ctx, standbyClaim)).To(Succeed())

			cmd.ID = uuid.New()
			Expect(queue.StartCommand(ctx, &cmd)).ToNot(Succeed())
			sourceNode := ExpectNodeExists(ctx, env.Client, sourceCandidate.Name())
			Expect(sourceNode.Spec.Taints).ToNot(ContainElement(v1.DisruptedNoScheduleTaint))
		})
	})
})
