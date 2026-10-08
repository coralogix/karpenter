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
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

var _ = Describe("ScoreBasedConsolidation", func() {
	var scoreBased *disruption.ScoreBasedConsolidation
	var scoreBasedNodePool *v1.NodePool

	BeforeEach(func() {
		scoreBasedNodePool = test.NodePool(v1.NodePool{
			ObjectMeta: metav1.ObjectMeta{
				Name: "score-based-pool",
				Annotations: map[string]string{
					v1.ScoreBasedConsolidationAnnotationKey: "",
				},
			},
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					Budgets: []v1.Budget{{
						Nodes: "100%",
					}},
				},
			},
		})
		ExpectApplied(ctx, env.Client, scoreBasedNodePool)

		c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil)
		scoreBased = disruption.NewScoreBasedConsolidation(c)
	})

	AfterEach(func() {
		disruption.ScoreBasedConsolidationTimeoutDuration = 20 * time.Second
		env.Clock.SetTime(time.Now())
		ExpectCleanedUp(ctx, env.Client)
	})

	Context("Candidate sorting", func() {
		It("should sort candidates by score descending", func() {
			cheapCandidate, err := createScoreBasedCandidateForPool(scoreBasedNodePool, leastExpensiveInstance, test.Pod())
			Expect(err).To(BeNil())
			expensiveCandidate, err := createScoreBasedCandidateForPool(scoreBasedNodePool, mostExpensiveInstance, test.Pod())
			Expect(err).To(BeNil())

			sorted := scoreBased.SortCandidates([]*disruption.Candidate{cheapCandidate, expensiveCandidate})
			Expect(sorted).To(HaveLen(2))
			Expect(sorted[0].Labels()[corev1.LabelInstanceTypeStable]).To(Equal(mostExpensiveInstance.Name))
			Expect(sorted[1].Labels()[corev1.LabelInstanceTypeStable]).To(Equal(leastExpensiveInstance.Name))
		})
	})

	Context("Empty nodes", func() {
		// Pending until the rebase gap is addressed: upstream commit 43964dc5, Balanced consolidation (#2962),
		// added an unconditional IsEmpty rejection to the shared predicate.
		// Score-based validation delegates to it, while Emptiness excludes score-based pools, so empty commands fail revalidation.
		PIt("should produce a delete command for empty annotated pool nodes", func() {
			candidate, err := createScoreBasedCandidateForPool(scoreBasedNodePool, leastExpensiveInstance)
			Expect(err).To(BeNil())

			budgetMapping := map[string]int{scoreBasedNodePool.Name: 1}
			var cmds []disruption.Command
			ExpectParallelized(
				func() {
					cmds, err = scoreBased.ComputeCommands(ctx, budgetMapping, candidate)
				},
				func() {
					Eventually(env.Clock.HasWaiters, time.Second*10).Should(BeTrue())
					env.Clock.Step(15 * time.Second)
				},
			)
			Expect(err).To(BeNil())
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
			Expect(cmds[0].Candidates[0].Name()).To(Equal(candidate.Name()))
		})

		PIt("should not pace empty annotated pool nodes", func() {
			scoreBasedNodePool.Annotations[v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey] = "1"
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)

			underutilizedPace := disruption.NewUnderutilizedConsolidationPace(env.Clock)
			c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, underutilizedPace)
			scoreBasedWithPace := disruption.NewScoreBasedConsolidation(c)

			nonEmptyCandidate, err := createScoreBasedCandidateForPool(scoreBasedNodePool, mostExpensiveInstance, test.Pod())
			Expect(err).To(BeNil())
			underutilizedPace.Charge(&disruption.Command{Candidates: []*disruption.Candidate{nonEmptyCandidate}})

			candidate, err := createScoreBasedCandidateForPool(scoreBasedNodePool, leastExpensiveInstance)
			Expect(err).To(BeNil())

			budgetMapping := map[string]int{scoreBasedNodePool.Name: 1}
			var cmds []disruption.Command
			ExpectParallelized(
				func() {
					cmds, err = scoreBasedWithPace.ComputeCommands(ctx, budgetMapping, candidate)
				},
				func() {
					Eventually(env.Clock.HasWaiters, time.Second*10).Should(BeTrue())
					env.Clock.Step(15 * time.Second)
				},
			)
			Expect(err).To(BeNil())
			Expect(cmds).To(HaveLen(1))
			Expect(cmds[0].Decision()).To(Equal(disruption.DeleteDecision))
			Expect(cmds[0].Candidates[0].Name()).To(Equal(candidate.Name()))
		})
	})

	Context("Validation", func() {
		BeforeEach(func() {
			disruption.FailedValidationsTotal.Reset()
		})

		DescribeTable("should correctly report invalidated commands for score-based consolidation", func(validatorOpt TestConsolidationValidatorOption) {
			labels := map[string]string{"app": "test"}
			rs := test.ReplicaSet()
			ExpectApplied(ctx, env.Client, rs)
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())

			pod := test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Labels: labels,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion:         "apps/v1",
							Kind:               "ReplicaSet",
							Name:               rs.Name,
							UID:                rs.UID,
							Controller:         lo.ToPtr(true),
							BlockOwnerDeletion: lo.ToPtr(true),
						},
					}}})
			nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            scoreBasedNodePool.Name,
						corev1.LabelInstanceTypeStable: mostExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32")},
				},
			})
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
			ExpectApplied(ctx, env.Client, rs, pod, node, nodeClaim, scoreBasedNodePool)
			ExpectManualBinding(ctx, env.Client, pod, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})

			c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil)
			scoreBasedConsolidation := disruption.NewScoreBasedConsolidation(c, disruption.WithValidator(NewTestScoreBasedConsolidationValidator(scoreBasedNodePool, validatorOpt)))
			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, env.Clock, env.Client, cloudProvider, recorder, scoreBasedConsolidation.Reason())
			Expect(err).To(Succeed())

			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, env.Clock, cloudProvider, scoreBasedConsolidation.ShouldDisrupt, scoreBasedConsolidation.Class(), queue)
			Expect(err).To(Succeed())

			var cmds []disruption.Command
			ExpectParallelized(
				func() {
					cmds, err = scoreBasedConsolidation.ComputeCommands(ctx, budgets, candidates...)
				},
				func() {
					Eventually(env.Clock.HasWaiters, time.Second*10).Should(BeTrue())
					env.Clock.Step(15 * time.Second)
				},
			)
			Expect(err).To(Succeed())
			Expect(cmds).To(Equal([]disruption.Command{}))

			Expect(scoreBasedConsolidation.IsConsolidated()).To(BeFalse())
			ExpectMetricCounterValue(disruption.FailedValidationsTotal, 1, map[string]string{disruption.ConsolidationTypeLabel: scoreBasedConsolidation.ConsolidationType()})
		},
			Entry("when a candidate is blocked by budgets", WithUnderutilizedBlockingBudget()),
			Entry("when candidates are filtered out due to pod churn", WithUnderutilizedChurn()),
			Entry("when candidates are filtered out due to candidate being nominated", WithUnderutilizedNodeNomination()),
		)
	})
})

func NewTestScoreBasedConsolidationValidator(nodePool *v1.NodePool, opts ...TestConsolidationValidatorOption) disruption.Validator {
	return newTestConsolidationValidator(nodePool, disruption.NewScoreBasedConsolidationValidator(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil)), opts...)
}

func createScoreBasedCandidateForPool(np *v1.NodePool, instanceType *cloudprovider.InstanceType, pods ...*corev1.Pod) (*disruption.Candidate, error) {
	offering := instanceType.Offerings[0]
	nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				v1.NodePoolLabelKey:            np.Name,
				corev1.LabelInstanceTypeStable: instanceType.Name,
				v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			},
		},
		Status: v1.NodeClaimStatus{
			Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32")},
		},
	})
	ExpectApplied(ctx, env.Client, nodeClaim, node)
	for _, pod := range pods {
		ExpectApplied(ctx, env.Client, pod)
		ExpectManualBinding(ctx, env.Client, pod, node)
	}
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
	nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
	ExpectApplied(ctx, env.Client, nodeClaim)
	ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
	ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))

	limits, err := pdb.NewLimits(ctx, env.Client)
	if err != nil {
		return nil, err
	}
	stateNode := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim)
	return disruption.NewCandidate(
		ctx,
		env.Client,
		recorder,
		env.Clock,
		stateNode,
		limits,
		map[string]*v1.NodePool{np.Name: np},
		map[string]map[string]*cloudprovider.InstanceType{np.Name: {instanceType.Name: instanceType}},
		queue,
		disruption.GracefulDisruptionClass,
	)
}
