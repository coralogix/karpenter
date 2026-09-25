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
	"fmt"
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
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var _ = Describe("ScoreBasedConsolidation", func() {
	var scoreBased *disruption.ScoreBasedConsolidation
	var scoreBasedNodePool *v1.NodePool
	var scoreBasedNodePoolMap map[string]*v1.NodePool
	var scoreBasedInstanceTypeMap map[string]map[string]*cloudprovider.InstanceType

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

		scoreBasedNodePoolMap = map[string]*v1.NodePool{
			scoreBasedNodePool.Name: scoreBasedNodePool,
		}
		scoreBasedInstanceTypeMap = map[string]map[string]*cloudprovider.InstanceType{
			scoreBasedNodePool.Name: {
				leastExpensiveInstance.Name: leastExpensiveInstance,
				mostExpensiveInstance.Name:  mostExpensiveInstance,
			},
		}

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
			cheapCandidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, leastExpensiveInstance)
			Expect(err).To(BeNil())
			expensiveCandidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(BeNil())

			sorted := scoreBased.SortCandidates(append(cheapCandidates, expensiveCandidates...))
			Expect(sorted).To(HaveLen(2))
			Expect(sorted[0].Labels()[corev1.LabelInstanceTypeStable]).To(Equal(mostExpensiveInstance.Name))
			Expect(sorted[1].Labels()[corev1.LabelInstanceTypeStable]).To(Equal(leastExpensiveInstance.Name))
		})
	})

	Context("Reclamation", func() {
		It("should reclaim the rounded-up half despite zero or one NodePool budget", func() {
			scoreBasedNodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
			budgetOneNodePool := test.NodePool(v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{
					Name: "score-based-budget-one-pool",
					Annotations: map[string]string{
						v1.ScoreBasedConsolidationAnnotationKey: "",
					},
				},
				Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					Budgets:             []v1.Budget{{Nodes: "1"}},
				}},
			})

			var nodeClaims []*v1.NodeClaim
			var nodes []*corev1.Node
			objects := []client.Object{scoreBasedNodePool, budgetOneNodePool}
			for _, nodePool := range []*v1.NodePool{scoreBasedNodePool, budgetOneNodePool} {
				for i := 0; i < 3; i++ {
					instanceType := mostExpensiveInstance
					if i == 1 {
						instanceType = leastExpensiveInstance
					}
					offering := instanceType.Offerings[0]
					nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
						ObjectMeta: metav1.ObjectMeta{
							Name: fmt.Sprintf("reclamation-%s-nodeclaim-%d", nodePool.Name, i),
							Labels: map[string]string{
								v1.NodePoolLabelKey:            nodePool.Name,
								corev1.LabelInstanceTypeStable: instanceType.Name,
								v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
								corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
							},
						},
						Status: v1.NodeClaimStatus{
							Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32")},
						},
					})
					node.Name = fmt.Sprintf("reclamation-%s-node-%d", nodePool.Name, i)
					nodeClaims = append(nodeClaims, nodeClaim)
					nodes = append(nodes, node)
					objects = append(objects, nodeClaim, node)
				}
			}
			ExpectApplied(ctx, env.Client, objects...)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, scoreBased.ShouldDisrupt, scoreBased.Class(), queue)
			Expect(err).To(Succeed())
			Expect(candidates).To(HaveLen(6), "empty nodes should be reclamation candidates even before the Consolidatable condition is set")

			var commands []disruption.Command
			ExpectParallelized(
				func() {
					commands, err = scoreBased.ComputeCommands(ctx, map[string]int{
						scoreBasedNodePool.Name: 0,
						budgetOneNodePool.Name:  1,
					}, candidates...)
				},
				func() {
					Eventually(fakeClock.HasWaiters, time.Second*10).Should(BeTrue())
					fakeClock.Step(15 * time.Second)
				},
			)
			Expect(err).To(Succeed())
			Expect(commands).To(HaveLen(1))
			Expect(commands[0].Decision()).To(Equal(disruption.DeleteDecision))
			Expect(commands[0].Action).To(Equal(disruption.DeleteAction))
			Expect(commands[0].Candidates).To(HaveLen(4))
			selectedByPool := map[string][]*disruption.Candidate{}
			for _, candidate := range commands[0].Candidates {
				selectedByPool[candidate.NodePool.Name] = append(selectedByPool[candidate.NodePool.Name], candidate)
			}
			Expect(selectedByPool[scoreBasedNodePool.Name]).To(HaveLen(2))
			Expect(selectedByPool[budgetOneNodePool.Name]).To(HaveLen(2))
			expectedHigherCostPerVCPU := mostExpensiveInstance
			mostExpensiveCPU := mostExpensiveInstance.Capacity[corev1.ResourceCPU]
			leastExpensiveCPU := leastExpensiveInstance.Capacity[corev1.ResourceCPU]
			mostExpensiveCostPerVCPU := mostExpensiveInstance.Offerings[0].Price / mostExpensiveCPU.AsApproximateFloat64()
			leastExpensiveCostPerVCPU := leastExpensiveInstance.Offerings[0].Price / leastExpensiveCPU.AsApproximateFloat64()
			if leastExpensiveCostPerVCPU > mostExpensiveCostPerVCPU {
				expectedHigherCostPerVCPU = leastExpensiveInstance
			}
			for _, candidate := range commands[0].Candidates {
				Expect(candidate.Labels()[corev1.LabelInstanceTypeStable]).To(Equal(expectedHigherCostPerVCPU.Name), "each pool should select its highest cost-per-vCPU empty nodes")
			}
		})

		DescribeTable("should reject an empty node that becomes unsafe during validation", func(unsafeState string) {
			scoreBasedNodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)
			offering := leastExpensiveInstance.Offerings[0]
			nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            scoreBasedNodePool.Name,
						corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32")},
				},
			})
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, scoreBased.ShouldDisrupt, scoreBased.Class(), queue)
			Expect(err).To(Succeed())
			Expect(candidates).To(HaveLen(1))

			var commands []disruption.Command
			ExpectParallelized(
				func() {
					commands, err = scoreBased.ComputeCommands(ctx, map[string]int{scoreBasedNodePool.Name: 0}, candidates...)
				},
				func() {
					Eventually(fakeClock.HasWaiters, time.Second*10).Should(BeTrue())
					switch unsafeState {
					case "nominated":
						cluster.NominateNodeForPod(ctx, node.Spec.ProviderID)
					case "activating":
						stored := &v1.NodeClaim{}
						Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), stored)).To(Succeed())
						standby.SetNodeClaimActivating(stored, true)
						ExpectApplied(ctx, env.Client, stored)
					}
					fakeClock.Step(15 * time.Second)
				},
			)
			Expect(err).To(Succeed())
			Expect(commands).To(BeEmpty())
		},
			Entry("nominated", "nominated"),
			Entry("activating", "activating"),
		)
	})

	Context("Budgets", func() {
		It("should keep non-empty compaction blocked by a zero Underutilized budget", func() {
			scoreBasedNodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)

			candidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(Succeed())
			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, scoreBased.Reason())
			Expect(err).To(Succeed())
			Expect(budgets[scoreBasedNodePool.Name]).To(Equal(0))

			commands, err := scoreBased.ComputeCommands(ctx, budgets, candidates...)
			Expect(err).To(Succeed())
			Expect(commands).To(BeEmpty())
		})
	})

	Context("Empty nodes", func() {
		It("should produce a delete command for empty annotated pool nodes", func() {
			nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            scoreBasedNodePool.Name,
						corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32")},
				},
			})
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))

			limits, err := pdb.NewLimits(ctx, env.Client)
			Expect(err).To(BeNil())
			stateNode := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim)
			candidate, err := disruption.NewCandidate(
				ctx,
				env.Client,
				recorder,
				env.Clock,
				stateNode,
				limits,
				scoreBasedNodePoolMap,
				scoreBasedInstanceTypeMap,
				queue,
				disruption.GracefulDisruptionClass,
			)
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
			Expect(cmds[0].Candidates[0].Name()).To(Equal(node.Name))
		})

		It("should compact only a feasible non-empty node when reclamation is not due", func() {
			scoreBasedNodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey] = fakeClock.Now().Add(-30 * time.Second).Format(time.RFC3339Nano)
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)

			var emptyNodeClaims []*v1.NodeClaim
			var emptyNodes []*corev1.Node
			objects := []client.Object{}
			for i := 0; i < 2; i++ {
				offering := leastExpensiveInstance.Offerings[0]
				nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("non-compaction-nodeclaim-%d", i),
						Labels: map[string]string{
							v1.NodePoolLabelKey:            scoreBasedNodePool.Name,
							corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
							v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
							corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
						},
					},
					Status: v1.NodeClaimStatus{
						Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32")},
					},
				})
				node.Name = fmt.Sprintf("non-compaction-node-%d", i)
				nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
				emptyNodeClaims = append(emptyNodeClaims, nodeClaim)
				emptyNodes = append(emptyNodes, node)
				objects = append(objects, nodeClaim, node)
				if i == 1 {
					daemon := test.Pod(test.PodOptions{ObjectMeta: metav1.ObjectMeta{
						Name:            "daemon-only-pod",
						Namespace:       "default",
						OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "daemon", UID: "daemon-uid"}},
					}})
					daemon.Spec.NodeName = node.Name
					objects = append(objects, daemon)
				}
			}
			ExpectApplied(ctx, env.Client, objects...)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, emptyNodes, emptyNodeClaims)
			for _, nodeClaim := range emptyNodeClaims {
				ExpectApplied(ctx, env.Client, nodeClaim)
				ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))
			}

			nonEmptyCandidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(Succeed())
			expectedCandidate := nonEmptyCandidates[0]
			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, scoreBased.ShouldDisrupt, scoreBased.Class(), queue)
			Expect(err).To(Succeed())
			Expect(candidates).To(HaveLen(1))
			Expect(candidates[0].Name()).To(Equal(expectedCandidate.Name()))

			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, scoreBased.Reason())
			Expect(err).To(Succeed())
			var commands []disruption.Command
			ExpectParallelized(
				func() {
					commands, err = scoreBased.ComputeCommands(ctx, budgets, candidates...)
				},
				func() {
					Eventually(fakeClock.HasWaiters, time.Second*10).Should(BeTrue())
					fakeClock.Step(15 * time.Second)
				},
			)
			Expect(err).To(Succeed())
			Expect(commands).To(HaveLen(1))
			Expect(commands[0].Action).To(Equal(disruption.EvacuateAction))
			Expect(commands[0].Candidates).To(HaveLen(1))
			Expect(commands[0].Candidates[0].Name()).To(Equal(expectedCandidate.Name()))
		})

		It("should not pace empty annotated pool nodes", func() {
			scoreBasedNodePool.Annotations[v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey] = "1"
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)

			underutilizedPace := disruption.NewUnderutilizedConsolidationPace(env.Clock)
			c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, underutilizedPace)
			scoreBasedWithPace := disruption.NewScoreBasedConsolidation(c)

			nonEmptyCandidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(BeNil())
			underutilizedPace.Charge(&disruption.Command{Candidates: nonEmptyCandidates})

			nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						v1.NodePoolLabelKey:            scoreBasedNodePool.Name,
						corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
						v1.CapacityTypeLabelKey:        leastExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
						corev1.LabelTopologyZone:       leastExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any(),
					},
				},
				Status: v1.NodeClaimStatus{
					Allocatable: map[corev1.ResourceName]resource.Quantity{corev1.ResourceCPU: resource.MustParse("32")},
				},
			})
			ExpectApplied(ctx, env.Client, nodeClaim, node)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
			nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
			ExpectApplied(ctx, env.Client, nodeClaim)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(node))
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))

			limits, err := pdb.NewLimits(ctx, env.Client)
			Expect(err).To(BeNil())
			stateNode := ExpectStateNodeExistsForNodeClaim(cluster, nodeClaim)
			candidate, err := disruption.NewCandidate(
				ctx,
				env.Client,
				recorder,
				env.Clock,
				stateNode,
				limits,
				scoreBasedNodePoolMap,
				scoreBasedInstanceTypeMap,
				queue,
				disruption.GracefulDisruptionClass,
			)
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
			Expect(cmds[0].Candidates[0].Name()).To(Equal(node.Name))
		})
	})

	Context("Validation", func() {
		BeforeEach(func() {
			disruption.FailedValidationsTotal.Reset()
		})

		It("should reject a compaction candidate that becomes empty during validation", func() {
			candidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(Succeed())
			candidate := candidates[0]
			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, scoreBased.Reason())
			Expect(err).To(Succeed())

			var commands []disruption.Command
			ExpectParallelized(
				func() {
					commands, err = scoreBased.ComputeCommands(ctx, budgets, candidates...)
				},
				func() {
					Eventually(fakeClock.HasWaiters, time.Second*10).Should(BeTrue())
					pods := &corev1.PodList{}
					Expect(env.Client.List(ctx, pods)).To(Succeed())
					var boundPod *corev1.Pod
					for i := range pods.Items {
						if pods.Items[i].Spec.NodeName == candidate.Node.Name {
							boundPod = &pods.Items[i]
							break
						}
					}
					Expect(boundPod).NotTo(BeNil())
					Expect(env.Client.Delete(ctx, boundPod)).To(Succeed())
					ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(candidate.Node))
					fakeClock.Step(15 * time.Second)
				},
			)
			Expect(err).To(Succeed())
			Expect(commands).To(BeEmpty())
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

func createScoreBasedCandidatesForPool(np *v1.NodePool, instanceType *cloudprovider.InstanceType) ([]*disruption.Candidate, error) {
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
	pod := test.Pod()
	ExpectApplied(ctx, env.Client, nodeClaim, node, pod)
	ExpectManualBinding(ctx, env.Client, pod, node)
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
	candidate, err := disruption.NewCandidate(
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
	if err != nil {
		return nil, err
	}
	return []*disruption.Candidate{candidate}, nil
}
