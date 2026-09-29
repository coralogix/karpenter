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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/cxtracing"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var _ = Describe("ScoreBasedConsolidation", func() {
	var scoreBased *disruption.ScoreBasedConsolidation
	var scoreBasedReclamation *disruption.ScoreBasedReclamation
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
		scoreBasedReclamation = disruption.NewScoreBasedReclamation(c)
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
		It("should reclaim all empty nodes despite zero or one NodePool budget", func() {
			Expect(scoreBasedReclamation.Reason()).To(Equal(v1.DisruptionReasonEmpty))
			Expect(scoreBasedReclamation.ConsolidationType()).To(Equal(disruption.ScoreBasedReclamationType))
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

			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, scoreBasedReclamation.ShouldDisrupt, scoreBasedReclamation.Class(), queue)
			Expect(err).To(Succeed())
			Expect(candidates).To(HaveLen(6), "empty nodes should be reclamation candidates even before the Consolidatable condition is set")
			compactionCandidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, scoreBased.ShouldDisrupt, scoreBased.Class(), queue)
			Expect(err).To(Succeed())
			Expect(compactionCandidates).To(BeEmpty(), "empty nodes should not be compaction candidates")

			var commands []disruption.Command
			ExpectParallelized(
				func() {
					commands, err = scoreBasedReclamation.ComputeCommands(ctx, map[string]int{
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
			Expect(commands[0].Candidates).To(HaveLen(6))
			selectedByPool := map[string][]*disruption.Candidate{}
			for _, candidate := range commands[0].Candidates {
				selectedByPool[candidate.NodePool.Name] = append(selectedByPool[candidate.NodePool.Name], candidate)
			}
			Expect(selectedByPool[scoreBasedNodePool.Name]).To(HaveLen(3))
			Expect(selectedByPool[budgetOneNodePool.Name]).To(HaveLen(3))
			expectedHigherCostPerVCPU := mostExpensiveInstance
			mostExpensiveCPU := mostExpensiveInstance.Capacity[corev1.ResourceCPU]
			leastExpensiveCPU := leastExpensiveInstance.Capacity[corev1.ResourceCPU]
			mostExpensiveCostPerVCPU := mostExpensiveInstance.Offerings[0].Price / mostExpensiveCPU.AsApproximateFloat64()
			leastExpensiveCostPerVCPU := leastExpensiveInstance.Offerings[0].Price / leastExpensiveCPU.AsApproximateFloat64()
			if leastExpensiveCostPerVCPU > mostExpensiveCostPerVCPU {
				expectedHigherCostPerVCPU = leastExpensiveInstance
			}
			for _, selected := range selectedByPool {
				Expect(selected[0].Labels()[corev1.LabelInstanceTypeStable]).To(Equal(expectedHigherCostPerVCPU.Name))
				Expect(selected[1].Labels()[corev1.LabelInstanceTypeStable]).To(Equal(expectedHigherCostPerVCPU.Name))
				Expect(selected[2].Labels()[corev1.LabelInstanceTypeStable]).NotTo(Equal(expectedHigherCostPerVCPU.Name), "the experimental all-empty policy also selects the lower-cost node")
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
			candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, scoreBasedReclamation.ShouldDisrupt, scoreBasedReclamation.Class(), queue)
			Expect(err).To(Succeed())
			Expect(candidates).To(HaveLen(1))

			var commands []disruption.Command
			ExpectParallelized(
				func() {
					commands, err = scoreBasedReclamation.ComputeCommands(ctx, map[string]int{scoreBasedNodePool.Name: 0}, candidates...)
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

	Context("Controller", func() {
		It("counts an iteration with no action after all methods complete", func() {
			controller := disruption.NewController(fakeClock, env.Client, prov, cloudProvider, recorder, cluster, queue,
				disruption.WithMethods(&controllerBoundaryMethod{reason: v1.DisruptionReasonEmpty, typeLabel: "loop-metric-test"}))

			result, err := controller.Reconcile(ctx)
			Expect(err).To(Succeed())
			Expect(result.RequeueAfter).To(Equal(10 * time.Second))
			ExpectMetricCounterValue(disruption.DisruptionLoopIterationsTotal, 1, map[string]string{"outcome": "no_action"})
			_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_loop_iterations_total", map[string]string{"outcome": "command"})
			Expect(found).To(BeFalse())
		})

		It("does not count a no-action outcome when a method errors before starting a command", func() {
			_, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(Succeed())

			method := &controllerBoundaryMethod{
				reason:     v1.DisruptionReasonEmpty,
				typeLabel:  "loop-metric-test",
				computeErr: fmt.Errorf("decision failed"),
			}
			controller := disruption.NewController(fakeClock, env.Client, prov, cloudProvider, recorder, cluster, queue, disruption.WithMethods(method))

			_, err = controller.Reconcile(ctx)
			Expect(err).To(HaveOccurred())
			_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_loop_iterations_total", map[string]string{"outcome": "no_action"})
			Expect(found).To(BeFalse())
			_, found = FindMetricWithLabelValues("karpenter_voluntary_disruption_loop_iterations_total", map[string]string{"outcome": "command"})
			Expect(found).To(BeFalse())
		})

		It("traces an iteration with a span for each attempted method", func() {
			previousProvider := otel.GetTracerProvider()
			spanRecorder := tracetest.NewSpanRecorder()
			otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
			DeferCleanup(func() { otel.SetTracerProvider(previousProvider) })

			controller := disruption.NewController(fakeClock, env.Client, prov, cloudProvider, recorder, cluster, queue,
				disruption.WithMethods(NewMethodsWithRealValidator()...))
			parentCtx, endParent := cxtracing.Start(ctx, "incoming")
			_, err := controller.Reconcile(parentCtx)
			endParent()
			Expect(err).To(Succeed())

			spans := map[string]sdktrace.ReadOnlySpan{}
			for _, span := range spanRecorder.Ended() {
				spans[span.Name()] = span
			}
			loop := spans["karpenter.disruption.loop"]
			Expect(loop).NotTo(BeNil())
			Expect(loop.Parent().IsValid()).To(BeFalse())
			Expect(loop.SpanContext().TraceID()).NotTo(Equal(spans["incoming"].SpanContext().TraceID()))
			for _, name := range []string{
				"karpenter.disruption.emptiness",
				"karpenter.disruption.score_based_reclamation",
				"karpenter.disruption.static_drift",
				"karpenter.disruption.drift",
				"karpenter.disruption.multi_node_consolidation",
				"karpenter.disruption.score_based_consolidation",
				"karpenter.disruption.single_node_consolidation",
			} {
				method := spans[name]
				Expect(method).NotTo(BeNil())
				Expect(method.Parent().SpanID()).To(Equal(loop.SpanContext().SpanID()))
			}
		})

		It("keeps scheduling simulations in separate traces from the disruption iteration", func() {
			previousProvider := otel.GetTracerProvider()
			spanRecorder := tracetest.NewSpanRecorder()
			otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
			DeferCleanup(func() { otel.SetTracerProvider(previousProvider) })

			loopCtx, endLoop := cxtracing.Start(ctx, "karpenter.disruption.loop")
			methodCtx, endMethod := cxtracing.Start(loopCtx, "karpenter.disruption.score_based_consolidation")
			simulator, err := disruption.NewSchedulingSimulator(methodCtx, env.Client, cluster, prov)
			Expect(err).To(Succeed())
			_, err = simulator.Simulate(methodCtx)
			Expect(err).To(Succeed())
			endMethod()
			endLoop()

			spans := map[string]sdktrace.ReadOnlySpan{}
			for _, span := range spanRecorder.Ended() {
				spans[span.Name()] = span
			}
			loop := spans["karpenter.disruption.loop"]
			method := spans["karpenter.disruption.score_based_consolidation"]
			simulation := spans["karpenter.disruption.simulate_scheduling"]
			phase := spans["karpenter.disruption.simulate_scheduling.solve"]
			Expect(loop).NotTo(BeNil())
			Expect(method).NotTo(BeNil())
			Expect(simulation).NotTo(BeNil())
			Expect(phase).NotTo(BeNil())
			Expect(method.Parent().SpanID()).To(Equal(loop.SpanContext().SpanID()))
			Expect(simulation.Parent().IsValid()).To(BeFalse())
			Expect(simulation.SpanContext().TraceID()).NotTo(Equal(loop.SpanContext().TraceID()))
			Expect(phase.Parent().SpanID()).To(Equal(simulation.SpanContext().SpanID()))
		})

		It("logs when candidate discovery finds no score-based compaction candidates", func() {
			var output strings.Builder
			logger := funcr.New(func(_, args string) {
				output.WriteString(args)
				output.WriteByte('\n')
			}, funcr.Options{})
			controller := disruption.NewController(fakeClock, env.Client, prov, cloudProvider, recorder, cluster, queue, disruption.WithMethods(scoreBased))

			_, err := controller.Reconcile(log.IntoContext(ctx, logger))
			Expect(err).To(Succeed())
			Expect(output.String()).To(ContainSubstring("score-based consolidation made no move"))
			Expect(output.String()).To(ContainSubstring(`"reason"="no_eligible_candidates"`))
		})

		It("places score-based reclamation immediately after empty consolidation", func() {
			methods := NewMethodsWithRealValidator()
			Expect(methods).To(HaveLen(7))
			Expect(methods[0]).To(BeAssignableToTypeOf(&disruption.Emptiness{}))
			Expect(methods[1]).To(BeAssignableToTypeOf(&disruption.ScoreBasedReclamation{}))
			Expect(methods[2]).To(BeAssignableToTypeOf(&disruption.StaticDrift{}))
		})

		It("retries later methods on the next pass after reclamation starts a command", func() {
			_, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(Succeed())
			reclamation := &controllerBoundaryMethod{reason: v1.DisruptionReasonEmpty, typeLabel: disruption.ScoreBasedReclamationType, commandFirst: true}
			compaction := &controllerBoundaryMethod{reason: v1.DisruptionReasonUnderutilized, typeLabel: disruption.ScoreBasedConsolidationType}
			controller := disruption.NewController(fakeClock, env.Client, prov, cloudProvider, recorder, cluster, queue, disruption.WithMethods(reclamation, compaction))

			result, err := controller.Reconcile(ctx)
			Expect(err).To(Succeed())
			Expect(result.RequeueAfter).To(BeNumerically("<", time.Second))
			ExpectMetricCounterValue(disruption.DisruptionLoopIterationsTotal, 1, map[string]string{"outcome": "command"})
			Expect(reclamation.computeCalls).To(Equal(1))
			Expect(compaction.computeCalls).To(BeZero(), "the controller should stop after reclamation starts a command")
			Expect(queue.GetCommands()).To(HaveLen(1))
			queue.CompleteCommand(queue.GetCommands()[0])

			_, err = controller.Reconcile(ctx)
			Expect(err).To(Succeed())
			ExpectMetricCounterValue(disruption.DisruptionLoopIterationsTotal, 1, map[string]string{"outcome": "no_action"})
			Expect(reclamation.computeCalls).To(Equal(2))
			Expect(compaction.computeCalls).To(Equal(1), "compaction should run on the retry pass")
		})

		It("starts returned commands and reports a later decision error", func() {
			candidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(Succeed())

			computeErr := fmt.Errorf("compaction simulation failed")
			method := partialCommandMethod{computeErr: computeErr}
			controller := disruption.NewController(fakeClock, env.Client, prov, cloudProvider, recorder, cluster, queue, disruption.WithMethods(method))
			defer queue.CompleteCommand(&disruption.Command{Candidates: candidates})

			_, err = controller.Reconcile(ctx)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring(computeErr.Error()))
			Expect(queue.HasAny(candidates[0].ProviderID())).To(BeTrue(), "the valid partial command should be queued despite the decision error")
			ExpectMetricCounterValue(disruption.DisruptionLoopIterationsTotal, 1, map[string]string{"outcome": "command"})
			_, found := FindMetricWithLabelValues("karpenter_voluntary_disruption_loop_iterations_total", map[string]string{"outcome": "no_action"})
			Expect(found).To(BeFalse())
		})
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

		It("reports an exhausted effective budget caused by an in-flight disruption", func() {
			scoreBasedNodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "1"}}
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)
			candidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(Succeed())

			inFlightNodeClaim, inFlightNode := test.NodeClaimAndNode(v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name: "in-flight-budget-nodeclaim",
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
			ExpectApplied(ctx, env.Client, inFlightNodeClaim, inFlightNode)
			ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, []*corev1.Node{inFlightNode}, []*v1.NodeClaim{inFlightNodeClaim})
			ExpectMakeNodesNotReady(ctx, env.Client, inFlightNode)
			ExpectReconcileSucceeded(ctx, nodeStateController, client.ObjectKeyFromObject(inFlightNode))

			recorder.Reset()
			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, scoreBased.Reason())
			Expect(err).To(Succeed())
			Expect(budgets[scoreBasedNodePool.Name]).To(Equal(0))
			Expect(recorder.Calls("DisruptionBlocked")).To(BeZero(), "the configured allowance is positive, so the helper should not report a fully blocking budget")

			var logOutput strings.Builder
			logCtx := log.IntoContext(ctx, funcr.New(func(_, args string) { logOutput.WriteString(args) }, funcr.Options{}))
			commands, err := scoreBased.ComputeCommands(logCtx, budgets, candidates...)
			Expect(err).To(Succeed())
			Expect(commands).To(BeEmpty())
			Expect(recorder.DetectedEvent("No allowed Underutilized disruptions because the NodePool's allowed disruptions are already in flight")).To(BeTrue())
			Expect(logOutput.String()).To(ContainSubstring(`"budgetBlockedNodePools"=["score-based-pool"]`))
		})

		It("does not report effective exhaustion when the configured allowance is zero", func() {
			scoreBasedNodePool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0"}}
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)
			candidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(Succeed())

			recorder.Reset()
			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, scoreBased.Reason())
			Expect(err).To(Succeed())
			Expect(budgets[scoreBasedNodePool.Name]).To(Equal(0))
			Expect(recorder.Calls("DisruptionBlocked")).To(Equal(1), "the budget builder already reports a configured allowance of zero")

			commands, err := scoreBased.ComputeCommands(ctx, budgets, candidates...)
			Expect(err).To(Succeed())
			Expect(commands).To(BeEmpty())
			Expect(recorder.Calls("DisruptionBlocked")).To(Equal(1), "score-based consolidation should not duplicate the configured-zero budget event")
		})
	})

	Context("Pacing", func() {
		It("reports NodePool pacing when the rate limit blocks compaction", func() {
			scoreBasedNodePool.Annotations[v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey] = "1"
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)
			candidates, err := createScoreBasedCandidatesForPool(scoreBasedNodePool, mostExpensiveInstance)
			Expect(err).To(Succeed())

			pace := disruption.NewUnderutilizedConsolidationPace(fakeClock)
			pace.Charge(&disruption.Command{Candidates: candidates})
			paced := disruption.NewScoreBasedConsolidation(disruption.MakeConsolidation(fakeClock, cluster, env.Client, prov, cloudProvider, recorder, queue, pace))
			recorder.Reset()
			var logOutput strings.Builder
			logCtx := log.IntoContext(ctx, funcr.New(func(_, args string) { logOutput.WriteString(args) }, funcr.Options{}))
			commands, err := paced.ComputeCommands(logCtx, map[string]int{scoreBasedNodePool.Name: 1}, candidates...)
			Expect(err).To(Succeed())
			Expect(commands).To(BeEmpty())
			Expect(recorder.DetectedEvent(fmt.Sprintf("Underutilized consolidation is waiting for NodePool pace limit %s=1 disruptions per minute", v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey))).To(BeTrue())
			Expect(recorder.Calls("DisruptionBlocked")).To(Equal(1))
			Expect(logOutput.String()).To(ContainSubstring(`"paceBlockedNodePools"=["score-based-pool"]`))
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
					cmds, err = scoreBasedReclamation.ComputeCommands(ctx, budgetMapping, candidate)
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

		It("should only return non-empty nodes to compaction", func() {

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
			reclamationCandidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, scoreBasedReclamation.ShouldDisrupt, scoreBasedReclamation.Class(), queue)
			Expect(err).To(Succeed())
			Expect(reclamationCandidates).To(HaveLen(2), "reclamation should consider the two empty nodes and exclude the non-empty compaction candidate")

			budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, cluster, fakeClock, env.Client, cloudProvider, recorder, scoreBased.Reason())
			Expect(err).To(Succeed())
			recorder.Reset()
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
			Expect(recorder.Calls("ConsolidationCandidate")).To(Equal(2), "the selected move should emit one event on its Node and NodeClaim")
		})

		It("should not pace empty annotated pool nodes", func() {
			scoreBasedNodePool.Annotations[v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey] = "1"
			ExpectApplied(ctx, env.Client, scoreBasedNodePool)

			underutilizedPace := disruption.NewUnderutilizedConsolidationPace(env.Clock)
			c := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, underutilizedPace)
			scoreBasedWithPace := disruption.NewScoreBasedReclamation(c)

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

			budgetMapping := map[string]int{scoreBasedNodePool.Name: 0}
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

			recorder.Reset()
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
			Expect(recorder.Calls("ConsolidationCandidate")).To(BeZero(), "a rejected move should not be reported as a consolidation candidate")

			Expect(scoreBasedConsolidation.IsConsolidated()).To(BeFalse())
			ExpectMetricCounterValue(disruption.FailedValidationsTotal, 1, map[string]string{disruption.ConsolidationTypeLabel: scoreBasedConsolidation.ConsolidationType()})
		},
			Entry("when a candidate is blocked by budgets", WithUnderutilizedBlockingBudget()),
			Entry("when candidates are filtered out due to pod churn", WithUnderutilizedChurn()),
			Entry("when candidates are filtered out due to candidate being nominated", WithUnderutilizedNodeNomination()),
		)
	})
})

type partialCommandMethod struct {
	computeErr error
}

type controllerBoundaryMethod struct {
	reason       v1.DisruptionReason
	typeLabel    string
	commandFirst bool
	computeErr   error
	computeCalls int
}

func (*controllerBoundaryMethod) ShouldDisrupt(context.Context, *disruption.Candidate) bool {
	return true
}

func (m *controllerBoundaryMethod) ComputeCommands(_ context.Context, _ map[string]int, candidates ...*disruption.Candidate) ([]disruption.Command, error) {
	m.computeCalls++
	if m.commandFirst && m.computeCalls == 1 {
		return []disruption.Command{{Action: disruption.DeleteAction, Candidates: []*disruption.Candidate{candidates[0]}}}, nil
	}
	return nil, m.computeErr
}

func (m *controllerBoundaryMethod) Reason() v1.DisruptionReason { return m.reason }

func (*controllerBoundaryMethod) Class() string { return disruption.GracefulDisruptionClass }

func (m *controllerBoundaryMethod) ConsolidationType() string { return m.typeLabel }

func (m partialCommandMethod) ShouldDisrupt(context.Context, *disruption.Candidate) bool {
	return true
}

func (m partialCommandMethod) ComputeCommands(_ context.Context, _ map[string]int, candidates ...*disruption.Candidate) ([]disruption.Command, error) {
	return []disruption.Command{{
		Action:     disruption.DeleteAction,
		Candidates: []*disruption.Candidate{candidates[0]},
	}}, m.computeErr
}

func (partialCommandMethod) Reason() v1.DisruptionReason { return v1.DisruptionReasonUnderutilized }

func (partialCommandMethod) Class() string { return disruption.GracefulDisruptionClass }

func (partialCommandMethod) ConsolidationType() string { return "partial-command-test" }

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
