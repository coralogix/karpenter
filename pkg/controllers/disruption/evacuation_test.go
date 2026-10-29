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
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/test"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Evacuation", func() {
	DescribeTable("blocks new pods while evacuating and leaves the source NodeClaim in standby", func(policy v1.ConsolidationPolicy) {
		pool, destination, source := givenStandbyDestinationAndSource()
		pool.Spec.Disruption.ConsolidationPolicy = policy
		pool.Annotations[v1.DisruptionPacingPerMinuteAnnotationKey] = "0.1"
		ExpectApplied(ctx, env.Client, pool)

		reconcileThroughValidationDelay(disruptionController)

		commands := queue.GetCommands()
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates).To(HaveLen(1))
		Expect(commands[0].Reason()).To(Equal(v1.DisruptionReasonUnderutilized))
		Expect(disruptionPacing.CandidateAllowed(pool, 0)).To(BeFalse())
		node := queuedSourceNode()
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
		Expect(node.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
		expectNotStandby(destination)

		result, err := queue.Reconcile(ctx, source)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).NotTo(BeZero())
		expectNodeClaimRetained(source)
		finishEvacuation(source)
		expectMarkedStandby(node)
		Expect(node.Spec.Taints).NotTo(ContainElement(v1.DisruptedNoScheduleTaint))
		Expect(queue.IsEmpty()).To(BeTrue())
	},
		Entry("WhenEmptyOrUnderutilized", v1.ConsolidationPolicyWhenEmptyOrUnderutilized),
		Entry("Balanced", v1.ConsolidationPolicyBalanced),
	)

	It("waits for replacement readiness and leaves both the replacement and standby source", func() {
		source, node, pod := givenEvacuationSource()

		reconcileThroughValidationDelay(disruptionController)

		commands := queue.GetCommands()
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Replacements).To(HaveLen(1))
		replacement := &v1.NodeClaim{}
		Expect(env.Client.Get(ctx, client.ObjectKey{Name: commands[0].Replacements[0].Name}, replacement)).To(Succeed())
		ExpectObjectReconciled(ctx, env.Client, queue, source)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
		replacement, replacementNode := ExpectNodeClaimDeployedAndStateUpdated(ctx, env.Client, cluster, cloudProvider, replacement)
		refreshStandbyState(replacementNode, replacement)
		ExpectObjectReconciled(ctx, env.Client, queue, source)

		finishEvacuation(source)
		expectMarkedStandby(node)
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(2))
		expectNodeClaimRetained(replacement)
	})

	It("does not start when the Underutilized budget is zero", func() {
		pool, _, source := givenStandbyDestinationAndSource()
		pool.Spec.Disruption.Budgets = []v1.Budget{{Nodes: "0", Reasons: []v1.DisruptionReason{v1.DisruptionReasonUnderutilized}}}
		ExpectApplied(ctx, env.Client, pool)

		reconcileDisruption()

		Expect(queue.IsEmpty()).To(BeTrue())
		expectNodeClaimRetained(source)
	})

	DescribeTable("stops retrying blocked evictions after evacuation times out", func(annotation string, timeout time.Duration) {
		pool, _, source := givenStandbyDestinationAndSource()
		if annotation != "" {
			pool.Annotations["karpenter.coralogix.net/evacuation-timeout"] = annotation
			ExpectApplied(ctx, env.Client, pool)
		}
		reconcileThroughValidationDelay(disruptionController)
		sourceNode := queuedSourceNode()
		pods := &corev1.PodList{}
		Expect(env.Client.List(ctx, pods)).To(Succeed())
		Expect(pods.Items).To(HaveLen(1))
		pod := &pods.Items[0]
		pdb := test.PodDisruptionBudget(test.PDBOptions{
			Labels: pod.Labels, MaxUnavailable: lo.ToPtr(intstr.FromInt(0)),
		})
		ExpectApplied(ctx, env.Client, pdb)
		ExpectObjectReconciled(ctx, env.Client, queue, source)
		env.Clock.Step(timeout - time.Second)
		ExpectObjectReconciled(ctx, env.Client, queue, source)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(sourceNode), sourceNode)).To(Succeed())
		Expect(sourceNode.Spec.Taints).To(ContainElement(v1.DisruptedNoScheduleTaint))
		env.Clock.Step(2 * time.Second)
		ExpectDeleted(ctx, env.Client, pdb)
		ExpectObjectReconciled(ctx, env.Client, queue, source)
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.DeletionTimestamp.IsZero()).To(BeTrue())
		Expect(queue.IsEmpty()).To(BeTrue())
		expectNoDisruptedTaint(sourceNode)
		expectNodeClaimRetained(source)
	}, Entry("default timeout", "", 5*time.Minute), Entry("custom timeout", "20m", 20*time.Minute))

	It("does not evacuate a node running a pod that tolerates the disruption taint", func() {
		_, _, source := givenStandbyDestinationAndSource()
		pods := &corev1.PodList{}
		Expect(env.Client.List(ctx, pods)).To(Succeed())
		Expect(pods.Items).To(HaveLen(1))
		pod := &pods.Items[0]
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{Key: v1.DisruptedNoScheduleTaint.Key, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
		ExpectApplied(ctx, env.Client, pod)
		reconcileDisruptionWithOptionalValidationDelay()
		Expect(queue.IsEmpty()).To(BeTrue())
		expectNodeClaimRetained(source)
	})

	DescribeTable("recovers an interrupted evacuation after restart", func(empty bool) {
		_, destination, source := givenStandbyDestinationAndSource()
		reconcileThroughValidationDelay(disruptionController)
		sourceNode := queuedSourceNode()
		restartDuringEvacuation(empty, sourceNode, destination)

		reconcileDisruption()

		expectNoDisruptedTaint(sourceNode)
		if empty {
			expectMarkedStandby(sourceNode)
		} else {
			refreshStandbyStates([]*corev1.Node{sourceNode, destination}, ExpectNodeClaims(ctx, env.Client))
			reconcileThroughValidationDelay(disruptionController)
			Expect(queue.GetCommands()).To(HaveLen(1))
			ExpectObjectReconciled(ctx, env.Client, queue, source)
			finishEvacuation(source)
			expectMarkedStandby(sourceNode)
		}
		expectNodeClaimRetained(source)
	}, Entry("already empty", true), Entry("still running pods", false))
})

// reconcileDisruptionWithOptionalValidationDelay runs reconcileDisruption and, if it blocks on the score-based validation delay, steps the fake clock past it.
func reconcileDisruptionWithOptionalValidationDelay() {
	GinkgoHelper()
	done := make(chan struct{})
	ExpectParallelized(
		func() {
			defer close(done)
			reconcileDisruption()
		},
		func() {
			for {
				select {
				case <-done:
					return
				default:
					if env.Clock.HasWaiters() {
						env.Clock.Step(15 * time.Second)
					}
					time.Sleep(time.Millisecond)
				}
			}
		},
	)
}

// finishEvacuation drives the eviction API and simulates kubelet completing pod deletion.
func finishEvacuation(source *v1.NodeClaim) {
	GinkgoHelper()
	sourceNode := queuedSourceNode()
	pods := &corev1.PodList{}
	Expect(env.Client.List(ctx, pods)).To(Succeed())
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Spec.NodeName != sourceNode.Name {
			continue
		}
		Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(pod), pod)).To(Succeed())
		Expect(pod.DeletionTimestamp.IsZero()).To(BeFalse())
		ExpectDeleted(ctx, env.Client, pod)
	}
	ExpectObjectReconciled(ctx, env.Client, queue, source)
	expectNodeClaimRetained(source)
	Expect(cluster.DeepCopyNodes().Active()).To(ContainElement(HaveField("NodeClaim.Name", source.Name)))
}

func expectNodeClaimRetained(nodeClaim *v1.NodeClaim) {
	GinkgoHelper()
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), nodeClaim)).To(Succeed())
	Expect(nodeClaim.DeletionTimestamp.IsZero()).To(BeTrue())
}

func givenEvacuationSource() (*v1.NodeClaim, *corev1.Node, *corev1.Pod) {
	GinkgoHelper()
	expensive := paceOnDemandInstanceType("evacuation-source", 10)
	cheap := paceOnDemandInstanceType("evacuation-replacement", 1)
	cloudProvider.InstanceTypes = []*cloudprovider.InstanceType{expensive, cheap}
	clusterCost.Reset()
	ExpectSingletonReconciled(ctx, pricingController)
	pool := scoreBasedStandbyNodePool(v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
		map[string]string{v1.ReclamationDelayAnnotationKey: "1h"}, []v1.Budget{{Nodes: "100%"}})
	pool.Spec.Template.Spec.Requirements = append(pool.Spec.Template.Spec.Requirements, v1.NodeSelectorRequirementWithMinValues{
		Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand},
	})
	source, node := standbyTestNodeClaim(pool)
	source.Labels[corev1.LabelInstanceTypeStable] = expensive.Name
	source.Labels[corev1.LabelTopologyZone] = "test-zone-1a"
	node.Labels = lo.Assign(node.Labels, source.Labels)
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, pool, source, node, rs)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
	pod := paceReplicaSetPods(rs, 1)[0]
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
	ExpectApplied(ctx, env.Client, pod)
	ExpectManualBinding(ctx, env.Client, pod, node)
	refreshStandbyState(node, source)
	return source, node, pod
}

func restartDuringEvacuation(empty bool, sourceNode, destination *corev1.Node) {
	GinkgoHelper()
	if empty {
		pods := &corev1.PodList{}
		Expect(env.Client.List(ctx, pods)).To(Succeed())
		for i := range pods.Items {
			ExpectDeleted(ctx, env.Client, &pods.Items[i])
		}
	}
	// A new controller process has neither queued commands nor deletion marks or nominations.
	cluster.Reset()
	*queue = lo.FromPtr(disruption.NewQueue(env.Client, recorder, cluster, env.Clock, prov))
	refreshStandbyStates([]*corev1.Node{sourceNode, destination}, ExpectNodeClaims(ctx, env.Client))
}

func queuedSourceNode() *corev1.Node {
	GinkgoHelper()
	return queue.GetCommands()[0].Candidates[0].Node
}

func expectNoDisruptedTaint(node *corev1.Node) {
	GinkgoHelper()
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
	Expect(node.Spec.Taints).NotTo(ContainElement(v1.DisruptedNoScheduleTaint))
}
