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
)

func groupedPacingAnnotations(group, rate string) map[string]string {
	return map[string]string{
		v1.DisruptionPacingGroupAnnotationKey:     group,
		v1.DisruptionPacingPerMinuteAnnotationKey: rate,
		v1.DisruptionPacingPerBatchAnnotationKey:  "1",
	}
}

func groupedPacingNodePool(name, group, rate string, static bool) *v1.NodePool {
	np := v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Annotations: groupedPacingAnnotations(group, rate),
		},
		Spec: v1.NodePoolSpec{
			Disruption: v1.Disruption{
				Budgets:             []v1.Budget{{Nodes: "100%"}},
				ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			},
		},
	}
	if static {
		replicas := int64(1)
		np.Spec.Replicas = &replicas
		return test.StaticNodePool(np)
	}
	return test.NodePool(np)
}

var _ = Describe("Shared disruption pacing", func() {
	It("shares the drift cooldown across NodePools", func() {
		nodePools := []*v1.NodePool{
			groupedPacingNodePool("shared-drift-a", "shared-drift", "0.2", false),
			groupedPacingNodePool("shared-drift-b", "shared-drift", "0.20", false),
		}
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)

		nodeClaims := make([]*v1.NodeClaim, 0, len(nodePools))
		nodes := make([]*corev1.Node, 0, len(nodePools))
		for _, nodePool := range nodePools {
			claims, poolNodes := paceConsolidatableNodes(nodePool, 1)
			claims[0].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			nodeClaims = append(nodeClaims, claims[0])
			nodes = append(nodes, poolNodes[0])
		}
		pods := paceReplicaSetPods(rs, len(nodePools))
		for i := range nodePools {
			ExpectApplied(ctx, env.Client, nodePools[i], nodeClaims[i], nodes[i], pods[i])
			ExpectManualBinding(ctx, env.Client, pods[i], nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		disruptionPacing = disruption.NewDisruptionPacing(env.Clock)
		drift := disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock, disruptionPacing)
		disruptionController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(drift), disruption.WithDisruptionPacing(disruptionPacing))

		ExpectSingletonReconciled(ctx, disruptionController)
		commands := queue.GetCommands()
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates).To(HaveLen(1))
		Expect(commands[0].Candidates[0].NodePool.Annotations[v1.DisruptionPacingGroupAnnotationKey]).To(Equal("shared-drift"))

		// The other pool has a pending candidate, but the group's five-minute
		// cooldown still blocks it until the shared scope becomes eligible.
		env.Clock.Step(5*time.Minute - time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(1))

		env.Clock.Step(time.Second)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(2))
	})

	It("applies the static drift batch cap across the whole method pass", func() {
		nodePools := []*v1.NodePool{
			groupedPacingNodePool("shared-static-a", "shared-static", "0.2", true),
			groupedPacingNodePool("shared-static-b", "shared-static", "0.2", true),
		}
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs)

		nodeClaims := make([]*v1.NodeClaim, 0, len(nodePools))
		nodes := make([]*corev1.Node, 0, len(nodePools))
		for _, nodePool := range nodePools {
			claims, poolNodes := paceConsolidatableNodes(nodePool, 1)
			claims[0].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
			nodeClaims = append(nodeClaims, claims[0])
			nodes = append(nodes, poolNodes[0])
		}
		pods := paceReplicaSetPods(rs, len(nodePools))
		for i := range nodePools {
			ExpectApplied(ctx, env.Client, nodePools[i], nodeClaims[i], nodes[i], pods[i])
			ExpectManualBinding(ctx, env.Client, pods[i], nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		disruptionPacing = disruption.NewDisruptionPacing(env.Clock)
		staticDrift := disruption.NewStaticDrift(cluster, prov, cloudProvider, disruptionPacing)
		disruptionController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(staticDrift), disruption.WithDisruptionPacing(disruptionPacing))

		ExpectSingletonReconciled(ctx, disruptionController)
		commands := queue.GetCommands()
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates).To(HaveLen(1))
		Expect(commands[0].Candidates[0].NodePool.Annotations[v1.DisruptionPacingGroupAnnotationKey]).To(Equal("shared-static"))
	})

	It("blocks a group for an inactive member conflict and resumes after correction", func() {
		activePool := groupedPacingNodePool("shared-conflict-active", "shared-conflict", "0.2", false)
		inactivePool := groupedPacingNodePool("shared-conflict-inactive", "shared-conflict", "0.3", false)
		rs := test.ReplicaSet()
		ExpectApplied(ctx, env.Client, rs, activePool, inactivePool)

		nodeClaims, nodes := paceConsolidatableNodes(activePool, 1)
		nodeClaims[0].StatusConditions().SetTrue(v1.ConditionTypeDrifted)
		pods := paceReplicaSetPods(rs, 1)
		ExpectApplied(ctx, env.Client, nodeClaims[0], nodes[0], pods[0])
		ExpectManualBinding(ctx, env.Client, pods[0], nodes[0])
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		disruptionPacing = disruption.NewDisruptionPacing(env.Clock)
		drift := disruption.NewDrift(env.Client, cluster, prov, recorder, env.Clock, disruptionPacing)
		disruptionController = disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost,
			disruption.WithMethods(drift), disruption.WithDisruptionPacing(disruptionPacing))

		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(BeEmpty())
		warning, found := lo.Find(recorder.Events(), func(event events.Event) bool {
			return event.Type == corev1.EventTypeWarning && event.Reason == "InvalidDisruptionPacing"
		})
		Expect(found).To(BeTrue())
		for _, setting := range []string{"shared-conflict", activePool.Name, inactivePool.Name, "0.2", "0.3"} {
			Expect(warning.Message).To(ContainSubstring(setting))
		}

		inactivePool.Annotations = groupedPacingAnnotations("shared-conflict", "0.2")
		ExpectApplied(ctx, env.Client, inactivePool)
		ExpectSingletonReconciled(ctx, disruptionController)
		Expect(queue.GetCommands()).To(HaveLen(1))
	})
})
