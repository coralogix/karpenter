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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var _ = Describe("Reclamation execution", func() {
	It("records queue-owned reclamation metrics without command callbacks", func() {
		disruption.ReclamationNodeRemovalsTotal.Reset()
		poolName := "reclamation-exec-" + uuid.NewString()
		reclamationPool := test.NodePool(v1.NodePool{
			ObjectMeta: metav1.ObjectMeta{
				Name: poolName,
				Annotations: map[string]string{
					v1.ScoreBasedConsolidationAnnotationKey: "",
				},
			},
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				},
			},
		})
		zone := mostExpensiveOffering.Requirements.Get(corev1.LabelTopologyZone).Any()
		capacityType := mostExpensiveOffering.Requirements.Get(v1.CapacityTypeLabelKey).Any()
		standbyClaim, standbyNode := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				v1.NodePoolLabelKey:            poolName,
				corev1.LabelInstanceTypeStable: leastExpensiveInstance.Name,
				v1.CapacityTypeLabelKey:        capacityType,
				corev1.LabelTopologyZone:       zone,
			}},
			Status: v1.NodeClaimStatus{
				Capacity:    leastExpensiveInstance.Capacity,
				Allocatable: leastExpensiveInstance.Capacity,
			},
		})
		standbySince := env.Clock.Now().Add(-30 * time.Second)
		standby.SetNodeClaimStandby(standbyClaim, true, standbySince)
		standby.SetNodeTaint(standbyNode, true)

		ExpectApplied(ctx, env.Client, reclamationPool, standbyClaim, standbyNode)
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{standbyNode}, []*v1.NodeClaim{standbyClaim})
		stateNode := ExpectStateNodeExistsForNodeClaim(cluster, standbyClaim)

		consolidation := disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil)
		reclamation := disruption.NewReclamation(consolidation)
		cmds, err := reclamation.ComputeCommands(ctx, map[string]int{}, &disruption.Candidate{StateNode: stateNode, NodePool: reclamationPool})
		Expect(err).ToNot(HaveOccurred())
		Expect(cmds).To(HaveLen(1))
		cmds[0].CreationTimestamp = env.Clock.Now()
		cmds[0].ID = uuid.New()

		Expect(queue.StartCommand(ctx, &cmds[0])).To(Succeed())
		ExpectObjectReconciled(ctx, env.Client, queue, stateNode.NodeClaim)
		ExpectMetricCounterValue(disruption.ReclamationNodeRemovalsTotal, 1, map[string]string{metrics.NodePoolLabel: poolName})
		ExpectNodeClaimsCascadeDeletion(ctx, env.Client, standbyClaim)
		ExpectNotFound(ctx, env.Client, standbyClaim, standbyNode)
		Expect(queue.HasAny(stateNode.ProviderID())).To(BeFalse())
	})
})
