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

package provisioning_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var _ = Describe("Provisioning", func() {
	Context("Standby capacity", func() {
		var nodeClaimStateController *informer.NodeClaimController

		BeforeEach(func() {
			clusterCost := cost.NewClusterCost(ctx, cloudProvider, env.Client)
			nodeClaimStateController = informer.NewNodeClaimController(env.Client, cloudProvider, cluster, clusterCost)
		})

		It("keeps standby capacity unavailable to ordinary scheduling", func() {
			nodePool := test.NodePool()
			nodeClaim, node := standbySchedulingNode(nodePool)
			standby.SetNodeClaimStandby(nodeClaim, true, env.Clock.Now())
			standby.SetNodeTaint(node, true)
			ExpectApplied(ctx, env.Client, nodePool, nodeClaim, node)
			ExpectMakeNodeClaimsInitialized(ctx, env.Client, env.Clock, nodeClaim)
			ExpectMakeNodesInitialized(ctx, env.Client, env.Clock, node)
			ExpectReconcileSucceeded(ctx, nodeClaimStateController, client.ObjectKeyFromObject(nodeClaim))
			ExpectReconcileSucceeded(ctx, nodeController, client.ObjectKeyFromObject(node))

			pod := test.UnschedulablePod()
			ExpectApplied(ctx, env.Client, pod)
			results, err := prov.Schedule(ctx)
			Expect(err).ToNot(HaveOccurred())
			Expect(results.ExistingNodeToPodMapping()[nodeClaim.Name]).To(BeEmpty())
			Expect(results.NewNodeClaims).ToNot(BeEmpty())

			storedNode := ExpectNodeExists(ctx, env.Client, node.Name)
			storedClaim := &v1.NodeClaim{}
			Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), storedClaim)).To(Succeed())
			Expect(standby.IsNodeClaimStandby(storedClaim)).To(BeTrue())
			Expect(standby.HasNodeTaint(storedNode)).To(BeTrue())
		})
	})
})

func standbySchedulingNode(nodePool *v1.NodePool) (*v1.NodeClaim, *corev1.Node) {
	its, err := cloudProvider.GetInstanceTypes(ctx, nodePool)
	Expect(err).ToNot(HaveOccurred())
	instanceType := its[0]
	offering := instanceType.Offerings[0]
	nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodePoolLabelKey:            nodePool.Name,
			corev1.LabelInstanceTypeStable: instanceType.Name,
			v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
			corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
		}},
		Status: v1.NodeClaimStatus{
			Capacity:    instanceType.Capacity,
			Allocatable: instanceType.Capacity,
		},
	})
	return nodeClaim, node
}
