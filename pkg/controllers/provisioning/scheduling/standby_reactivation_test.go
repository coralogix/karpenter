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

package scheduling_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var _ = Describe("Standby reactivation", func() {
	var nodePool *v1.NodePool
	BeforeEach(func() {
		nodePool = test.NodePool()
		ExpectApplied(ctx, env.Client, nodePool)
	})

	It("places a pending pod on a standby node and clears standby without a new NodeClaim", func() {
		nodeClaim, node := givenMarkedStandbyNode(nodePool)
		pod := test.UnschedulablePod()

		ExpectProvisioned(ctx, env.Client, cluster, cloudProvider, prov, pod)

		Expect(ExpectScheduled(ctx, env.Client, pod).Name).To(Equal(node.Name))
		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(1))
		expectNotStandby(node, nodeClaim)
		Expect(cluster.IsNodeNominated(node.Spec.ProviderID)).To(BeTrue())
	})

	It("places a buffer pod on a standby node and clears standby without nominating the node", func() {
		nodeClaim, node := givenMarkedStandbyNode(nodePool)
		bufferCtx, bufferProvisioner := givenCapacityBuffer(1)

		ExpectProvisioned(bufferCtx, env.Client, cluster, cloudProvider, bufferProvisioner)

		Expect(ExpectNodeClaims(ctx, env.Client)).To(HaveLen(1))
		expectNotStandby(node, nodeClaim)
		Expect(cluster.IsNodeNominated(node.Spec.ProviderID)).To(BeFalse())
	})
})

// givenCapacityBuffer applies a ready CapacityBuffer and returns a context with buffers enabled and a provisioner whose
// virtual pod cache has not been populated yet, so it reads the buffer.
func givenCapacityBuffer(replicas int32) (context.Context, *provisioning.Provisioner) {
	buffer := test.ReadyBuffer(test.RandomName(), replicas)
	buffer.UID = ""
	buffer.Status.Conditions[0].LastTransitionTime = metav1.Now()
	template := &corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: buffer.Spec.PodTemplateRef.Name, Namespace: buffer.Namespace},
		Template:   corev1.PodTemplateSpec{Spec: test.Pod().Spec},
	}
	ExpectApplied(ctx, env.Client, template, buffer)
	DeferCleanup(func() { ExpectDeleted(ctx, env.Client, template, buffer) })
	bufferCtx := options.ToContext(ctx, test.Options(test.OptionsFields{FeatureGates: test.FeatureGates{CapacityBuffer: lo.ToPtr(true)}}))
	return bufferCtx, provisioning.NewProvisioner(env.Client, events.NewRecorder(&record.FakeRecorder{}), cloudProvider, cluster, env.Clock,
		deviceallocation.NewController(env.Client), virtualpods.NewVirtualPodCache(env.Client))
}

func givenMarkedStandbyNode(nodePool *v1.NodePool) (*v1.NodeClaim, *corev1.Node) {
	nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      map[string]string{v1.NodePoolLabelKey: nodePool.Name},
			Annotations: map[string]string{v1.StandbyNodeClaimAnnotationKey: env.Clock.Now().UTC().Format(time.RFC3339Nano)},
		},
		Spec: v1.NodeClaimSpec{Taints: []corev1.Taint{v1.StandbyNoScheduleTaint}},
		Status: v1.NodeClaimStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10"),
				corev1.ResourceMemory: resource.MustParse("10Gi"),
				corev1.ResourcePods:   resource.MustParse("110"),
			},
		},
	})
	ExpectApplied(ctx, env.Client, nodeClaim, node)
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, []*corev1.Node{node}, []*v1.NodeClaim{nodeClaim})
	return nodeClaim, node
}

func expectNotStandby(node *corev1.Node, nodeClaim *v1.NodeClaim) {
	GinkgoHelper()
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
	Expect(standby.HasStandbyTaint(node)).To(BeFalse())
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(nodeClaim), nodeClaim)).To(Succeed())
	Expect(nodeClaim.Annotations).NotTo(HaveKey(v1.StandbyNodeClaimAnnotationKey))
}
