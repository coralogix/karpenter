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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("Score-based savings selection", func() {
	It("evacuates the higher-saving source and leaves it standby", func() {
		source, node, otherNode := givenTwoEvacuationSources()
		reconcileThroughValidationDelay(disruptionController)
		commands := queue.GetCommands()
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates[0].Name()).To(Equal(node.Name))
		ExpectObjectReconciled(ctx, env.Client, queue, source)
		finishEvacuation(source)
		expectMarkedStandby(node)
		expectNodeWithoutStandbyTaint(otherNode)
	})
})

func givenTwoEvacuationSources() (*v1.NodeClaim, *corev1.Node, *corev1.Node) {
	GinkgoHelper()
	source, node, _ := givenEvacuationSource()
	pool := &v1.NodePool{}
	Expect(env.Client.Get(ctx, client.ObjectKey{Name: source.Labels[v1.NodePoolLabelKey]}, pool)).To(Succeed())
	medium := paceOnDemandInstanceType("less-expensive-source", 5)
	cloudProvider.InstanceTypes = append(cloudProvider.InstanceTypes, medium)
	clusterCost.Reset()
	ExpectSingletonReconciled(ctx, pricingController)
	other, otherNode := standbyTestNodeClaim(pool)
	other.Labels[corev1.LabelInstanceTypeStable] = medium.Name
	other.Labels[corev1.LabelTopologyZone] = "test-zone-1a"
	otherNode.Labels = other.Labels
	rs := test.ReplicaSet()
	ExpectApplied(ctx, env.Client, other, otherNode, rs)
	Expect(env.Client.Get(ctx, client.ObjectKeyFromObject(rs), rs)).To(Succeed())
	pod := paceReplicaSetPods(rs, 1)[0]
	pod.Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
	ExpectApplied(ctx, env.Client, pod)
	ExpectManualBinding(ctx, env.Client, pod, otherNode)
	refreshStandbyState(otherNode, other)
	return source, node, otherNode
}
