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
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/status"
	"github.com/go-logr/logr/funcr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	cloudproviderfake "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("ScoreBasedMemoryConsolidation", func() {
	DescribeTable("uses the pool-wide memory ratio to rank simulated source nodes", func(weight, expectedSourceType, expectedSavings string) {
		const poolName = "memory-consolidation-pool"
		nodePool := test.NodePool(v1.NodePool{
			ObjectMeta: metav1.ObjectMeta{
				Name: poolName,
				Annotations: map[string]string{
					v1.ScoreBasedConsolidationAnnotationKey:       "",
					v1.ScoreBasedMemoryBalanceWeightAnnotationKey: weight,
				},
			},
			Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
				ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
				ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				Budgets:             []v1.Budget{{Nodes: "1"}},
			}},
		})
		nodePool.StatusConditions().SetTrue(status.ConditionReady)

		compactInstance := scoreBasedMemoryInstanceType("memory-c", "8Gi", 0.40)
		richInstance := scoreBasedMemoryInstanceType("memory-r", "32Gi", 0.30)
		anchorInstance := scoreBasedMemoryInstanceType("memory-anchor-c", "8Gi", 0.01)
		instanceTypes := []*cloudprovider.InstanceType{compactInstance, richInstance, anchorInstance}
		cloudProvider.InstanceTypes = instanceTypes
		cloudProvider.InstanceTypesForNodePool[nodePool.Name] = instanceTypes

		nodeClaims, nodes, pods := scoreBasedMemoryConsolidationObjects(nodePool, instanceTypes)
		objects := []client.Object{nodePool}
		for i := range nodeClaims {
			objects = append(objects, nodeClaims[i], nodes[i], pods[i])
		}
		ExpectApplied(ctx, env.Client, objects...)
		for i := range pods {
			ExpectManualBinding(ctx, env.Client, pods[i], nodes[i])
		}
		ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, env.Clock, nodeStateController, nodeClaimStateController, nodes, nodeClaims)

		method := disruption.NewScoreBasedConsolidation(disruption.MakeConsolidation(env.Clock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil))
		controller := disruption.NewController(env.Clock, env.Client, prov, cloudProvider, recorder, cluster, queue, clusterCost, disruption.WithMethods(method))
		var logOutput strings.Builder
		logCtx := log.IntoContext(ctx, funcr.New(func(_, args string) { logOutput.WriteString(args) }, funcr.Options{}))
		done := make(chan struct{})
		var reconcileErr error
		go func() {
			defer close(done)
			_, reconcileErr = controller.Reconcile(logCtx)
		}()
		Eventually(func() bool {
			select {
			case <-done:
				return true
			default:
				return env.Clock.HasWaiters()
			}
		}, 10*time.Second).Should(BeTrue())
		if env.Clock.HasWaiters() {
			env.Clock.Step(15 * time.Second)
		}
		Eventually(done, 10*time.Second).Should(BeClosed())
		Expect(reconcileErr).To(Succeed())

		commands := queue.GetCommands()
		Expect(commands).To(HaveLen(1), "the controller should start the highest-ranked feasible move")
		command := commands[0]
		Expect(command.Action).To(Equal(disruption.EvacuateAction))
		Expect(command.Candidates).To(HaveLen(1), "a one-node Underutilized budget prevents a batch")
		Expect(command.Replacements).To(BeEmpty(), "the remaining nodes have room for every source pod")
		Expect(command.Candidates[0].Labels()[corev1.LabelInstanceTypeStable]).To(Equal(expectedSourceType))
		Expect(command.SourceCost()).To(BeNumerically(">", 0))
		Expect(logOutput.String()).To(ContainSubstring(fmt.Sprintf(`"estimatedSavingsUSDPerHour"=%s`, expectedSavings)), "the selected simulation should report positive hourly savings")
		Expect(logOutput.String()).To(ContainSubstring(`"memoryBalanceCorrection"=`), "the controller should select using the snapshot-based memory score")
	},
		Entry("prefers the higher-priced C-like node when the memory correction is disabled", "0", "memory-c", "0.4"),
		Entry("reverses that choice when memory surplus is weighted", "100", "memory-r", "0.3"),
	)
})

func scoreBasedMemoryInstanceType(name, memory string, price float64) *cloudprovider.InstanceType {
	return cloudproviderfake.NewInstanceType(name,
		cloudproviderfake.WithResources(corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("4"),
			corev1.ResourceMemory: resource.MustParse(memory),
			corev1.ResourcePods:   resource.MustParse("100"),
		}),
		cloudproviderfake.WithOfferings(cloudprovider.Offering{
			Available: true,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone: "test-zone-1",
			}),
			Price: price,
		}),
	)
}

func scoreBasedMemoryConsolidationObjects(nodePool *v1.NodePool, instanceTypes []*cloudprovider.InstanceType) ([]*v1.NodeClaim, []*corev1.Node, []*corev1.Pod) {
	GinkgoHelper()
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var pods []*corev1.Pod
	for i, instanceType := range instanceTypes {
		offering := instanceType.Offerings[0]
		capacity := instanceType.Capacity.DeepCopy()
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("memory-consolidation-nodeclaim-%d", i),
				Labels: map[string]string{
					v1.NodePoolLabelKey:            nodePool.Name,
					corev1.LabelInstanceTypeStable: instanceType.Name,
					corev1.LabelArchStable:         instanceType.Requirements.Get(corev1.LabelArchStable).Any(),
					corev1.LabelOSStable:           instanceType.Requirements.Get(corev1.LabelOSStable).Any(),
					v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
					corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
				},
			},
			Status: v1.NodeClaimStatus{Capacity: capacity.DeepCopy(), Allocatable: capacity.DeepCopy()},
		})
		node.Name = fmt.Sprintf("memory-consolidation-node-%d", i)
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		pod := test.Pod(test.PodOptions{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("memory-consolidation-pod-%d", i)},
			ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			}},
		})
		nodeClaims = append(nodeClaims, nodeClaim)
		nodes = append(nodes, node)
		pods = append(pods, pod)
	}
	return nodeClaims, nodes, pods
}
