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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

var _ = Describe("ScoreBasedMoveSetSearch", func() {
	var (
		sourcePool *v1.NodePool
		keeperPool *v1.NodePool
		method     *disruption.ScoreBasedConsolidation
	)

	BeforeEach(func() {
		sourcePool = moveSetSearchNodePool("move-set-source-pool", true)
		ExpectApplied(ctx, env.Client, sourcePool)
		method = disruption.NewScoreBasedConsolidation(disruption.MakeConsolidation(fakeClock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil))
	})

	AfterEach(func() {
		disruption.ScoreBasedConsolidationTimeoutDuration = 15 * time.Second
	})

	It("batches zero-replacement singleton moves into an N-to-zero evacuation", func() {
		keeperPool = moveSetSearchNodePool("move-set-keeper-pool", false)
		ExpectApplied(ctx, env.Client, keeperPool)
		cloudProvider.InstanceTypesForNodePool[keeperPool.Name] = cloudProvider.InstanceTypes

		keeperType := instanceTypeForCPU(cloudProvider.InstanceTypes, 8)
		Expect(keeperType).NotTo(BeNil())
		createMoveSetSearchNodes(sourcePool, mostExpensiveInstance, []string{"2", "2"})
		createMoveSetSearchNodes(keeperPool, keeperType, []string{""})

		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, method.ShouldDisrupt, method.Class(), queue)
		Expect(err).To(Succeed())
		Expect(candidates).To(HaveLen(2))

		commands := computeMoveSetSearchCommands(method, map[string]int{sourcePool.Name: 2}, candidates...)
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Action).To(Equal(disruption.EvacuateAction))
		Expect(commands[0].Candidates).To(HaveLen(2), "both source nodes should be selected after their zero-replacement singleton moves seed the joint simulation")
		Expect(commands[0].Replacements).To(BeEmpty())
		Expect(commands[0].Results.AllNonPendingPodsScheduled()).To(BeTrue(), "the real scheduler simulation should place both source pods on the keeper")

		// A two-node batch cannot exceed the same NodePool's one-node disruption allowance.
		commands = computeMoveSetSearchCommands(method, map[string]int{sourcePool.Name: 1}, candidates...)
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Candidates).To(HaveLen(1))
	})

	It("finds a positive-savings two-to-one pair when neither singleton has an economic replacement", func() {
		sourceType := moveSetSearchInstanceType("pair-source", "4", "8Gi", 0.30)
		replacementType := moveSetSearchInstanceType("pair-replacement", "8", "16Gi", 0.50)
		Expect(replacementType.Offerings.Cheapest().Price).To(BeNumerically(">", sourceType.Offerings.Cheapest().Price), "one source cannot pay for an economical replacement")
		Expect(replacementType.Offerings.Cheapest().Price).To(BeNumerically("<", sourceType.Offerings.Cheapest().Price*2), "the pair can pay for the replacement")
		cloudProvider.InstanceTypesForNodePool[sourcePool.Name] = []*cloudprovider.InstanceType{sourceType, replacementType}
		createMoveSetSearchNodes(sourcePool, sourceType, []string{"3", "3"})

		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, method.ShouldDisrupt, method.Class(), queue)
		Expect(err).To(Succeed())
		Expect(candidates).To(HaveLen(2))
		for _, candidate := range candidates {
			singleton := disruption.NewScoreBasedConsolidation(disruption.MakeConsolidation(fakeClock, cluster, env.Client, prov, cloudProvider, recorder, queue, nil))
			commands, err := singleton.ComputeCommands(ctx, map[string]int{sourcePool.Name: 2}, candidate)
			Expect(err).To(Succeed())
			Expect(commands).To(BeEmpty(), "a real single-candidate simulation should find no positive-savings command")
		}

		commands := computeMoveSetSearchCommands(method, map[string]int{sourcePool.Name: 2}, candidates...)
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Action).To(Equal(disruption.EvacuateAction))
		Expect(commands[0].Candidates).To(HaveLen(2))
		Expect(commands[0].Replacements).To(HaveLen(1))
		Expect(commands[0].Replacements[0].InstanceTypeOptions[0].Name).To(Equal(replacementType.Name))
		Expect(commands[0].Results.AllNonPendingPodsScheduled()).To(BeTrue(), "both source pods should fit the replacement selected by the joint simulation")

		// The pair costs two disruptions, so a one-node budget must prevent its selection.
		commands = computeMoveSetSearchCommands(method, map[string]int{sourcePool.Name: 1}, candidates...)
		Expect(commands).To(BeEmpty())
	})

	It("keeps the best singleton when individually feasible moves conflict in the joint simulation", func() {
		keeperPool = moveSetSearchNodePool("move-set-conflict-keeper-pool", false)
		ExpectApplied(ctx, env.Client, keeperPool)
		cloudProvider.InstanceTypesForNodePool[keeperPool.Name] = cloudProvider.InstanceTypes

		keeperType := instanceTypeForCPU(cloudProvider.InstanceTypes, 8)
		Expect(keeperType).NotTo(BeNil())
		createMoveSetSearchNodes(sourcePool, mostExpensiveInstance, []string{"4", "4"})
		createMoveSetSearchNodes(keeperPool, keeperType, []string{"4"})

		candidates, err := disruption.GetCandidates(ctx, cluster, env.Client, recorder, fakeClock, cloudProvider, method.ShouldDisrupt, method.Class(), queue)
		Expect(err).To(Succeed())
		Expect(candidates).To(HaveLen(2))

		commands := computeMoveSetSearchCommands(method, map[string]int{sourcePool.Name: 2}, candidates...)
		Expect(commands).To(HaveLen(1))
		Expect(commands[0].Action).To(Equal(disruption.EvacuateAction))
		Expect(commands[0].Candidates).To(HaveLen(1), "the joint move needs a replacement because the keeper cannot fit both pods")
		Expect(commands[0].Replacements).To(BeEmpty())
		Expect(commands[0].Results.AllNonPendingPodsScheduled()).To(BeTrue())
	})
})

func moveSetSearchNodePool(name string, scoreBased bool) *v1.NodePool {
	annotations := map[string]string{}
	if scoreBased {
		annotations[v1.ScoreBasedConsolidationAnnotationKey] = ""
	}
	return test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations},
		Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
			Budgets:             []v1.Budget{{Nodes: "100%"}},
		}},
	})
}

func createMoveSetSearchNodes(nodePool *v1.NodePool, instanceType *cloudprovider.InstanceType, podCPUs []string) {
	GinkgoHelper()
	var nodeClaims []*v1.NodeClaim
	var nodes []*corev1.Node
	var pods []*corev1.Pod
	objects := []client.Object{nodePool}
	offering := instanceType.Offerings[0]
	capacity := instanceType.Capacity.DeepCopy()
	capacity[corev1.ResourcePods] = resource.MustParse("100")
	for i, cpu := range podCPUs {
		nodeClaim, node := test.NodeClaimAndNode(v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("%s-nodeclaim-%d", nodePool.Name, i),
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
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
		nodeClaims = append(nodeClaims, nodeClaim)
		nodes = append(nodes, node)
		objects = append(objects, nodeClaim, node)
		if cpu != "" {
			pods = append(pods, test.Pod(test.PodOptions{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-pod-%d", nodePool.Name, i)},
				ResourceRequirements: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(cpu),
					corev1.ResourceMemory: resource.MustParse("1Gi"),
				}},
			}))
			objects = append(objects, pods[len(pods)-1])
		}
	}
	ExpectApplied(ctx, env.Client, objects...)
	for i := range pods {
		ExpectManualBinding(ctx, env.Client, pods[i], nodes[i])
	}
	ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, env.Client, nodeStateController, nodeClaimStateController, nodes, nodeClaims)
}

func moveSetSearchInstanceType(name, cpu, memory string, price float64) *cloudprovider.InstanceType {
	return fake.NewInstanceType(fake.InstanceTypeOptions{
		Name: name,
		Resources: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse(memory),
			corev1.ResourcePods:   resource.MustParse("100"),
		},
		Offerings: []*cloudprovider.Offering{{
			Available: true,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone: "test-zone-1",
			}),
			Price: price,
		}},
	})
}

func instanceTypeForCPU(instanceTypes []*cloudprovider.InstanceType, cpu int64) *cloudprovider.InstanceType {
	minimumMemory := resource.MustParse("8Gi")
	for _, instanceType := range instanceTypes {
		if instanceType.Capacity.Cpu().Value() == cpu && instanceType.Capacity.Memory().Cmp(minimumMemory) >= 0 && instanceType.Offerings.Cheapest().Requirements.Get(v1.CapacityTypeLabelKey).Any() == v1.CapacityTypeOnDemand {
			return instanceType
		}
	}
	return nil
}

func computeMoveSetSearchCommands(method *disruption.ScoreBasedConsolidation, budgets map[string]int, candidates ...*disruption.Candidate) []disruption.Command {
	GinkgoHelper()
	var commands []disruption.Command
	var err error
	result := make(chan struct{}, 1)
	go func() {
		commands, err = method.ComputeCommands(ctx, budgets, candidates...)
		result <- struct{}{}
	}()
	Eventually(func() bool { return fakeClock.HasWaiters() || len(result) > 0 }, 10*time.Second).Should(BeTrue())
	if fakeClock.HasWaiters() {
		fakeClock.Step(15 * time.Second)
	}
	Eventually(func() bool { return len(result) > 0 }, 10*time.Second).Should(BeTrue())
	<-result
	Expect(err).To(Succeed())
	return commands
}
