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

package disruption

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

func TestScoreBasedMemorySnapshotUsesWholePoolAndExcludesDaemonSetRequests(t *testing.T) {
	ctx := context.Background()
	clk := clocktesting.NewFakeClock(time.Now())
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	cluster := state.NewCluster(clk, kubeClient, nil)

	first := addScoreBasedMemoryStateNode(t, ctx, cluster, "pool", "first", "4", "16Gi", "0.5", "1Gi", "2", "3Gi")
	addScoreBasedMemoryStateNode(t, ctx, cluster, "pool", "second", "4", "8Gi", "0.5", "1Gi", "2", "6Gi")
	config := scoreBasedMemoryConfig{surplus: 0.2, weight: 10}
	configs := map[string]scoreBasedMemoryConfig{"pool": config}

	// The candidate set can contain only the first node, while the scheduling
	// simulator snapshot must retain the second node for the pool-wide baseline.
	fullPool := newScoreBasedMemorySnapshot(cluster.DeepCopyNodes(), configs)
	candidateOnly := newScoreBasedMemorySnapshot(state.StateNodes{first}, configs)
	profile := requireScoreBasedMemoryPool(t, fullPool)
	candidateProfile := requireScoreBasedMemoryPool(t, candidateOnly)
	if len(profile.shapes) != 2 {
		t.Fatalf("full pool profile has %d node shapes, want both active nodes", len(profile.shapes))
	}
	for _, test := range []struct {
		name      string
		got       float64
		want      float64
		tolerance float64
	}{
		{name: "allocatable CPU excluding DaemonSets", got: profile.capacityCPU, want: 7, tolerance: 1e-9},
		{name: "allocatable memory excluding DaemonSets", got: profile.capacityMemory, want: 22 * 1024 * 1024 * 1024, tolerance: 1e-3},
		{name: "non-DaemonSet requested CPU", got: profile.requestCPU, want: 4, tolerance: 1e-9},
		{name: "non-DaemonSet requested memory", got: profile.requestMemory, want: 9 * 1024 * 1024 * 1024, tolerance: 1e-3},
		{name: "capacity memory/CPU ratio", got: profile.capacityRatio, want: (22.0 / 7.0) * 1024 * 1024 * 1024, tolerance: 1e-3},
	} {
		if math.Abs(test.got-test.want) > test.tolerance {
			t.Errorf("pool %s = %v, want %v", test.name, test.got, test.want)
		}
	}
	if profile.pressure == candidateProfile.pressure {
		t.Fatalf("pool-wide pressure %v should include the non-candidate node; candidate-only pressure was %v", profile.pressure, candidateProfile.pressure)
	}
}

func requireScoreBasedMemoryPool(t *testing.T, snapshot *scoreBasedMemorySnapshot) *scoreBasedMemoryPool {
	t.Helper()
	if snapshot == nil {
		t.Fatal("expected memory snapshot")
	}
	pool, found := snapshot.pools["pool"]
	if !found || pool == nil || !pool.valid {
		t.Fatal("expected valid memory profile for pool \"pool\"")
	}
	return pool
}

func TestScoreBasedConsolidationMemoryConfigInvalidatesConsolidatedCache(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	cluster := state.NewCluster(clk, kubeClient, nil)
	method := NewScoreBasedConsolidation(MakeConsolidation(clk, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil))
	candidate := scoreBasedTestCandidate(true)
	method.markConsolidated()

	var output strings.Builder
	ctx := log.IntoContext(context.Background(), funcr.New(func(_, args string) { output.WriteString(args) }, funcr.Options{}))
	if _, err := method.ComputeCommands(ctx, nil, candidate); err != nil {
		t.Fatalf("ComputeCommands() with absent memory annotations = %v", err)
	}
	if !strings.Contains(output.String(), `"reason"="cluster_already_marked_consolidated"`) {
		t.Fatalf("absent memory annotations should preserve consolidated short-circuit, got logs %q", output.String())
	}

	output.Reset()
	candidate.NodePool.Annotations[v1.ScoreBasedMemoryBalanceWeightAnnotationKey] = "1"
	if _, err := method.ComputeCommands(ctx, map[string]int{candidate.NodePool.Name: 0}, candidate); err != nil {
		t.Fatalf("ComputeCommands() after enabling memory balance = %v", err)
	}
	if strings.Contains(output.String(), `"reason"="cluster_already_marked_consolidated"`) {
		t.Fatalf("a memory annotation change should invalidate the consolidated cache, got logs %q", output.String())
	}
}

func TestScoreBasedMoveSetScoreMustBePositiveWithOrWithoutMemoryScoring(t *testing.T) {
	candidate := scoreBasedTestCandidate(true)
	candidate.Price = 1
	command := Command{Candidates: []*Candidate{candidate}}
	compute := func(context.Context, ...*Candidate) (Command, error) { return command, nil }
	scoring := func(enabled bool) func(Command) moveSetScoring {
		return func(Command) moveSetScoring {
			return moveSetScoring{
				Score:         0,
				BaseScore:     1,
				MemoryBalance: scoreBasedMemoryScore{Enabled: enabled, Correction: -1},
			}
		}
	}

	enabledStats := &moveSetSearchStats{}
	if got := evaluateMoveSetWith(context.Background(), moveSet{Nodes: []*Candidate{candidate}}, compute, enabledStats, nil, scoring(true)); got != nil {
		t.Fatal("expected a non-positive memory-adjusted score to veto an otherwise profitable move")
	}
	if got := enabledStats.nonPositiveScoreMoveSetCount(); got != 1 {
		t.Fatalf("enabled non-positive score count = %d, want 1", got)
	}

	disabledStats := &moveSetSearchStats{}
	if got := evaluateMoveSetWith(context.Background(), moveSet{Nodes: []*Candidate{candidate}}, compute, disabledStats, nil, scoring(false)); got != nil {
		t.Fatal("non-positive score should veto a move when memory scoring is disabled too")
	}
	if got := disabledStats.nonPositiveScoreMoveSetCount(); got != 1 {
		t.Fatalf("disabled non-positive score count = %d, want 1", got)
	}
}

func TestScoreBasedMoveSetRejectsUnknownPriceDespitePositiveUtilisationAndMemoryDisabled(t *testing.T) {
	candidate := scoreBasedTestCandidate(true)
	candidate.Price = 1
	candidate.instanceType.Capacity = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
	command := Command{Candidates: []*Candidate{candidate}}
	compute := func(context.Context, ...*Candidate) (Command, error) { return command, nil }
	score := moveSetUtilisationScore(command)
	if score <= 0 {
		t.Fatalf("test setup utilization score = %v, want positive", score)
	}
	scoring := func(Command) moveSetScoring {
		return moveSetScoring{
			Score: score, BaseScore: score,
			Cost: scoreBasedCostScore{Enabled: true, Valid: false},
		}
	}
	stats := &moveSetSearchStats{}
	if got := evaluateMoveSetWith(context.Background(), moveSet{Nodes: []*Candidate{candidate}}, compute, stats, nil, scoring); got != nil {
		t.Fatal("unknown source pricing must reject the move even with positive utilization and memory scoring disabled")
	}
	if got := stats.nonPositiveScoreMoveSetCount(); got != 1 {
		t.Fatalf("invalid-price score count = %d, want 1", got)
	}
}

func addScoreBasedMemoryStateNode(t *testing.T, ctx context.Context, cluster *state.Cluster, poolName, name, cpu, memory, daemonCPU, daemonMemory, workloadCPU, workloadMemory string) *state.StateNode {
	t.Helper()
	providerID := "test://" + name
	labels := map[string]string{
		v1.NodePoolLabelKey:            poolName,
		v1.NodeInitializedLabelKey:     "true",
		v1.NodeRegisteredLabelKey:      "true",
		corev1.LabelInstanceTypeStable: "test.instance",
	}
	nodeClaim := &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim-" + name, Labels: labels},
		Status:     v1.NodeClaimStatus{ProviderID: providerID},
	}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-" + name, Labels: labels},
		Spec:       corev1.NodeSpec{ProviderID: providerID},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse(memory),
		}},
	}
	controller := true
	daemonSetPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "daemon-" + name,
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "DaemonSet", Name: "daemon", Controller: &controller,
			}},
		},
		Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(daemonCPU), corev1.ResourceMemory: resource.MustParse(daemonMemory),
		}}}}},
	}
	workloadPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "workload-" + name, Namespace: "default"},
		Spec: corev1.PodSpec{NodeName: node.Name, Containers: []corev1.Container{{Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(workloadCPU), corev1.ResourceMemory: resource.MustParse(workloadMemory),
		}}}}},
	}
	cluster.UpdateNodeClaim(nodeClaim)
	if err := cluster.UpdateNodeWithPods(ctx, node, []*corev1.Pod{daemonSetPod, workloadPod}); err != nil {
		t.Fatalf("UpdateNodeWithPods(%s) = %v", name, err)
	}
	for stateNode := range cluster.Nodes() {
		if stateNode.Name() == node.Name {
			return stateNode
		}
	}
	t.Fatalf("expected state node %q in cluster", node.Name)
	return nil
}
