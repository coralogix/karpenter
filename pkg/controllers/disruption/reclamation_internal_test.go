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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	fakecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestReclamationCandidateDiscoveryCountsBlockedStandbyAndSkipsOtherNodes(t *testing.T) {
	resetReclamationStandbyNodeMetricsForTest()
	defer resetReclamationStandbyNodeMetricsForTest()

	ctx := context.Background()
	clock := clocktesting.NewFakeClock(time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC))
	pool := reclamationTestPool("enabled-pool")
	zeroPool := reclamationTestPool("zero-pool")
	disabledPool := test.NodePool(v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "disabled-pool"}})
	blockedClaim, blockedNode := reclamationTestNode(pool.Name, "blocked-standby", true, false, false)
	blockedClaim.Annotations[v1.DoNotDisruptAnnotationKey] = "true"
	activeClaim, activeNode := reclamationTestNode(pool.Name, "active-node", false, false, false)
	readyClaim, readyNode := reclamationTestNode(pool.Name, "ready-standby", true, true, false)
	retainedClaim, retainedNode := reclamationTestNode(disabledPool.Name, "retained-standby", true, false, false)
	deletedClaim, deletedNode := reclamationTestNode(pool.Name, "deleted-standby", true, false, true)
	deletedNodeClaimTime := metav1.NewTime(time.Now())
	deletedNode.DeletionTimestamp = &deletedNodeClaimTime
	deletedNode.Finalizers = []string{"test.karpenter.sh/finalizer"}

	baseClient := reclamationTestClient(pool, zeroPool, disabledPool, blockedClaim, blockedNode, activeClaim, activeNode, readyClaim, readyNode, retainedClaim, retainedNode, deletedClaim, deletedNode)
	cluster := state.NewCluster(clock, baseClient, nil)
	for _, nodeClaim := range []*v1.NodeClaim{blockedClaim, activeClaim, readyClaim, retainedClaim, deletedClaim} {
		cluster.UpdateNodeClaim(nodeClaim)
	}
	for _, node := range []*corev1.Node{blockedNode, activeNode, readyNode, retainedNode, deletedNode} {
		if err := cluster.UpdateNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	countingClient := &countingPodListClient{Client: baseClient}
	recorder := events.NewRecorder(&record.FakeRecorder{})
	queue := newTestQueue(countingClient, recorder, cluster, clock, nil)
	reclamation := NewReclamation(MakeConsolidation(clock, cluster, countingClient, nil, fakecloudprovider.NewCloudProvider(), recorder, queue, nil))

	candidates, err := reclamation.candidates(ctx)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, "ready-standby", candidates[0].Node.Name)
	require.Equal(t, 3, countingClient.podListCalls, "the ready standby and retained out-of-scope marker need candidate reads; only the ready node gets an occupancy check, while the unmarked node is skipped")
	assertReclamationStandbyNodeMetric(t, pool.Name, 2, true)
	assertReclamationStandbyNodeMetric(t, disabledPool.Name, 1, true)
	assertReclamationStandbyNodeMetric(t, zeroPool.Name, 0, true)
	assertReclamationStandbyNodeMetric(t, "missing-pool", 0, false)

	updateReclamationStandbyNodeMetrics(map[string]int{"new-pool": 2})
	assertReclamationStandbyNodeMetric(t, pool.Name, 0, false)
	assertReclamationStandbyNodeMetric(t, disabledPool.Name, 0, false)
	assertReclamationStandbyNodeMetric(t, "new-pool", 2, true)
}

type countingPodListClient struct {
	client.Client
	podListCalls int
}

func (c *countingPodListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		c.podListCalls++
	}
	return c.Client.List(ctx, list, opts...)
}

func reclamationTestClient(objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objects...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", reclamationPodNodeIndex).
		Build()
}

func reclamationPodNodeIndex(obj client.Object) []string {
	pod := obj.(*corev1.Pod)
	if pod.Spec.NodeName == "" {
		return nil
	}
	return []string{pod.Spec.NodeName}
}

func reclamationTestPool(name string) *v1.NodePool {
	return test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""}},
		Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		}},
	})
}

func reclamationTestNode(poolName, name string, standbyMarker, standbyTaint, deleting bool) (*v1.NodeClaim, *corev1.Node) {
	providerID := "provider://" + name
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:   name + "-claim",
		Labels: map[string]string{v1.NodePoolLabelKey: poolName},
	}, Status: v1.NodeClaimStatus{ProviderID: providerID}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: name,
		Labels: map[string]string{
			v1.NodePoolLabelKey:            poolName,
			v1.NodeInitializedLabelKey:     "true",
			corev1.LabelInstanceTypeStable: "m6i.large",
		},
	}, Spec: corev1.NodeSpec{ProviderID: providerID}}
	if standbyMarker {
		if nodeClaim.Annotations == nil {
			nodeClaim.Annotations = map[string]string{}
		}
		nodeClaim.Annotations[standby.NodeClaimAnnotationKey] = "true"
	}
	if standbyTaint {
		standby.SetNodeTaint(node, true)
	}
	if deleting {
		deletionTimestamp := metav1.NewTime(time.Now())
		nodeClaim.DeletionTimestamp = &deletionTimestamp
		nodeClaim.Finalizers = []string{"test.karpenter.sh/finalizer"}
	}
	return nodeClaim, node
}

func reclamationCandidate(pool *v1.NodePool, claim *v1.NodeClaim, node *corev1.Node) *Candidate {
	stateNode := state.NewNode()
	stateNode.Node = node
	stateNode.NodeClaim = claim
	return &Candidate{StateNode: stateNode, NodePool: pool}
}

func assertReclamationStandbyNodeMetric(t *testing.T, pool string, want float64, wantFound bool) {
	t.Helper()
	metricFamilies, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range metricFamilies {
		if family.GetName() != "karpenter_nodepools_standby_nodes" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metric.GetLabel()[0].GetValue() == pool {
				if !wantFound {
					t.Fatalf("metric series for nodepool=%q unexpectedly exists", pool)
				}
				if got := metric.GetGauge().GetValue(); got != want {
					t.Fatalf("metric value for nodepool=%q = %v, want %v", pool, got, want)
				}
				return
			}
		}
	}
	if wantFound {
		t.Fatalf("metric series for nodepool=%q was not found", pool)
	}
}

func resetReclamationStandbyNodeMetricsForTest() {
	reclamationStandbyNodeMetricStore.ReplaceAll(nil)
	ReclamationStandbyNodes.Reset()
}

func TestStandbyLifecycleEnabledForReclamation(t *testing.T) {
	tests := []struct {
		name    string
		pool    *v1.NodePool
		enabled bool
	}{
		{
			name: "when-empty policy on annotated pool",
			pool: &v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "score-pool",
					Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""},
				},
				Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmpty,
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				}},
			},
		},
		{
			name: "unannotated pool",
			pool: &v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{Name: "score-pool"},
				Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					ConsolidateAfter:    v1.MustParseNillableDuration(v1.Never),
				}},
			},
		},
		{
			name: "annotated pool ignores consolidateAfter never",
			pool: &v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "score-pool",
					Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""},
				},
				Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
					ConsolidateAfter:    v1.MustParseNillableDuration(v1.Never),
				}},
			},
			enabled: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := standbyLifecycleEnabled(tc.pool); got != tc.enabled {
				t.Fatalf("standbyLifecycleEnabled() = %v, want %v", got, tc.enabled)
			}
		})
	}
}

func TestStandbyReclamationDelayElapsed(t *testing.T) {
	now := time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{
		Name:        "score-pool",
		Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""},
	}}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{}}}
	standby.SetNodeClaimStandby(nodeClaim, true, now.Add(-10*time.Second))
	if standbyReclamationDelayElapsed(nodePool, nodeClaim, now) {
		t.Fatal("expected 10s standby age to remain within the default 15s reclamation delay")
	}
	standby.SetNodeClaimStandby(nodeClaim, true, now.Add(-15*time.Second))
	if standbyReclamationDelayElapsed(nodePool, nodeClaim, now) {
		t.Fatal("expected exactly 15s to remain within the reclamation delay")
	}
	standby.SetNodeClaimStandby(nodeClaim, true, now.Add(-15*time.Second-time.Nanosecond))
	if !standbyReclamationDelayElapsed(nodePool, nodeClaim, now) {
		t.Fatal("expected reclamation delay to elapse just after 15s")
	}
	nodeClaim.Annotations[standby.NodeClaimAnnotationKey] = "true"
	if !standbyReclamationDelayElapsed(nodePool, nodeClaim, now) {
		t.Fatal("legacy standby marker should skip reclamation delay")
	}
	nodePool.Annotations[v1.ReclamationStandbyDelayAnnotationKey] = "0s"
	standby.SetNodeClaimStandby(nodeClaim, true, now)
	if !standbyReclamationDelayElapsed(nodePool, nodeClaim, now) {
		t.Fatal("0s delay should disable the reclamation delay")
	}
}

func TestReclamationSkipsStandbyWithinReclamationDelay(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)
	clock := clocktesting.NewFakeClock(now)
	pool := reclamationTestPool("due-pool")
	youngSince := now.Add(-5 * time.Second)
	youngClaim, youngNode := reclamationTestNode("due-pool", "young-standby", true, true, false)
	standby.SetNodeClaimStandby(youngClaim, true, youngSince)
	kubeClient := reclamationTestClient(pool, youngClaim, youngNode)
	cluster := state.NewCluster(clock, kubeClient, nil)
	cluster.UpdateNodeClaim(youngClaim)
	if err := cluster.UpdateNode(ctx, youngNode); err != nil {
		t.Fatal(err)
	}
	reclamation := NewReclamation(MakeConsolidation(clock, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil))
	candidate := reclamationCandidate(pool, youngClaim, youngNode)
	if reclamation.ShouldDisrupt(ctx, candidate) {
		t.Fatal("ShouldDisrupt() = true, want false while reclamation delay has not elapsed")
	}
}

func TestReclamationComputeCommandSelectsEligibleCandidatesAcrossPools(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 10, 12, 0, 0, 0, time.UTC)
	clock := clocktesting.NewFakeClock(now)
	poolA := reclamationTestPool("pool-a")
	poolB := reclamationTestPool("pool-b")
	standbySince := now.Add(-30 * time.Second)
	claimA, nodeA := reclamationTestNode("pool-a", "ready-a", true, true, false)
	standby.SetNodeClaimStandby(claimA, true, standbySince)
	claimB, nodeB := reclamationTestNode("pool-b", "ready-b", true, true, false)
	standby.SetNodeClaimStandby(claimB, true, standbySince)
	kubeClient := reclamationTestClient(poolA, poolB, claimA, nodeA, claimB, nodeB)
	cluster := state.NewCluster(clock, kubeClient, nil)
	for _, nodeClaim := range []*v1.NodeClaim{claimA, claimB} {
		cluster.UpdateNodeClaim(nodeClaim)
	}
	for _, node := range []*corev1.Node{nodeA, nodeB} {
		if err := cluster.UpdateNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	recorder := events.NewRecorder(&record.FakeRecorder{})
	reclamation := NewReclamation(MakeConsolidation(clock, cluster, kubeClient, nil, nil, recorder, nil, nil))
	cmd := reclamation.computeReclamationCommand(ctx, []*Candidate{
		reclamationCandidate(poolA, claimA, nodeA),
		reclamationCandidate(poolB, claimB, nodeB),
	})
	require.NotNil(t, cmd)
	require.Len(t, cmd.Candidates, 2)
	got := []string{cmd.Candidates[0].Node.Name, cmd.Candidates[1].Node.Name}
	require.Equal(t, []string{"ready-a", "ready-b"}, got)
}

func TestNodeEmptyForReclamation(t *testing.T) {
	ctx := context.Background()
	t.Run("ignores daemonset terminal mirror and terminating workload", func(t *testing.T) {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
		daemon := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "daemon",
				Namespace:       "default",
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "daemon"}},
			},
			Spec: corev1.PodSpec{NodeName: node.Name},
		}
		terminating := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "terminating",
				Namespace:         "default",
				DeletionTimestamp: &metav1.Time{Time: time.Now()},
				Finalizers:        []string{"test.finalizer"},
			},
			Spec: corev1.PodSpec{NodeName: node.Name},
		}
		terminal := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "terminal", Namespace: "default", Finalizers: []string{"test.finalizer"}},
			Spec:       corev1.PodSpec{NodeName: node.Name},
			Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
		}
		mirror := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            "mirror",
				Namespace:       "default",
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Node", Name: node.Name}},
			},
			Spec: corev1.PodSpec{NodeName: node.Name},
		}
		kubeClient := reclamationTestClient(node, daemon, terminating, terminal, mirror)

		if empty, err := nodeEmpty(ctx, kubeClient, node); err != nil || empty {
			t.Fatalf("nodeEmpty() = (%v, %v), want (false, nil) while a nonterminal workload pod is terminating", empty, err)
		}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(terminating), terminating); err != nil {
			t.Fatal(err)
		}
		terminating.Finalizers = nil
		if err := kubeClient.Update(ctx, terminating); err != nil {
			t.Fatal(err)
		}
		if empty, err := nodeEmpty(ctx, kubeClient, node); err != nil || !empty {
			t.Fatalf("nodeEmpty() = (%v, %v), want (true, nil) with only DaemonSet, terminal, and mirror pods bound", empty, err)
		}
	})

	t.Run("treats zero-request bound pod as occupied", func(t *testing.T) {
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-2"}}
		zeroRequestPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "zero-request", Namespace: "default"},
			Spec: corev1.PodSpec{
				NodeName: node.Name,
				Containers: []corev1.Container{{
					Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}},
				}},
			},
		}
		kubeClient := reclamationTestClient(node, zeroRequestPod)
		if empty, err := nodeEmpty(ctx, kubeClient, node); err != nil || empty {
			t.Fatalf("nodeEmpty() = (%v, %v), want (false, nil) for a bound zero-request workload pod", empty, err)
		}
	})
}

func TestReclamationBeforeDeleteRechecksPodsAndStandbyMarker(t *testing.T) {
	ctx := options.ToContext(context.Background(), test.Options())
	now := time.Date(2026, time.September, 25, 13, 0, 0, 0, time.UTC)
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{
		Name:        "score-pool",
		Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""},
	}, Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
		ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
		ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
	}}}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:   "reclamation-claim",
		Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name},
	}, Status: v1.NodeClaimStatus{ProviderID: "provider-1"}}
	nodeClaim.Annotations = map[string]string{standby.NodeClaimAnnotationKey: "true"}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "reclamation-node",
		Labels: map[string]string{
			v1.NodePoolLabelKey:        nodePool.Name,
			v1.NodeInitializedLabelKey: "true",
		},
	}, Spec: corev1.NodeSpec{ProviderID: nodeClaim.Status.ProviderID}}
	standby.SetNodeTaint(node, true)
	kubeClient := reclamationTestClient(nodePool, nodeClaim, node)
	clk := clocktesting.NewFakeClock(now)
	cluster := state.NewCluster(clk, kubeClient, nil)
	queue := newTestQueue(kubeClient, events.NewRecorder(&record.FakeRecorder{}), cluster, clk, nil)
	stateNode := state.NewNode()
	stateNode.Node = node
	stateNode.NodeClaim = nodeClaim
	candidate := &Candidate{StateNode: stateNode, NodePool: nodePool}
	eligible, err := queue.reclamationCandidateBeforeDelete(ctx, candidate)
	require.NoError(t, err)
	require.True(t, eligible, "empty node should be eligible for reclamation")

	workload := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late-pod", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name}}
	if err := kubeClient.Create(ctx, workload); err != nil {
		t.Fatal(err)
	}
	eligible, err = queue.reclamationCandidateBeforeDelete(ctx, candidate)
	require.NoError(t, err)
	require.False(t, eligible, "newly bound pod should make node ineligible for reclamation")
	if err := kubeClient.Delete(ctx, workload); err != nil {
		t.Fatal(err)
	}
	cluster.UpdateNodeClaim(nodeClaim)
	if err := cluster.UpdateNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	cluster.NominateNodeForPod(ctx, candidate.ProviderID())
	eligible, err = queue.reclamationCandidateBeforeDelete(ctx, candidate)
	require.NoError(t, err)
	require.False(t, eligible, "nominated node should be ineligible for reclamation")
	candidate.ClearNomination()

	storedNodeClaim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), storedNodeClaim); err != nil {
		t.Fatal(err)
	}
	standby.SetNodeClaimStandby(storedNodeClaim, false, time.Time{})
	if err := kubeClient.Update(ctx, storedNodeClaim); err != nil {
		t.Fatal(err)
	}
	eligible, err = queue.reclamationCandidateBeforeDelete(ctx, candidate)
	require.NoError(t, err)
	require.False(t, eligible, "node with removed standby marker should be ineligible for reclamation")
}

func TestReclamationRejectsBufferReservedNode(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 25, 13, 0, 0, 0, time.UTC)
	nodePool := reclamationTestPool("score-pool")
	standbySince := now.Add(-30 * time.Second)
	nodeClaim, node := reclamationTestNode(nodePool.Name, "buffer-standby", true, true, false)
	standby.SetNodeClaimStandby(nodeClaim, true, standbySince)
	kubeClient := reclamationTestClient(nodePool, nodeClaim, node)
	clk := clocktesting.NewFakeClock(now)
	cluster := state.NewCluster(clk, kubeClient, nil)
	cluster.UpdateNodeClaim(nodeClaim)
	if err := cluster.UpdateNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	cluster.UpdateBufferPodCounts(map[string]int{node.Spec.ProviderID: 1})
	reclamation := NewReclamation(MakeConsolidation(clk, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil))
	candidate := reclamationCandidate(nodePool, nodeClaim, node)
	if reclamation.ShouldDisrupt(ctx, candidate) {
		t.Fatal("ShouldDisrupt() = true, want false for buffer-reserved standby node")
	}
	queue := newTestQueue(kubeClient, events.NewRecorder(&record.FakeRecorder{}), cluster, clk, nil)
	if eligible, err := queue.reclamationCandidateBeforeDelete(ctx, candidate); err != nil || eligible {
		t.Fatalf("reclamationCandidateBeforeDelete() = (%t, %v), want (false, nil) for buffer reservation", eligible, err)
	}
}
