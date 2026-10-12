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
	"errors"
	"testing"
	"time"

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
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestReclamationEmptyNodeMetricsIncludeAllConfiguredPools(t *testing.T) {
	resetScoreBasedEmptyNodeMetricForTest()
	defer resetScoreBasedEmptyNodeMetricForTest()

	ctx := context.Background()
	now := time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
	clock := clocktesting.NewFakeClock(now)
	duePool := reclamationTestPool("due-pool")
	otherPool := reclamationTestPool("other-pool")
	zeroPool := reclamationTestPool("zero-pool")
	unconfiguredPool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "unconfigured-pool"}}

	standbyClaim, standbyNode := reclamationTestNode("due-pool", "standby-node", true, true, false)
	otherClaim, otherNode := reclamationTestNode("due-pool", "other-node", false, false, false)
	emptyOtherClaim, emptyOtherNode := reclamationTestNode("due-pool", "empty-other-node", false, false, false)
	markerOnlyClaim, markerOnlyNode := reclamationTestNode("other-pool", "marker-only-node", true, false, false)
	terminatingClaim, terminatingNode := reclamationTestNode("due-pool", "terminating-node", false, false, true)
	workload := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bound", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: otherNode.Name}}
	kubeClient := reclamationTestClient(duePool, otherPool, zeroPool, unconfiguredPool, standbyClaim, standbyNode, otherClaim, otherNode, emptyOtherClaim, emptyOtherNode, markerOnlyClaim, markerOnlyNode, terminatingClaim, terminatingNode, workload)
	cluster := state.NewCluster(clock, kubeClient, nil)
	for _, nodeClaim := range []*v1.NodeClaim{standbyClaim, otherClaim, emptyOtherClaim, markerOnlyClaim, terminatingClaim} {
		cluster.UpdateNodeClaim(nodeClaim)
	}
	for _, node := range []*corev1.Node{standbyNode, otherNode, emptyOtherNode, markerOnlyNode, terminatingNode} {
		if err := cluster.UpdateNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	consolidator := MakeConsolidation(clock, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	reclamation := NewReclamation(consolidator)

	reclamation.refreshReclamationInventory(ctx)
	assertScoreBasedEmptyNodeMetric(t, "due-pool", reclamationEmptyNodeStateStandby, 1, true)
	assertScoreBasedEmptyNodeMetric(t, "due-pool", reclamationEmptyNodeStateOther, 1, true)
	assertScoreBasedEmptyNodeMetric(t, "other-pool", reclamationEmptyNodeStateStandby, 0, true)
	assertScoreBasedEmptyNodeMetric(t, "other-pool", reclamationEmptyNodeStateOther, 1, true)
	assertScoreBasedEmptyNodeMetric(t, "zero-pool", reclamationEmptyNodeStateStandby, 0, true)
	assertScoreBasedEmptyNodeMetric(t, "zero-pool", reclamationEmptyNodeStateOther, 0, true)
	assertScoreBasedEmptyNodeMetric(t, "unconfigured-pool", reclamationEmptyNodeStateOther, 0, false)
}

func TestReclamationEmptyNodeMetricsKeepLastCompleteScanAndRemoveOutOfScopePools(t *testing.T) {
	resetScoreBasedEmptyNodeMetricForTest()
	defer resetScoreBasedEmptyNodeMetricForTest()

	ctx := context.Background()
	clock := clocktesting.NewFakeClock(time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC))
	oldPool := reclamationTestPool("old-pool")
	oldClaim, oldNode := reclamationTestNode("old-pool", "old-node", false, false, false)
	kubeClient := reclamationTestClient(oldPool, oldClaim, oldNode)
	cluster := state.NewCluster(clock, kubeClient, nil)
	cluster.UpdateNodeClaim(oldClaim)
	if err := cluster.UpdateNode(ctx, oldNode); err != nil {
		t.Fatal(err)
	}
	consolidator := MakeConsolidation(clock, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	reclamation := NewReclamation(consolidator)
	reclamation.refreshReclamationInventory(ctx)
	assertScoreBasedEmptyNodeMetric(t, "old-pool", reclamationEmptyNodeStateOther, 1, true)

	newPool := reclamationTestPool("new-pool")
	newClaim, newNode := reclamationTestNode("new-pool", "new-node", false, false, false)
	if err := kubeClient.Delete(ctx, oldPool); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Create(ctx, newPool); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Create(ctx, newClaim); err != nil {
		t.Fatal(err)
	}
	if err := kubeClient.Create(ctx, newNode); err != nil {
		t.Fatal(err)
	}
	cluster.UpdateNodeClaim(newClaim)
	if err := cluster.UpdateNode(ctx, newNode); err != nil {
		t.Fatal(err)
	}

	reclamation.kubeClient = errorOnPodListClient{Client: kubeClient, err: errors.New("pod list failed")}
	reclamation.refreshReclamationInventory(ctx)
	assertScoreBasedEmptyNodeMetric(t, "old-pool", reclamationEmptyNodeStateOther, 1, true)
	assertScoreBasedEmptyNodeMetric(t, "new-pool", reclamationEmptyNodeStateOther, 0, false)

	reclamation.kubeClient = kubeClient
	reclamation.refreshReclamationInventory(ctx)
	assertScoreBasedEmptyNodeMetric(t, "old-pool", reclamationEmptyNodeStateOther, 0, false)
	assertScoreBasedEmptyNodeMetric(t, "new-pool", reclamationEmptyNodeStateOther, 1, true)
}

type errorOnPodListClient struct {
	client.Client
	err error
}

func (c errorOnPodListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return c.err
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
	return &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""}},
		Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		}},
	}
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

func assertScoreBasedEmptyNodeMetric(t *testing.T, pool, state string, want float64, wantFound bool) {
	t.Helper()
	metricFamilies, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range metricFamilies {
		if family.GetName() != "karpenter_nodepools_empty_nodes" {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["nodepool"] == pool && labels["state"] == state {
				if !wantFound {
					t.Fatalf("metric series for nodepool=%q state=%q unexpectedly exists", pool, state)
				}
				if got := metric.GetGauge().GetValue(); got != want {
					t.Fatalf("metric value for nodepool=%q state=%q = %v, want %v", pool, state, got, want)
				}
				return
			}
		}
	}
	if wantFound {
		t.Fatalf("metric series for nodepool=%q state=%q was not found", pool, state)
	}
}

func resetScoreBasedEmptyNodeMetricForTest() {
	reclamationEmptyNodeMetricState.Lock()
	defer reclamationEmptyNodeMetricState.Unlock()
	ReclamationEmptyNodes.Reset()
	reclamationEmptyNodeMetricState.pools = map[string]struct{}{}
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
	disabledPool := reclamationTestPool("disabled-pool")
	disabledPool.Annotations = nil
	standbySince := now.Add(-30 * time.Second)
	claimA, nodeA := reclamationTestNode("pool-a", "ready-a", true, true, false)
	standby.SetNodeClaimStandby(claimA, true, standbySince)
	claimB, nodeB := reclamationTestNode("pool-b", "ready-b", true, true, false)
	standby.SetNodeClaimStandby(claimB, true, standbySince)
	youngClaim, youngNode := reclamationTestNode("pool-a", "young", true, true, false)
	standby.SetNodeClaimStandby(youngClaim, true, now.Add(-5*time.Second))
	activatingClaim, activatingNode := reclamationTestNode("pool-a", "activating", true, true, false)
	standby.SetNodeClaimStandby(activatingClaim, true, standbySince)
	standby.SetNodeClaimActivating(activatingClaim, true)
	disabledClaim, disabledNode := reclamationTestNode("disabled-pool", "disabled", true, true, false)
	standby.SetNodeClaimStandby(disabledClaim, true, standbySince)
	kubeClient := reclamationTestClient(poolA, poolB, disabledPool, claimA, nodeA, claimB, nodeB, youngClaim, youngNode, activatingClaim, activatingNode, disabledClaim, disabledNode)
	cluster := state.NewCluster(clock, kubeClient, nil)
	for _, nodeClaim := range []*v1.NodeClaim{claimA, claimB, youngClaim, activatingClaim, disabledClaim} {
		cluster.UpdateNodeClaim(nodeClaim)
	}
	for _, node := range []*corev1.Node{nodeA, nodeB, youngNode, activatingNode, disabledNode} {
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
	if cmd == nil {
		t.Fatal("expected reclamation command for eligible standby nodes")
	}
	if len(cmd.Candidates) != 2 {
		t.Fatalf("selected candidate count = %d, want 2 eligible nodes across pools", len(cmd.Candidates))
	}
	got := []string{cmd.Candidates[0].Node.Name, cmd.Candidates[1].Node.Name}
	want := []string{"ready-a", "ready-b"}
	if got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("selected candidates = %v, want ordered %v", got, want)
	}
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

func TestReclamationBeforeDeleteRechecksPodsAndActivation(t *testing.T) {
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
	queue := NewTestQueue(kubeClient, events.NewRecorder(&record.FakeRecorder{}), cluster, clk, nil, nil)
	stateNode := state.NewNode()
	stateNode.Node = node
	stateNode.NodeClaim = nodeClaim
	candidate := &Candidate{StateNode: stateNode, NodePool: nodePool}
	if err := queue.reclamationCandidateBeforeDelete(ctx, candidate); err != nil {
		t.Fatalf("reclamationCandidateBeforeDelete() error for an empty node = %v", err)
	}

	workload := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late-pod", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name}}
	if err := kubeClient.Create(ctx, workload); err != nil {
		t.Fatal(err)
	}
	if err := queue.reclamationCandidateBeforeDelete(ctx, candidate); !IsUnrecoverableError(err) {
		t.Fatalf("reclamationCandidateBeforeDelete() error with a newly bound pod = %v, want unrecoverable", err)
	}
	if err := kubeClient.Delete(ctx, workload); err != nil {
		t.Fatal(err)
	}
	cluster.UpdateNodeClaim(nodeClaim)
	if err := cluster.UpdateNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	cluster.NominateNodeForPod(ctx, candidate.ProviderID())
	if err := queue.reclamationCandidateBeforeDelete(ctx, candidate); !IsUnrecoverableError(err) {
		t.Fatalf("reclamationCandidateBeforeDelete() error for a nominated node = %v, want unrecoverable", err)
	}
	candidate.ClearNomination()

	storedNodeClaim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), storedNodeClaim); err != nil {
		t.Fatal(err)
	}
	standby.SetNodeClaimActivating(storedNodeClaim, true)
	if err := kubeClient.Update(ctx, storedNodeClaim); err != nil {
		t.Fatal(err)
	}
	if err := queue.reclamationCandidateBeforeDelete(ctx, candidate); !IsUnrecoverableError(err) {
		t.Fatalf("reclamationCandidateBeforeDelete() error for an activating node = %v, want unrecoverable", err)
	}
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
	queue := NewTestQueue(kubeClient, events.NewRecorder(&record.FakeRecorder{}), cluster, clk, nil, nil)
	if err := queue.reclamationCandidateBeforeDelete(ctx, candidate); !IsUnrecoverableError(err) {
		t.Fatalf("reclamationCandidateBeforeDelete() error = %v, want unrecoverable for buffer reservation", err)
	}
}

func TestReclamationComputeCommandSucceedsWhenInventoryCollectionFails(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.October, 10, 12, 0, 0, 0, time.UTC)
	clock := clocktesting.NewFakeClock(now)
	pool := reclamationTestPool("pool-a")
	standbySince := now.Add(-30 * time.Second)
	claim, node := reclamationTestNode("pool-a", "ready-a", true, true, false)
	standby.SetNodeClaimStandby(claim, true, standbySince)
	kubeClient := reclamationTestClient(pool, claim, node)
	cluster := state.NewCluster(clock, kubeClient, nil)
	cluster.UpdateNodeClaim(claim)
	if err := cluster.UpdateNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	reclamation := NewReclamation(MakeConsolidation(clock, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil))
	reclamation.kubeClient = errorOnPodListClient{Client: kubeClient, err: errors.New("pod list failed")}
	cmd := reclamation.computeReclamationCommand(ctx, []*Candidate{reclamationCandidate(pool, claim, node)})
	if cmd == nil || len(cmd.Candidates) != 1 {
		t.Fatalf("computeReclamationCommand() = %v, want one selected candidate", cmd)
	}
}
