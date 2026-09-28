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
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestScoreBasedReclamationEmptyNodeMetricsIncludeAllConfiguredPools(t *testing.T) {
	resetScoreBasedEmptyNodeMetricForTest()
	defer resetScoreBasedEmptyNodeMetricForTest()

	ctx := context.Background()
	now := time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
	clock := clocktesting.NewFakeClock(now)
	duePool := scoreBasedReclamationTestPool("due-pool", "")
	notDuePool := scoreBasedReclamationTestPool("not-due-pool", now.Format(time.RFC3339Nano))
	zeroPool := scoreBasedReclamationTestPool("zero-pool", "")
	unconfiguredPool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "unconfigured-pool"}}

	standbyClaim, standbyNode := scoreBasedReclamationTestNode("due-pool", "standby-node", true, true, false)
	otherClaim, otherNode := scoreBasedReclamationTestNode("due-pool", "other-node", false, false, false)
	emptyOtherClaim, emptyOtherNode := scoreBasedReclamationTestNode("due-pool", "empty-other-node", false, false, false)
	markerOnlyClaim, markerOnlyNode := scoreBasedReclamationTestNode("not-due-pool", "marker-only-node", true, false, false)
	terminatingClaim, terminatingNode := scoreBasedReclamationTestNode("due-pool", "terminating-node", false, false, true)
	workload := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bound", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: otherNode.Name}}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(duePool, notDuePool, zeroPool, unconfiguredPool, standbyClaim, standbyNode, otherClaim, otherNode, emptyOtherClaim, emptyOtherNode, markerOnlyClaim, markerOnlyNode, terminatingClaim, terminatingNode, workload).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).Build()
	cluster := state.NewCluster(clock, kubeClient, nil)
	for _, nodeClaim := range []*v1.NodeClaim{standbyClaim, otherClaim, emptyOtherClaim, markerOnlyClaim, terminatingClaim} {
		cluster.UpdateNodeClaim(nodeClaim)
	}
	for _, node := range []*corev1.Node{standbyNode, otherNode, emptyOtherNode, markerOnlyNode, terminatingNode} {
		if err := cluster.UpdateNode(ctx, node); err != nil {
			t.Fatal(err)
		}
	}
	consolidation := MakeConsolidation(clock, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	reclamation := NewScoreBasedReclamation(consolidation)

	dueCounts, err := reclamation.emptyReclamationNodeCounts(ctx)
	if err != nil {
		t.Fatalf("emptyReclamationNodeCounts() error = %v", err)
	}
	if dueCounts["due-pool"] != 2 {
		t.Fatalf("due-pool empty count = %d, want 2", dueCounts["due-pool"])
	}
	if ExperimentalReclamationRemoveAllEmptyImmediately {
		if dueCounts["not-due-pool"] != 1 {
			t.Fatalf("not-due-pool empty count = %d, want 1 while experimental immediate reclamation is enabled", dueCounts["not-due-pool"])
		}
	} else if _, ok := dueCounts["not-due-pool"]; ok {
		t.Fatal("non-due pool should not be included in reclamation selection counts")
	}
	assertScoreBasedEmptyNodeMetric(t, "due-pool", scoreBasedEmptyNodeStateStandby, 1, true)
	assertScoreBasedEmptyNodeMetric(t, "due-pool", scoreBasedEmptyNodeStateOther, 1, true)
	assertScoreBasedEmptyNodeMetric(t, "not-due-pool", scoreBasedEmptyNodeStateStandby, 0, true)
	assertScoreBasedEmptyNodeMetric(t, "not-due-pool", scoreBasedEmptyNodeStateOther, 1, true)
	assertScoreBasedEmptyNodeMetric(t, "zero-pool", scoreBasedEmptyNodeStateStandby, 0, true)
	assertScoreBasedEmptyNodeMetric(t, "zero-pool", scoreBasedEmptyNodeStateOther, 0, true)
	assertScoreBasedEmptyNodeMetric(t, "unconfigured-pool", scoreBasedEmptyNodeStateOther, 0, false)
}

func TestScoreBasedReclamationEmptyNodeMetricsKeepLastCompleteScanAndRemoveOutOfScopePools(t *testing.T) {
	resetScoreBasedEmptyNodeMetricForTest()
	defer resetScoreBasedEmptyNodeMetricForTest()

	ctx := context.Background()
	clock := clocktesting.NewFakeClock(time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC))
	oldPool := scoreBasedReclamationTestPool("old-pool", "")
	oldClaim, oldNode := scoreBasedReclamationTestNode("old-pool", "old-node", false, false, false)
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(oldPool, oldClaim, oldNode).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).Build()
	cluster := state.NewCluster(clock, kubeClient, nil)
	cluster.UpdateNodeClaim(oldClaim)
	if err := cluster.UpdateNode(ctx, oldNode); err != nil {
		t.Fatal(err)
	}
	consolidation := MakeConsolidation(clock, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	reclamation := NewScoreBasedReclamation(consolidation)
	if _, err := reclamation.emptyReclamationNodeCounts(ctx); err != nil {
		t.Fatalf("initial emptyReclamationNodeCounts() error = %v", err)
	}
	assertScoreBasedEmptyNodeMetric(t, "old-pool", scoreBasedEmptyNodeStateOther, 1, true)

	newPool := scoreBasedReclamationTestPool("new-pool", "")
	newClaim, newNode := scoreBasedReclamationTestNode("new-pool", "new-node", false, false, false)
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
	if _, err := reclamation.emptyReclamationNodeCounts(ctx); err == nil {
		t.Fatal("emptyReclamationNodeCounts() error = nil, want pod-list error")
	}
	assertScoreBasedEmptyNodeMetric(t, "old-pool", scoreBasedEmptyNodeStateOther, 1, true)
	assertScoreBasedEmptyNodeMetric(t, "new-pool", scoreBasedEmptyNodeStateOther, 0, false)

	reclamation.kubeClient = kubeClient
	if _, err := reclamation.emptyReclamationNodeCounts(ctx); err != nil {
		t.Fatalf("complete emptyReclamationNodeCounts() error = %v", err)
	}
	assertScoreBasedEmptyNodeMetric(t, "old-pool", scoreBasedEmptyNodeStateOther, 0, false)
	assertScoreBasedEmptyNodeMetric(t, "new-pool", scoreBasedEmptyNodeStateOther, 1, true)
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

func scoreBasedReclamationTestPool(name, lastReclamation string) *v1.NodePool {
	annotations := map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""}
	if lastReclamation != "" {
		annotations[v1.ScoreBasedLastReclamationAnnotationKey] = lastReclamation
	}
	return &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations},
		Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		}},
	}
}

func scoreBasedReclamationTestNode(poolName, name string, standbyMarker, standbyTaint, deleting bool) (*v1.NodeClaim, *corev1.Node) {
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
		standby.SetNodeClaimStandby(nodeClaim, true)
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
	scoreBasedEmptyNodeMetricState.Lock()
	defer scoreBasedEmptyNodeMetricState.Unlock()
	ScoreBasedReclamationEmptyNodes.Reset()
	scoreBasedEmptyNodeMetricState.pools = map[string]struct{}{}
}

func TestScoreBasedReclamationDueUsesConfiguredInterval(t *testing.T) {
	if ExperimentalReclamationRemoveAllEmptyImmediately {
		t.Skip("interval gating is disabled while experimental immediate reclamation is enabled")
	}
	now := time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{
		Name: "score-pool",
		Annotations: map[string]string{
			v1.ScoreBasedConsolidationAnnotationKey:       "",
			v1.ScoreBasedLastReclamationAnnotationKey:     now.Add(-15*time.Second - time.Nanosecond).Format(time.RFC3339Nano),
			v1.ScoreBasedReclamationIntervalAnnotationKey: "15s",
		},
	}, Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
		ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
		ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
	}}}
	if !scoreBasedReclamationDue(nodePool, now) {
		t.Fatal("expected configured reclamation interval to become due after it elapsed")
	}
	nodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey] = now.Add(-15 * time.Second).Format(time.RFC3339Nano)
	if scoreBasedReclamationDue(nodePool, now) {
		t.Fatal("reclamation should become due only after the interval has elapsed")
	}
	if defaultScoreBasedReclamationInterval != time.Minute {
		t.Fatalf("default reclamation interval = %s, want 1m", defaultScoreBasedReclamationInterval)
	}
	for _, intervalValue := range []string{"", "invalid", "0s", "-1s"} {
		t.Run("default fallback for "+intervalValue, func(t *testing.T) {
			nodePool.Annotations[v1.ScoreBasedReclamationIntervalAnnotationKey] = intervalValue
			nodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey] = now.Add(-time.Minute).Format(time.RFC3339Nano)
			if scoreBasedReclamationDue(nodePool, now) {
				t.Fatal("default interval should not be due at exactly one minute")
			}
			nodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey] = now.Add(-time.Minute - time.Nanosecond).Format(time.RFC3339Nano)
			if !scoreBasedReclamationDue(nodePool, now) {
				t.Fatal("default interval should be due just after one minute")
			}
		})
	}
	nodePool.Annotations[v1.ScoreBasedReclamationIntervalAnnotationKey] = "2m"
	nodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey] = now.Add(-2 * time.Minute).Format(time.RFC3339Nano)
	if scoreBasedReclamationDue(nodePool, now) {
		t.Fatal("explicit 2m override should not be due at exactly two minutes")
	}
	nodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey] = now.Add(-2*time.Minute - time.Nanosecond).Format(time.RFC3339Nano)
	if !scoreBasedReclamationDue(nodePool, now) {
		t.Fatal("explicit 2m override should be due just after two minutes")
	}
}

func TestScoreBasedReclamationRequiresEligibleConsolidationPolicy(t *testing.T) {
	now := time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
	for name, disruption := range map[string]v1.Disruption{
		"when-empty policy": {
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmpty,
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			nodePool := &v1.NodePool{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "score-pool",
					Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""},
				},
				Spec: v1.NodePoolSpec{Disruption: disruption},
			}
			if scoreBasedReclamationDue(nodePool, now) {
				t.Fatal("reclamation should be disabled when the consolidation policy does not allow it")
			}
		})
	}

	t.Run("unannotated pool", func(t *testing.T) {
		nodePool := &v1.NodePool{
			ObjectMeta: metav1.ObjectMeta{Name: "score-pool"},
			Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
				ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
				ConsolidateAfter:    v1.MustParseNillableDuration(v1.Never),
			}},
		}
		if scoreBasedReclamationDue(nodePool, now) {
			t.Fatal("reclamation should remain disabled for an unannotated pool")
		}
	})
}

func TestScoreBasedReclamationIgnoresConsolidateAfter(t *testing.T) {
	now := time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
	nodePool := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "score-pool",
			Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""},
		},
		Spec: v1.NodePoolSpec{Disruption: v1.Disruption{
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			ConsolidateAfter:    v1.MustParseNillableDuration(v1.Never),
		}},
	}
	if !scoreBasedReclamationDue(nodePool, now) {
		t.Fatal("annotated reclamation must remain eligible when consolidateAfter is Never")
	}
}

func TestReclamationRemovalCountRoundsUp(t *testing.T) {
	if ExperimentalReclamationRemoveAllEmptyImmediately {
		t.Skip("half-batch sizing is disabled while experimental immediate reclamation is enabled")
	}
	for count, want := range map[int]int{0: 0, 1: 1, 2: 1, 3: 2, 4: 2, 5: 3} {
		if got := reclamationRemovalCount(count); got != want {
			t.Errorf("reclamationRemovalCount(%d) = %d, want %d", count, got, want)
		}
	}
}

func TestSortReclamationCandidatesByCostPerVCPU(t *testing.T) {
	candidate := func(name string, price float64, cpu string) *Candidate {
		offering := &cloudprovider.Offering{
			Price: price,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone: "us-east-1a",
			}),
		}
		instanceType := &cloudprovider.InstanceType{
			Name:      name,
			Offerings: cloudprovider.Offerings{offering},
			Capacity:  corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
		}
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
			corev1.LabelInstanceTypeStable: name,
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			corev1.LabelTopologyZone:       "us-east-1a",
		}}}
		stateNode := state.NewNode()
		stateNode.Node = node
		return &Candidate{StateNode: stateNode, instanceType: instanceType}
	}
	cheapPerVCPU := candidate("cheap-per-vcpu", 1, "8")
	expensivePerVCPU := candidate("expensive-per-vcpu", 1, "2")
	candidates := []*Candidate{cheapPerVCPU, expensivePerVCPU}
	sortReclamationCandidates(candidates)
	if candidates[0] != expensivePerVCPU {
		t.Fatal("expected the candidate with the highest cost per vCPU to rank first")
	}
}

func TestReclamationEmptyCountsBoundNonDaemonPodsUntilRemoved(t *testing.T) {
	ctx := context.Background()
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
		ObjectMeta: metav1.ObjectMeta{Name: "terminal", Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: node.Name},
		Status:     corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(node, daemon, terminating, terminal).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).Build()

	if empty, err := reclamationEmpty(ctx, kubeClient, node); err != nil || empty {
		t.Fatalf("reclamationEmpty() = (%v, %v), want (false, nil) while non-daemon pod is terminating", empty, err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(terminating), terminating); err != nil {
		t.Fatal(err)
	}
	terminating.Finalizers = nil
	if err := kubeClient.Update(ctx, terminating); err != nil {
		t.Fatal(err)
	}
	if empty, err := reclamationEmpty(ctx, kubeClient, node); err != nil || empty {
		t.Fatalf("reclamationEmpty() = (%v, %v), want (false, nil) while a terminal non-daemon pod remains bound", empty, err)
	}
	if err := kubeClient.Delete(ctx, terminal); err != nil {
		t.Fatal(err)
	}
	if empty, err := reclamationEmpty(ctx, kubeClient, node); err != nil || !empty {
		t.Fatalf("reclamationEmpty() = (%v, %v), want (true, nil) after non-daemon pod is gone", empty, err)
	}
}

func TestReclamationEmptyDoesNotUseResourceRequests(t *testing.T) {
	ctx := context.Background()
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}
	zeroRequestPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "zero-request", Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: node.Name,
			Containers: []corev1.Container{{
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")}},
			}},
		},
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(node, zeroRequestPod).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).Build()
	if empty, err := reclamationEmpty(ctx, kubeClient, node); err != nil || empty {
		t.Fatalf("reclamationEmpty() = (%v, %v), want (false, nil) for a bound zero-request workload pod", empty, err)
	}
}

func TestReclamationDeleteSuccessPersistsNodePoolTimestamp(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 25, 12, 30, 0, 123456789, time.UTC)
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "score-pool"}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(nodePool).Build()
	consolidation := MakeConsolidation(clocktesting.NewFakeClock(now), nil, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	reclamation := NewScoreBasedReclamation(consolidation)
	candidate := &Candidate{NodePool: nodePool}
	if err := reclamation.reclamationDeleteSucceeded(ctx, candidate); err != nil {
		t.Fatalf("reclamationDeleteSucceeded() error = %v", err)
	}
	stored := &v1.NodePool{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodePool), stored); err != nil {
		t.Fatal(err)
	}
	got := stored.Annotations[v1.ScoreBasedLastReclamationAnnotationKey]
	if got != now.Format(time.RFC3339Nano) {
		t.Fatalf("last reclamation timestamp = %q, want %q", got, now.Format(time.RFC3339Nano))
	}
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
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "reclamation-node",
		Labels: map[string]string{
			v1.NodePoolLabelKey:        nodePool.Name,
			v1.NodeInitializedLabelKey: "true",
		},
	}, Spec: corev1.NodeSpec{ProviderID: nodeClaim.Status.ProviderID}}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(nodePool, nodeClaim, node).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).Build()
	clk := clocktesting.NewFakeClock(now)
	cluster := state.NewCluster(clk, kubeClient, nil)
	consolidation := MakeConsolidation(clk, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	reclamation := NewScoreBasedReclamation(consolidation)
	stateNode := state.NewNode()
	stateNode.Node = node
	stateNode.NodeClaim = nodeClaim
	candidate := &Candidate{StateNode: stateNode, NodePool: nodePool}
	beforeDelete := reclamation.reclamationBeforeDelete([]*Candidate{candidate})
	if err := beforeDelete(ctx); err != nil {
		t.Fatalf("BeforeDelete() error for an empty node = %v", err)
	}

	workload := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late-pod", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name}}
	if err := kubeClient.Create(ctx, workload); err != nil {
		t.Fatal(err)
	}
	if err := beforeDelete(ctx); !IsUnrecoverableError(err) {
		t.Fatalf("BeforeDelete() error with a newly bound pod = %v, want unrecoverable", err)
	}
	if err := kubeClient.Delete(ctx, workload); err != nil {
		t.Fatal(err)
	}
	cluster.UpdateNodeClaim(nodeClaim)
	if err := cluster.UpdateNode(ctx, node); err != nil {
		t.Fatal(err)
	}
	cluster.NominateNodeForPod(ctx, candidate.ProviderID())
	if err := beforeDelete(ctx); !IsUnrecoverableError(err) {
		t.Fatalf("BeforeDelete() error for a nominated node = %v, want unrecoverable", err)
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
	if err := beforeDelete(ctx); !IsUnrecoverableError(err) {
		t.Fatalf("BeforeDelete() error for an activating node = %v, want unrecoverable", err)
	}
}
