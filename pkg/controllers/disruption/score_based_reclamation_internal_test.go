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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestScoreBasedReclamationDueUsesConfiguredInterval(t *testing.T) {
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
	nodePool.Annotations[v1.ScoreBasedReclamationIntervalAnnotationKey] = "0s"
	nodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey] = now.Add(-defaultScoreBasedReclamationInterval).Format(time.RFC3339Nano)
	if scoreBasedReclamationDue(nodePool, now) {
		t.Fatal("invalid zero interval should safely use the default interval")
	}
	nodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey] = now.Add(-defaultScoreBasedReclamationInterval - time.Nanosecond).Format(time.RFC3339Nano)
	if !scoreBasedReclamationDue(nodePool, now) {
		t.Fatal("expected default interval to become due after it elapsed")
	}
}

func TestScoreBasedReclamationRequiresEligibleConsolidationPolicy(t *testing.T) {
	now := time.Date(2026, time.September, 25, 12, 0, 0, 0, time.UTC)
	for name, disruption := range map[string]v1.Disruption{
		"when-empty policy": {
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmpty,
			ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
		},
		"consolidation disabled": {
			ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
			ConsolidateAfter:    v1.MustParseNillableDuration(v1.Never),
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
}

func TestReclamationRemovalCountRoundsUp(t *testing.T) {
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
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(node, daemon, terminating).
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
	if empty, err := reclamationEmpty(ctx, kubeClient, node); err != nil || !empty {
		t.Fatalf("reclamationEmpty() = (%v, %v), want (true, nil) after non-daemon pod is gone", empty, err)
	}
}

func TestReclamationDeleteSuccessPersistsNodePoolTimestamp(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, time.September, 25, 12, 30, 0, 123456789, time.UTC)
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "score-pool"}}
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(nodePool).Build()
	consolidation := MakeConsolidation(clocktesting.NewFakeClock(now), nil, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	scoreBased := &ScoreBasedConsolidation{consolidation: consolidation}
	candidate := &Candidate{NodePool: nodePool}
	if err := scoreBased.reclamationDeleteSucceeded(ctx, candidate); err != nil {
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
	ctx := context.Background()
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
	scoreBased := &ScoreBasedConsolidation{consolidation: consolidation}
	stateNode := state.NewNode()
	stateNode.Node = node
	stateNode.NodeClaim = nodeClaim
	candidate := &Candidate{StateNode: stateNode, NodePool: nodePool}
	beforeDelete := scoreBased.reclamationBeforeDelete([]*Candidate{candidate})
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
