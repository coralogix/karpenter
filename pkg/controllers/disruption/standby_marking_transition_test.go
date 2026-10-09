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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestStandbyMarkingQueueRecoversWhenPodBindsDuringTransition(t *testing.T) {
	h := newStandbyQueueHarness(t)
	injected := false
	h.client.afterPatch = func(ctx context.Context, object client.Object) error {
		node, ok := object.(*corev1.Node)
		if !ok || !hasDisruptionNoScheduleTaint(node) || injected {
			return nil
		}
		injected = true
		return h.createWorkload(ctx, "late-workload", nil)
	}
	if err := h.start(); err != nil {
		t.Fatalf("starting empty-node standby transition: %v", err)
	}
	h.assertNodeActive(t)
	if !injected {
		t.Fatal("expected the workload to bind after the temporary disruption taint was added")
	}
	h.assertWorkloadPresent(t, "late-workload")
	if h.eventCount() != 0 {
		t.Fatal("a transition recovered to active capacity must not emit a standby-marked event")
	}
}

func TestCleanupRecoversOccupiedStandbyNodeWithDoNotDisruptPod(t *testing.T) {
	ctx := context.Background()
	kubeClient, cluster, clk, storedNode, storedClaim := occupiedStandbyRecoveryFixture(t, ctx)
	coordinator := standbypkg.NewCoordinator(kubeClient, kubeClient, clk)
	queue := NewQueue(kubeClient, kubeClient, events.NewRecorder(record.NewFakeRecorder(1)), cluster, clk, nil, coordinator)
	controller := &Controller{queue: queue, kubeClient: kubeClient, apiReader: kubeClient, cluster: cluster}
	if err := controller.cleanupStaleDisruptionState(ctx); err != nil {
		t.Fatalf("recovering occupied standby node: %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(storedNode), storedNode); err != nil {
		t.Fatalf("getting Node after recovery: %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(storedClaim), storedClaim); err != nil {
		t.Fatalf("getting NodeClaim after recovery: %v", err)
	}
	if standby.IsNodeClaimStandby(storedClaim) || standby.HasNodeTaint(storedNode) {
		t.Fatal("occupied standby recovery left standby markers on active capacity")
	}
}

func occupiedStandbyRecoveryFixture(t *testing.T, ctx context.Context) (client.Client, *state.Cluster, clock.Clock, *corev1.Node, *v1.NodeClaim) {
	t.Helper()
	clk := clocktesting.NewFakeClock(time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC))
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:   "standby-claim",
		UID:    types.UID("standby-claim-uid"),
		Labels: map[string]string{v1.NodePoolLabelKey: "standby-pool"},
	}, Status: v1.NodeClaimStatus{ProviderID: "provider-id"}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-node",
		UID:  types.UID("standby-node-uid"),
		Labels: map[string]string{
			v1.NodePoolLabelKey:        nodeClaim.Labels[v1.NodePoolLabelKey],
			v1.NodeInitializedLabelKey: "true",
		},
	}, Spec: corev1.NodeSpec{ProviderID: nodeClaim.Status.ProviderID}}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(nodeClaim, node).
		WithIndex(&corev1.Pod{}, "spec.nodeName", standbyMarkingPodNodeIndex).
		Build()
	storedClaim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), storedClaim); err != nil {
		t.Fatalf("getting NodeClaim: %v", err)
	}
	standby.SetNodeClaimStandby(storedClaim, true, clk.Now())
	if err := kubeClient.Update(ctx, storedClaim); err != nil {
		t.Fatalf("marking NodeClaim standby: %v", err)
	}
	storedNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(node), storedNode); err != nil {
		t.Fatalf("getting Node: %v", err)
	}
	standby.SetNodeTaint(storedNode, true)
	if err := kubeClient.Update(ctx, storedNode); err != nil {
		t.Fatalf("tainting standby Node: %v", err)
	}
	if err := kubeClient.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "protected-workload", Namespace: "default",
			Annotations: map[string]string{v1.DoNotDisruptAnnotationKey: "true"},
		},
		Spec: corev1.PodSpec{NodeName: storedNode.Name},
	}); err != nil {
		t.Fatalf("creating protected workload: %v", err)
	}
	cluster := state.NewCluster(clk, kubeClient, nil)
	cluster.UpdateNodeClaim(storedClaim)
	if err := cluster.UpdateNode(ctx, storedNode); err != nil {
		t.Fatalf("refreshing cluster Node: %v", err)
	}
	return kubeClient, cluster, clk, storedNode, storedClaim
}

type standbyQueueHarness struct {
	ctx       context.Context
	client    *standbyTransitionClient
	apiReader client.Reader
	node      *corev1.Node
	nodeClaim *v1.NodeClaim
	cluster   *state.Cluster
	queue     *Queue
	method    *StandbyMarking
	recorder  *record.FakeRecorder
}

type standbyTransitionClient struct {
	client.Client
	afterPatch func(context.Context, client.Object) error
}

func (c *standbyTransitionClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if err := c.Client.Patch(ctx, object, patch, options...); err != nil {
		return err
	}
	if c.afterPatch != nil {
		return c.afterPatch(ctx, object)
	}
	return nil
}

func newStandbyQueueHarness(t *testing.T) *standbyQueueHarness {
	t.Helper()
	ctx := context.Background()
	nodePool := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "standby-pool", Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""}},
		Spec:       v1.NodePoolSpec{Disruption: v1.Disruption{ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized}},
	}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-claim",
		UID:  types.UID("standby-claim-uid"),
		Labels: map[string]string{
			v1.NodePoolLabelKey:            nodePool.Name,
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			corev1.LabelInstanceTypeStable: "m5.large",
		},
	}}
	nodeClaim.Status.ProviderID = "provider-id"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "standby-node",
		UID:  types.UID("standby-node-uid"),
		Labels: map[string]string{
			v1.NodePoolLabelKey:            nodePool.Name,
			v1.NodeInitializedLabelKey:     "true",
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			corev1.LabelInstanceTypeStable: "m5.large",
		},
	}, Spec: corev1.NodeSpec{ProviderID: nodeClaim.Status.ProviderID}}
	base := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(nodePool, nodeClaim, node).
		WithIndex(&corev1.Pod{}, "spec.nodeName", standbyMarkingPodNodeIndex).
		Build()
	wrapped := &standbyTransitionClient{Client: base}
	clk := clocktesting.NewFakeClock(time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC))
	fakeRecorder := record.NewFakeRecorder(20)
	ctxRecorder := events.NewRecorder(fakeRecorder)
	cluster := state.NewCluster(clk, base, nil)
	cluster.UpdateNodeClaim(nodeClaim.DeepCopy())
	if err := cluster.UpdateNode(ctx, node.DeepCopy()); err != nil {
		t.Fatalf("initializing cluster state: %v", err)
	}
	coordinator := standbypkg.NewCoordinator(base, wrapped, clk)
	queue := NewQueue(wrapped, base, ctxRecorder, cluster, clk, nil, coordinator)
	method := NewStandbyMarking(MakeConsolidation(clk, cluster, wrapped, nil, nil, ctxRecorder, queue, nil))
	return &standbyQueueHarness{
		ctx: ctx, client: wrapped, apiReader: base, node: node.DeepCopy(), nodeClaim: nodeClaim.DeepCopy(),
		cluster: cluster, queue: queue, method: method, recorder: fakeRecorder,
	}
}

func standbyMarkingPodNodeIndex(object client.Object) []string {
	pod := object.(*corev1.Pod)
	if pod.Spec.NodeName == "" {
		return nil
	}
	return []string{pod.Spec.NodeName}
}

func (h *standbyQueueHarness) start() error {
	command := Command{Method: h.method, Action: StandbyAction, Candidates: []*Candidate{{
		StateNode: &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()},
		NodePool:  &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: h.node.Labels[v1.NodePoolLabelKey], Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""}}},
	}}}
	return h.queue.StartCommand(h.ctx, &command)
}

func (h *standbyQueueHarness) createWorkload(ctx context.Context, name string, annotations map[string]string) error {
	return h.client.Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: annotations}, Spec: corev1.PodSpec{NodeName: h.node.Name}})
}

func (h *standbyQueueHarness) assertNodeActive(t *testing.T) {
	t.Helper()
	node := &corev1.Node{}
	if err := h.apiReader.Get(h.ctx, client.ObjectKeyFromObject(h.node), node); err != nil {
		t.Fatalf("getting Node: %v", err)
	}
	claim := &v1.NodeClaim{}
	if err := h.apiReader.Get(h.ctx, client.ObjectKeyFromObject(h.nodeClaim), claim); err != nil {
		t.Fatalf("getting NodeClaim: %v", err)
	}
	assertNoStandbyState(t, node, claim)
	if h.queue.HasAny(h.nodeClaim.Status.ProviderID) {
		t.Fatal("standby transition left an in-progress queue reservation")
	}
}

func assertNoStandbyState(t *testing.T, node *corev1.Node, claim *v1.NodeClaim) {
	t.Helper()
	if standby.IsNodeClaimStandby(claim) || standby.IsNodeClaimActivating(claim) || standby.HasNodeTaint(node) {
		t.Fatalf("active Node still has standby state: claimAnnotations=%v nodeTaints=%v", claim.Annotations, node.Spec.Taints)
	}
	if hasDisruptionNoScheduleTaint(node) {
		t.Fatal("late workload left the temporary disruption taint on the active node")
	}
}

func (h *standbyQueueHarness) assertWorkloadPresent(t *testing.T, name string) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := h.apiReader.Get(h.ctx, client.ObjectKey{Namespace: "default", Name: name}, pod); err != nil {
		t.Fatalf("getting workload Pod: %v", err)
	}
	if pod.Spec.NodeName != h.node.Name {
		t.Fatalf("workload bound to Node %q, want %q", pod.Spec.NodeName, h.node.Name)
	}
}

func (h *standbyQueueHarness) eventCount() int {
	count := 0
	for {
		select {
		case event := <-h.recorder.Events:
			if strings.Contains(event, "Standby") || strings.Contains(event, "Evacu") {
				count++
			}
		default:
			return count
		}
	}
}

func hasDisruptionNoScheduleTaint(node *corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.MatchTaint(&v1.DisruptedNoScheduleTaint) {
			return true
		}
	}
	return false
}
