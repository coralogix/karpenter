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
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestStandbyTransitionRollsBackWhenPodBindsAfterTaint(t *testing.T) {
	h := newStandbyTransitionHarness(t)
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

func TestStandbyTransitionRecoversPodBoundAfterStandbyMarker(t *testing.T) {
	h := newStandbyTransitionHarness(t)
	h.client.afterPatch = func(ctx context.Context, object client.Object) error {
		claim, ok := object.(*v1.NodeClaim)
		if !ok || !standby.IsNodeClaimStandby(claim) {
			return nil
		}
		return h.createWorkload(ctx, "late-workload", nil)
	}
	if err := h.start(); err != nil {
		t.Fatalf("starting empty-node standby transition: %v", err)
	}
	h.assertNodeActive(t)
	h.assertWorkloadPresent(t, "late-workload")
	if h.eventCount() != 0 {
		t.Fatal("a transition recovered to active capacity must not emit a standby-marked event")
	}
}

func TestStandbyTransitionDoesNotRetaintNodeActivatedAfterClaimMarker(t *testing.T) {
	h := newStandbyTransitionHarness(t)
	h.client.afterPatch = func(ctx context.Context, object client.Object) error {
		claim, ok := object.(*v1.NodeClaim)
		if !ok || !standby.IsNodeClaimStandby(claim) {
			return nil
		}
		node := &corev1.Node{}
		if err := h.apiReader.Get(ctx, types.NamespacedName{Name: h.node.Name}, node); err != nil {
			return err
		}
		return h.provisioner.ActivateStandbyNodes(ctx, standby.ActivationSourceRecovery, &state.StateNode{Node: node, NodeClaim: claim.DeepCopy()})
	}
	if err := h.start(); err != nil {
		t.Fatalf("starting empty-node standby transition: %v", err)
	}
	h.assertNodeActive(t)
	if h.eventCount() != 0 {
		t.Fatal("an activation that wins the race must not emit a standby-marked event")
	}
}

func TestCleanupRecoversOccupiedStandbyNodeWithDoNotDisruptPod(t *testing.T) {
	h := newStandbyTransitionHarness(t)
	storedNode := &corev1.Node{}
	if err := h.apiReader.Get(h.ctx, client.ObjectKeyFromObject(h.node), storedNode); err != nil {
		t.Fatalf("getting Node: %v", err)
	}
	storedClaim := &v1.NodeClaim{}
	if err := h.apiReader.Get(h.ctx, client.ObjectKeyFromObject(h.nodeClaim), storedClaim); err != nil {
		t.Fatalf("getting NodeClaim: %v", err)
	}
	standby.SetNodeClaimStandby(storedClaim, true)
	if err := h.apiReader.(client.Client).Update(h.ctx, storedClaim); err != nil {
		t.Fatalf("marking NodeClaim standby: %v", err)
	}
	standby.SetNodeTaint(storedNode, true)
	if err := h.apiReader.(client.Client).Update(h.ctx, storedNode); err != nil {
		t.Fatalf("tainting standby Node: %v", err)
	}
	if err := h.createWorkload(h.ctx, "protected-workload", map[string]string{v1.DoNotDisruptAnnotationKey: "true"}); err != nil {
		t.Fatalf("creating protected workload: %v", err)
	}
	h.cluster.UpdateNodeClaim(storedClaim)
	if err := h.cluster.UpdateNode(h.ctx, storedNode); err != nil {
		t.Fatalf("refreshing cluster Node: %v", err)
	}

	controller := &Controller{queue: h.queue, kubeClient: h.client, apiReader: h.apiReader, cluster: h.cluster, provisioner: h.provisioner}
	if err := controller.cleanupStaleDisruptionState(h.ctx); err != nil {
		t.Fatalf("recovering occupied standby node: %v", err)
	}
	h.assertNodeActive(t)
	h.assertWorkloadPresent(t, "protected-workload")
	if h.eventCount() != 0 {
		t.Fatal("recovering occupied standby capacity must not emit a standby or evacuation event")
	}
}

func TestStandbyActionHasNoConsolidationSavings(t *testing.T) {
	candidate := &Candidate{
		StateNode: &state.StateNode{Node: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
			corev1.LabelTopologyZone: "us-east-1a",
		}}}},
		instanceType: &cloudprovider.InstanceType{Offerings: cloudprovider.Offerings{&cloudprovider.Offering{
			Price: 1.25,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone: "us-east-1a",
			}),
		}}},
	}
	deleteCommand := Command{Candidates: []*Candidate{candidate}, Action: DeleteAction}
	if got := deleteCommand.EstimatedSavings(); got != 1.25 {
		t.Fatalf("delete command estimated savings = %v, want 1.25", got)
	}
	standbyCommand := Command{Candidates: []*Candidate{candidate}, Action: StandbyAction}
	if got := standbyCommand.EstimatedSavings(); got != 0 {
		t.Fatalf("standby command estimated savings = %v, want 0", got)
	}
}

type standbyTransitionHarness struct {
	ctx         context.Context
	base        client.Client
	client      *standbyTransitionClient
	apiReader   client.Reader
	node        *corev1.Node
	nodeClaim   *v1.NodeClaim
	nodePool    *v1.NodePool
	cluster     *state.Cluster
	provisioner *provisioning.Provisioner
	queue       *Queue
	method      *ScoreBasedStandby
	recorder    *record.FakeRecorder
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
		if err := c.afterPatch(ctx, object); err != nil {
			return err
		}
	}
	return nil
}

func newStandbyTransitionHarness(t *testing.T) *standbyTransitionHarness {
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
			corev1.LabelTopologyZone:       "us-east-1a",
		},
	}}
	node.Spec.ProviderID = nodeClaim.Status.ProviderID
	base := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(nodePool, nodeClaim, node).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(object client.Object) []string {
			pod := object.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).Build()
	wrapped := &standbyTransitionClient{Client: base}
	clk := clocktesting.NewFakeClock(time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC))
	fakeRecorder := record.NewFakeRecorder(20)
	ctxRecorder := events.NewRecorder(fakeRecorder)
	cluster := state.NewCluster(clk, base, nil)
	cluster.UpdateNodeClaim(nodeClaim.DeepCopy())
	if err := cluster.UpdateNode(ctx, node.DeepCopy()); err != nil {
		t.Fatalf("initializing cluster state: %v", err)
	}
	provisioner := provisioning.NewProvisioner(base, ctxRecorder, nil, cluster, clk)
	provisioner.SetAPIReader(base)
	queue := NewQueue(wrapped, ctxRecorder, cluster, clk, provisioner)
	queue.SetAPIReader(base)
	method := NewScoreBasedStandby(MakeConsolidation(clk, cluster, wrapped, nil, nil, ctxRecorder, queue, nil))
	return &standbyTransitionHarness{
		ctx: ctx, base: base, client: wrapped, apiReader: base, node: node.DeepCopy(), nodeClaim: nodeClaim.DeepCopy(), nodePool: nodePool.DeepCopy(), cluster: cluster, provisioner: provisioner, queue: queue, method: method, recorder: fakeRecorder,
	}
}

func (h *standbyTransitionHarness) start() error {
	command := Command{Method: h.method, Action: StandbyAction, Candidates: []*Candidate{{
		StateNode: &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()},
		NodePool:  h.nodePool.DeepCopy(),
	}}}
	return h.queue.StartCommand(h.ctx, &command)
}

func (h *standbyTransitionHarness) createWorkload(ctx context.Context, name string, annotations map[string]string) error {
	return h.base.Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: annotations}, Spec: corev1.PodSpec{NodeName: h.node.Name}})
}

func (h *standbyTransitionHarness) assertNodeActive(t *testing.T) {
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
	internal := h.internalStateNode(t)
	assertNoStandbyState(t, internal.Node, internal.NodeClaim)
}

func (h *standbyTransitionHarness) internalStateNode(t *testing.T) *state.StateNode {
	t.Helper()
	var internal *state.StateNode
	for _, stateNode := range h.cluster.DeepCopyNodes() {
		if stateNode.ProviderID() == h.nodeClaim.Status.ProviderID {
			internal = stateNode
			break
		}
	}
	if internal == nil {
		t.Fatal("standby transition removed the node from internal cluster state")
	}
	return internal
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

func (h *standbyTransitionHarness) assertWorkloadPresent(t *testing.T, name string) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := h.apiReader.Get(h.ctx, client.ObjectKey{Namespace: "default", Name: name}, pod); err != nil {
		t.Fatalf("getting workload Pod: %v", err)
	}
	if pod.Spec.NodeName != h.node.Name {
		t.Fatalf("workload bound to Node %q, want %q", pod.Spec.NodeName, h.node.Name)
	}
}

func (h *standbyTransitionHarness) eventCount() int {
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
