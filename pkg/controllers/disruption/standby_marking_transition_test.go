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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
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
	command := Command{Method: h.method, Action: StandbyAction, Candidates: []*Candidate{{
		StateNode: &state.StateNode{Node: h.node.DeepCopy(), NodeClaim: h.nodeClaim.DeepCopy()},
		NodePool:  &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: h.node.Labels[v1.NodePoolLabelKey], Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""}}},
	}}}
	require.NoError(t, h.queue.StartCommand(h.ctx, &command))
	node := &corev1.Node{}
	claim := &v1.NodeClaim{}
	require.NoError(t, h.apiReader.Get(h.ctx, client.ObjectKeyFromObject(h.node), node))
	require.NoError(t, h.apiReader.Get(h.ctx, client.ObjectKeyFromObject(h.nodeClaim), claim))
	require.False(t, standby.IsNodeClaimStandby(claim) || standby.IsNodeClaimActivating(claim) || standby.HasNodeTaint(node))
	require.False(t, hasDisruptionNoScheduleTaint(node))
	require.False(t, h.queue.HasAny(h.nodeClaim.Status.ProviderID))
	require.True(t, injected, "expected the workload to bind after the temporary disruption taint was added")
	pod := &corev1.Pod{}
	require.NoError(t, h.apiReader.Get(h.ctx, client.ObjectKey{Namespace: "default", Name: "late-workload"}, pod))
	require.Equal(t, h.node.Name, pod.Spec.NodeName)
	require.Zero(t, h.eventCount(), "a transition recovered to active capacity must not emit a standby-marked event")
}

func TestEvacuationQueueRetainsBarrierUntilStandbyPersistenceCompletes(t *testing.T) {
	h := newEvacuationBarrierHarness(t)
	result, err := h.queue.Reconcile(h.ctx, h.nodeClaim)
	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
	require.False(t, h.cmd.Succeeded)
	liveNode := &corev1.Node{}
	liveClaim := &v1.NodeClaim{}
	require.NoError(t, h.base.Get(h.ctx, client.ObjectKeyFromObject(h.node), liveNode))
	require.NoError(t, h.base.Get(h.ctx, client.ObjectKeyFromObject(h.nodeClaim), liveClaim))
	require.True(t, hasDisruptionNoScheduleTaint(liveNode), "disruption barrier taint must remain until evacuation completes")
	require.False(t, standbypkg.StandbyEntryComplete(liveNode, liveClaim), "standby entry must remain incomplete while persistence is failing")

	h.allow()
	result, err = h.queue.Reconcile(h.ctx, h.nodeClaim)
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, h.cmd.Succeeded)
	require.NoError(t, h.base.Get(h.ctx, client.ObjectKeyFromObject(h.node), liveNode))
	require.NoError(t, h.base.Get(h.ctx, client.ObjectKeyFromObject(h.nodeClaim), liveClaim))
	require.False(t, hasDisruptionNoScheduleTaint(liveNode))
	require.True(t, standbypkg.StandbyEntryComplete(liveNode, liveClaim), "evacuation should release the barrier and finish standby entry")
}

type evacuationBarrierHarness struct {
	ctx       context.Context
	base      client.Client
	queue     *Queue
	cmd       *Command
	node      *corev1.Node
	nodeClaim *v1.NodeClaim
	allow     func()
}

func newEvacuationBarrierHarness(t *testing.T) *evacuationBarrierHarness {
	t.Helper()
	ctx := context.Background()
	clk := clocktesting.NewFakeClock(time.Date(2026, time.October, 5, 10, 0, 0, 0, time.UTC))
	nodePool, nodeClaim, node := newStandbyTransitionObjects("evac-node", "evac-claim", "provider-evac", v1.DisruptedNoScheduleTaint)
	base := newStandbyTransitionClient(nodePool, nodeClaim, node)
	wrapped := &standbyTransitionClient{Client: base}
	blockTaintWrites := true
	wrapped.beforeNodePatch = func(node *corev1.Node) error {
		if !blockTaintWrites {
			return nil
		}
		claim := &v1.NodeClaim{}
		if err := wrapped.Get(ctx, client.ObjectKeyFromObject(nodeClaim), claim); err != nil {
			return err
		}
		if standby.IsNodeClaimStandby(claim) && standby.HasNodeTaint(node) {
			return errors.New("injected transient standby persistence failure")
		}
		return nil
	}
	coordinator := standbypkg.NewCoordinator(base, wrapped)
	cluster := state.NewCluster(clk, wrapped, nil)
	cluster.UpdateNodeClaim(nodeClaim.DeepCopy())
	require.NoError(t, cluster.UpdateNode(ctx, node.DeepCopy()))
	recorder := events.NewRecorder(record.NewFakeRecorder(1))
	queue := NewQueue(wrapped, base, recorder, cluster, clk, nil, coordinator, terminator.NewQueue(base, recorder))
	cmd := &Command{
		Method: NewDrift(wrapped, cluster, nil, recorder, clk), Action: EvacuateAction, CreationTimestamp: clk.Now(),
		Candidates: []*Candidate{{StateNode: &state.StateNode{Node: node.DeepCopy(), NodeClaim: nodeClaim.DeepCopy()}, NodePool: nodePool}},
	}
	queue.ProviderIDToCommand[nodeClaim.Status.ProviderID] = cmd
	return &evacuationBarrierHarness{
		ctx: ctx, base: base, queue: queue, cmd: cmd, node: node, nodeClaim: nodeClaim,
		allow: func() { blockTaintWrites = false },
	}
}

func TestCleanupRecoversOccupiedStandbyNodeWithDoNotDisruptPod(t *testing.T) {
	ctx := context.Background()
	kubeClient, cluster, clk, storedNode, storedClaim := occupiedStandbyRecoveryFixture(t, ctx)
	coordinator := standbypkg.NewCoordinator(kubeClient, kubeClient)
	queue := NewQueue(kubeClient, kubeClient, events.NewRecorder(record.NewFakeRecorder(1)), cluster, clk, nil, coordinator, nil)
	require.NoError(t, queue.cleanupStaleDisruptionState(ctx))
	require.NoError(t, kubeClient.Get(ctx, client.ObjectKeyFromObject(storedNode), storedNode))
	require.NoError(t, kubeClient.Get(ctx, client.ObjectKeyFromObject(storedClaim), storedClaim))
	require.False(t, standby.IsNodeClaimStandby(storedClaim) || standby.HasNodeTaint(storedNode))
}

func occupiedStandbyRecoveryFixture(t *testing.T, ctx context.Context) (client.Client, *state.Cluster, clock.Clock, *corev1.Node, *v1.NodeClaim) {
	t.Helper()
	clk := clocktesting.NewFakeClock(time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC))
	nodePool, nodeClaim, node := newStandbyTransitionObjects("standby-node", "standby-claim", "provider-id")
	kubeClient := newStandbyTransitionClient(nodePool, nodeClaim, node)
	storedClaim := &v1.NodeClaim{}
	require.NoError(t, kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), storedClaim))
	standby.SetNodeClaimStandby(storedClaim, true, clk.Now())
	require.NoError(t, kubeClient.Update(ctx, storedClaim))
	storedNode := &corev1.Node{}
	require.NoError(t, kubeClient.Get(ctx, client.ObjectKeyFromObject(node), storedNode))
	standby.SetNodeTaint(storedNode, true)
	require.NoError(t, kubeClient.Update(ctx, storedNode))
	require.NoError(t, kubeClient.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "protected-workload", Namespace: "default",
			Annotations: map[string]string{v1.DoNotDisruptAnnotationKey: "true"},
		},
		Spec: corev1.PodSpec{NodeName: storedNode.Name},
	}))
	cluster := state.NewCluster(clk, kubeClient, nil)
	cluster.UpdateNodeClaim(storedClaim)
	require.NoError(t, cluster.UpdateNode(ctx, storedNode))
	return kubeClient, cluster, clk, storedNode, storedClaim
}

type standbyQueueHarness struct {
	ctx       context.Context
	client    *standbyTransitionClient
	apiReader client.Reader
	node      *corev1.Node
	nodeClaim *v1.NodeClaim
	queue     *Queue
	method    *StandbyMarking
	recorder  *record.FakeRecorder
}

type standbyTransitionClient struct {
	client.Client
	afterPatch      func(context.Context, client.Object) error
	beforeNodePatch func(*corev1.Node) error
}

func (c *standbyTransitionClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if node, ok := object.(*corev1.Node); ok && c.beforeNodePatch != nil {
		if err := c.beforeNodePatch(node); err != nil {
			return err
		}
	}
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
	nodePool, nodeClaim, node := newStandbyTransitionObjects("standby-node", "standby-claim", "provider-id")
	base := newStandbyTransitionClient(nodePool, nodeClaim, node)
	wrapped := &standbyTransitionClient{Client: base}
	clk := clocktesting.NewFakeClock(time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC))
	fakeRecorder := record.NewFakeRecorder(20)
	ctxRecorder := events.NewRecorder(fakeRecorder)
	cluster := state.NewCluster(clk, base, nil)
	cluster.UpdateNodeClaim(nodeClaim.DeepCopy())
	require.NoError(t, cluster.UpdateNode(ctx, node.DeepCopy()))
	coordinator := standbypkg.NewCoordinator(base, wrapped)
	queue := NewQueue(wrapped, base, ctxRecorder, cluster, clk, nil, coordinator, nil)
	method := NewStandbyMarking(MakeConsolidation(clk, cluster, wrapped, nil, nil, ctxRecorder, queue, nil))
	return &standbyQueueHarness{
		ctx: ctx, client: wrapped, apiReader: base, node: node.DeepCopy(), nodeClaim: nodeClaim.DeepCopy(),
		queue: queue, method: method, recorder: fakeRecorder,
	}
}

func newStandbyTransitionObjects(nodeName, claimName, providerID string, taints ...corev1.Taint) (*v1.NodePool, *v1.NodeClaim, *corev1.Node) {
	nodePool := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "standby-pool", Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""}},
		Spec:       v1.NodePoolSpec{Disruption: v1.Disruption{ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized}},
	}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: claimName,
		UID:  types.UID(claimName + "-uid"),
		Labels: map[string]string{
			v1.NodePoolLabelKey:            nodePool.Name,
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			corev1.LabelInstanceTypeStable: "m5.large",
		},
	}, Status: v1.NodeClaimStatus{ProviderID: providerID}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: nodeName,
		UID:  types.UID(nodeName + "-uid"),
		Labels: map[string]string{
			v1.NodePoolLabelKey:            nodePool.Name,
			v1.NodeInitializedLabelKey:     "true",
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			corev1.LabelInstanceTypeStable: "m5.large",
		},
	}, Spec: corev1.NodeSpec{ProviderID: providerID, Taints: taints}}
	return nodePool, nodeClaim, node
}

func newStandbyTransitionClient(objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objects...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", standbyMarkingPodNodeIndex).
		Build()
}

func standbyMarkingPodNodeIndex(object client.Object) []string {
	pod := object.(*corev1.Pod)
	if pod.Spec.NodeName == "" {
		return nil
	}
	return []string{pod.Spec.NodeName}
}

func (h *standbyQueueHarness) createWorkload(ctx context.Context, name string, annotations map[string]string) error {
	return h.client.Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Annotations: annotations}, Spec: corev1.PodSpec{NodeName: h.node.Name}})
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
