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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestReclamationConditionalDeleteRevalidatesConcurrentActivation(t *testing.T) {
	h := newReclamationDeleteRaceHarness(t, false, activateNodeClaimBeforeConditionalDelete)
	requireReconciliationRequeues(t, h)
	requireDeleteAndValidationCalls(t, h, 1, 1)
	requireReconciliationCompletes(t, h)
	requireDeleteAndValidationCalls(t, h, 1, 2)
	require.True(t, standby.IsNodeClaimActivating(getRaceNodeClaim(t, h)))
	require.False(t, h.queue.HasAny(h.claim.Status.ProviderID), "rejected command should complete after fresh activation validation")
}

func TestReclamationConditionalDeleteProtectsSameNameReplacement(t *testing.T) {
	h := newReclamationDeleteRaceHarness(t, false, func(ctx context.Context, kubeClient client.Client, _ client.Object) error {
		oldClaim := &v1.NodeClaim{}
		key := types.NamespacedName{Name: "race-claim"}
		if err := kubeClient.Get(ctx, key, oldClaim); err != nil {
			return err
		}
		if err := kubeClient.Delete(ctx, oldClaim); err != nil {
			return err
		}
		replacement := oldClaim.DeepCopy()
		replacement.UID = types.UID("replacement-uid")
		replacement.ResourceVersion = ""
		return kubeClient.Create(ctx, replacement)
	})

	result, err := h.queue.Reconcile(h.ctx, h.claim)
	if err != nil {
		t.Fatalf("first reconcile error = %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("first reconcile should retry when the claim is replaced after validation")
	}
	if h.client.deleteCalls != 1 {
		t.Fatalf("conditional delete attempts after replacement race = %d, want 1", h.client.deleteCalls)
	}

	_, err = h.queue.Reconcile(h.ctx, h.claim)
	if err != nil {
		t.Fatalf("second reconcile error = %v", err)
	}
	if h.client.deleteCalls != 1 {
		t.Fatalf("conditional delete attempts after replacement was observed = %d, want no retry", h.client.deleteCalls)
	}
	stored := &v1.NodeClaim{}
	if err := h.client.Get(h.ctx, types.NamespacedName{Name: "race-claim"}, stored); err != nil {
		t.Fatalf("getting replacement NodeClaim: %v", err)
	}
	if stored.UID != types.UID("replacement-uid") {
		t.Fatalf("stored NodeClaim UID = %q, want replacement UID", stored.UID)
	}
}

func TestReclamationRetriesDeleteForAlreadyTerminatingClaim(t *testing.T) {
	h := newReclamationDeleteRaceHarness(t, true, updateTerminatingClaimBeforeConditionalDelete)
	requireReconciliationRequeues(t, h)
	requireReconciliationCompletes(t, h)
	requireDeleteAndValidationCalls(t, h, 2, 2)
	require.False(t, h.queue.HasAny(h.claim.Status.ProviderID), "successfully retried deletion should complete the queue command")
	require.False(t, getRaceNodeClaim(t, h).DeletionTimestamp.IsZero(), "retry should preserve the already-started termination")
}

func activateNodeClaimBeforeConditionalDelete(ctx context.Context, kubeClient client.Client, _ client.Object) error {
	claim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: "race-claim"}, claim); err != nil {
		return err
	}
	oldResourceVersion := claim.ResourceVersion
	standby.SetNodeClaimActivating(claim, true)
	if err := kubeClient.Update(ctx, claim); err != nil {
		return err
	}
	updated := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), updated); err != nil {
		return err
	}
	if updated.ResourceVersion == oldResourceVersion {
		return fmt.Errorf("fake client did not advance resourceVersion after activation update")
	}
	return nil
}

func updateTerminatingClaimBeforeConditionalDelete(ctx context.Context, kubeClient client.Client, _ client.Object) error {
	claim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, types.NamespacedName{Name: "race-claim"}, claim); err != nil {
		return err
	}
	oldResourceVersion := claim.ResourceVersion
	claim.Annotations["test.karpenter.sh/concurrent-update"] = "true"
	if err := kubeClient.Update(ctx, claim); err != nil {
		return err
	}
	updated := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), updated); err != nil {
		return err
	}
	if updated.ResourceVersion == oldResourceVersion {
		return fmt.Errorf("fake client did not advance resourceVersion after concurrent update")
	}
	return nil
}

func requireReconciliationRequeues(t *testing.T, h *reclamationDeleteRaceHarness) {
	t.Helper()
	result, err := h.queue.Reconcile(h.ctx, h.claim)
	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
}

func requireReconciliationCompletes(t *testing.T, h *reclamationDeleteRaceHarness) {
	t.Helper()
	result, err := h.queue.Reconcile(h.ctx, h.claim)
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
}

func requireDeleteAndValidationCalls(t *testing.T, h *reclamationDeleteRaceHarness, deletes, validations int) {
	t.Helper()
	require.Equal(t, deletes, h.client.deleteCalls)
	require.Equal(t, validations, h.beforeDeleteCalls)
}

func getRaceNodeClaim(t *testing.T, h *reclamationDeleteRaceHarness) *v1.NodeClaim {
	t.Helper()
	claim := &v1.NodeClaim{}
	require.NoError(t, h.client.Get(h.ctx, client.ObjectKeyFromObject(h.claim), claim))
	return claim
}

func TestReclamationDoesNotDeleteWhenLiveNodeStartsTerminating(t *testing.T) {
	h := newReclamationDeleteRaceHarness(t, false, nil)
	node := &corev1.Node{}
	if err := h.client.Get(h.ctx, types.NamespacedName{Name: "race"}, node); err != nil {
		t.Fatalf("getting live Node: %v", err)
	}
	node.Finalizers = []string{"test.karpenter.sh/finalizer"}
	if err := h.client.Update(h.ctx, node); err != nil {
		t.Fatalf("adding Node finalizer: %v", err)
	}
	if err := h.client.Client.Delete(h.ctx, node); err != nil {
		t.Fatalf("starting live Node deletion: %v", err)
	}

	_, err := h.queue.Reconcile(h.ctx, h.claim)
	if err != nil {
		t.Fatalf("reconcile error = %v", err)
	}
	if h.client.deleteCalls != 0 {
		t.Fatalf("conditional delete attempts = %d, want 0 for a deleting Node", h.client.deleteCalls)
	}
	storedClaim := &v1.NodeClaim{}
	if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(h.claim), storedClaim); err != nil {
		t.Fatalf("getting NodeClaim after rejecting deleting Node: %v", err)
	}
}

func TestReclamationDoesNotDeleteWhenNodeClaimReceivesDoNotDisrupt(t *testing.T) {
	h := newReclamationDeleteRaceHarness(t, false, nil)
	claim := &v1.NodeClaim{}
	if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(h.claim), claim); err != nil {
		t.Fatalf("getting live NodeClaim: %v", err)
	}
	claim.Annotations = map[string]string{v1.DoNotDisruptAnnotationKey: "true"}
	if err := h.client.Update(h.ctx, claim); err != nil {
		t.Fatalf("setting live NodeClaim do-not-disrupt annotation: %v", err)
	}

	_, err := h.queue.Reconcile(h.ctx, h.claim)
	if err != nil {
		t.Fatalf("reconcile error = %v", err)
	}
	if h.client.deleteCalls != 0 {
		t.Fatalf("conditional delete attempts = %d, want 0 for a protected NodeClaim", h.client.deleteCalls)
	}
	storedClaim := &v1.NodeClaim{}
	if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(h.claim), storedClaim); err != nil {
		t.Fatalf("getting protected NodeClaim after rejecting reclamation: %v", err)
	}
	if storedClaim.Annotations[v1.DoNotDisruptAnnotationKey] != "true" {
		t.Fatal("do-not-disrupt annotation was lost")
	}
}

type reclamationDeleteRaceHarness struct {
	ctx               context.Context
	claim             *v1.NodeClaim
	queue             *Queue
	client            *reclamationConditionalDeleteClient
	beforeDeleteCalls int
}

type reclamationConditionalDeleteClient struct {
	client.Client
	deleteCalls  int
	interference func(context.Context, client.Client, client.Object) error
}

func (c *reclamationConditionalDeleteClient) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	deleteOptions := &client.DeleteOptions{}
	for _, option := range options {
		option.ApplyToDelete(deleteOptions)
	}
	if deleteOptions.Preconditions == nil || deleteOptions.Preconditions.UID == nil || deleteOptions.Preconditions.ResourceVersion == nil {
		return fmt.Errorf("reclamation delete omitted UID or resourceVersion preconditions")
	}
	c.deleteCalls++
	if c.interference != nil {
		interference := c.interference
		c.interference = nil
		if err := interference(ctx, c.Client, object); err != nil {
			return err
		}
	}
	current := &v1.NodeClaim{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(object), current); err != nil {
		return err
	}
	if current.UID != *deleteOptions.Preconditions.UID || current.ResourceVersion != *deleteOptions.Preconditions.ResourceVersion {
		return apierrors.NewConflict(schema.GroupResource{Group: "karpenter.sh", Resource: "nodeclaims"}, object.GetName(), fmt.Errorf("conditional delete preconditions no longer match"))
	}
	return c.Client.Delete(ctx, object, options...)
}

func newReclamationDeleteRaceHarness(t *testing.T, deleting bool, interference func(context.Context, client.Client, client.Object) error) *reclamationDeleteRaceHarness {
	t.Helper()
	ctx := options.ToContext(context.Background(), test.Options())
	now := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
	clk := clocktesting.NewFakeClock(now)
	nodePool := reclamationTestPool("reclamation-race-pool")
	claim, node := reclamationTestNode(nodePool.Name, "race", true, true, deleting)
	claim.UID = types.UID("original-uid")
	node.UID = types.UID("race-node-uid")
	baseClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(nodePool, claim, node).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(object client.Object) []string {
			pod := object.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).Build()
	kubeClient := &reclamationConditionalDeleteClient{Client: baseClient, interference: interference}
	storedClaim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), storedClaim); err != nil {
		t.Fatalf("getting test NodeClaim: %v", err)
	}
	storedNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(node), storedNode); err != nil {
		t.Fatalf("getting test Node: %v", err)
	}
	cluster := state.NewCluster(clk, kubeClient, nil)
	cluster.UpdateNodeClaim(storedClaim)
	if err := cluster.UpdateNode(ctx, storedNode); err != nil {
		t.Fatalf("adding test Node to cluster state: %v", err)
	}
	stateNode := state.NewNode()
	stateNode.Node = storedNode
	stateNode.NodeClaim = storedClaim
	candidate := &Candidate{StateNode: stateNode, NodePool: nodePool}
	queue := NewQueue(kubeClient, events.NewRecorder(&record.FakeRecorder{}), cluster, clk, nil)
	consolidationMethod := MakeConsolidation(clk, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), queue, nil)
	reclamation := NewReclamation(consolidationMethod)
	h := &reclamationDeleteRaceHarness{ctx: ctx, claim: storedClaim, queue: queue, client: kubeClient}
	cmd := &Command{
		Method:            reclamation,
		CreationTimestamp: clk.Now(),
		Candidates:        []*Candidate{candidate},
		BeforeDelete: func(ctx context.Context, candidates []*Candidate) error {
			h.beforeDeleteCalls++
			return reclamation.reclamationBeforeDelete(ctx, candidates)
		},
		DeleteWithPreconditions: true,
	}
	queue.ProviderIDToCommand[storedClaim.Status.ProviderID] = cmd
	return h
}
