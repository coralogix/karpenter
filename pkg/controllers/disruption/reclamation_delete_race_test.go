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
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestReclamationConditionalDeleteRevalidatesRemovedStandbyMarker(t *testing.T) {
	h := newReclamationDeleteRaceHarness(t, false, removeStandbyMarkerBeforeConditionalDelete)
	requireReconciliationRequeues(t, h)
	requireDeleteCalls(t, h, 1)
	requireReconciliationCompletes(t, h)
	requireDeleteCalls(t, h, 1)
	require.False(t, standby.IsNodeClaimStandby(getRaceNodeClaim(t, h)))
	require.False(t, h.queue.HasAny(h.claim.Status.ProviderID), "rejected command should complete after fresh standby marker validation")
}

func TestReclamationConditionalDeleteProtectsSameNameReplacement(t *testing.T) {
	h := newReclamationDeleteRaceHarness(t, false, func(ctx context.Context, kubeClient client.Client, object client.Object) error {
		oldClaim := &v1.NodeClaim{}
		key := client.ObjectKeyFromObject(object)
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
	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
	require.Equal(t, 1, h.client.deleteCalls)

	_, err = h.queue.Reconcile(h.ctx, h.claim)
	require.NoError(t, err)
	require.Equal(t, 1, h.client.deleteCalls)
	stored := &v1.NodeClaim{}
	require.NoError(t, h.client.Get(h.ctx, client.ObjectKeyFromObject(h.claim), stored))
	require.Equal(t, types.UID("replacement-uid"), stored.UID)
}

func TestReclamationRetriesDeleteForAlreadyTerminatingClaim(t *testing.T) {
	h := newReclamationDeleteRaceHarness(t, true, updateTerminatingClaimBeforeConditionalDelete)
	requireReconciliationRequeues(t, h)
	requireReconciliationCompletes(t, h)
	requireDeleteCalls(t, h, 2)
	require.False(t, h.queue.HasAny(h.claim.Status.ProviderID), "successfully retried deletion should complete the queue command")
	require.False(t, getRaceNodeClaim(t, h).DeletionTimestamp.IsZero(), "retry should preserve the already-started termination")
}

func removeStandbyMarkerBeforeConditionalDelete(ctx context.Context, kubeClient client.Client, object client.Object) error {
	claim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(object), claim); err != nil {
		return err
	}
	oldResourceVersion := claim.ResourceVersion
	standby.SetNodeClaimStandby(claim, false, time.Time{})
	if err := kubeClient.Update(ctx, claim); err != nil {
		return err
	}
	updated := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(claim), updated); err != nil {
		return err
	}
	if updated.ResourceVersion == oldResourceVersion {
		return fmt.Errorf("fake client did not advance resourceVersion after standby marker removal")
	}
	return nil
}

func updateTerminatingClaimBeforeConditionalDelete(ctx context.Context, kubeClient client.Client, object client.Object) error {
	claim := &v1.NodeClaim{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(object), claim); err != nil {
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

func requireDeleteCalls(t *testing.T, h *reclamationDeleteRaceHarness, deletes int) {
	t.Helper()
	require.Equal(t, deletes, h.client.deleteCalls)
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
	require.NoError(t, h.client.Get(h.ctx, client.ObjectKeyFromObject(h.cmd.Candidates[0].Node), node))
	node.Finalizers = []string{"test.karpenter.sh/finalizer"}
	require.NoError(t, h.client.Update(h.ctx, node))
	require.NoError(t, h.client.Client.Delete(h.ctx, node))

	_, err := h.queue.Reconcile(h.ctx, h.claim)
	require.NoError(t, err)
	require.Zero(t, h.client.deleteCalls)
	storedClaim := &v1.NodeClaim{}
	require.NoError(t, h.client.Get(h.ctx, client.ObjectKeyFromObject(h.claim), storedClaim))
}

func TestReclamationDoesNotDeleteWhenNodeClaimReceivesDoNotDisrupt(t *testing.T) {
	h := newReclamationDeleteRaceHarness(t, false, nil)
	claim := &v1.NodeClaim{}
	require.NoError(t, h.client.Get(h.ctx, client.ObjectKeyFromObject(h.claim), claim))
	claim.Annotations = map[string]string{v1.DoNotDisruptAnnotationKey: "true"}
	require.NoError(t, h.client.Update(h.ctx, claim))

	_, err := h.queue.Reconcile(h.ctx, h.claim)
	require.NoError(t, err)
	require.Zero(t, h.client.deleteCalls)
	storedClaim := &v1.NodeClaim{}
	require.NoError(t, h.client.Get(h.ctx, client.ObjectKeyFromObject(h.claim), storedClaim))
	require.Equal(t, "true", storedClaim.Annotations[v1.DoNotDisruptAnnotationKey])
}

type reclamationDeleteRaceHarness struct {
	ctx    context.Context
	claim  *v1.NodeClaim
	queue  *Queue
	cmd    *Command
	client *reclamationConditionalDeleteClient
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
	kubeClient := &reclamationConditionalDeleteClient{interference: interference}
	harness := newReclamationQueueHarness(t, 1, kubeClient)
	if deleting {
		claim := &v1.NodeClaim{}
		require.NoError(t, kubeClient.Get(harness.ctx, client.ObjectKeyFromObject(harness.claim), claim))
		claim.Finalizers = []string{"test.karpenter.sh/finalizer"}
		require.NoError(t, kubeClient.Update(harness.ctx, claim))
		require.NoError(t, kubeClient.Client.Delete(harness.ctx, claim))
		require.NoError(t, kubeClient.Get(harness.ctx, client.ObjectKeyFromObject(harness.claim), harness.claim))
	}
	return &reclamationDeleteRaceHarness{ctx: harness.ctx, claim: harness.claim, queue: harness.queue, cmd: harness.cmd, client: kubeClient}
}
