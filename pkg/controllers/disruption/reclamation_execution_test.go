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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestReclamationExecutionSkipsBufferReservedCandidateInBatch(t *testing.T) {
	h := newReclamationExecutionHarness(t, 2)
	h.queue.cluster.UpdateBufferPodCounts(map[string]int{h.candidates[0].ProviderID(): 1})
	require.NoError(t, h.queue.deleteCandidates(h.ctx, h.cmd))
	require.Equal(t, 1, h.tracker.deleteCallCount())
}

func TestReclamationExecutionSkipsInvalidCandidateInBatch(t *testing.T) {
	h := newReclamationExecutionHarness(t, 2)
	workload := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late-pod", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: h.candidates[0].Node.Name}}
	require.NoError(t, h.kubeClient.Create(h.ctx, workload))
	require.NoError(t, h.queue.deleteCandidates(h.ctx, h.cmd))
	require.Equal(t, 1, h.tracker.deleteCallCount())
}

func TestReclamationExecutionSkipsCandidateWhenCachedDelayIncreases(t *testing.T) {
	h := newReclamationExecutionHarness(t, 1)
	candidate := h.candidates[0]
	candidate.NodePool.Annotations[v1.ReclamationStandbyDelayAnnotationKey] = "15s"

	pool := &v1.NodePool{}
	require.NoError(t, h.kubeClient.Get(h.ctx, client.ObjectKeyFromObject(candidate.NodePool), pool))
	pool.Annotations[v1.ReclamationStandbyDelayAnnotationKey] = "15s"
	require.NoError(t, h.kubeClient.Update(h.ctx, pool))

	claim := &v1.NodeClaim{}
	require.NoError(t, h.kubeClient.Get(h.ctx, client.ObjectKeyFromObject(candidate.NodeClaim), claim))
	standby.SetNodeClaimStandby(claim, true, h.queue.clock.Now().Add(-30*time.Second))
	require.NoError(t, h.kubeClient.Update(h.ctx, claim))

	pool.Annotations[v1.ReclamationStandbyDelayAnnotationKey] = "1h"
	require.NoError(t, h.kubeClient.Update(h.ctx, pool))

	_, err := h.queue.Reconcile(h.ctx, candidate.NodeClaim)
	require.NoError(t, err)
	require.Zero(t, h.tracker.deleteCallCount(), "candidate should remain standby for 30s under the current 1h delay")
	require.False(t, h.queue.HasAny(candidate.ProviderID()), "skipping the candidate should complete its queue command")
	storedClaim := &v1.NodeClaim{}
	require.NoError(t, h.kubeClient.Get(h.ctx, client.ObjectKeyFromObject(candidate.NodeClaim), storedClaim))
	require.True(t, standby.IsNodeClaimStandby(storedClaim), "claim should remain in standby")
}

func TestReclamationExecutionBoundedConcurrency(t *testing.T) {
	const candidateCount = 20
	h := newReclamationExecutionHarness(t, candidateCount)
	execErr := runBlockedReclamationDeletes(t, h)
	require.NoError(t, execErr)
	require.Equal(t, candidateCount, h.tracker.deleteCallCount())
	require.LessOrEqual(t, h.tracker.maxInFlight(), maxConcurrentCandidateDeletes)
}

func runBlockedReclamationDeletes(t *testing.T, h *reclamationExecutionHarness) error {
	t.Helper()
	h.tracker.blockDeletes = true
	done := make(chan struct{})
	var execErr error
	go func() {
		execErr = h.queue.deleteCandidates(h.ctx, h.cmd)
		close(done)
	}()
	waitForReclamationDeleteGate(t, h.tracker, len(h.cmd.Candidates))
	h.tracker.unblockDeletes()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reclamation batch to finish")
	}
	return execErr
}

func waitForReclamationDeleteGate(t *testing.T, tracker *reclamationExecutionDeleteTracker, candidateCount int) {
	t.Helper()
	waitUntil := time.Now().Add(5 * time.Second)
	for time.Now().Before(waitUntil) {
		if tracker.maxInFlight() > maxConcurrentCandidateDeletes {
			t.Fatalf("max in-flight deletes = %d, want at most %d", tracker.maxInFlight(), maxConcurrentCandidateDeletes)
		}
		if tracker.inFlight() == maxConcurrentCandidateDeletes || tracker.deleteCallCount() == candidateCount {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if tracker.inFlight() < maxConcurrentCandidateDeletes && tracker.deleteCallCount() != candidateCount {
		t.Fatalf("in-flight deletes = %d, want %d before releasing the gate", tracker.inFlight(), maxConcurrentCandidateDeletes)
	}
}

func TestReclamationExecutionPartialBatchRetry(t *testing.T) {
	tracker := &reclamationExecutionDeleteTracker{release: make(chan struct{})}
	h := newReclamationQueueHarness(t, 2, tracker)
	tracker.failDeleteOnce = map[string]bool{h.cmd.Candidates[1].ProviderID(): true}

	result, err := h.queue.Reconcile(h.ctx, h.claim)
	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
	require.False(t, h.cmd.Succeeded)
	require.Equal(t, 1, tracker.deleteCallCount())
	require.True(t, h.queue.HasAny(h.cmd.Candidates[0].ProviderID()))

	result, err = h.queue.Reconcile(h.ctx, h.claim)
	require.NoError(t, err)
	require.Zero(t, result.RequeueAfter)
	require.True(t, h.cmd.Succeeded)
	require.Equal(t, 2, tracker.deleteCallCount())
	require.False(t, h.queue.HasAny(h.cmd.Candidates[1].ProviderID()))
}

func TestReclamationExecutionContextCancellation(t *testing.T) {
	h := newReclamationExecutionHarness(t, 4)
	h.tracker.blockDeletes = true
	gate := make(chan struct{})
	h.tracker.inFlightGate = gate
	ctx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() {
		done <- h.queue.deleteCandidates(ctx, h.cmd)
	}()
	select {
	case <-gate:
	case <-time.After(2 * time.Second):
		t.Fatal("deleteCandidates() did not reach the expected concurrency gate")
	}
	cancel()
	var execErr error
	select {
	case execErr = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cancellation")
	}
	require.ErrorIs(t, execErr, context.Canceled)
	require.Less(t, h.tracker.deleteCallCount(), 4)
	h.tracker.unblockDeletes()
}

type reclamationExecutionHarness struct {
	ctx        context.Context
	kubeClient *reclamationExecutionDeleteTracker
	queue      *Queue
	cmd        *Command
	candidates []*Candidate
	tracker    *reclamationExecutionDeleteTracker
}

type reclamationExecutionDeleteTracker struct {
	client.Client
	deleteCalls    int
	blockDeletes   bool
	failDeleteOnce map[string]bool
	release        chan struct{}
	inFlightGate   chan struct{}
	mu             sync.Mutex
	inFlightNow    int
	maxObserved    int
}

func (c *reclamationExecutionDeleteTracker) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	if _, ok := object.(*v1.NodeClaim); !ok {
		return c.Client.Delete(ctx, object, options...)
	}
	if err := c.validateReclamationDeletePreconditions(options); err != nil {
		return err
	}
	providerID := object.(*v1.NodeClaim).Status.ProviderID
	if err := c.beginReclamationDelete(providerID); err != nil {
		return err
	}
	if err := c.waitForDelete(ctx); err != nil {
		return err
	}
	c.finishReclamationDelete()
	return c.Client.Delete(ctx, object, options...)
}

func (c *reclamationExecutionDeleteTracker) validateReclamationDeletePreconditions(options []client.DeleteOption) error {
	deleteOptions := &client.DeleteOptions{}
	for _, option := range options {
		option.ApplyToDelete(deleteOptions)
	}
	if deleteOptions.Preconditions == nil || deleteOptions.Preconditions.UID == nil || deleteOptions.Preconditions.ResourceVersion == nil || *deleteOptions.Preconditions.ResourceVersion == "" {
		return fmt.Errorf("reclamation delete omitted UID or resourceVersion preconditions")
	}
	return nil
}

func (c *reclamationExecutionDeleteTracker) beginReclamationDelete(providerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failDeleteOnce[providerID] {
		c.failDeleteOnce[providerID] = false
		return fmt.Errorf("injected recoverable delete failure for %s", providerID)
	}
	c.inFlightNow++
	if c.inFlightNow > c.maxObserved {
		c.maxObserved = c.inFlightNow
	}
	if c.inFlightGate != nil && c.inFlightNow == 2 {
		select {
		case <-c.inFlightGate:
		default:
			close(c.inFlightGate)
		}
	}
	return nil
}

func (c *reclamationExecutionDeleteTracker) waitForDelete(ctx context.Context) error {
	c.mu.Lock()
	block := c.blockDeletes
	c.mu.Unlock()
	if !block {
		return nil
	}
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		c.inFlightNow--
		c.mu.Unlock()
		return ctx.Err()
	}
}

func (c *reclamationExecutionDeleteTracker) finishReclamationDelete() {
	c.mu.Lock()
	c.inFlightNow--
	c.deleteCalls++
	c.mu.Unlock()
}

func (c *reclamationExecutionDeleteTracker) deleteCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleteCalls
}

func (c *reclamationExecutionDeleteTracker) inFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inFlightNow
}

func (c *reclamationExecutionDeleteTracker) maxInFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxObserved
}

func (c *reclamationExecutionDeleteTracker) unblockDeletes() {
	select {
	case <-c.release:
	default:
		close(c.release)
	}
}

func newReclamationExecutionHarness(t *testing.T, candidateCount int) *reclamationExecutionHarness {
	t.Helper()
	tracker := &reclamationExecutionDeleteTracker{release: make(chan struct{})}
	h := newReclamationQueueHarness(t, candidateCount, tracker)
	return &reclamationExecutionHarness{
		ctx: h.ctx, kubeClient: tracker, queue: h.queue, cmd: h.cmd, candidates: h.cmd.Candidates, tracker: tracker,
	}
}
