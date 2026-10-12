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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
)

func TestReclamationExecutionRefusesStalePlanning(t *testing.T) {
	h := newReclamationExecutionHarness(t, 1)
	workload := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "late-pod", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: h.candidates[0].Node.Name}}
	if err := h.kubeClient.Create(h.ctx, workload); err != nil {
		t.Fatal(err)
	}
	err := h.queue.deleteCandidates(h.ctx, h.ctx, h.cmd)
	if !IsUnrecoverableError(err) {
		t.Fatalf("deleteCandidates() error = %v, want unrecoverable emptiness rejection", err)
	}
	if h.tracker.deleteCalls != 0 {
		t.Fatalf("delete attempts = %d, want 0", h.tracker.deleteCalls)
	}
}

func TestReclamationExecutionDeletesEligibleControl(t *testing.T) {
	h := newReclamationExecutionHarness(t, 1)
	if err := h.queue.deleteCandidates(h.ctx, h.ctx, h.cmd); err != nil {
		t.Fatalf("deleteCandidates() error = %v", err)
	}
	if h.tracker.deleteCalls != 1 {
		t.Fatalf("delete attempts = %d, want 1", h.tracker.deleteCalls)
	}
}

func TestReclamationExecutionBoundedConcurrency(t *testing.T) {
	const candidateCount = 20
	h := newReclamationExecutionHarness(t, candidateCount)
	execErr := runBlockedReclamationDeletes(t, h)
	if execErr != nil {
		t.Fatalf("deleteCandidates() error = %v", execErr)
	}
	if h.tracker.deleteCalls != candidateCount {
		t.Fatalf("delete attempts = %d, want %d", h.tracker.deleteCalls, candidateCount)
	}
	if h.tracker.maxInFlight() > maxConcurrentCandidateDeletes {
		t.Fatalf("max in-flight deletes = %d, want at most %d", h.tracker.maxInFlight(), maxConcurrentCandidateDeletes)
	}
}

func runBlockedReclamationDeletes(t *testing.T, h *reclamationExecutionHarness) error {
	t.Helper()
	h.tracker.blockDeletes = true
	done := make(chan struct{})
	var execErr error
	go func() {
		execErr = h.queue.deleteCandidates(h.ctx, h.ctx, h.cmd)
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
		if tracker.inFlight() == maxConcurrentCandidateDeletes || tracker.deleteCalls == candidateCount {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if tracker.inFlight() < maxConcurrentCandidateDeletes && tracker.deleteCalls != candidateCount {
		t.Fatalf("in-flight deletes = %d, want %d before releasing the gate", tracker.inFlight(), maxConcurrentCandidateDeletes)
	}
}

func TestReclamationExecutionPartialBatchRetry(t *testing.T) {
	h := newReclamationExecutionHarness(t, 2)
	workload := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "blocker", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: h.candidates[1].Node.Name}}
	if err := h.kubeClient.Create(h.ctx, workload); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.deleteCandidates(h.ctx, h.ctx, h.cmd); !IsUnrecoverableError(err) {
		t.Fatalf("first deleteCandidates() error = %v, want unrecoverable", err)
	}
	if h.tracker.deleteCalls != 1 {
		t.Fatalf("delete attempts after partial failure = %d, want 1", h.tracker.deleteCalls)
	}
	if err := h.kubeClient.Delete(h.ctx, workload); err != nil {
		t.Fatal(err)
	}
	if err := h.queue.deleteCandidates(h.ctx, h.ctx, h.cmd); err != nil {
		t.Fatalf("second deleteCandidates() error = %v", err)
	}
	if h.tracker.deleteCalls != 2 {
		t.Fatalf("delete attempts after retry = %d, want 2", h.tracker.deleteCalls)
	}
}

func TestReclamationExecutionContextCancellation(t *testing.T) {
	h := newReclamationExecutionHarness(t, 4)
	h.tracker.blockDeletes = true
	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = h.queue.deleteCandidates(ctx, h.ctx, h.cmd)
		close(done)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for h.tracker.inFlight() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if h.tracker.inFlight() < 2 {
		t.Fatal("deleteCandidates() did not start enough work before cancellation")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for cancellation")
	}
	if h.tracker.deleteCalls >= 4 {
		t.Fatalf("delete attempts after cancellation = %d, want fewer than 4", h.tracker.deleteCalls)
	}
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
	deleteCalls  int
	blockDeletes bool
	release      chan struct{}
	mu           sync.Mutex
	inFlightNow  int
	maxObserved  int
}

func (c *reclamationExecutionDeleteTracker) Delete(ctx context.Context, object client.Object, options ...client.DeleteOption) error {
	if _, ok := object.(*v1.NodeClaim); !ok {
		return c.Client.Delete(ctx, object, options...)
	}
	deleteOptions := &client.DeleteOptions{}
	for _, option := range options {
		option.ApplyToDelete(deleteOptions)
	}
	if deleteOptions.Preconditions == nil || deleteOptions.Preconditions.UID == nil || deleteOptions.Preconditions.ResourceVersion == nil || *deleteOptions.Preconditions.ResourceVersion == "" {
		return fmt.Errorf("reclamation delete omitted UID or resourceVersion preconditions")
	}
	c.mu.Lock()
	c.inFlightNow++
	if c.inFlightNow > c.maxObserved {
		c.maxObserved = c.inFlightNow
	}
	block := c.blockDeletes
	c.mu.Unlock()
	if block {
		select {
		case <-c.release:
		case <-ctx.Done():
			c.mu.Lock()
			c.inFlightNow--
			c.mu.Unlock()
			return ctx.Err()
		}
	}
	c.mu.Lock()
	c.inFlightNow--
	c.mu.Unlock()
	c.deleteCalls++
	return c.Client.Delete(ctx, object, options...)
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
	ctx := options.ToContext(context.Background(), test.Options())
	now := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
	clk := clocktesting.NewFakeClock(now)
	pool := reclamationTestManagedPool("reclamation-exec-pool")
	objects := []client.Object{pool}
	candidates := make([]*Candidate, 0, candidateCount)
	for i := range candidateCount {
		name := fmt.Sprintf("exec-node-%02d", i)
		claim, node := reclamationTestNode(pool.Name, name, true, true, false)
		claim.UID = types.UID(fmt.Sprintf("claim-uid-%02d", i))
		node.UID = types.UID(fmt.Sprintf("node-uid-%02d", i))
		node.Labels[v1.NodeInitializedLabelKey] = "true"
		objects = append(objects, claim, node)
		candidates = append(candidates, reclamationCandidate(pool, claim, node))
	}
	baseClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(objects...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", reclamationPodNodeIndex).
		Build()
	tracker := &reclamationExecutionDeleteTracker{Client: baseClient, release: make(chan struct{})}
	cluster := state.NewCluster(clk, tracker, nil)
	for _, candidate := range candidates {
		storedClaim := &v1.NodeClaim{}
		if err := tracker.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), storedClaim); err != nil {
			t.Fatalf("getting test NodeClaim: %v", err)
		}
		storedNode := &corev1.Node{}
		if err := tracker.Get(ctx, client.ObjectKeyFromObject(candidate.Node), storedNode); err != nil {
			t.Fatalf("getting test Node: %v", err)
		}
		cluster.UpdateNodeClaim(storedClaim)
		if err := cluster.UpdateNode(ctx, storedNode); err != nil {
			t.Fatalf("adding test Node to cluster state: %v", err)
		}
		candidate.Node = storedNode
		candidate.NodeClaim = storedClaim
	}
	queue := NewTestQueue(tracker, events.NewRecorder(&record.FakeRecorder{}), cluster, clk, nil)
	consolidationMethod := MakeConsolidation(clk, cluster, tracker, nil, nil, events.NewRecorder(&record.FakeRecorder{}), queue, nil)
	reclamation := NewReclamation(consolidationMethod)
	cmd := &Command{
		Method:            reclamation,
		Action:            DeleteAction,
		CreationTimestamp: clk.Now(),
		Candidates:        candidates,
	}
	return &reclamationExecutionHarness{
		ctx: ctx, kubeClient: tracker, queue: queue, cmd: cmd, candidates: candidates, tracker: tracker,
	}
}
