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

	corev1 "k8s.io/api/core/v1"
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

type reclamationQueueHarness struct {
	ctx        context.Context
	kubeClient client.Client
	queue      *Queue
	cmd        *Command
	candidate  *Candidate
	claim      *v1.NodeClaim
}

func newReclamationQueueHarness(t *testing.T, candidateCount int, kubeClient client.Client) *reclamationQueueHarness {
	t.Helper()
	ctx := options.ToContext(context.Background(), test.Options())
	now := time.Date(2026, time.September, 29, 12, 0, 0, 0, time.UTC)
	clk := clocktesting.NewFakeClock(now)
	pool := reclamationTestManagedPool("reclamation-queue-pool")
	objects := []client.Object{pool}
	candidates := make([]*Candidate, 0, candidateCount)
	for i := range candidateCount {
		name := fmt.Sprintf("queue-node-%02d", i)
		claim, node := reclamationTestNode(pool.Name, name, true, true, false)
		claim.UID = types.UID(fmt.Sprintf("claim-uid-%02d", i))
		node.UID = types.UID(fmt.Sprintf("node-uid-%02d", i))
		node.Labels[v1.NodeInitializedLabelKey] = "true"
		objects = append(objects, claim, node)
		candidates = append(candidates, reclamationCandidate(pool, claim, node))
	}
	if tracker, ok := kubeClient.(*reclamationExecutionDeleteTracker); ok && tracker.Client == nil {
		tracker.Client = fake.NewClientBuilder().
			WithScheme(scheme.Scheme).
			WithObjects(objects...).
			WithIndex(&corev1.Pod{}, "spec.nodeName", reclamationPodNodeIndex).
			Build()
	} else if kubeClient == nil {
		kubeClient = fake.NewClientBuilder().
			WithScheme(scheme.Scheme).
			WithObjects(objects...).
			WithIndex(&corev1.Pod{}, "spec.nodeName", reclamationPodNodeIndex).
			Build()
	}
	cluster := state.NewCluster(clk, kubeClient, nil)
	for _, candidate := range candidates {
		storedClaim := &v1.NodeClaim{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), storedClaim); err != nil {
			t.Fatalf("getting test NodeClaim: %v", err)
		}
		storedNode := &corev1.Node{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.Node), storedNode); err != nil {
			t.Fatalf("getting test Node: %v", err)
		}
		cluster.UpdateNodeClaim(storedClaim)
		if err := cluster.UpdateNode(ctx, storedNode); err != nil {
			t.Fatalf("adding test Node to cluster state: %v", err)
		}
		candidate.Node = storedNode
		candidate.NodeClaim = storedClaim
	}
	queue := NewTestQueue(kubeClient, events.NewRecorder(&record.FakeRecorder{}), cluster, clk, nil)
	consolidationMethod := MakeConsolidation(clk, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), queue, nil)
	reclamation := NewReclamation(consolidationMethod)
	cmd := &Command{
		Method:            reclamation,
		Action:            DeleteAction,
		CreationTimestamp: clk.Now(),
		Candidates:        candidates,
	}
	for _, candidate := range candidates {
		queue.ProviderIDToCommand[candidate.ProviderID()] = cmd
	}
	return &reclamationQueueHarness{
		ctx: ctx, kubeClient: kubeClient, queue: queue, cmd: cmd,
		candidate: candidates[0], claim: candidates[0].NodeClaim,
	}
}
