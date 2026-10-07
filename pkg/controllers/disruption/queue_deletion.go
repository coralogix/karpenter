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

	"go.uber.org/multierr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

type deletionCandidatePhase uint8

const (
	deletionPending deletionCandidatePhase = iota
	deletionCallbackPending
	deletionComplete
)

// deletionExecutionState tracks each candidate's delete and callback lifecycle across
// queue retries, preventing a callback failure from repeating delete side effects.
type deletionExecutionState struct {
	candidates map[string]deletionCandidatePhase
}

// deleteCandidates removes source NodeClaims after replacement readiness has been checked.
// The termination controller handles the eventual deletion of the corresponding Nodes.
func (q *Queue) deleteCandidates(ctx, callbackCtx context.Context, cmd *Command) error {
	q.Lock()
	if cmd.deletionExecution == nil {
		cmd.deletionExecution = &deletionExecutionState{}
	}
	progress := cmd.deletionExecution
	if progress.candidates == nil {
		progress.candidates = map[string]deletionCandidatePhase{}
	}
	q.Unlock()

	errs := make([]error, len(cmd.Candidates))
	workqueue.ParallelizeUntil(ctx, len(cmd.Candidates), len(cmd.Candidates), func(i int) {
		if err := q.deleteCandidate(ctx, cmd, progress, cmd.Candidates[i]); err != nil {
			errs[i] = err
		}
	})
	// On error, the queue retries. If the command timeout is reached, Reconcile marks it failed.
	if err := multierr.Combine(errs...); err != nil {
		return err
	}
	if cmd.OnSuccess != nil {
		return cmd.OnSuccess(callbackCtx)
	}
	return nil
}

func (q *Queue) deleteCandidate(ctx context.Context, cmd *Command, progress *deletionExecutionState, candidate *Candidate) error {
	providerID := candidate.ProviderID()
	q.Lock()
	phase := progress.candidates[providerID]
	q.Unlock()
	switch phase {
	case deletionComplete:
		return nil
	case deletionCallbackPending:
		return q.runDeleteSuccessCallback(ctx, cmd, progress, candidate, providerID)
	case deletionPending:
	default:
		return NewUnrecoverableError(fmt.Errorf("unknown deletion phase %d", phase))
	}
	if cmd.BeforeDelete != nil {
		if err := cmd.BeforeDelete(ctx, []*Candidate{candidate}); err != nil {
			return fmt.Errorf("validating node before deletion, %w", err)
		}
	}
	if err := q.deleteNodeClaim(ctx, cmd, candidate); err != nil {
		return client.IgnoreNotFound(err)
	}

	q.Lock()
	progress.candidates[providerID] = deletionCallbackPending
	q.Unlock()
	q.recorder.Publish(disruptionevents.Terminating(candidate.Node, candidate.NodeClaim, string(cmd.Reason()))...)
	metrics.NodeClaimsDisruptedTotal.Inc(map[string]string{
		metrics.ReasonLabel:       pretty.ToSnakeCase(string(cmd.Reason())),
		metrics.NodePoolLabel:     candidate.NodeClaim.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel: candidate.NodeClaim.Labels[v1.CapacityTypeLabelKey],
	})
	return q.runDeleteSuccessCallback(ctx, cmd, progress, candidate, providerID)
}

func (q *Queue) deleteNodeClaim(ctx context.Context, cmd *Command, candidate *Candidate) error {
	if cmd.DeleteWithPreconditions {
		if candidate.NodeClaim == nil || candidate.NodeClaim.UID == "" || candidate.NodeClaim.ResourceVersion == "" {
			return NewUnrecoverableError(fmt.Errorf("conditional deletion requires a fresh NodeClaim UID and resourceVersion"))
		}
		uid, resourceVersion := candidate.NodeClaim.UID, candidate.NodeClaim.ResourceVersion
		return q.kubeClient.Delete(ctx, candidate.NodeClaim, &client.DeleteOptions{Preconditions: &metav1.Preconditions{
			UID:             &uid,
			ResourceVersion: &resourceVersion,
		}})
	}
	return retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
		return q.kubeClient.Delete(ctx, candidate.NodeClaim)
	})
}

func (q *Queue) runDeleteSuccessCallback(ctx context.Context, cmd *Command, progress *deletionExecutionState, candidate *Candidate, providerID string) error {
	if cmd.OnDeleteSuccess == nil {
		q.Lock()
		progress.candidates[providerID] = deletionComplete
		q.Unlock()
		return nil
	}
	if err := cmd.OnDeleteSuccess(ctx, candidate); err != nil {
		return err
	}
	q.Lock()
	progress.candidates[providerID] = deletionComplete
	q.Unlock()
	return nil
}

func (q *Queue) handleDeletionFailure(ctx context.Context, cmd *Command, err error) {
	stateNodes := make([]*state.StateNode, 0, len(cmd.Candidates))
	for _, candidate := range cmd.Candidates {
		stateNodes = append(stateNodes, candidate.StateNode)
	}
	cleanupErr := state.RequireNoScheduleTaint(ctx, q.kubeClient, false, stateNodes...)
	cleanupErr = multierr.Combine(cleanupErr, state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, stateNodes...))
	log.FromContext(ctx).Error(multierr.Combine(err, cleanupErr), "failed terminating nodes while executing a disruption command")
}
