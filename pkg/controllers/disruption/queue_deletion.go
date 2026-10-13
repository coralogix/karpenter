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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

const maxConcurrentCandidateDeletes = 16

type deletionProgress struct {
	done    bool
	deleted bool
}

func (q *Queue) deleteCandidates(ctx context.Context, cmd *Command) error {
	if cmd.deletionProgress == nil {
		cmd.deletionProgress = make([]deletionProgress, len(cmd.Candidates))
	}

	workers := min(len(cmd.Candidates), maxConcurrentCandidateDeletes)
	errs := make([]error, len(cmd.Candidates))
	workqueue.ParallelizeUntil(ctx, workers, len(cmd.Candidates), func(i int) {
		if err := q.deleteCandidate(ctx, cmd, &cmd.deletionProgress[i], cmd.Candidates[i]); err != nil {
			errs[i] = err
		}
	})
	if err := multierr.Combine(errs...); err != nil {
		return err
	}
	if _, ok := cmd.Method.(*Reclamation); ok {
		deleted := make([]*Candidate, 0, len(cmd.Candidates))
		for i := range cmd.deletionProgress {
			if cmd.deletionProgress[i].deleted {
				deleted = append(deleted, cmd.Candidates[i])
			}
		}
		if len(deleted) > 0 {
			q.reportReclamationBatchSucceeded(ctx, deleted)
		}
	}
	return nil
}

func (q *Queue) deleteCandidate(ctx context.Context, cmd *Command, progress *deletionProgress, candidate *Candidate) error {
	if progress.done {
		return nil
	}
	if _, ok := cmd.Method.(*Reclamation); ok {
		eligible, err := q.reclamationCandidateBeforeDelete(ctx, candidate)
		if err != nil {
			return fmt.Errorf("validating node before deletion, %w", err)
		}
		if !eligible {
			q.cleanupSkippedReclamationCandidate(ctx, candidate)
			progress.done = true
			return nil
		}
	}
	if err := q.deleteNodeClaim(ctx, cmd, candidate); err != nil {
		if apierrors.IsNotFound(err) {
			progress.done = true
			return nil
		}
		return err
	}
	q.recorder.Publish(disruptionevents.Terminating(candidate.Node, candidate.NodeClaim, string(cmd.Reason()))...)
	metrics.NodeClaimsDisruptedTotal.Inc(map[string]string{
		metrics.ReasonLabel:       pretty.ToSnakeCase(string(cmd.Reason())),
		metrics.NodePoolLabel:     candidate.NodeClaim.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel: candidate.NodeClaim.Labels[v1.CapacityTypeLabelKey],
	})
	if _, ok := cmd.Method.(*Reclamation); ok {
		q.reportReclamationCandidateDeleted(candidate)
	}
	progress.done = true
	progress.deleted = true
	return nil
}

func (q *Queue) cleanupSkippedReclamationCandidate(ctx context.Context, candidate *Candidate) {
	q.cluster.UnmarkForDeletion(candidate.ProviderID())
	stateNodes := []*state.StateNode{candidate.StateNode}
	_ = state.RequireNoScheduleTaint(ctx, q.kubeClient, false, stateNodes...)
	_ = state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, stateNodes...)
}

func (q *Queue) deleteNodeClaim(ctx context.Context, cmd *Command, candidate *Candidate) error {
	if _, ok := cmd.Method.(*Reclamation); ok {
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

func (q *Queue) handleDeletionFailure(ctx context.Context, cmd *Command, err error) {
	stateNodes := make([]*state.StateNode, 0, len(cmd.Candidates))
	for _, candidate := range cmd.Candidates {
		stateNodes = append(stateNodes, candidate.StateNode)
	}
	cleanupErr := state.RequireNoScheduleTaint(ctx, q.kubeClient, false, stateNodes...)
	cleanupErr = multierr.Combine(cleanupErr, state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, stateNodes...))
	log.FromContext(ctx).Error(multierr.Combine(err, cleanupErr), "failed terminating nodes while executing a disruption command")
}
