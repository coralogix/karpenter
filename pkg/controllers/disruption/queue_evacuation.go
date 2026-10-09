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

	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

type evacuationExecutionState struct {
	evictionPods []*corev1.Pod
}

func (q *Queue) handleEvacuationFailure(ctx, callbackCtx context.Context, cmd *Command, err error) {
	q.cancelEvacuationPods(cmd)
	rollbackErr := q.rollbackEvacuation(ctx, cmd.Candidates)
	q.recordEvacuationOutcome(callbackCtx, cmd, evacuationOutcomeFailed, multierr.Combine(err, rollbackErr))
}

func (q *Queue) handleEvacuationSuccess(ctx context.Context, cmd *Command) {
	q.recordEvacuationOutcome(ctx, cmd, evacuationOutcomeCompleted, nil)
}

func (q *Queue) evacuate(ctx context.Context, cmd *Command) error {
	if q.evictionQueue == nil {
		return NewUnrecoverableError(fmt.Errorf("evacuation requires the shared pod eviction queue"))
	}
	waiting, err := q.queueEvacuationPods(ctx, cmd)
	if err != nil {
		return err
	}
	if waiting > 0 {
		return fmt.Errorf("waiting for %d non-daemon pod(s) to leave evacuation nodes", waiting)
	}
	q.cancelEvacuationPods(cmd)
	return q.completeEvacuation(ctx, cmd)
}

func (q *Queue) queueEvacuationPods(ctx context.Context, cmd *Command) (int, error) {
	var waiting int
	queued := map[terminator.QueueKey]struct{}{}
	for _, pod := range q.evacuationPods(cmd) {
		queued[terminator.NewQueueKey(pod)] = struct{}{}
	}
	for _, candidate := range cmd.Candidates {
		if candidate.Node == nil || candidate.NodeClaim == nil {
			return 0, NewUnrecoverableError(fmt.Errorf("evacuation requires a registered node and NodeClaim"))
		}
		occupyingPods, err := nodeOccupyingPods(ctx, q.apiReader, candidate.Node)
		if err != nil {
			return 0, fmt.Errorf("listing pods on evacuation node, %w", err)
		}
		waiting += len(occupyingPods)
		evictable := lo.Filter(occupyingPods, func(p *corev1.Pod, _ int) bool { return pod.IsEvictable(p, q.clock, q.recorder) })
		q.addEvictablePods(cmd, queued, evictable)
	}
	return waiting, nil
}

func (q *Queue) addEvictablePods(cmd *Command, queued map[terminator.QueueKey]struct{}, pods []*corev1.Pod) {
	if len(pods) == 0 {
		return
	}
	q.evictionQueue.Add(pods...)
	var newlyQueued []*corev1.Pod
	for _, pod := range pods {
		key := terminator.NewQueueKey(pod)
		if _, exists := queued[key]; exists {
			continue
		}
		newlyQueued = append(newlyQueued, pod)
		queued[key] = struct{}{}
	}
	if len(newlyQueued) == 0 {
		return
	}
	q.Lock()
	if cmd.evacuationExecution == nil {
		cmd.evacuationExecution = &evacuationExecutionState{}
	}
	cmd.evacuationExecution.evictionPods = append(cmd.evacuationExecution.evictionPods, newlyQueued...)
	q.Unlock()
}

func (q *Queue) cancelEvacuationPods(cmd *Command) {
	if q.evictionQueue != nil {
		q.evictionQueue.Cancel(q.evacuationPods(cmd)...)
	}
}

func (q *Queue) evacuationPods(cmd *Command) []*corev1.Pod {
	q.RLock()
	defer q.RUnlock()
	if cmd.evacuationExecution == nil {
		return nil
	}
	return append([]*corev1.Pod(nil), cmd.evacuationExecution.evictionPods...)
}

func (q *Queue) completeEvacuation(ctx context.Context, cmd *Command) error {
	for _, candidate := range cmd.Candidates {
		if err := q.enterStandbyAfterEvacuation(ctx, candidate); err != nil {
			return fmt.Errorf("persisting standby state, %w", err)
		}
	}
	stateNodes := lo.Map(cmd.Candidates, func(c *Candidate, _ int) *state.StateNode { return c.StateNode })
	if err := state.RequireNoScheduleTaint(ctx, q.kubeClient, false, stateNodes...); err != nil {
		return fmt.Errorf("removing temporary disruption taint after evacuation, %w", err)
	}
	if err := state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, stateNodes...); err != nil {
		return fmt.Errorf("clearing disruption reason after evacuation, %w", err)
	}
	return nil
}

func (q *Queue) rollbackEvacuation(ctx context.Context, candidates []*Candidate) error {
	var uncordon []*state.StateNode
	var errs []error
	for _, candidate := range candidates {
		current := &v1.NodeClaim{}
		if err := q.kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), current); err != nil {
			if apierrors.IsNotFound(err) {
				uncordon = append(uncordon, candidate.StateNode)
				continue
			}
			errs = append(errs, fmt.Errorf("checking standby marker before evacuation rollback, %w", err))
			continue
		}
		if standby.IsNodeClaimStandby(current) {
			if err := q.standbyCoordinator.EnsureStandbyTaintForClaim(ctx, candidate.Node, current); err != nil {
				errs = append(errs, fmt.Errorf("restoring standby taint before evacuation rollback, %w", err))
				continue
			}
		}
		uncordon = append(uncordon, candidate.StateNode)
	}
	uncordonErr := state.RequireNoScheduleTaint(ctx, q.kubeClient, false, uncordon...)
	stateNodes := lo.Map(candidates, func(c *Candidate, _ int) *state.StateNode { return c.StateNode })
	conditionErr := state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, stateNodes...)
	return multierr.Combine(append(errs, uncordonErr, conditionErr)...)
}

func (q *Queue) enterStandbyAfterEvacuation(ctx context.Context, candidate *Candidate) error {
	return retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
		result, err := q.standbyCoordinator.EnterStandby(ctx, standbypkg.EnterStandbyRequest{
			NodeRef:      candidate.Node,
			NodeClaimRef: candidate.NodeClaim,
			ProviderID:   candidate.ProviderID(),
		}, standbypkg.EnterStandbyOptions{
			BarrierTaintPresent: true,
			Since:               q.clock.Now(),
		})
		if err != nil {
			return err
		}
		switch result.Outcome {
		case standbypkg.OutcomeCompleted, standbypkg.OutcomeUnchanged, standbypkg.OutcomeSkipped:
			return nil
		default:
			return fmt.Errorf("unexpected evacuation standby outcome %q", result.Outcome)
		}
	})
}

func (q *Queue) recordEvacuationOutcome(ctx context.Context, cmd *Command, outcome string, err error) {
	duration := q.clock.Since(cmd.CreationTimestamp)
	reason := pretty.ToSnakeCase(string(cmd.Reason()))
	if outcome == evacuationOutcomeCompleted {
		seenNodeClaims := map[string]struct{}{}
		for _, candidate := range cmd.Candidates {
			if candidate == nil || candidate.NodeClaim == nil {
				continue
			}
			nodeClaim := candidate.NodeClaim
			nodeClaimID := nodeClaim.Name
			if nodeClaimID == "" {
				nodeClaimID = nodeClaim.Status.ProviderID
			}
			if _, seen := seenNodeClaims[nodeClaimID]; seen {
				continue
			}
			seenNodeClaims[nodeClaimID] = struct{}{}
			NodeClaimsEvacuatedTotal.Inc(map[string]string{
				metrics.ReasonLabel:       reason,
				metrics.NodePoolLabel:     nodeClaim.Labels[v1.NodePoolLabelKey],
				metrics.CapacityTypeLabel: nodeClaim.Labels[v1.CapacityTypeLabelKey],
			})
		}
	}
	EvacuationCommandsTotal.Inc(map[string]string{
		evacuationOutcomeLabel: outcome,
		metrics.ReasonLabel:    reason,
	})
	EvacuationDurationSeconds.Observe(duration.Seconds(), map[string]string{
		metrics.ReasonLabel: reason,
	})

	logger := log.FromContext(ctx).WithValues(
		"command-id", cmd.ID.String(),
		"outcome", outcome,
		metrics.ReasonLabel, reason,
		"disrupted-node-count", len(cmd.Candidates),
		"duration", duration,
	)
	if err != nil {
		logger.Error(err, "evacuation failed")
		return
	}
	logger.Info("evacuation completed; source nodes handed off to standby")
}
