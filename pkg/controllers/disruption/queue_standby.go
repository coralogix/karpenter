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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/standbymetrics"
	"sigs.k8s.io/karpenter/pkg/metrics"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

//nolint:gocyclo // Reservation, transition, and per-candidate reporting form one synchronous command.
func (q *Queue) startStandbyCommand(ctx context.Context, cmd *Command) error {
	providerIDs := lo.Map(cmd.Candidates, func(candidate *Candidate, _ int) string { return candidate.ProviderID() })
	q.Lock()
	for _, providerID := range providerIDs {
		if _, exists := q.ProviderIDToCommand[providerID]; exists {
			q.Unlock()
			return fmt.Errorf("standby candidate is already being disrupted")
		}
	}
	for _, providerID := range providerIDs {
		q.ProviderIDToCommand[providerID] = cmd
	}
	q.Unlock()
	defer func() {
		q.Lock()
		defer q.Unlock()
		for _, providerID := range providerIDs {
			if q.ProviderIDToCommand[providerID] == cmd {
				delete(q.ProviderIDToCommand, providerID)
			}
		}
	}()

	if _, ok := cmd.Method.(*StandbyMarking); !ok {
		return fmt.Errorf("standby action requires StandbyMarking method")
	}
	var transitioned []*Candidate
	var errs []error
	for _, candidate := range cmd.Candidates {
		marked, err := q.markEmptyNodeStandby(ctx, candidate)
		if err != nil {
			errs = append(errs, fmt.Errorf("marking NodeClaim %q standby, %w", candidate.NodeClaim.Name, err))
			continue
		}
		if marked {
			transitioned = append(transitioned, candidate)
		}
	}
	if len(transitioned) > 0 {
		standbyNodePoolsMarked := map[string]int{}
		for _, candidate := range transitioned {
			standbymetrics.StandbyNodesMarkedTotal.Inc(map[string]string{metrics.NodePoolLabel: candidate.NodePool.Name})
			standbyNodePoolsMarked[candidate.NodePool.Name]++
			q.recorder.Publish(disruptionevents.StandbyMarked(candidate.Node, candidate.NodeClaim)...)
		}
		log.FromContext(ctx).Info("marked empty nodes as standby",
			"nodeCount", len(transitioned),
			"nodePoolCounts", standbyNodePoolsMarked,
		)
	}
	return multierr.Combine(errs...)
}

func (q *Queue) markEmptyNodeStandby(ctx context.Context, candidate *Candidate) (bool, error) {
	result, err := q.standbyCoordinator.EnterStandby(ctx, standbypkg.EnterStandbyRequest{
		NodeRef:      candidate.Node,
		NodeClaimRef: candidate.NodeClaim,
		ProviderID:   candidate.ProviderID(),
	}, standbypkg.EnterStandbyOptions{
		Since: q.clock.Now(),
		Eligibility: func(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim) (bool, error) {
			return q.standbyCandidateEligibleForTransition(ctx, candidate, node, nodeClaim)
		},
		PostBarrierEligibility: func(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim) (bool, error) {
			return q.standbyCandidateOccupiedAfterBarrier(ctx, candidate, node, nodeClaim)
		},
	})
	if err != nil {
		return false, err
	}
	switch result.Outcome {
	case standbypkg.OutcomeCompleted:
		if err := q.updateStandbyState(ctx, result.Node, result.NodeClaim); err != nil {
			return false, err
		}
		candidate.Node = result.Node
		candidate.NodeClaim = result.NodeClaim
		return true, nil
	case standbypkg.OutcomeRecovered, standbypkg.OutcomeSkipped, standbypkg.OutcomeUnchanged:
		if result.Node != nil && result.NodeClaim != nil {
			if err := q.updateStandbyState(ctx, result.Node, result.NodeClaim); err != nil {
				return false, err
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("unexpected standby entry outcome %q", result.Outcome)
	}
}

//nolint:gocyclo // Identity, cached policy, nomination, and live occupancy checks are one transition precondition.
func (q *Queue) standbyCandidateEligibleForTransition(ctx context.Context, candidate *Candidate, node *corev1.Node, nodeClaim *v1.NodeClaim) (bool, error) {
	if candidate == nil || candidate.Node == nil || candidate.NodeClaim == nil || candidate.NodePool == nil {
		return false, fmt.Errorf("standby candidate is missing its Node, NodeClaim, or NodePool")
	}
	if !node.DeletionTimestamp.IsZero() || !nodeClaim.DeletionTimestamp.IsZero() || nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() ||
		standby.IsNodeClaimStandby(nodeClaim) || standby.IsNodeClaimActivating(nodeClaim) || standby.HasNodeTaint(node) ||
		node.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" ||
		node.Annotations[v1.DoNotDisruptAnnotationKey] == "true" || nodeClaim.Annotations[v1.DoNotDisruptAnnotationKey] == "true" ||
		q.cluster.IsNodeNominated(candidate.ProviderID()) {
		return false, nil
	}
	if q.cluster.HasBufferPods(candidate.ProviderID()) {
		return false, nil
	}
	nodePool := &v1.NodePool{}
	if err := q.kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.NodePool), nodePool); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting standby candidate NodePool, %w", err)
	}
	if !standbyLifecycleEnabled(nodePool) {
		return false, nil
	}
	if node.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name || nodeClaim.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name ||
		node.Labels[v1.NodeInitializedLabelKey] != "true" {
		return false, nil
	}
	empty, err := nodeEmpty(ctx, q.apiReader, node)
	if err != nil || !empty {
		return false, err
	}
	return true, nil
}

func (q *Queue) standbyCandidateOccupiedAfterBarrier(ctx context.Context, candidate *Candidate, node *corev1.Node, _ *v1.NodeClaim) (bool, error) {
	if q.cluster.IsNodeNominated(candidate.ProviderID()) || q.cluster.HasBufferPods(candidate.ProviderID()) {
		return false, nil
	}
	empty, err := nodeEmpty(ctx, q.apiReader, node)
	if err != nil || !empty {
		return false, err
	}
	return true, nil
}

func (q *Queue) updateStandbyState(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim) error {
	var pods corev1.PodList
	if err := q.apiReader.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return fmt.Errorf("listing pods after standby transition, %w", err)
	}
	q.cluster.UpdateNodeClaim(nodeClaim)
	return q.cluster.UpdateNodeWithPods(ctx, node, lo.ToSlicePtr(pods.Items))
}
