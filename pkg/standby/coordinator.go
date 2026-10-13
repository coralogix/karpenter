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

package standby

import (
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

// Coordinator owns complete standby transitions using live API reads.
type Coordinator struct {
	reader client.Reader
	writer client.Client
}

func NewCoordinator(reader client.Reader, writer client.Client) *Coordinator {
	return &Coordinator{reader: reader, writer: writer}
}

func (c *Coordinator) Activate(ctx context.Context, source utilstandby.ActivationSource, nodes ...*state.StateNode) (ActivationResult, error) {
	var result ActivationResult
	for _, node := range nodes {
		pair, err := c.activateNode(ctx, source, node)
		if err != nil {
			return result, fmt.Errorf("activating node %q, %w", node.Name(), err)
		}
		if pair != nil {
			result.Activated = append(result.Activated, *pair)
		}
	}
	return result, nil
}

// EnterStandbyOptions configures empty marking versus post-evacuation persistence.
type EnterStandbyOptions struct {
	// BarrierTaintPresent skips applying the temporary disruption scheduling barrier.
	BarrierTaintPresent bool
	Since               time.Time
	// Eligibility receives live Node and NodeClaim objects that the coordinator has already
	// verified match EnterStandbyRequest (UIDs and provider IDs) before invoking the callback.
	Eligibility func(context.Context, *corev1.Node, *v1.NodeClaim) (bool, error)
	// PostBarrierEligibility is evaluated after the barrier is applied during empty marking,
	// with the same input guarantee as Eligibility.
	PostBarrierEligibility func(context.Context, *corev1.Node, *v1.NodeClaim) (bool, error)
}

type EnterStandbyRequest struct {
	NodeRef      *corev1.Node
	NodeClaimRef *v1.NodeClaim
	ProviderID   string
}

func (c *Coordinator) EnterStandby(ctx context.Context, req EnterStandbyRequest, opts EnterStandbyOptions) (TransitionResult, error) {
	if req.NodeRef == nil || req.NodeClaimRef == nil {
		return TransitionResult{Outcome: OutcomeFailed}, fmt.Errorf("standby entry requires a Node and NodeClaim")
	}
	if opts.BarrierTaintPresent {
		return c.enterStandbyPostEvacuation(ctx, req, opts)
	}
	return c.enterStandbyEmptyMarking(ctx, req, opts)
}

//nolint:gocyclo // Post-evacuation persistence keeps marker and taint writes in one retry loop.
func (c *Coordinator) enterStandbyPostEvacuation(ctx context.Context, req EnterStandbyRequest, opts EnterStandbyOptions) (TransitionResult, error) {
	var result TransitionResult
	err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
		node, nodeClaim, matches, err := c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
		if err != nil {
			return err
		}
		if !matches {
			result = TransitionResult{Outcome: OutcomeSkipped}
			return nil
		}
		if utilstandby.IsNodeClaimActivating(nodeClaim) || utilstandby.HasNodeActivationMarker(node) {
			result = TransitionResult{Outcome: OutcomeSkipped, Node: node, NodeClaim: nodeClaim}
			return nil
		}
		if StandbyEntryComplete(node, nodeClaim) {
			result = TransitionResult{Outcome: OutcomeUnchanged, Node: node, NodeClaim: nodeClaim}
			return nil
		}
		if opts.Eligibility != nil {
			eligible, err := opts.Eligibility(ctx, node, nodeClaim)
			if err != nil {
				return err
			}
			if !eligible {
				result = TransitionResult{Outcome: OutcomeSkipped, Node: node, NodeClaim: nodeClaim}
				return nil
			}
		}
		if !utilstandby.IsNodeClaimStandby(nodeClaim) {
			if err := c.patchNodeClaimStandby(ctx, nodeClaim, true, opts.Since); err != nil {
				return err
			}
		}
		node, _, matches, err = c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
		if err != nil {
			return err
		}
		if !matches {
			result = TransitionResult{Outcome: OutcomeSkipped}
			return nil
		}
		if !utilstandby.HasNodeTaint(node) {
			if err := c.patchNodeStandbyTaint(ctx, node, true); err != nil {
				return err
			}
		}
		node, nodeClaim, matches, err = c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
		if err != nil {
			return err
		}
		if !matches {
			result = TransitionResult{Outcome: OutcomeSkipped}
			return nil
		}
		if !StandbyEntryComplete(node, nodeClaim) {
			return fmt.Errorf("standby entry incomplete after post-evacuation persistence")
		}
		result = TransitionResult{Outcome: OutcomeCompleted, Node: node, NodeClaim: nodeClaim}
		return nil
	})
	if err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	return result, nil
}

//nolint:gocyclo // Empty marking keeps live rechecks, optimistic patches, and rollback steps together.
func (c *Coordinator) enterStandbyEmptyMarking(ctx context.Context, req EnterStandbyRequest, opts EnterStandbyOptions) (TransitionResult, error) {
	var nodeClaim *v1.NodeClaim
	node, _, eligible, err := c.readEligiblePair(ctx, req, opts.Eligibility)
	if err != nil || !eligible {
		if err != nil {
			return TransitionResult{Outcome: OutcomeFailed}, err
		}
		return TransitionResult{Outcome: OutcomeSkipped}, nil
	}
	if err := patchDisruptionTaint(ctx, c.writer, node, true); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return TransitionResult{Outcome: OutcomeSkipped}, nil
		}
		return TransitionResult{Outcome: OutcomeFailed}, err
	}

	_, nodeClaim, eligible, err = c.readEligiblePair(ctx, req, c.postBarrierEligibility(opts))
	if err != nil || !eligible {
		rollbackErr := removeDisruptionTaint(ctx, c.reader, c.writer, req.NodeRef.Name)
		return TransitionResult{Outcome: OutcomeSkipped}, multierr.Combine(err, rollbackErr)
	}
	if err := c.patchNodeClaimStandby(ctx, nodeClaim, true, opts.Since); err != nil {
		rollbackErr := removeDisruptionTaint(ctx, c.reader, c.writer, req.NodeRef.Name)
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return TransitionResult{Outcome: OutcomeSkipped}, rollbackErr
		}
		return TransitionResult{Outcome: OutcomeFailed}, multierr.Combine(err, rollbackErr)
	}

	node, nodeClaim, matches, err := c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
	if err != nil {
		rollbackErr := removeDisruptionTaint(ctx, c.reader, c.writer, req.NodeRef.Name)
		return TransitionResult{Outcome: OutcomeFailed}, multierr.Combine(err, rollbackErr)
	}
	if !matches {
		rollbackErr := removeDisruptionTaint(ctx, c.reader, c.writer, req.NodeRef.Name)
		return TransitionResult{Outcome: OutcomeSkipped}, rollbackErr
	}
	if !utilstandby.IsNodeClaimStandby(nodeClaim) || utilstandby.IsNodeClaimActivating(nodeClaim) || node.Annotations[utilstandby.NodeClaimActivatingAnnotationKey] == "true" {
		if err := removeDisruptionTaint(ctx, c.reader, c.writer, node.Name); err != nil {
			return TransitionResult{Outcome: OutcomeFailed}, err
		}
		return TransitionResult{Outcome: OutcomeSkipped}, nil
	}
	if err := c.patchNodeStandbyTaint(ctx, node, true); err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if err := removeDisruptionTaint(ctx, c.reader, c.writer, node.Name); err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	node, nodeClaim, matches, err = c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
	if err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if !matches {
		return TransitionResult{Outcome: OutcomeSkipped}, nil
	}

	recovered, err := c.recoverOccupiedStandby(ctx, node, nodeClaim)
	if err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if recovered {
		return TransitionResult{Outcome: OutcomeRecovered, Node: node, NodeClaim: nodeClaim}, nil
	}
	if !utilstandby.IsNodeClaimStandby(nodeClaim) || utilstandby.IsNodeClaimActivating(nodeClaim) || !utilstandby.HasNodeTaint(node) {
		return TransitionResult{Outcome: OutcomeSkipped, Node: node, NodeClaim: nodeClaim}, nil
	}
	return TransitionResult{Outcome: OutcomeCompleted, Node: node, NodeClaim: nodeClaim}, nil
}

func (c *Coordinator) postBarrierEligibility(opts EnterStandbyOptions) func(context.Context, *corev1.Node, *v1.NodeClaim) (bool, error) {
	if opts.PostBarrierEligibility != nil {
		return opts.PostBarrierEligibility
	}
	return opts.Eligibility
}

func (c *Coordinator) readEligiblePair(ctx context.Context, req EnterStandbyRequest, eligibility func(context.Context, *corev1.Node, *v1.NodeClaim) (bool, error)) (*corev1.Node, *v1.NodeClaim, bool, error) {
	node, nodeClaim, matches, err := c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
	if err != nil || !matches {
		return nil, nil, false, err
	}
	if eligibility == nil {
		return node, nodeClaim, true, nil
	}
	eligible, err := eligibility(ctx, node, nodeClaim)
	return node, nodeClaim, eligible, err
}

func (c *Coordinator) recoverOccupiedStandby(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim) (bool, error) {
	empty, err := NodeEmpty(ctx, c.reader, node)
	if err != nil || empty {
		return false, err
	}
	return c.recoverOccupiedStandbyPair(ctx, node, nodeClaim)
}

func (c *Coordinator) recoverOccupiedStandbyPair(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim) (bool, error) {
	stateNode := &state.StateNode{Node: node.DeepCopy(), NodeClaim: nodeClaim.DeepCopy()}
	actResult, err := c.Activate(ctx, utilstandby.ActivationSourceRecovery, stateNode)
	if err != nil {
		return false, fmt.Errorf("restoring occupied standby capacity, %w", err)
	}
	if len(actResult.Activated) == 0 {
		return false, fmt.Errorf("occupied standby NodeClaim %q has not completed activation", nodeClaim.Name)
	}
	pair := actResult.Activated[0]
	if err := removeDisruptionTaint(ctx, c.reader, c.writer, pair.Node.Name); err != nil {
		return false, err
	}
	*node, *nodeClaim = *pair.Node, *pair.NodeClaim
	return true, nil
}

type RepairRequest struct {
	NodeRef      *corev1.Node
	NodeClaimRef *v1.NodeClaim
	ProviderID   string
}

//nolint:gocyclo // Repair keeps live identity, occupancy, and mutation steps in one operation.
func (c *Coordinator) RepairStandbyNode(ctx context.Context, req RepairRequest) (TransitionResult, error) {
	node, nodeClaim, matches, err := c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
	if err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if !matches ||
		!node.DeletionTimestamp.IsZero() || !nodeClaim.DeletionTimestamp.IsZero() ||
		nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
		return TransitionResult{Outcome: OutcomeSkipped}, nil
	}
	repaired, err := c.repairOrphanStandbyTaint(ctx, node, nodeClaim)
	if err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if repaired {
		return TransitionResult{Outcome: OutcomeRecovered, Node: node, NodeClaim: nodeClaim}, nil
	}
	if utilstandby.IsNodeClaimActivating(nodeClaim) || utilstandby.HasNodeActivationMarker(node) {
		stateNode := &state.StateNode{Node: node.DeepCopy(), NodeClaim: nodeClaim.DeepCopy()}
		actResult, err := c.Activate(ctx, utilstandby.ActivationSourceRecovery, stateNode)
		if err != nil {
			return TransitionResult{Outcome: OutcomeFailed}, err
		}
		if len(actResult.Activated) > 0 {
			pair := actResult.Activated[0]
			return TransitionResult{Outcome: OutcomeRecovered, Node: pair.Node, NodeClaim: pair.NodeClaim}, nil
		}
		node, nodeClaim, matches, err = c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
		if err != nil || !matches {
			if err != nil {
				return TransitionResult{Outcome: OutcomeFailed}, err
			}
			return TransitionResult{Outcome: OutcomeSkipped}, nil
		}
	}
	if utilstandby.IsNodeClaimStandby(nodeClaim) && !utilstandby.IsNodeClaimActivating(nodeClaim) &&
		!utilstandby.HasNodeActivationMarker(node) && !utilstandby.HasNodeTaint(node) {
		if err := c.applyNodeStandbyTaint(ctx, node); err != nil {
			return TransitionResult{Outcome: OutcomeFailed}, err
		}
		utilstandby.SetNodeTaint(node, true)
		return TransitionResult{Outcome: OutcomeRecovered, Node: node, NodeClaim: nodeClaim}, nil
	}
	empty, err := NodeEmpty(ctx, c.reader, node)
	if err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if empty {
		if StandbyEntryComplete(node, nodeClaim) {
			return TransitionResult{Outcome: OutcomeUnchanged, Node: node, NodeClaim: nodeClaim}, nil
		}
		return TransitionResult{Outcome: OutcomeSkipped, Node: node, NodeClaim: nodeClaim}, nil
	}
	recovered, err := c.recoverOccupiedStandbyPair(ctx, node, nodeClaim)
	if err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if recovered {
		return TransitionResult{Outcome: OutcomeRecovered, Node: node, NodeClaim: nodeClaim}, nil
	}
	return TransitionResult{Outcome: OutcomeSkipped, Node: node, NodeClaim: nodeClaim}, nil
}

func (c *Coordinator) repairOrphanStandbyTaint(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim) (bool, error) {
	if node == nil || nodeClaim == nil {
		return false, nil
	}
	claimStandby := utilstandby.IsNodeClaimStandby(nodeClaim)
	claimActivating := utilstandby.IsNodeClaimActivating(nodeClaim)
	nodeActivating := node.Annotations[utilstandby.NodeClaimActivatingAnnotationKey] == "true"
	if claimStandby || claimActivating || nodeActivating {
		return false, nil
	}
	if !utilstandby.HasNodeTaint(node) {
		return false, nil
	}
	if err := c.patchNodeStandbyTaint(ctx, node, false); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("removing orphan standby taint from Node %q, %w", node.Name, err)
	}
	return true, nil
}

func (c *Coordinator) EnsureStandbyTaintForClaim(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim) error {
	return c.ensureStandbyTaintForClaim(ctx, node, nodeClaim)
}

func ActivationComplete(node *corev1.Node, nodeClaim *v1.NodeClaim) bool {
	if node == nil || nodeClaim == nil {
		return false
	}
	return !utilstandby.IsNodeClaimStandby(nodeClaim) && !utilstandby.IsNodeClaimActivating(nodeClaim) &&
		!utilstandby.HasNodeTaint(node) && node.Annotations[utilstandby.NodeClaimActivatingAnnotationKey] != "true"
}

func FilterActivating(nodes state.StateNodes) []*state.StateNode {
	return lo.Filter(nodes, func(node *state.StateNode, _ int) bool {
		return node != nil && !node.MarkedForDeletion() &&
			(utilstandby.IsNodeClaimActivating(node.NodeClaim) || utilstandby.HasNodeActivationMarker(node.Node))
	})
}
