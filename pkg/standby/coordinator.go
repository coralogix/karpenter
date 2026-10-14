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

	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

// Coordinator owns standby entry and cleanup repair using live API reads.
type Coordinator struct {
	reader client.Reader
	writer client.Client
}

func NewCoordinator(reader client.Reader, writer client.Client) *Coordinator {
	return &Coordinator{reader: reader, writer: writer}
}

type EnterStandbyOptions struct {
	// BarrierTaintPresent skips applying the temporary disruption scheduling barrier.
	BarrierTaintPresent bool
	Since               time.Time
	// Eligibility receives live objects verified against the request's UIDs and provider ID.
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
	if !utilstandby.IsNodeClaimStandby(nodeClaim) {
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
	if !StandbyEntryComplete(node, nodeClaim) {
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

type RepairRequest = EnterStandbyRequest

// RepairStandbyNode restores consistency between the standby marker and taint.
// Occupancy is checked before restoring a missing taint so late bindings recover to active capacity.
//
//nolint:gocyclo // Live identity, occupancy, and optimistic writes stay together in one repair operation.
func (c *Coordinator) RepairStandbyNode(ctx context.Context, req RepairRequest) (TransitionResult, error) {
	node, nodeClaim, matches, err := c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
	if err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if !matches || !node.DeletionTimestamp.IsZero() || !nodeClaim.DeletionTimestamp.IsZero() ||
		nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
		return TransitionResult{Outcome: OutcomeSkipped}, nil
	}

	if !utilstandby.IsNodeClaimStandby(nodeClaim) {
		if !utilstandby.HasNodeTaint(node) {
			return TransitionResult{Outcome: OutcomeUnchanged, Node: node, NodeClaim: nodeClaim}, nil
		}
		if err := c.patchNodeStandbyTaint(ctx, node, false); err != nil {
			return TransitionResult{Outcome: OutcomeFailed}, fmt.Errorf("removing orphan standby taint from Node %q, %w", node.Name, err)
		}
		return TransitionResult{Outcome: OutcomeRecovered, Node: node, NodeClaim: nodeClaim}, nil
	}

	empty, err := NodeEmpty(ctx, c.reader, node)
	if err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if !empty {
		if err := c.patchNodeClaimStandby(ctx, nodeClaim, false, time.Time{}); err != nil {
			return TransitionResult{Outcome: OutcomeFailed}, fmt.Errorf("removing standby marker from occupied NodeClaim %q, %w", nodeClaim.Name, err)
		}
		node, nodeClaim, matches, err = c.readLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
		if err != nil {
			return TransitionResult{Outcome: OutcomeFailed}, err
		}
		if !matches || !node.DeletionTimestamp.IsZero() || !nodeClaim.DeletionTimestamp.IsZero() ||
			nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() || utilstandby.IsNodeClaimStandby(nodeClaim) {
			return TransitionResult{Outcome: OutcomeSkipped}, nil
		}
		if utilstandby.HasNodeTaint(node) {
			if err := c.patchNodeStandbyTaint(ctx, node, false); err != nil {
				return TransitionResult{Outcome: OutcomeFailed}, fmt.Errorf("removing standby taint from occupied Node %q, %w", node.Name, err)
			}
		}
		return TransitionResult{Outcome: OutcomeRecovered, Node: node, NodeClaim: nodeClaim}, nil
	}

	if !utilstandby.HasNodeTaint(node) {
		if err := c.applyNodeStandbyTaint(ctx, node); err != nil {
			return TransitionResult{Outcome: OutcomeFailed}, fmt.Errorf("restoring standby taint on Node %q, %w", node.Name, err)
		}
		return TransitionResult{Outcome: OutcomeRecovered, Node: node, NodeClaim: nodeClaim}, nil
	}
	return TransitionResult{Outcome: OutcomeUnchanged, Node: node, NodeClaim: nodeClaim}, nil
}
