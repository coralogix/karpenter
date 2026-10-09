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
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/standbymetrics"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

// Coordinator owns complete standby transitions using live API reads.
type Coordinator struct {
	reader client.Reader
	writer client.Client
	clock  clock.Clock
}

func NewCoordinator(reader client.Reader, writer client.Client, clk clock.Clock) *Coordinator {
	if clk == nil {
		clk = clock.RealClock{}
	}
	return &Coordinator{reader: reader, writer: writer, clock: clk}
}

func TestCoordinator(c client.Client, clk clock.Clock) *Coordinator {
	return NewCoordinator(c, c, clk)
}

func (c *Coordinator) lifecycle() utilstandby.Lifecycle {
	return utilstandby.NewLifecycle(c.reader, c.writer)
}

func (c *Coordinator) Activate(ctx context.Context, source utilstandby.ActivationSource, nodes ...*state.StateNode) error {
	var errs []error
	for _, node := range nodes {
		if node == nil || node.NodeClaim == nil ||
			(!utilstandby.IsNodeClaimStandby(node.NodeClaim) && !utilstandby.IsNodeClaimActivating(node.NodeClaim) && !utilstandby.HasNodeActivationMarker(node.Node)) {
			continue
		}
		if err := c.activateNode(ctx, source, node); err != nil {
			errs = append(errs, fmt.Errorf("activating node %q, %w", node.Name(), err))
		}
	}
	return multierr.Combine(errs...)
}

func (c *Coordinator) activateNode(ctx context.Context, source utilstandby.ActivationSource, stateNode *state.StateNode) error {
	return c.lifecycle().Activate(ctx, stateNode.Node, stateNode.NodeClaim, source, func(resolvedSource utilstandby.ActivationSource) {
		standbymetrics.RecordActivation(ctx, stateNode, resolvedSource)
	})
}

// EnterStandbyOptions configures empty marking versus post-evacuation persistence.
type EnterStandbyOptions struct {
	// BarrierTaintPresent skips applying the temporary disruption scheduling barrier.
	BarrierTaintPresent bool
	Since               time.Time
	// Eligibility is evaluated on live Node and NodeClaim reads at each protocol boundary.
	Eligibility func(context.Context, *corev1.Node, *v1.NodeClaim) (bool, error)
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
	lifecycle := c.lifecycle()
	var result TransitionResult
	err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := c.reader.Get(ctx, client.ObjectKeyFromObject(req.NodeClaimRef), nodeClaim); err != nil {
			return err
		}
		if utilstandby.IsNodeClaimStandby(nodeClaim) {
			result = TransitionResult{Outcome: OutcomeUnchanged, Node: req.NodeRef, NodeClaim: nodeClaim}
			return nil
		}
		if opts.Eligibility != nil {
			node := &corev1.Node{}
			if err := c.reader.Get(ctx, client.ObjectKeyFromObject(req.NodeRef), node); err != nil {
				return err
			}
			eligible, err := opts.Eligibility(ctx, node, nodeClaim)
			if err != nil {
				return err
			}
			if !eligible {
				result = TransitionResult{Outcome: OutcomeSkipped, Node: node, NodeClaim: nodeClaim}
				return nil
			}
		}
		if err := lifecycle.PatchNodeClaimStandby(ctx, nodeClaim, true, opts.Since); err != nil {
			return err
		}
		node := &corev1.Node{}
		if err := c.reader.Get(ctx, client.ObjectKeyFromObject(req.NodeRef), node); err != nil {
			return err
		}
		if err := lifecycle.EnsureNodeStandbyTaint(ctx, node); err != nil {
			return err
		}
		if err := c.reader.Get(ctx, client.ObjectKeyFromObject(req.NodeRef), node); err != nil {
			return err
		}
		if err := c.reader.Get(ctx, client.ObjectKeyFromObject(req.NodeClaimRef), nodeClaim); err != nil {
			return err
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
	lifecycle := c.lifecycle()
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

	node, nodeClaim, eligible, err = c.readEligiblePair(ctx, req, opts.Eligibility)
	if err != nil || !eligible {
		rollbackErr := removeDisruptionTaint(ctx, c.reader, c.writer, req.NodeRef.Name)
		return TransitionResult{Outcome: OutcomeSkipped}, multierr.Combine(err, rollbackErr)
	}
	if err := lifecycle.PatchNodeClaimStandby(ctx, nodeClaim, true, opts.Since); err != nil {
		rollbackErr := removeDisruptionTaint(ctx, c.reader, c.writer, req.NodeRef.Name)
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return TransitionResult{Outcome: OutcomeSkipped}, rollbackErr
		}
		return TransitionResult{Outcome: OutcomeFailed}, multierr.Combine(err, rollbackErr)
	}

	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(req.NodeRef), node); err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	nodeClaim = &v1.NodeClaim{}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(req.NodeClaimRef), nodeClaim); err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if nodeClaim.UID != req.NodeClaimRef.UID {
		return TransitionResult{Outcome: OutcomeSkipped}, nil
	}
	if !utilstandby.IsNodeClaimStandby(nodeClaim) || utilstandby.IsNodeClaimActivating(nodeClaim) || node.Annotations[utilstandby.NodeClaimActivatingAnnotationKey] == "true" {
		if err := removeDisruptionTaint(ctx, c.reader, c.writer, node.Name); err != nil {
			return TransitionResult{Outcome: OutcomeFailed}, err
		}
		return TransitionResult{Outcome: OutcomeSkipped}, nil
	}
	if err := lifecycle.PatchNodeStandbyTaint(ctx, node, true); err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if err := removeDisruptionTaint(ctx, c.reader, c.writer, node.Name); err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(req.NodeRef), node); err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
	}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(req.NodeClaimRef), nodeClaim); err != nil {
		return TransitionResult{Outcome: OutcomeFailed}, err
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

func (c *Coordinator) readEligiblePair(ctx context.Context, req EnterStandbyRequest, eligibility func(context.Context, *corev1.Node, *v1.NodeClaim) (bool, error)) (*corev1.Node, *v1.NodeClaim, bool, error) {
	node, nodeClaim, matches, err := c.lifecycle().ReadLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
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
	stateNode := &state.StateNode{Node: node.DeepCopy(), NodeClaim: nodeClaim.DeepCopy()}
	if err := c.Activate(ctx, utilstandby.ActivationSourceRecovery, stateNode); err != nil {
		return false, fmt.Errorf("restoring occupied standby capacity, %w", err)
	}
	refreshedNode := &corev1.Node{}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(node), refreshedNode); err != nil {
		return false, err
	}
	refreshedNodeClaim := &v1.NodeClaim{}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaim), refreshedNodeClaim); err != nil {
		return false, err
	}
	if utilstandby.IsNodeClaimStandby(refreshedNodeClaim) || utilstandby.IsNodeClaimActivating(refreshedNodeClaim) || utilstandby.HasNodeTaint(refreshedNode) {
		return false, fmt.Errorf("occupied standby NodeClaim %q has not completed activation", nodeClaim.Name)
	}
	if err := removeDisruptionTaint(ctx, c.reader, c.writer, refreshedNode.Name); err != nil {
		return false, err
	}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(node), refreshedNode); err != nil {
		return false, err
	}
	*node, *nodeClaim = *refreshedNode, *refreshedNodeClaim
	return true, nil
}

type RepairRequest struct {
	NodeRef      *corev1.Node
	NodeClaimRef *v1.NodeClaim
	ProviderID   string
}

func (c *Coordinator) RepairOccupied(ctx context.Context, req RepairRequest) error {
	node, nodeClaim, matches, err := c.lifecycle().ReadLivePair(ctx, req.NodeRef, req.NodeClaimRef, req.ProviderID)
	if err != nil {
		return err
	}
	if !matches {
		return nil
	}
	empty, err := NodeEmpty(ctx, c.reader, node)
	if err != nil {
		return err
	}
	if empty {
		return nil
	}
	activationNode := &state.StateNode{Node: node, NodeClaim: nodeClaim}
	if err := c.Activate(ctx, utilstandby.ActivationSourceRecovery, activationNode); err != nil {
		return fmt.Errorf("activating occupied standby Node %q, %w", node.Name, err)
	}
	if err := removeDisruptionTaint(ctx, c.reader, c.writer, node.Name); err != nil {
		return fmt.Errorf("removing temporary disruption taint from recovered Node %q, %w", node.Name, err)
	}
	return nil
}

func (c *Coordinator) RepairOrphanStandbyTaint(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim) (bool, error) {
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
	if err := c.lifecycle().PatchNodeStandbyTaint(ctx, node, false); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("removing orphan standby taint from Node %q, %w", node.Name, err)
	}
	return true, nil
}

func (c *Coordinator) EnsureStandbyTaintForClaim(ctx context.Context, node *corev1.Node, nodeClaim *v1.NodeClaim) error {
	return c.lifecycle().EnsureStandbyTaintForClaim(ctx, node, nodeClaim)
}

func (c *Coordinator) ReadLivePair(ctx context.Context, nodeRef *corev1.Node, nodeClaimRef *v1.NodeClaim, providerID string) (*corev1.Node, *v1.NodeClaim, bool, error) {
	return c.lifecycle().ReadLivePair(ctx, nodeRef, nodeClaimRef, providerID)
}

func (c *Coordinator) RemoveDisruptionTaint(ctx context.Context, nodeName string) error {
	return removeDisruptionTaint(ctx, c.reader, c.writer, nodeName)
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
