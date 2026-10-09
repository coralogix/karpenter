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

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func (c *Controller) cleanupStaleDisruptionState(ctx context.Context) error {
	if err := c.recoverOccupiedStandbyNodes(ctx); err != nil {
		return fmt.Errorf("recovering occupied standby nodes, %w", err)
	}
	// Karpenter taints nodes with a karpenter.sh/disruption taint as part of the disruption process while it progresses in memory.
	// If Karpenter restarts or fails with an error during a disruption action, some nodes can be left tainted.
	// Idempotently remove this taint from candidates that are not in the orchestration queue before continuing.
	outdatedNodes := lo.Reject(c.cluster.DeepCopyNodes(), func(s *state.StateNode, _ int) bool {
		return c.queue.HasAny(s.ProviderID()) || s.MarkedForDeletion() || standby.IsNodeClaimActivating(s.NodeClaim)
	})
	standbyNodes := lo.Filter(outdatedNodes, func(s *state.StateNode, _ int) bool { return standby.IsNodeClaimStandby(s.NodeClaim) })
	if err := c.ensureStandbyTaints(ctx, standbyNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("restoring standby taint, %w", err), "taint", standby.NodeTaintKey)
	}
	if err := state.RequireNoScheduleTaint(ctx, c.kubeClient, false, outdatedNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("removing taint from nodes, %w", err), "taint", pretty.Taint(v1.DisruptedNoScheduleTaint))
	}
	if err := state.ClearNodeClaimsCondition(ctx, c.kubeClient, c.clock, v1.ConditionTypeDisruptionReason, outdatedNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("removing condition from nodeclaims, %w", err), "condition", v1.ConditionTypeDisruptionReason)
	}
	return nil
}

//nolint:gocyclo // This recovery pass intentionally keeps each live-state check beside the mutation it gates.
func (c *Controller) recoverOccupiedStandbyNodes(ctx context.Context) error {
	reader := c.apiReader
	if reader == nil {
		reader = c.kubeClient
	}
	for _, stateNode := range c.cluster.DeepCopyNodes() {
		if stateNode == nil || stateNode.Node == nil || stateNode.NodeClaim == nil || stateNode.MarkedForDeletion() || c.queue.HasAny(stateNode.ProviderID()) {
			continue
		}
		if !standby.IsNodeClaimStandby(stateNode.NodeClaim) && !standby.IsNodeClaimActivating(stateNode.NodeClaim) &&
			!standby.HasNodeTaint(stateNode.Node) && stateNode.Node.Annotations[standby.NodeClaimActivatingAnnotationKey] != "true" {
			continue
		}
		node, nodeClaim, matches, err := standby.NewLifecycle(reader, c.kubeClient).ReadLivePair(ctx, stateNode.Node, stateNode.NodeClaim, stateNode.ProviderID())
		if err != nil {
			return fmt.Errorf("getting live standby objects for Node %q, %w", stateNode.Name(), err)
		}
		if !matches ||
			!node.DeletionTimestamp.IsZero() || !nodeClaim.DeletionTimestamp.IsZero() ||
			nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
			continue
		}
		claimStandby := standby.IsNodeClaimStandby(nodeClaim)
		claimActivating := standby.IsNodeClaimActivating(nodeClaim)
		nodeActivating := node.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true"
		if !claimStandby && !claimActivating && !nodeActivating {
			if standby.HasNodeTaint(node) {
				// A crash between tainting a node and persisting its standby marker can leave a
				// node permanently unschedulable. Remove only the orphan taint, using the live
				// Node resourceVersion so a concurrent activation marker wins.
				if err := standby.NewLifecycle(reader, c.kubeClient).PatchNodeStandbyTaint(ctx, node, false); err != nil {
					if errors.IsConflict(err) || errors.IsNotFound(err) {
						continue
					}
					return fmt.Errorf("removing orphan standby taint from Node %q, %w", node.Name, err)
				}
				if err := updateStandbyState(ctx, c.cluster, reader, node, nodeClaim); err != nil {
					return fmt.Errorf("refreshing Node %q after removing orphan standby taint, %w", node.Name, err)
				}
			}
			continue
		}
		empty, err := nodeEmpty(ctx, reader, node)
		if err != nil {
			return fmt.Errorf("checking standby Node %q for bound workloads, %w", node.Name, err)
		}
		if empty {
			continue
		}
		activationNode := &state.StateNode{Node: node, NodeClaim: nodeClaim}
		if err := c.provisioner.ActivateStandbyNodes(ctx, standby.ActivationSourceRecovery, activationNode); err != nil {
			return fmt.Errorf("activating occupied standby Node %q, %w", node.Name, err)
		}
		if err := c.queue.removeDisruptionTaint(ctx, node.Name); err != nil {
			return fmt.Errorf("removing temporary disruption taint from recovered Node %q, %w", node.Name, err)
		}
		if err := c.refreshStandbyState(ctx, node.Name, nodeClaim.Name); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) refreshStandbyState(ctx context.Context, nodeName, nodeClaimName string) error {
	reader := c.apiReader
	if reader == nil {
		reader = c.kubeClient
	}
	node := &corev1.Node{}
	if err := reader.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return fmt.Errorf("refreshing standby Node %q, %w", nodeName, err)
	}
	nodeClaim := &v1.NodeClaim{}
	if err := reader.Get(ctx, client.ObjectKey{Name: nodeClaimName}, nodeClaim); err != nil {
		return fmt.Errorf("refreshing standby NodeClaim %q, %w", nodeClaimName, err)
	}
	if standby.IsNodeClaimStandby(nodeClaim) || standby.IsNodeClaimActivating(nodeClaim) || standby.HasNodeTaint(node) || node.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" {
		return fmt.Errorf("standby activation for NodeClaim %q has not completed", nodeClaimName)
	}
	return updateStandbyState(ctx, c.cluster, reader, node, nodeClaim)
}

func (c *Controller) ensureStandbyTaints(ctx context.Context, nodes ...*state.StateNode) error {
	for _, stateNode := range nodes {
		if err := c.ensureStandbyTaint(ctx, stateNode); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) ensureStandbyTaint(ctx context.Context, stateNode *state.StateNode) error {
	reader := c.apiReader
	if reader == nil {
		reader = c.kubeClient
	}
	return standby.NewLifecycle(reader, c.kubeClient).EnsureStandbyTaintForClaim(ctx, stateNode.Node, stateNode.NodeClaim)
}

func (c *Controller) refreshEmptyReclamationInventory(ctx context.Context, disruption Method) error {
	reclamation, ok := disruption.(*Reclamation)
	if !ok {
		return nil
	}
	if err := reclamation.refreshReclamationInventory(ctx); err != nil {
		return fmt.Errorf("updating reclamation inventory, %w", err)
	}
	return nil
}

func (c *Controller) budgetMappingForMethod(ctx context.Context, disruption Method) (map[string]int, error) {
	if _, budgetExempt := disruption.(*Reclamation); budgetExempt {
		return map[string]int{}, nil
	}
	if _, budgetExempt := disruption.(*StandbyMarking); budgetExempt {
		return map[string]int{}, nil
	}
	return BuildDisruptionBudgetMapping(ctx, c.cluster, c.clock, c.kubeClient, c.cloudProvider, c.recorder, disruption.Reason())
}
