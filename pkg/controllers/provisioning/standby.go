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

package provisioning

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	karpenterMetrics "sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/standby"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (p *Provisioner) resumeStandbyActivations(ctx context.Context, nodes state.StateNodes) error {
	activating := lo.Filter(nodes, func(node *state.StateNode, _ int) bool {
		return node != nil && !node.MarkedForDeletion() &&
			(standby.IsNodeClaimActivating(node.NodeClaim) || hasNodeActivationMarker(node.Node))
	})
	if len(activating) == 0 {
		return nil
	}
	if err := p.ActivateStandbyNodes(ctx, standby.ActivationSourceRecovery, activating...); err != nil {
		return err
	}
	for _, node := range activating {
		standby.SetNodeClaimStandby(node.NodeClaim, false)
		standby.SetNodeClaimActivating(node.NodeClaim, false)
		standby.SetNodeTaint(node.Node, false)
		if node.Node != nil {
			delete(node.Node.Annotations, standby.NodeClaimActivatingAnnotationKey)
		}
	}
	return nil
}

func (p *Provisioner) activateScheduledStandbyNodes(ctx context.Context, results pscheduling.Results) error {
	selected := lo.FilterMap(results.ExistingNodes, func(node *pscheduling.ExistingNode, _ int) (*state.StateNode, bool) {
		if node == nil || len(node.Pods) == 0 || !standby.IsNodeClaimStandby(node.NodeClaim) {
			return nil, false
		}
		return node.StateNode, true
	})
	return p.ActivateStandbyNodes(ctx, standby.ActivationSourceProvisioning, selected...)
}

// ActivateStandbyNodes activates retained nodes selected as scheduling
// destinations. The NodeClaim marker is used as a persistent reservation so
// reclamation cannot remove a node while its taint is being changed. The source
// is persisted with that reservation so an interrupted activation keeps its origin.
func (p *Provisioner) ActivateStandbyNodes(ctx context.Context, source standby.ActivationSource, nodes ...*state.StateNode) error {
	var errs []error
	for _, node := range nodes {
		if node == nil || node.NodeClaim == nil ||
			(!standby.IsNodeClaimStandby(node.NodeClaim) && !standby.IsNodeClaimActivating(node.NodeClaim) && !hasNodeActivationMarker(node.Node)) {
			continue
		}
		if err := p.activateStandbyNode(ctx, source, node); err != nil {
			errs = append(errs, fmt.Errorf("activating node %q, %w", node.Name(), err))
		}
	}
	return multierr.Combine(errs...)
}

func (p *Provisioner) activateStandbyNode(ctx context.Context, source standby.ActivationSource, stateNode *state.StateNode) error {
	activationStarted, source, err := p.reserveStandbyActivation(ctx, stateNode.NodeClaim, source)
	if err != nil {
		return fmt.Errorf("reserving nodeclaim activation, %w", err)
	}
	if !activationStarted && !hasNodeActivationMarker(stateNode.Node) {
		// Another activation already completed this NodeClaim.
		return nil
	}
	if stateNode.Node == nil {
		return fmt.Errorf("nodeclaim has no registered node")
	}
	if err := p.setNodeActivationMarker(ctx, stateNode.Node.Name, true); err != nil {
		return fmt.Errorf("marking node activation, %w", err)
	}
	activated, err := p.removeStandbyNodeTaint(ctx, stateNode.Node.Name)
	if err != nil {
		return fmt.Errorf("removing standby taint, %w", err)
	}
	if activated {
		recordStandbyActivation(ctx, stateNode, source)
	}
	if err := p.finishStandbyActivation(ctx, stateNode.NodeClaim); err != nil {
		return fmt.Errorf("clearing activation markers, %w", err)
	}
	if err := p.setNodeActivationMarker(ctx, stateNode.Node.Name, false); err != nil {
		return fmt.Errorf("clearing node activation marker, %w", err)
	}
	return nil
}

func recordStandbyActivation(ctx context.Context, stateNode *state.StateNode, source standby.ActivationSource) {
	nodePool := stateNode.NodeClaim.Labels[v1.NodePoolLabelKey]
	if nodePool == "" {
		nodePool = "unknown"
	}
	instanceType := stateNode.NodeClaim.Labels[corev1.LabelInstanceTypeStable]
	if instanceType == "" {
		instanceType = "unknown"
	}
	StandbyNodesActivatedTotal.Inc(map[string]string{
		karpenterMetrics.NodePoolLabel:     nodePool,
		standbyActivationInstanceTypeLabel: instanceType,
		standbyActivationSourceLabel:       string(source),
	})
	log.FromContext(ctx).WithValues(
		"Node", klog.KObj(stateNode.Node),
		"NodeClaim", klog.KObj(stateNode.NodeClaim),
		"NodePool", klog.KRef("", nodePool),
	).Info("activated standby node")
}

func (p *Provisioner) reserveStandbyActivation(ctx context.Context, stateNodeClaim *v1.NodeClaim, requestedSource standby.ActivationSource) (bool, standby.ActivationSource, error) {
	var activationStarted bool
	activationSource := standby.ActivationSourceRecovery
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := p.kubeClient.Get(ctx, client.ObjectKeyFromObject(stateNodeClaim), nodeClaim); err != nil {
			return err
		}
		if !nodeClaim.DeletionTimestamp.IsZero() {
			return fmt.Errorf("nodeclaim is deleting")
		}
		if standby.IsNodeClaimActivating(nodeClaim) {
			activationStarted = true
			activationSource = normalizeStandbyActivationSource(standby.NodeClaimActivationSource(nodeClaim))
			return nil
		}
		if !standby.IsNodeClaimStandby(nodeClaim) {
			return nil
		}
		stored := nodeClaim.DeepCopy()
		activationSource = normalizeStandbyActivationSource(requestedSource)
		standby.SetNodeClaimActivating(nodeClaim, true)
		standby.SetNodeClaimActivationSource(nodeClaim, activationSource)
		if err := p.kubeClient.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		activationStarted = true
		return nil
	})
	return activationStarted, activationSource, err
}

func normalizeStandbyActivationSource(source standby.ActivationSource) standby.ActivationSource {
	switch source {
	case standby.ActivationSourceProvisioning, standby.ActivationSourceCompaction:
		return source
	default:
		return standby.ActivationSourceRecovery
	}
}

func hasNodeActivationMarker(node *corev1.Node) bool {
	return node != nil && node.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true"
}

func (p *Provisioner) setNodeActivationMarker(ctx context.Context, nodeName string, activating bool) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node := &corev1.Node{}
		if err := p.kubeClient.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			return err
		}
		if activating == hasNodeActivationMarker(node) {
			return nil
		}
		stored := node.DeepCopy()
		if activating {
			if node.Annotations == nil {
				node.Annotations = map[string]string{}
			}
			node.Annotations[standby.NodeClaimActivatingAnnotationKey] = "true"
		} else {
			delete(node.Annotations, standby.NodeClaimActivatingAnnotationKey)
		}
		return p.kubeClient.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	})
}

func (p *Provisioner) removeStandbyNodeTaint(ctx context.Context, nodeName string) (bool, error) {
	var activated bool
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node := &corev1.Node{}
		if err := p.kubeClient.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			return err
		}
		if !standby.HasNodeTaint(node) {
			return nil
		}
		stored := node.DeepCopy()
		standby.SetNodeTaint(node, false)
		if err := p.kubeClient.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		activated = true
		return nil
	})
	return activated, err
}

func (p *Provisioner) finishStandbyActivation(ctx context.Context, stateNodeClaim *v1.NodeClaim) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := p.kubeClient.Get(ctx, client.ObjectKeyFromObject(stateNodeClaim), nodeClaim); err != nil {
			return err
		}
		if !standby.IsNodeClaimStandby(nodeClaim) && !standby.IsNodeClaimActivating(nodeClaim) && standby.NodeClaimActivationSource(nodeClaim) == "" {
			return nil
		}
		stored := nodeClaim.DeepCopy()
		standby.SetNodeClaimStandby(nodeClaim, false)
		standby.SetNodeClaimActivating(nodeClaim, false)
		standby.SetNodeClaimActivationSource(nodeClaim, "")
		return p.kubeClient.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	})
}
