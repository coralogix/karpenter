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
	"time"

	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	karpenterMetrics "sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/standby"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

func (p *Provisioner) resumeStandbyActivations(ctx context.Context, nodes state.StateNodes) error {
	activating := lo.Filter(nodes, func(node *state.StateNode, _ int) bool {
		return node != nil && !node.MarkedForDeletion() &&
			(standby.IsNodeClaimActivating(node.NodeClaim) || standby.HasNodeActivationMarker(node.Node))
	})
	if len(activating) == 0 {
		return nil
	}
	if err := p.ActivateStandbyNodes(ctx, standby.ActivationSourceRecovery, activating...); err != nil {
		return err
	}
	for _, node := range activating {
		standby.SetNodeClaimStandby(node.NodeClaim, false, time.Time{})
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
			(!standby.IsNodeClaimStandby(node.NodeClaim) && !standby.IsNodeClaimActivating(node.NodeClaim) && !standby.HasNodeActivationMarker(node.Node)) {
			continue
		}
		if err := p.activateStandbyNode(ctx, source, node); err != nil {
			errs = append(errs, fmt.Errorf("activating node %q, %w", node.Name(), err))
		}
	}
	return multierr.Combine(errs...)
}

func (p *Provisioner) activateStandbyNode(ctx context.Context, source standby.ActivationSource, stateNode *state.StateNode) error {
	return standby.NewLifecycle(p.apiObjectReader(), p.kubeClient).Activate(ctx, stateNode.Node, stateNode.NodeClaim, source, func(resolvedSource standby.ActivationSource) {
		recordStandbyActivation(ctx, stateNode, resolvedSource)
	})
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
