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
	"time"

	"github.com/samber/lo"

	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func (p *Provisioner) resumeStandbyActivations(ctx context.Context, nodes state.StateNodes) error {
	activating := standbypkg.FilterActivating(nodes)
	if len(activating) == 0 {
		return nil
	}
	if err := p.standbyCoordinator.Activate(ctx, standby.ActivationSourceRecovery, activating...); err != nil {
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
	return p.standbyCoordinator.Activate(ctx, standby.ActivationSourceProvisioning, selected...)
}
