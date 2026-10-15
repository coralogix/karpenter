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
	"go.uber.org/multierr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func (q *Queue) cleanupStaleDisruptionState(ctx context.Context) error {
	if err := q.repairStandbyNodes(ctx); err != nil {
		log.FromContext(ctx).Error(err, "repairing standby nodes")
	}
	outdatedNodes := lo.Reject(q.cluster.DeepCopyNodes(), func(s *state.StateNode, _ int) bool {
		return q.HasAny(s.ProviderID()) || s.MarkedForDeletion() || standby.IsNodeClaimActivating(s.NodeClaim)
	})
	if err := state.RequireNoScheduleTaint(ctx, q.kubeClient, false, outdatedNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("removing taint from nodes, %w", err), "taint", pretty.Taint(v1.DisruptedNoScheduleTaint))
	}
	if err := state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, outdatedNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("removing condition from nodeclaims, %w", err), "condition", v1.ConditionTypeDisruptionReason)
	}
	return nil
}

//nolint:gocyclo // Each cluster node keeps its eligibility checks beside the repair call.
func (q *Queue) repairStandbyNodes(ctx context.Context) error {
	coordinator := q.standbyCoordinator
	var errs []error
	for _, stateNode := range q.cluster.DeepCopyNodes() {
		if stateNode == nil || stateNode.Node == nil || stateNode.NodeClaim == nil || stateNode.MarkedForDeletion() || q.HasAny(stateNode.ProviderID()) {
			continue
		}
		if !standby.IsNodeClaimStandby(stateNode.NodeClaim) && !standby.IsNodeClaimActivating(stateNode.NodeClaim) &&
			!standby.HasNodeTaint(stateNode.Node) && stateNode.Node.Annotations[standby.NodeClaimActivatingAnnotationKey] != "true" {
			continue
		}
		result, err := coordinator.RepairStandbyNode(ctx, standbypkg.RepairRequest{
			NodeRef:      stateNode.Node,
			NodeClaimRef: stateNode.NodeClaim,
			ProviderID:   stateNode.ProviderID(),
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("repairing standby Node %q, %w", stateNode.Name(), err))
			continue
		}
		if result.Outcome != standbypkg.OutcomeRecovered || result.Node == nil || result.NodeClaim == nil {
			continue
		}
		if err := q.updateStandbyState(ctx, result.Node, result.NodeClaim); err != nil {
			errs = append(errs, fmt.Errorf("refreshing Node %q after standby repair, %w", result.Node.Name, err))
		}
	}
	return multierr.Combine(errs...)
}
