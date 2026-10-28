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

	"go.uber.org/multierr"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

// repairStandbyState clears standby when the node is no longer empty and backfills a standby taint with no usable timestamp.
func repairStandbyState(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, recorder events.Recorder, clk clock.Clock, nodePoolMap map[string]*v1.NodePool) error {
	var errs []error
	for _, node := range cluster.DeepCopyNodes().Active() {
		if err := repairStandbyNode(ctx, kubeClient, recorder, clk, nodePoolMap, node); err != nil {
			errs = append(errs, err)
		}
	}
	return multierr.Combine(errs...)
}

// if the node is in standby, but its pool no longer qualifies, standby status is cleared
// if the node is in standby, but no longer empty, standby status is cleared
// if the node is in standby but the timestamp is incorrect, it is set to now
func repairStandbyNode(ctx context.Context, kubeClient client.Client, recorder events.Recorder, clk clock.Clock, nodePoolMap map[string]*v1.NodePool, node *state.StateNode) error {
	if !isStandby(node) {
		return nil
	}
	if !scoreBasedStandbyPool(nodePoolMap[node.Labels()[v1.NodePoolLabelKey]]) {
		return standby.Clear(ctx, kubeClient, node.Node, node.NodeClaim)
	}
	reschedulable, err := node.CurrentlyReschedulablePods(ctx, kubeClient, clk, recorder)
	if err != nil {
		return err
	}
	if computeRescheduleDisruptionCost(ctx, reschedulable) > PerNodeBaseDisruptionCost {
		return standby.Clear(ctx, kubeClient, node.Node, node.NodeClaim)
	}
	if standby.HasStandbyTaint(node.Node) {
		if _, hasTS := standby.Timestamp(node.NodeClaim); !hasTS {
			return standby.Mark(ctx, kubeClient, node.Node, node.NodeClaim, clk.Now())
		}
	}
	return nil
}

// isStandby reports whether a managed, initialized node carries any standby mark.
func isStandby(node *state.StateNode) bool {
	if node.Node == nil || node.NodeClaim == nil || !node.Managed() || !node.Initialized() {
		return false
	}
	return standby.IsMarked(node.Node, node.NodeClaim)
}

// candidateEmptyForStandbyWork also requires no buffer pods: they are virtual, so IsEmpty cannot see them on the node.
func candidateEmptyForStandbyWork(cluster *state.Cluster, c *Candidate) bool {
	return scoreBasedStandbyPool(c.NodePool) && c.IsEmpty() && !cluster.HasBufferPods(c.ProviderID())
}

func scoreBasedStandbyPool(np *v1.NodePool) bool {
	if np == nil || !NodePoolUsesScoreBasedConsolidation(np) {
		return false
	}
	if np.Spec.Replicas != nil {
		return false
	}
	return np.Spec.Disruption.ConsolidateAfter.Duration != nil
}
