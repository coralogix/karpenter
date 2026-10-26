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
	"time"

	"github.com/samber/lo"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

// RepairStandbyState clears standby when the node is no longer empty and backfills a standby taint with no usable timestamp.
func RepairStandbyState(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, recorder events.Recorder, clk clock.Clock, nodePoolMap map[string]*v1.NodePool) error {
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
	if !hasStandbyMark(node) {
		return nil
	}
	if !scoreBasedStandbyPool(nodePoolMap[node.Labels()[v1.NodePoolLabelKey]]) {
		return clearStandbyMark(ctx, kubeClient, node)
	}
	reschedulable, err := node.CurrentlyReschedulablePods(ctx, kubeClient, clk, recorder)
	if err != nil {
		return err
	}
	if computeRescheduleDisruptionCost(ctx, reschedulable) > PerNodeBaseDisruptionCost {
		return clearStandbyMark(ctx, kubeClient, node)
	}
	if nodeHasStandbyTaint(node.Node) {
		if _, hasTS := standbyTimestamp(node.NodeClaim); !hasTS {
			return setStandbyTimestamp(ctx, kubeClient, node.NodeClaim, clk.Now())
		}
	}
	return nil
}

// hasStandbyMark reports whether a managed, initialized node carries any standby mark.
func hasStandbyMark(node *state.StateNode) bool {
	if node.Node == nil || node.NodeClaim == nil || !node.Managed() || !node.Initialized() {
		return false
	}
	return nodeHasStandbyTaint(node.Node) || node.NodeClaim.Annotations[v1.StandbyNodeClaimAnnotationKey] != ""
}

func markNodeStandby(ctx context.Context, kubeClient client.Client, clk clock.Clock, candidate *Candidate) error {
	// Marking only runs on nodes without the taint, so an existing timestamp is stale.
	// Write the timestamp first: a failed taint write then leaves no taint, never a taint with an old timestamp.
	if err := setStandbyTimestamp(ctx, kubeClient, candidate.NodeClaim, clk.Now()); err != nil {
		return err
	}
	return addStandbyNoScheduleTaint(ctx, kubeClient, candidate.StateNode)
}

func clearStandbyMark(ctx context.Context, kubeClient client.Client, node *state.StateNode) error {
	if err := removeStandbyNoScheduleTaint(ctx, kubeClient, node); err != nil {
		return err
	}
	return clearStandbyNodeClaimAnnotation(ctx, kubeClient, node.NodeClaim)
}

func candidateEligibleForStandbyWork(cluster *state.Cluster, c *Candidate) bool {
	if c == nil || c.NodePool == nil || !scoreBasedStandbyPool(c.NodePool) {
		return false
	}
	if cluster.HasBufferPods(c.ProviderID()) {
		return false
	}
	return true
}

func candidateEmptyForStandbyWork(cluster *state.Cluster, c *Candidate) bool {
	return candidateEligibleForStandbyWork(cluster, c) && c.IsEmpty()
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

func nodeHasStandbyTaint(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	_, found := lo.Find(node.Spec.Taints, func(taint corev1.Taint) bool {
		return taint.MatchTaint(&v1.StandbyNoScheduleTaint)
	})
	return found
}

func setStandbyTimestamp(ctx context.Context, kubeClient client.Client, nodeClaim *v1.NodeClaim, at time.Time) error {
	if nodeClaim == nil {
		return nil
	}
	return retryIgnoringNotFound(func() error {
		stored := &v1.NodeClaim{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), stored); err != nil {
			return err
		}
		patch := stored.DeepCopy()
		if patch.Annotations == nil {
			patch.Annotations = map[string]string{}
		}
		patch.Annotations[v1.StandbyNodeClaimAnnotationKey] = at.UTC().Format(time.RFC3339Nano)
		return kubeClient.Patch(ctx, patch, client.MergeFrom(stored))
	})
}

func standbyTimestamp(nodeClaim *v1.NodeClaim) (time.Time, bool) {
	if nodeClaim == nil {
		return time.Time{}, false
	}
	raw := nodeClaim.Annotations[v1.StandbyNodeClaimAnnotationKey]
	if raw == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return ts.UTC(), true
}

func addStandbyNoScheduleTaint(ctx context.Context, kubeClient client.Client, node *state.StateNode) error {
	if node.Node == nil || node.NodeClaim == nil {
		return nil
	}
	return retryIgnoringNotFound(func() error {
		storedNode := &corev1.Node{}
		if err := kubeClient.Get(ctx, client.ObjectKey{Name: node.Node.Name}, storedNode); err != nil {
			return err
		}
		if !storedNode.DeletionTimestamp.IsZero() {
			return nil
		}
		if nodeHasStandbyTaint(storedNode) {
			return nil
		}
		patch := storedNode.DeepCopy()
		patch.Spec.Taints = withoutStandbyNoScheduleTaint(patch.Spec.Taints)
		patch.Spec.Taints = append(patch.Spec.Taints, v1.StandbyNoScheduleTaint)
		return kubeClient.Patch(ctx, patch, client.MergeFromWithOptions(storedNode, client.MergeFromWithOptimisticLock{}))
	})
}

func removeStandbyNoScheduleTaint(ctx context.Context, kubeClient client.Client, node *state.StateNode) error {
	if node.Node == nil || node.NodeClaim == nil {
		return nil
	}
	return retryIgnoringNotFound(func() error {
		storedNode := &corev1.Node{}
		if err := kubeClient.Get(ctx, client.ObjectKey{Name: node.Node.Name}, storedNode); err != nil {
			return err
		}
		if !storedNode.DeletionTimestamp.IsZero() {
			return nil
		}
		if !nodeHasStandbyTaint(storedNode) {
			return nil
		}
		patch := storedNode.DeepCopy()
		patch.Spec.Taints = withoutStandbyNoScheduleTaint(patch.Spec.Taints)
		return kubeClient.Patch(ctx, patch, client.MergeFromWithOptions(storedNode, client.MergeFromWithOptimisticLock{}))
	})
}

func withoutStandbyNoScheduleTaint(taints []corev1.Taint) []corev1.Taint {
	return lo.Reject(taints, func(taint corev1.Taint, _ int) bool {
		return taint.MatchTaint(&v1.StandbyNoScheduleTaint)
	})
}

func clearStandbyNodeClaimAnnotation(ctx context.Context, kubeClient client.Client, nodeClaim *v1.NodeClaim) error {
	if nodeClaim == nil {
		return nil
	}
	return retryIgnoringNotFound(func() error {
		stored := &v1.NodeClaim{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), stored); err != nil {
			return err
		}
		if stored.Annotations[v1.StandbyNodeClaimAnnotationKey] == "" {
			return nil
		}
		patch := stored.DeepCopy()
		delete(patch.Annotations, v1.StandbyNodeClaimAnnotationKey)
		return kubeClient.Patch(ctx, patch, client.MergeFrom(stored))
	})
}

func retryIgnoringNotFound(op func() error) error {
	return retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, op)
}
