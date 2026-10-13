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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

//nolint:gocyclo // The final pre-delete validation keeps the ordered safety checks and refreshes together.
func (q *Queue) reclamationCandidateBeforeDelete(ctx context.Context, candidate *Candidate) error {
	if candidate == nil || candidate.NodeClaim == nil || candidate.Node == nil || candidate.NodePool == nil {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate is missing its Node, NodeClaim, or NodePool"))
	}
	nodeClaim := &v1.NodeClaim{}
	if err := q.apiReader.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
		if apierrors.IsNotFound(err) {
			// NotFound is OK: conditional delete cannot remove a same-name replacement.
			return nil
		}
		return fmt.Errorf("getting reclamation candidate NodeClaim before deletion, %w", err)
	}
	if nodeClaim.UID != candidate.NodeClaim.UID {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q was replaced", candidate.Name()))
	}
	if !nodeClaim.DeletionTimestamp.IsZero() {
		candidate.NodeClaim = nodeClaim
		return nil
	}
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is terminating", candidate.Name()))
	}
	if standby.IsNodeClaimActivating(nodeClaim) || q.cluster.IsNodeNominated(candidate.ProviderID()) {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is activating or nominated", candidate.Name()))
	}
	if q.cluster.HasBufferPods(candidate.ProviderID()) {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q has buffer pods", candidate.Name()))
	}
	if nodeClaim.Annotations[v1.DoNotDisruptAnnotationKey] == "true" {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is do-not-disrupt", candidate.Name()))
	}
	if !standby.IsNodeClaimStandby(nodeClaim) {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is no longer standby", candidate.Name()))
	}
	if !standbyReclamationDelayElapsed(candidate.NodePool, nodeClaim, q.clock.Now()) {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q has not completed the reclamation delay", candidate.Name()))
	}
	if nodeClaim.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q changed NodePool", candidate.Name()))
	}
	if err := q.reclamationNodePoolStillManaged(ctx, candidate.NodePool.Name); err != nil {
		return err
	}
	if err := q.reclamationNodeStillEmpty(ctx, candidate); err != nil {
		return err
	}
	candidate.NodeClaim = nodeClaim
	return nil
}

//nolint:gocyclo // These live Node checks form one final identity, standby, and emptiness validation.
func (q *Queue) reclamationNodeStillEmpty(ctx context.Context, candidate *Candidate) error {
	node := &corev1.Node{}
	if err := q.apiReader.Get(ctx, types.NamespacedName{Name: candidate.Node.Name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("getting reclamation candidate Node, %w", err)
	}
	if node.Spec.ProviderID != candidate.ProviderID() {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q changed provider ID", candidate.Name()))
	}
	if !node.DeletionTimestamp.IsZero() {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is deleting", candidate.Name()))
	}
	if node.UID != candidate.Node.UID || !standby.HasNodeTaint(node) || node.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is no longer tainted standby", candidate.Name()))
	}
	if node.Annotations[v1.DoNotDisruptAnnotationKey] == "true" || node.Labels[v1.NodeInitializedLabelKey] != "true" {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is no longer eligible", candidate.Name()))
	}
	empty, err := nodeEmpty(ctx, q.apiReader, node)
	if err != nil {
		return fmt.Errorf("checking reclamation candidate emptiness, %w", err)
	}
	if !empty {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is no longer empty", candidate.Name()))
	}
	candidate.Node = node
	return nil
}

func (q *Queue) reclamationNodePoolStillManaged(ctx context.Context, nodePoolName string) error {
	nodePool := &v1.NodePool{}
	if err := q.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
		if apierrors.IsNotFound(err) {
			return NewUnrecoverableError(fmt.Errorf("reclamation NodePool %q was deleted", nodePoolName))
		}
		return fmt.Errorf("getting reclamation NodePool, %w", err)
	}
	if !standbyLifecycleEnabled(nodePool) {
		return NewUnrecoverableError(fmt.Errorf("reclamation NodePool %q is no longer eligible for reclamation", nodePool.Name))
	}
	return nil
}

func (q *Queue) reportReclamationCandidateDeleted(candidate *Candidate) {
	ReclamationNodeRemovalsTotal.Inc(map[string]string{metrics.NodePoolLabel: candidate.NodePool.Name})
}

func (q *Queue) reportReclamationBatchSucceeded(ctx context.Context, candidates []*Candidate) {
	log.FromContext(ctx).Info("reclamation batch completed",
		"removedNodes", len(candidates),
		"nodePoolCounts", reclamationCandidateCounts(candidates),
	)
}
