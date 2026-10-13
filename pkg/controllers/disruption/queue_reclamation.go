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
func (q *Queue) reclamationCandidateBeforeDelete(ctx context.Context, candidate *Candidate) (bool, error) {
	nodeClaim := &v1.NodeClaim{}
	if err := q.apiReader.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
		if apierrors.IsNotFound(err) {
			// NotFound is OK: conditional delete cannot remove a same-name replacement.
			return true, nil
		}
		return false, fmt.Errorf("getting reclamation candidate NodeClaim before deletion, %w", err)
	}
	if nodeClaim.UID != candidate.NodeClaim.UID {
		return false, nil
	}
	if !nodeClaim.DeletionTimestamp.IsZero() {
		candidate.NodeClaim = nodeClaim
		return true, nil
	}
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
		return false, nil
	}
	if standby.IsNodeClaimActivating(nodeClaim) || q.cluster.IsNodeNominated(candidate.ProviderID()) {
		return false, nil
	}
	if q.cluster.HasBufferPods(candidate.ProviderID()) {
		return false, nil
	}
	if nodeClaim.Annotations[v1.DoNotDisruptAnnotationKey] == "true" {
		return false, nil
	}
	if !standby.IsNodeClaimStandby(nodeClaim) {
		return false, nil
	}
	if !standbyReclamationDelayElapsed(candidate.NodePool, nodeClaim, q.clock.Now()) {
		return false, nil
	}
	if nodeClaim.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name {
		return false, nil
	}
	if eligible, err := q.reclamationNodePoolStillManaged(ctx, candidate.NodePool.Name); err != nil || !eligible {
		return false, err
	}
	if eligible, err := q.reclamationNodeStillEmpty(ctx, candidate); err != nil || !eligible {
		return false, err
	}
	candidate.NodeClaim = nodeClaim
	return true, nil
}

//nolint:gocyclo // These live Node checks form one final identity, standby, and emptiness validation.
func (q *Queue) reclamationNodeStillEmpty(ctx context.Context, candidate *Candidate) (bool, error) {
	node := &corev1.Node{}
	if err := q.apiReader.Get(ctx, types.NamespacedName{Name: candidate.Node.Name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("getting reclamation candidate Node, %w", err)
	}
	if node.Spec.ProviderID != candidate.ProviderID() {
		return false, nil
	}
	if !node.DeletionTimestamp.IsZero() {
		return false, nil
	}
	if node.UID != candidate.Node.UID || !standby.HasNodeTaint(node) || node.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" {
		return false, nil
	}
	if node.Annotations[v1.DoNotDisruptAnnotationKey] == "true" || node.Labels[v1.NodeInitializedLabelKey] != "true" {
		return false, nil
	}
	empty, err := nodeEmpty(ctx, q.apiReader, node)
	if err != nil {
		return false, fmt.Errorf("checking reclamation candidate emptiness, %w", err)
	}
	if !empty {
		return false, nil
	}
	candidate.Node = node
	return true, nil
}

func (q *Queue) reclamationNodePoolStillManaged(ctx context.Context, nodePoolName string) (bool, error) {
	nodePool := &v1.NodePool{}
	if err := q.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("getting reclamation NodePool, %w", err)
	}
	return standbyLifecycleEnabled(nodePool), nil
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
