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

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

//nolint:gocyclo // Identity, deletion, standby, activation, and occupancy checks jointly protect one candidate source.
func (s *ScoreBasedConsolidation) validateCompactionCandidate(ctx context.Context, candidate *Candidate, nodePoolName string) error {
	currentNode := &corev1.Node{}
	if err := s.kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.Node), currentNode); err != nil {
		if apierrors.IsNotFound(err) {
			return NewChurnValidationError(fmt.Errorf("score-based compaction candidate Node %q no longer exists", candidate.Node.Name))
		}
		return fmt.Errorf("getting Node %q for score-based compaction validation, %w", candidate.Node.Name, err)
	}
	if currentNode.UID != candidate.Node.UID || currentNode.Spec.ProviderID != candidate.ProviderID() || !currentNode.DeletionTimestamp.IsZero() ||
		standby.HasNodeTaint(currentNode) || currentNode.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" ||
		currentNode.Labels[v1.NodePoolLabelKey] != nodePoolName {
		return NewChurnValidationError(fmt.Errorf("score-based compaction candidate Node %q changed identity or state", candidate.Node.Name))
	}
	currentNodeClaim := &v1.NodeClaim{}
	if err := s.kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), currentNodeClaim); err != nil {
		if apierrors.IsNotFound(err) {
			return NewChurnValidationError(fmt.Errorf("score-based compaction candidate NodeClaim %q no longer exists", candidate.NodeClaim.Name))
		}
		return fmt.Errorf("getting NodeClaim %q for score-based compaction validation, %w", candidate.NodeClaim.Name, err)
	}
	if currentNodeClaim.UID != candidate.NodeClaim.UID || !currentNodeClaim.DeletionTimestamp.IsZero() ||
		currentNodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() ||
		standby.IsNodeClaimStandby(currentNodeClaim) || standby.IsNodeClaimActivating(currentNodeClaim) ||
		currentNodeClaim.Labels[v1.NodePoolLabelKey] != nodePoolName {
		return NewChurnValidationError(fmt.Errorf("score-based compaction candidate NodeClaim %q changed state", candidate.NodeClaim.Name))
	}
	occupyingPods, err := nodeOccupyingPods(ctx, s.kubeClient, currentNode)
	if err != nil {
		return fmt.Errorf("checking occupancy for score-based compaction Node %q, %w", currentNode.Name, err)
	}
	if lo.NoneBy(occupyingPods, func(pod *corev1.Pod) bool {
		return podutils.IsActive(pod) && podutils.IsReschedulable(pod)
	}) {
		return NewChurnValidationError(fmt.Errorf("score-based compaction candidate Node %q no longer has reschedulable workload", currentNode.Name))
	}
	return nil
}

func (s *ScoreBasedConsolidation) validateCompactionCommand(ctx context.Context, cmd *Command) error {
	for _, candidate := range cmd.Candidates {
		if err := s.validateCompactionCandidate(ctx, candidate, candidate.NodePool.Name); err != nil {
			return err
		}
	}
	return nil
}
