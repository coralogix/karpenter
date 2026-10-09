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

//nolint:gocyclo // Identity, deletion, standby, activation, and occupancy checks jointly protect one live source.
func (s *ScoreBasedConsolidation) validateLiveCompactionCandidate(ctx context.Context, reader client.Reader, candidate *Candidate, nodePoolName string) error {
	if reader == nil {
		return fmt.Errorf("score-based compaction requires an API reader for live validation")
	}
	liveNode := &corev1.Node{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(candidate.Node), liveNode); err != nil {
		if apierrors.IsNotFound(err) {
			return NewChurnValidationError(fmt.Errorf("score-based compaction candidate Node %q no longer exists", candidate.Node.Name))
		}
		return fmt.Errorf("getting live Node %q for score-based compaction validation, %w", candidate.Node.Name, err)
	}
	if liveNode.UID != candidate.Node.UID || liveNode.Spec.ProviderID != candidate.ProviderID() || !liveNode.DeletionTimestamp.IsZero() ||
		standby.HasNodeTaint(liveNode) || liveNode.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" ||
		liveNode.Labels[v1.NodePoolLabelKey] != nodePoolName {
		return NewChurnValidationError(fmt.Errorf("score-based compaction candidate Node %q changed identity or state", candidate.Node.Name))
	}
	liveNodeClaim := &v1.NodeClaim{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), liveNodeClaim); err != nil {
		if apierrors.IsNotFound(err) {
			return NewChurnValidationError(fmt.Errorf("score-based compaction candidate NodeClaim %q no longer exists", candidate.NodeClaim.Name))
		}
		return fmt.Errorf("getting live NodeClaim %q for score-based compaction validation, %w", candidate.NodeClaim.Name, err)
	}
	if liveNodeClaim.UID != candidate.NodeClaim.UID || !liveNodeClaim.DeletionTimestamp.IsZero() ||
		liveNodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() ||
		standby.IsNodeClaimStandby(liveNodeClaim) || standby.IsNodeClaimActivating(liveNodeClaim) ||
		liveNodeClaim.Labels[v1.NodePoolLabelKey] != nodePoolName {
		return NewChurnValidationError(fmt.Errorf("score-based compaction candidate NodeClaim %q changed state", candidate.NodeClaim.Name))
	}
	livePods, err := nodeOccupyingPods(ctx, reader, liveNode)
	if err != nil {
		return fmt.Errorf("checking live occupancy for score-based compaction Node %q, %w", liveNode.Name, err)
	}
	if lo.NoneBy(livePods, func(pod *corev1.Pod) bool {
		return podutils.IsActive(pod) && podutils.IsReschedulable(pod)
	}) {
		return NewChurnValidationError(fmt.Errorf("score-based compaction candidate Node %q no longer has reschedulable workload", liveNode.Name))
	}
	return nil
}

func (s *ScoreBasedConsolidation) validateLiveCompactionCommand(ctx context.Context, cmd *Command) error {
	reader := s.apiReader()
	for _, candidate := range cmd.Candidates {
		if candidate == nil || candidate.NodePool == nil {
			return NewChurnValidationError(fmt.Errorf("score-based compaction candidate is missing its NodePool"))
		}
		if err := s.validateLiveCompactionCandidate(ctx, reader, candidate, candidate.NodePool.Name); err != nil {
			return err
		}
	}
	return nil
}

func (s *ScoreBasedConsolidation) apiReader() client.Reader {
	if s.queue != nil {
		return s.queue.apiReader
	}
	return s.kubeClient
}
