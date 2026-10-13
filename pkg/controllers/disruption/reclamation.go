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
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const ReclamationType = "reclamation"

type Reclamation struct {
	consolidation
}

func NewReclamation(c consolidation) *Reclamation {
	return &Reclamation{consolidation: c}
}

func (s *Reclamation) ShouldDisrupt(ctx context.Context, candidate *Candidate) bool {
	return s.isReclamationCandidateAvailable(ctx, candidate)
}

func (s *Reclamation) ComputeCommands(ctx context.Context, _ map[string]int, candidates ...*Candidate) ([]Command, error) {
	cmd := s.computeReclamationCommand(ctx, candidates)
	if cmd == nil {
		return []Command{}, nil
	}
	return []Command{*cmd}, nil
}

func (s *Reclamation) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonEmpty
}

func (s *Reclamation) Class() string {
	return GracefulDisruptionClass
}

func (s *Reclamation) ConsolidationType() string {
	return ReclamationType
}

func (s *Reclamation) computeReclamationCommand(ctx context.Context, candidates []*Candidate) *Command {
	s.refreshReclamationInventory(ctx)
	selected := selectReclamationCandidates(candidates)
	if len(selected) == 0 {
		return nil
	}

	logReclamationBatchSelected(ctx, selected)
	return &Command{
		Method:     s,
		Action:     DeleteAction,
		Candidates: selected,
	}
}

func logReclamationBatchSelected(ctx context.Context, candidates []*Candidate) {
	log.FromContext(ctx).V(1).Info("reclamation batch selected",
		"nodeCount", len(candidates),
		"nodePoolCounts", reclamationCandidateCounts(candidates),
	)
}

func reclamationCandidateCounts(candidates []*Candidate) map[string]int {
	counts := map[string]int{}
	for _, candidate := range candidates {
		counts[candidate.NodePool.Name]++
	}
	return counts
}

func selectReclamationCandidates(candidates []*Candidate) []*Candidate {
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].NodePool.Name != candidates[j].NodePool.Name {
			return candidates[i].NodePool.Name < candidates[j].NodePool.Name
		}
		return candidates[i].Name() < candidates[j].Name()
	})
	return candidates
}

func (s *Reclamation) refreshReclamationInventory(ctx context.Context) {
	emptyNodeCounts, err := s.collectReclamationInventory(ctx)
	if err != nil {
		log.FromContext(ctx).Error(err, "collecting reclamation empty-node inventory")
		return
	}
	updateReclamationEmptyNodeMetrics(emptyNodeCounts)
}

func (s *Reclamation) collectReclamationInventory(ctx context.Context) (map[string]emptyNodeCounts, error) {
	nodePools, inventoryCounts, err := s.reclamationPools(ctx)
	if err != nil {
		return nil, err
	}
	for _, node := range s.cluster.DeepCopyNodes() {
		poolName, isStandby, empty, err := s.emptyReclamationNodeInventory(ctx, node, nodePools)
		if err != nil {
			return nil, err
		}
		if !empty {
			continue
		}
		inventoryCounts[poolName] = inventoryCounts[poolName].add(isStandby)
	}
	return inventoryCounts, nil
}

func (s *Reclamation) reclamationPools(ctx context.Context) (map[string]*v1.NodePool, map[string]emptyNodeCounts, error) {
	var nodePoolList v1.NodePoolList
	if err := s.kubeClient.List(ctx, &nodePoolList); err != nil {
		return nil, nil, fmt.Errorf("listing NodePools for reclamation, %w", err)
	}
	nodePools := make(map[string]*v1.NodePool, len(nodePoolList.Items))
	inventoryCounts := make(map[string]emptyNodeCounts, len(nodePoolList.Items))
	for i := range nodePoolList.Items {
		nodePool := &nodePoolList.Items[i]
		if !standbyLifecycleEnabled(nodePool) {
			continue
		}
		nodePools[nodePool.Name] = nodePool
		inventoryCounts[nodePool.Name] = emptyNodeCounts{}
	}
	return nodePools, inventoryCounts, nil
}

func (s *Reclamation) emptyReclamationNodeInventory(ctx context.Context, node *state.StateNode, nodePools map[string]*v1.NodePool) (string, bool, bool, error) {
	if node.NodeClaim == nil || node.Node == nil || node.MarkedForDeletion() {
		return "", false, false, nil
	}
	poolName := node.Labels()[v1.NodePoolLabelKey]
	if nodePools[poolName] == nil {
		return "", false, false, nil
	}
	empty, err := nodeEmpty(ctx, s.kubeClient, node.Node)
	if err != nil {
		return "", false, false, fmt.Errorf("checking emptiness of node %q for reclamation, %w", node.Name(), err)
	}
	isStandby := standby.IsNodeClaimStandby(node.NodeClaim) && standby.HasNodeTaint(node.Node)
	return poolName, isStandby, empty, nil
}

func (s *Reclamation) reclamationCandidateNodeClaim(ctx context.Context, candidate *Candidate) (*v1.NodeClaim, bool, error) {
	nodeClaim := &v1.NodeClaim{}
	if err := s.kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("getting reclamation candidate NodeClaim, %w", err)
	}
	if !nodeClaim.DeletionTimestamp.IsZero() || nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
		return nil, false, nil
	}
	return nodeClaim, true, nil
}

//nolint:gocyclo // Keep the candidate's live NodeClaim, Node, and pod safety checks in one predicate.
func (s *Reclamation) isReclamationCandidateAvailable(ctx context.Context, candidate *Candidate) bool {
	if !s.reclamationCandidateEligible(candidate) {
		return false
	}
	currentNodeClaim, exists, err := s.reclamationCandidateNodeClaim(ctx, candidate)
	if err != nil || !exists {
		return false
	}
	if standby.IsNodeClaimActivating(currentNodeClaim) {
		return false
	}
	if !standby.IsNodeClaimStandby(currentNodeClaim) {
		return false
	}
	if !standbyReclamationDelayElapsed(candidate.NodePool, currentNodeClaim, s.clock.Now()) {
		return false
	}
	if currentNodeClaim.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name {
		return false
	}
	currentNode := &corev1.Node{}
	if err := s.kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.Node), currentNode); err != nil {
		return false
	}
	if currentNode.UID != candidate.Node.UID || !standby.HasNodeTaint(currentNode) || currentNode.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" {
		return false
	}
	if !currentNode.DeletionTimestamp.IsZero() || currentNode.Annotations[v1.DoNotDisruptAnnotationKey] == "true" || currentNodeClaim.Annotations[v1.DoNotDisruptAnnotationKey] == "true" {
		return false
	}
	if s.cluster != nil && s.cluster.HasBufferPods(candidate.ProviderID()) {
		return false
	}
	empty, err := nodeEmpty(ctx, s.kubeClient, currentNode)
	return err == nil && empty
}

func (s *Reclamation) reclamationCandidateEligible(candidate *Candidate) bool {
	if standby.IsNodeClaimActivating(candidate.NodeClaim) {
		return false
	}
	if !standbyLifecycleEnabled(candidate.NodePool) {
		return false
	}
	return standbyReclamationDelayElapsed(candidate.NodePool, candidate.NodeClaim, s.clock.Now())
}
