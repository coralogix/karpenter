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

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
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
	validator Validator
}

type reclamationInventory struct {
	emptyNodeCounts map[string]emptyNodeCounts
}

func NewReclamation(c consolidation, opts ...option.Function[MethodOptions]) *Reclamation {
	o := option.Resolve(append([]option.Function[MethodOptions]{WithValidator(NewReclamationValidator(c))}, opts...)...)
	return &Reclamation{
		consolidation: c,
		validator:     o.validator,
	}
}

func NewReclamationValidator(c consolidation) *reclamationValidator {
	s := &Reclamation{consolidation: c}
	return &reclamationValidator{
		validation: validation{
			clock:         c.clock,
			cluster:       c.cluster,
			kubeClient:    c.kubeClient,
			provisioner:   c.provisioner,
			cloudProvider: c.cloudProvider,
			recorder:      c.recorder,
			queue:         c.queue,
			reason:        v1.DisruptionReasonEmpty,
		},
		reclamation: s,
	}
}

func (s *Reclamation) ShouldDisrupt(ctx context.Context, candidate *Candidate) bool {
	return s.isReclamationCandidateAvailable(ctx, candidate)
}

func (s *Reclamation) ComputeCommands(ctx context.Context, _ map[string]int, candidates ...*Candidate) ([]Command, error) {
	cmd, err := s.computeReclamationCommand(ctx, candidates)
	if err != nil {
		return []Command{}, err
	}
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

func (s *Reclamation) computeReclamationCommand(ctx context.Context, candidates []*Candidate) (*Command, error) {
	if err := s.refreshReclamationInventory(ctx); err != nil {
		return nil, err
	}
	eligible := lo.Filter(candidates, func(candidate *Candidate, _ int) bool {
		return s.reclamationCandidateEligible(candidate)
	})
	selected := selectReclamationCandidates(eligible)
	if len(selected) == 0 {
		return nil, nil
	}

	cmd := Command{
		Method:     s,
		Action:     DeleteAction,
		Candidates: selected,
	}

	validated, err := s.validateReclamationCommand(ctx, cmd)
	if err != nil || validated == nil {
		return validated, err
	}
	logReclamationBatchSelected(ctx, validated.Candidates)
	return validated, nil
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
		if candidate == nil || candidate.NodePool == nil {
			continue
		}
		counts[candidate.NodePool.Name]++
	}
	return counts
}

func selectReclamationCandidates(candidates []*Candidate) []*Candidate {
	var selected []*Candidate
	for _, candidate := range candidates {
		if candidate == nil || candidate.NodePool == nil || !standbyLifecycleEnabled(candidate.NodePool) {
			continue
		}
		if !reclamationCandidateHasIdentity(candidate) {
			continue
		}
		selected = append(selected, candidate)
	}
	sort.Slice(selected, func(i, j int) bool {
		if selected[i].NodePool.Name != selected[j].NodePool.Name {
			return selected[i].NodePool.Name < selected[j].NodePool.Name
		}
		return selected[i].Name() < selected[j].Name()
	})
	return selected
}

func (s *Reclamation) validateReclamationCommand(ctx context.Context, cmd Command) (*Command, error) {
	validated, err := s.validator.Validate(ctx, cmd, 0)
	if err != nil {
		if IsValidationError(err) {
			reason := getValidationFailureReason(err)
			cmd.EmitRejectedEvents(s.recorder, reason)
			return nil, nil
		}
		return nil, fmt.Errorf("validating reclamation, %w", err)
	}
	if len(validated.Candidates) == 0 {
		return nil, nil
	}
	return &validated, nil
}

func (s *Reclamation) refreshReclamationInventory(ctx context.Context) error {
	inventory, err := s.collectReclamationInventory(ctx)
	if err != nil {
		return err
	}
	updateReclamationEmptyNodeMetrics(inventory.emptyNodeCounts)
	return nil
}

func (s *Reclamation) collectReclamationInventory(ctx context.Context) (*reclamationInventory, error) {
	nodePools, inventoryCounts, err := s.reclamationPools(ctx)
	if err != nil {
		return nil, err
	}
	inventory := &reclamationInventory{
		emptyNodeCounts: inventoryCounts,
	}
	for _, node := range s.cluster.DeepCopyNodes() {
		_, poolName, isStandby, empty, err := s.emptyReclamationNodeInventory(ctx, node, nodePools)
		if err != nil {
			return nil, err
		}
		if !empty {
			continue
		}
		inventory.emptyNodeCounts[poolName] = inventory.emptyNodeCounts[poolName].add(isStandby)
	}
	return inventory, nil
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

func (s *Reclamation) emptyReclamationNodeInventory(ctx context.Context, node *state.StateNode, nodePools map[string]*v1.NodePool) (*v1.NodePool, string, bool, bool, error) {
	if node.NodeClaim == nil || node.Node == nil || node.MarkedForDeletion() {
		return nil, "", false, false, nil
	}
	poolName := node.Labels()[v1.NodePoolLabelKey]
	nodePool := nodePools[poolName]
	if nodePool == nil {
		return nil, "", false, false, nil
	}
	empty, err := nodeEmpty(ctx, s.kubeClient, node.Node)
	if err != nil {
		return nil, "", false, false, fmt.Errorf("checking emptiness of node %q for reclamation, %w", node.Name(), err)
	}
	isStandby := standby.IsNodeClaimStandby(node.NodeClaim) && standby.HasNodeTaint(node.Node)
	return nodePool, poolName, isStandby, empty, nil
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
	empty, err := nodeEmpty(ctx, s.kubeClient, currentNode)
	return err == nil && empty
}

func (s *Reclamation) reclamationCandidateEligible(candidate *Candidate) bool {
	if candidate == nil || candidate.NodePool == nil || candidate.NodeClaim == nil || candidate.Node == nil {
		return false
	}
	if standby.IsNodeClaimActivating(candidate.NodeClaim) {
		return false
	}
	if !standbyLifecycleEnabled(candidate.NodePool) {
		return false
	}
	return standbyReclamationDelayElapsed(candidate.NodePool, candidate.NodeClaim, s.clock.Now())
}
