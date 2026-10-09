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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
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

// computeReclamationCommand chooses empty candidates for removal without applying NodePool
// disruption budgets. Reclamation runs as its own method before score-based compaction.
func (s *Reclamation) computeReclamationCommand(ctx context.Context, candidates []*Candidate) (*Command, error) {
	if err := s.refreshReclamationInventory(ctx); err != nil {
		return nil, err
	}
	byPool := s.reclamationCandidatesByPool(ctx, candidates)
	if len(byPool) == 0 {
		return nil, nil
	}

	selected := selectReclamationCandidates(byPool)
	if len(selected) == 0 {
		return nil, nil
	}

	cmd := Command{
		Method:     s,
		Action:     DeleteAction,
		Candidates: selected,
	}
	cmd.BeforeDelete = s.reclamationBeforeDelete
	cmd.DeleteWithPreconditions = true
	cmd.OnDeleteSuccess = s.reclamationDeleteSucceeded

	validated, err := s.validateReclamationCommand(ctx, cmd)
	if err != nil || validated == nil {
		return validated, err
	}
	validated.OnSuccess = reclamationBatchSucceeded(validated.Candidates)
	logReclamationBatchSelected(ctx, validated.Candidates)
	return validated, nil
}

func reclamationBatchSucceeded(candidates []*Candidate) func(context.Context) error {
	return func(ctx context.Context) error {
		log.FromContext(ctx).Info("reclamation batch completed",
			"removedNodes", len(candidates),
			"nodePoolCounts", reclamationCandidateCounts(candidates),
		)
		return nil
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
		if candidate == nil || candidate.NodePool == nil {
			continue
		}
		counts[candidate.NodePool.Name]++
	}
	return counts
}

func (s *Reclamation) reclamationCandidatesByPool(ctx context.Context, candidates []*Candidate) map[string][]*Candidate {
	byPool := map[string][]*Candidate{}
	for _, candidate := range candidates {
		if candidate == nil || candidate.NodePool == nil || !standbyLifecycleEnabled(candidate.NodePool) {
			continue
		}
		if !s.isReclamationCandidateAvailable(ctx, candidate) {
			continue
		}
		name := candidate.NodePool.Name
		byPool[name] = append(byPool[name], candidate)
	}
	return byPool
}

func selectReclamationCandidates(byPool map[string][]*Candidate) []*Candidate {
	poolNames := make([]string, 0, len(byPool))
	for name := range byPool {
		poolNames = append(poolNames, name)
	}
	sort.Strings(poolNames)
	var selected []*Candidate
	for _, name := range poolNames {
		poolCandidates := append([]*Candidate(nil), byPool[name]...)
		sort.Slice(poolCandidates, func(i, j int) bool {
			return poolCandidates[i].Name() < poolCandidates[j].Name()
		})
		selected = append(selected, poolCandidates...)
	}
	return selected
}

func (s *Reclamation) validateReclamationCommand(ctx context.Context, cmd Command) (*Command, error) {
	// Reclamation considers only nodes already marked and tainted as standby. Refresh candidates
	// immediately before admission; the queue performs a final live check and conditional delete.
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

// collectReclamationInventory counts managed, non-terminating empty nodes in configured score-based pools.
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
	empty, err := nodeEmpty(ctx, s.apiReader(), node.Node)
	if err != nil {
		return nil, "", false, false, fmt.Errorf("checking emptiness of node %q for reclamation, %w", node.Name(), err)
	}
	isStandby := standby.IsNodeClaimStandby(node.NodeClaim) && standby.HasNodeTaint(node.Node)
	return nodePool, poolName, isStandby, empty, nil
}

func (s *Reclamation) reclamationBeforeDelete(ctx context.Context, candidates []*Candidate) error {
	for _, candidate := range candidates {
		if err := s.reclamationCandidateBeforeDelete(ctx, candidate); err != nil {
			return err
		}
	}
	return nil
}

//nolint:gocyclo // The final pre-delete validation keeps the ordered safety checks and refreshes together.
func (s *Reclamation) reclamationCandidateBeforeDelete(ctx context.Context, candidate *Candidate) error {
	if candidate == nil || candidate.NodeClaim == nil || candidate.Node == nil || candidate.NodePool == nil {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate is missing its Node, NodeClaim, or NodePool"))
	}
	nodeClaim := &v1.NodeClaim{}
	if err := s.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
		if apierrors.IsNotFound(err) {
			// The original object is already gone. Queue's UID-preconditioned Delete will treat
			// NotFound as completion and cannot delete a same-name replacement.
			return nil
		}
		return fmt.Errorf("getting reclamation candidate NodeClaim before deletion, %w", err)
	}
	if nodeClaim.UID != candidate.NodeClaim.UID {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q was replaced", candidate.Name()))
	}
	if !nodeClaim.DeletionTimestamp.IsZero() {
		// Refresh the resourceVersion so an already-started deletion is idempotently completed.
		candidate.NodeClaim = nodeClaim
		return nil
	}
	if nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is terminating", candidate.Name()))
	}
	if standby.IsNodeClaimActivating(nodeClaim) || s.cluster.IsNodeNominated(candidate.ProviderID()) {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is activating or nominated", candidate.Name()))
	}
	if nodeClaim.Annotations[v1.DoNotDisruptAnnotationKey] == "true" {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is do-not-disrupt", candidate.Name()))
	}
	if !standby.IsNodeClaimStandby(nodeClaim) {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is no longer standby", candidate.Name()))
	}
	if !standbyReclamationSoakElapsed(candidate.NodePool, nodeClaim, s.clock.Now()) {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q has not completed the standby soak", candidate.Name()))
	}
	if nodeClaim.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q changed NodePool", candidate.Name()))
	}
	if err := s.reclamationNodePoolStillManaged(ctx, candidate.NodePool.Name); err != nil {
		return err
	}
	if err := s.reclamationNodeStillEmpty(ctx, candidate); err != nil {
		return err
	}
	candidate.NodeClaim = nodeClaim
	return nil
}

func (s *Reclamation) reclamationCandidateNodeClaim(ctx context.Context, candidate *Candidate) (*v1.NodeClaim, bool, error) {
	nodeClaim := &v1.NodeClaim{}
	if err := s.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
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

func (s *Reclamation) reclamationNodePoolStillManaged(ctx context.Context, nodePoolName string) error {
	nodePool := &v1.NodePool{}
	if err := s.apiReader().Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
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

//nolint:gocyclo // These live Node checks form one final identity, standby, and emptiness validation.
func (s *Reclamation) reclamationNodeStillEmpty(ctx context.Context, candidate *Candidate) error {
	node := &corev1.Node{}
	if err := s.apiReader().Get(ctx, types.NamespacedName{Name: candidate.Node.Name}, node); err != nil {
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
	empty, err := nodeEmpty(ctx, s.apiReader(), node)
	if err != nil {
		return fmt.Errorf("checking reclamation candidate emptiness, %w", err)
	}
	if !empty {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is no longer empty", candidate.Name()))
	}
	candidate.Node = node
	return nil
}

func (s *Reclamation) reclamationDeleteSucceeded(_ context.Context, candidate *Candidate) error {
	if candidate == nil || candidate.NodePool == nil {
		return fmt.Errorf("reclamation candidate is missing its NodePool")
	}
	ReclamationNodeRemovalsTotal.Inc(map[string]string{metrics.NodePoolLabel: candidate.NodePool.Name})
	return nil
}

func (s *Reclamation) apiReader() client.Reader {
	if s.queue != nil && s.queue.apiReader != nil {
		return s.queue.apiReader
	}
	return s.kubeClient
}

// isReclamationCandidateAvailable filters candidates that have become unsafe to remove since candidate collection.
//
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
	if !standbyReclamationSoakElapsed(candidate.NodePool, currentNodeClaim, s.clock.Now()) {
		return false
	}
	if currentNodeClaim.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name {
		return false
	}
	currentNode := &corev1.Node{}
	if err := s.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.Node), currentNode); err != nil {
		return false
	}
	if currentNode.UID != candidate.Node.UID || !standby.HasNodeTaint(currentNode) || currentNode.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" {
		return false
	}
	if !currentNode.DeletionTimestamp.IsZero() || currentNode.Annotations[v1.DoNotDisruptAnnotationKey] == "true" || currentNodeClaim.Annotations[v1.DoNotDisruptAnnotationKey] == "true" {
		return false
	}
	empty, err := nodeEmpty(ctx, s.apiReader(), currentNode)
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
	return standbyReclamationSoakElapsed(candidate.NodePool, candidate.NodeClaim, s.clock.Now())
}
