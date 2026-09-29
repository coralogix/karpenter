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
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const (
	ScoreBasedStandbyType = "score-based-standby"
	scoreBasedStandbySpan = "karpenter.disruption.score_based_standby"
)

// ScoreBasedStandby marks naturally empty active nodes in score-based NodePools as standby
// capacity. It does not evict pods, consume disruption budgets, or participate in compaction.
type ScoreBasedStandby struct {
	consolidation
}

func NewScoreBasedStandby(c consolidation) *ScoreBasedStandby {
	return &ScoreBasedStandby{consolidation: c}
}

//nolint:gocyclo // This predicate groups the complete empty-active-node eligibility gate.
func (s *ScoreBasedStandby) ShouldDisrupt(ctx context.Context, candidate *Candidate) bool {
	if candidate == nil || candidate.Node == nil || candidate.NodeClaim == nil || candidate.NodePool == nil {
		return false
	}
	if !scoreBasedReclamationConfigured(candidate.NodePool) {
		return false
	}
	if !candidate.NodeClaim.DeletionTimestamp.IsZero() {
		return false
	}
	if standby.IsNodeClaimStandby(candidate.NodeClaim) || standby.IsNodeClaimActivating(candidate.NodeClaim) || standby.HasNodeTaint(candidate.Node) {
		return false
	}
	if len(candidate.reschedulablePods) > 0 {
		return false
	}
	if s.cluster != nil && s.cluster.IsNodeNominated(candidate.ProviderID()) {
		return false
	}
	empty, err := reclamationEmpty(ctx, s.apiReader(), candidate.Node)
	return err == nil && empty
}

func (s *ScoreBasedStandby) ComputeCommands(_ context.Context, _ map[string]int, candidates ...*Candidate) ([]Command, error) {
	var selected []*Candidate
	for _, candidate := range candidates {
		if candidate != nil {
			selected = append(selected, candidate)
		}
	}
	if len(selected) == 0 {
		return nil, nil
	}
	return []Command{{Method: s, Action: StandbyAction, Candidates: selected}}, nil
}

func (s *ScoreBasedStandby) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonEmpty
}

func (s *ScoreBasedStandby) Class() string {
	return GracefulDisruptionClass
}

func (s *ScoreBasedStandby) ConsolidationType() string {
	return ScoreBasedStandbyType
}

func (s *ScoreBasedStandby) apiReader() client.Reader {
	if s.queue != nil && s.queue.apiReader != nil {
		return s.queue.apiReader
	}
	return s.kubeClient
}

// standbyCandidateMatchesLiveObjects verifies that the Node and NodeClaim still identify the
// same active, empty node before the queue changes its scheduling state.
//
//nolint:gocyclo // The live identity, policy, nomination, and pod checks are one transition precondition.
func (s *ScoreBasedStandby) standbyCandidateMatchesLiveObjects(ctx context.Context, candidate *Candidate) (*corev1.Node, *v1.NodeClaim, bool, error) {
	if candidate == nil || candidate.Node == nil || candidate.NodeClaim == nil || candidate.NodePool == nil {
		return nil, nil, false, fmt.Errorf("standby candidate is missing its Node, NodeClaim, or NodePool")
	}
	node := &corev1.Node{}
	if err := s.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.Node), node); err != nil {
		return nil, nil, false, client.IgnoreNotFound(err)
	}
	nodeClaim := &v1.NodeClaim{}
	if err := s.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
		return nil, nil, false, client.IgnoreNotFound(err)
	}
	if node.UID != candidate.Node.UID || nodeClaim.UID != candidate.NodeClaim.UID || node.Spec.ProviderID != candidate.ProviderID() {
		return nil, nil, false, nil
	}
	if !node.DeletionTimestamp.IsZero() || !nodeClaim.DeletionTimestamp.IsZero() || nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() ||
		standby.IsNodeClaimStandby(nodeClaim) || standby.IsNodeClaimActivating(nodeClaim) || standby.HasNodeTaint(node) ||
		node.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" ||
		node.Annotations[v1.DoNotDisruptAnnotationKey] == "true" || nodeClaim.Annotations[v1.DoNotDisruptAnnotationKey] == "true" ||
		(s.cluster != nil && s.cluster.IsNodeNominated(candidate.ProviderID())) {
		return nil, nil, false, nil
	}
	nodePool := &v1.NodePool{}
	if err := s.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.NodePool), nodePool); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, false, nil
		}
		return nil, nil, false, fmt.Errorf("getting standby candidate NodePool, %w", err)
	}
	if !scoreBasedReclamationConfigured(nodePool) {
		return nil, nil, false, nil
	}
	if node.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name || nodeClaim.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name ||
		node.Labels[v1.NodeInitializedLabelKey] != "true" {
		return nil, nil, false, nil
	}
	empty, err := reclamationEmpty(ctx, s.apiReader(), node)
	if err != nil || !empty {
		return nil, nil, false, err
	}
	return node, nodeClaim, true, nil
}

// updateStandbyState refreshes the internal cluster snapshot before another disruption pass can
// make a decision using the just-updated Node and NodeClaim.
func updateStandbyState(ctx context.Context, cluster *state.Cluster, reader client.Reader, node *corev1.Node, nodeClaim *v1.NodeClaim) error {
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return fmt.Errorf("listing pods after standby transition, %w", err)
	}
	cluster.UpdateNodeClaim(nodeClaim)
	return cluster.UpdateNodeWithPods(ctx, node, lo.ToSlicePtr(pods.Items))
}

// startStandbyCommand reserves the candidates while their empty-to-standby state change is
// committed. This is synchronous because there are no pods to evict or replacements to launch.
//
//nolint:gocyclo // Reservation, transition, and per-candidate reporting form one synchronous command.
func (q *Queue) startStandbyCommand(ctx context.Context, cmd *Command) error {
	providerIDs := lo.Map(cmd.Candidates, func(candidate *Candidate, _ int) string { return candidate.ProviderID() })
	q.Lock()
	for _, providerID := range providerIDs {
		if _, exists := q.ProviderIDToCommand[providerID]; exists {
			q.Unlock()
			return fmt.Errorf("standby candidate is already being disrupted")
		}
	}
	for _, providerID := range providerIDs {
		q.ProviderIDToCommand[providerID] = cmd
	}
	q.Unlock()
	defer func() {
		q.Lock()
		defer q.Unlock()
		for _, providerID := range providerIDs {
			if q.ProviderIDToCommand[providerID] == cmd {
				delete(q.ProviderIDToCommand, providerID)
			}
		}
	}()

	standbyMethod, ok := cmd.Method.(*ScoreBasedStandby)
	if !ok {
		return fmt.Errorf("standby action requires ScoreBasedStandby method")
	}
	var transitioned []*Candidate
	var errs []error
	for _, candidate := range cmd.Candidates {
		marked, err := q.markEmptyNodeStandby(ctx, standbyMethod, candidate)
		if err != nil {
			errs = append(errs, fmt.Errorf("marking NodeClaim %q standby, %w", candidate.NodeClaim.Name, err))
			continue
		}
		if marked {
			transitioned = append(transitioned, candidate)
		}
	}
	if len(transitioned) > 0 {
		standbyNodePoolsMarked := map[string]int{}
		for _, candidate := range transitioned {
			ScoreBasedStandbyNodesMarkedTotal.Inc(map[string]string{metrics.NodePoolLabel: candidate.NodePool.Name})
			standbyNodePoolsMarked[candidate.NodePool.Name]++
			q.recorder.Publish(disruptionevents.StandbyMarked(candidate.Node, candidate.NodeClaim)...)
		}
		log.FromContext(ctx).Info("marked empty nodes as standby",
			"nodeCount", len(transitioned),
			"nodePoolCounts", standbyNodePoolsMarked,
		)
	}
	return multierr.Combine(errs...)
}

//nolint:gocyclo // This transition keeps its live rechecks, optimistic patches, and rollback steps together.
func (q *Queue) markEmptyNodeStandby(ctx context.Context, method *ScoreBasedStandby, candidate *Candidate) (bool, error) {
	node, _, eligible, err := method.standbyCandidateMatchesLiveObjects(ctx, candidate)
	if err != nil || !eligible {
		return false, err
	}
	if err := q.patchDisruptionTaint(ctx, node, true); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}

	// Tainting prevents new scheduling in the steady state. Refresh every object and repeat the
	// emptiness check after the taint write to catch pods bound during candidate preparation.
	var nodeClaim *v1.NodeClaim
	_, nodeClaim, eligible, err = method.standbyCandidateMatchesLiveObjects(ctx, candidate)
	if err != nil || !eligible {
		rollbackErr := q.removeDisruptionTaint(ctx, candidate.Node.Name)
		return false, multierr.Combine(err, rollbackErr)
	}
	storedNodeClaim := nodeClaim.DeepCopy()
	standby.SetNodeClaimStandby(nodeClaim, true)
	if err := q.kubeClient.Patch(ctx, nodeClaim, client.MergeFromWithOptions(storedNodeClaim, client.MergeFromWithOptimisticLock{})); err != nil {
		rollbackErr := q.removeDisruptionTaint(ctx, candidate.Node.Name)
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return false, rollbackErr
		}
		return false, multierr.Combine(err, rollbackErr)
	}

	// Read the Node and NodeClaim after reserving the NodeClaim. The resourceVersion makes this
	// taint patch race with activation's Node marker patch; a conflict leaves the temporary
	// disruption taint in place for stale-state cleanup to resolve safely.
	node = &corev1.Node{}
	if err := method.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.Node), node); err != nil {
		return false, err
	}
	nodeClaim = &v1.NodeClaim{}
	if err := method.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
		return false, err
	}
	if nodeClaim.UID != candidate.NodeClaim.UID {
		return false, nil
	}
	if !standby.IsNodeClaimStandby(nodeClaim) || standby.IsNodeClaimActivating(nodeClaim) || node.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" {
		if err := q.removeDisruptionTaint(ctx, node.Name); err != nil {
			return false, err
		}
		return false, nil
	}
	storedNode := node.DeepCopy()
	standby.SetNodeTaint(node, true)
	if err := q.kubeClient.Patch(ctx, node, client.MergeFromWithOptions(storedNode, client.MergeFromWithOptimisticLock{})); err != nil {
		return false, err
	}
	if err := q.removeDisruptionTaint(ctx, node.Name); err != nil {
		return false, err
	}
	node = &corev1.Node{}
	if err := method.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.Node), node); err != nil {
		return false, err
	}
	nodeClaim = &v1.NodeClaim{}
	if err := method.apiReader().Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
		return false, err
	}

	// A pod may bind after the last emptiness read while the scheduler still has an older Node
	// snapshot. Use the normal activation reservation path to restore such capacity immediately.
	recovered, err := q.recoverOccupiedStandbyNode(ctx, method, node, nodeClaim)
	if err != nil {
		return false, err
	}
	if err := updateStandbyState(ctx, q.cluster, method.apiReader(), node, nodeClaim); err != nil {
		return false, err
	}
	if recovered || !standby.IsNodeClaimStandby(nodeClaim) || standby.IsNodeClaimActivating(nodeClaim) || !standby.HasNodeTaint(node) {
		return false, nil
	}
	candidate.Node = node
	candidate.NodeClaim = nodeClaim
	return true, nil
}

func (q *Queue) patchDisruptionTaint(ctx context.Context, node *corev1.Node, add bool) error {
	if node == nil {
		return fmt.Errorf("cannot taint a missing Node")
	}
	stored := node.DeepCopy()
	hasTaint := false
	for _, taint := range node.Spec.Taints {
		if taint.MatchTaint(&v1.DisruptedNoScheduleTaint) {
			hasTaint = true
			break
		}
	}
	if add && !hasTaint {
		node.Spec.Taints = append(node.Spec.Taints, v1.DisruptedNoScheduleTaint)
	}
	if !add && hasTaint {
		node.Spec.Taints = lo.Reject(node.Spec.Taints, func(taint corev1.Taint, _ int) bool {
			return taint.MatchTaint(&v1.DisruptedNoScheduleTaint)
		})
	}
	if equality.Semantic.DeepEqual(stored, node) {
		return nil
	}
	return q.kubeClient.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (q *Queue) removeDisruptionTaint(ctx context.Context, nodeName string) error {
	node := &corev1.Node{}
	if err := q.apiReader.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return client.IgnoreNotFound(err)
	}
	return q.patchDisruptionTaint(ctx, node, false)
}

//nolint:gocyclo // Recovery applies activation, removes the temporary taint, and refreshes both live objects.
func (q *Queue) recoverOccupiedStandbyNode(ctx context.Context, method *ScoreBasedStandby, node *corev1.Node, nodeClaim *v1.NodeClaim) (bool, error) {
	empty, err := reclamationEmpty(ctx, method.apiReader(), node)
	if err != nil || empty {
		return false, err
	}
	stateNode := &state.StateNode{Node: node.DeepCopy(), NodeClaim: nodeClaim.DeepCopy()}
	if err := q.provisioner.ActivateStandbyNodes(ctx, standby.ActivationSourceRecovery, stateNode); err != nil {
		return false, fmt.Errorf("restoring occupied standby capacity, %w", err)
	}
	refreshedNode := &corev1.Node{}
	if err := method.apiReader().Get(ctx, client.ObjectKeyFromObject(node), refreshedNode); err != nil {
		return false, err
	}
	refreshedNodeClaim := &v1.NodeClaim{}
	if err := method.apiReader().Get(ctx, client.ObjectKeyFromObject(nodeClaim), refreshedNodeClaim); err != nil {
		return false, err
	}
	if standby.IsNodeClaimStandby(refreshedNodeClaim) || standby.IsNodeClaimActivating(refreshedNodeClaim) || standby.HasNodeTaint(refreshedNode) {
		return false, fmt.Errorf("occupied standby NodeClaim %q has not completed activation", nodeClaim.Name)
	}
	if err := q.removeDisruptionTaint(ctx, refreshedNode.Name); err != nil {
		return false, err
	}
	refreshedNode = &corev1.Node{}
	if err := method.apiReader().Get(ctx, client.ObjectKeyFromObject(node), refreshedNode); err != nil {
		return false, err
	}
	*node, *nodeClaim = *refreshedNode, *refreshedNodeClaim
	return true, nil
}
