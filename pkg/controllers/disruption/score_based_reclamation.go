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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const defaultScoreBasedReclamationInterval = time.Minute

// ExperimentalReclamationRemoveAllEmptyImmediately is a temporary policy until reclamation pacing
// is finalized. When true, every configured pool with empty capacity is eligible on each pass and
// all eligible empty nodes are reclaimed (no half-batch or interval gate).
const ExperimentalReclamationRemoveAllEmptyImmediately = true

// computeReclamationCommand chooses empty candidates for removal without applying NodePool
// disruption budgets. It returns pools whose due empty capacity takes precedence over compaction,
// including when no candidates are currently eligible for removal.
func (s *ScoreBasedConsolidation) computeReclamationCommand(ctx context.Context, candidates []*Candidate) (*Command, map[string]bool, error) {
	emptyCounts, err := s.emptyReclamationNodeCounts(ctx)
	if err != nil {
		return nil, nil, err
	}
	now := s.clock.Now()
	reclamationPools := reclamationPoolsWithEmptyNodes(emptyCounts)
	byPool := s.reclamationCandidatesByPool(ctx, candidates, now)
	if len(byPool) == 0 {
		return nil, reclamationPools, nil
	}

	selected := selectReclamationCandidates(byPool, emptyCounts)
	if len(selected) == 0 {
		return nil, reclamationPools, nil
	}

	cmd := Command{
		Method:     s,
		Action:     DeleteAction,
		Candidates: selected,
	}
	cmd.BeforeDelete = s.reclamationBeforeDelete(selected)
	cmd.OnDeleteSuccess = s.reclamationDeleteSucceeded

	validated, pools, err := s.validateReclamationCommand(ctx, cmd, reclamationPools, now)
	if err != nil || validated == nil {
		return validated, pools, err
	}
	validated.OnSuccess = reclamationBatchSucceeded(validated.Candidates)
	logReclamationBatchSelected(ctx, validated.Candidates)
	return validated, pools, nil
}

func reclamationBatchSucceeded(candidates []*Candidate) func(context.Context) error {
	return func(ctx context.Context) error {
		log.FromContext(ctx).Info("score-based reclamation batch completed",
			"removedNodes", len(candidates),
			"nodePoolCounts", reclamationCandidateCounts(candidates),
		)
		return nil
	}
}

func logReclamationBatchSelected(ctx context.Context, candidates []*Candidate) {
	log.FromContext(ctx).V(1).Info("score-based reclamation batch selected",
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

func reclamationPoolsWithEmptyNodes(emptyCounts map[string]int) map[string]bool {
	reclamationPools := map[string]bool{}
	for name, count := range emptyCounts {
		if count > 0 {
			reclamationPools[name] = true
		}
	}
	return reclamationPools
}

func (s *ScoreBasedConsolidation) reclamationCandidatesByPool(ctx context.Context, candidates []*Candidate, now time.Time) map[string][]*Candidate {
	byPool := map[string][]*Candidate{}
	for _, candidate := range candidates {
		if candidate == nil || candidate.NodePool == nil || !scoreBasedReclamationDue(candidate.NodePool, now) {
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

func selectReclamationCandidates(byPool map[string][]*Candidate, emptyCounts map[string]int) []*Candidate {
	poolNames := make([]string, 0, len(byPool))
	for name := range byPool {
		poolNames = append(poolNames, name)
	}
	sort.Strings(poolNames)
	var selected []*Candidate
	for _, name := range poolNames {
		emptyCandidates := byPool[name]
		sortReclamationCandidates(emptyCandidates)
		removalCount := reclamationRemovalCount(emptyCounts[name])
		if removalCount > len(emptyCandidates) {
			removalCount = len(emptyCandidates)
		}
		selected = append(selected, emptyCandidates[:removalCount]...)
	}
	return selected
}

func (s *ScoreBasedConsolidation) validateReclamationCommand(ctx context.Context, cmd Command, reclamationPools map[string]bool, started time.Time) (*Command, map[string]bool, error) {
	// Re-fetch candidates after the usual consolidation TTL. Reclamation does not consume the
	// NodePool disruption budget, but it still validates eligibility and current emptiness.
	remainingValidationDelay := consolidationTTL - s.clock.Since(started)
	if remainingValidationDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, reclamationPools, ctx.Err()
		case <-s.clock.After(remainingValidationDelay):
		}
	}
	validated, _, err := selectFirstStillValidCommand(ctx, s.reclamationValidator(), s.recorder, []*moveSetEvaluation{{Command: cmd, Score: 1}})
	if err != nil {
		return nil, reclamationPools, err
	}
	if len(validated.Candidates) == 0 {
		return nil, reclamationPools, nil
	}
	return &validated, reclamationPools, nil
}

// emptyReclamationNodeCounts counts managed, non-terminating empty nodes in configured score-based
// pools. It returns due-pool counts for reclamation selection and publishes inventory counts for all
// configured pools, regardless of reclamation cadence.
func (s *ScoreBasedConsolidation) emptyReclamationNodeCounts(ctx context.Context) (map[string]int, error) {
	nodePools, inventoryCounts, err := s.scoreBasedReclamationPools(ctx)
	if err != nil {
		return nil, err
	}

	dueCounts := map[string]int{}
	now := s.clock.Now()
	for _, node := range s.cluster.DeepCopyNodes() {
		nodePool, poolName, isStandby, empty, err := s.emptyReclamationNodeInventory(ctx, node, nodePools)
		if err != nil {
			return nil, err
		}
		if !empty {
			continue
		}
		inventoryCounts[poolName] = inventoryCounts[poolName].add(isStandby)
		if scoreBasedReclamationDue(nodePool, now) {
			dueCounts[poolName]++
		}
	}
	updateScoreBasedEmptyNodeMetrics(inventoryCounts)
	return dueCounts, nil
}

func (s *ScoreBasedConsolidation) scoreBasedReclamationPools(ctx context.Context) (map[string]*v1.NodePool, map[string]scoreBasedEmptyNodeCounts, error) {
	var nodePoolList v1.NodePoolList
	if err := s.kubeClient.List(ctx, &nodePoolList); err != nil {
		return nil, nil, fmt.Errorf("listing NodePools for reclamation, %w", err)
	}
	nodePools := make(map[string]*v1.NodePool, len(nodePoolList.Items))
	inventoryCounts := make(map[string]scoreBasedEmptyNodeCounts, len(nodePoolList.Items))
	for i := range nodePoolList.Items {
		nodePool := &nodePoolList.Items[i]
		if !scoreBasedReclamationConfigured(nodePool) {
			continue
		}
		nodePools[nodePool.Name] = nodePool
		inventoryCounts[nodePool.Name] = scoreBasedEmptyNodeCounts{}
	}
	return nodePools, inventoryCounts, nil
}

func (s *ScoreBasedConsolidation) emptyReclamationNodeInventory(ctx context.Context, node *state.StateNode, nodePools map[string]*v1.NodePool) (*v1.NodePool, string, bool, bool, error) {
	if node.NodeClaim == nil || node.Node == nil || node.MarkedForDeletion() {
		return nil, "", false, false, nil
	}
	poolName := node.Labels()[v1.NodePoolLabelKey]
	nodePool := nodePools[poolName]
	if nodePool == nil {
		return nil, "", false, false, nil
	}
	empty, err := reclamationEmpty(ctx, s.kubeClient, node.Node)
	if err != nil {
		return nil, "", false, false, fmt.Errorf("checking emptiness of node %q for reclamation, %w", node.Name(), err)
	}
	isStandby := standby.IsNodeClaimStandby(node.NodeClaim) && standby.HasNodeTaint(node.Node)
	return nodePool, poolName, isStandby, empty, nil
}

func (s *ScoreBasedConsolidation) reclamationBeforeDelete(candidates []*Candidate) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, candidate := range candidates {
			if err := s.reclamationCandidateBeforeDelete(ctx, candidate); err != nil {
				return err
			}
		}
		return nil
	}
}

func (s *ScoreBasedConsolidation) reclamationCandidateBeforeDelete(ctx context.Context, candidate *Candidate) error {
	nodeClaim, exists, err := s.reclamationCandidateNodeClaim(ctx, candidate)
	if err != nil || !exists {
		return err
	}
	if standby.IsNodeClaimActivating(nodeClaim) || s.cluster.IsNodeNominated(candidate.ProviderID()) {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is activating or nominated", candidate.Name()))
	}
	if nodeClaim.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q changed NodePool", candidate.Name()))
	}
	if err := s.reclamationNodePoolStillManaged(ctx, candidate.NodePool.Name); err != nil {
		return err
	}
	return s.reclamationNodeStillEmpty(ctx, candidate)
}

func (s *ScoreBasedConsolidation) reclamationCandidateNodeClaim(ctx context.Context, candidate *Candidate) (*v1.NodeClaim, bool, error) {
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

func (s *ScoreBasedConsolidation) reclamationNodePoolStillManaged(ctx context.Context, nodePoolName string) error {
	nodePool := &v1.NodePool{}
	if err := s.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
		if apierrors.IsNotFound(err) {
			return NewUnrecoverableError(fmt.Errorf("reclamation NodePool %q was deleted", nodePoolName))
		}
		return fmt.Errorf("getting reclamation NodePool, %w", err)
	}
	if !scoreBasedReclamationConfigured(nodePool) {
		return NewUnrecoverableError(fmt.Errorf("reclamation NodePool %q is no longer eligible for reclamation", nodePool.Name))
	}
	return nil
}

func (s *ScoreBasedConsolidation) reclamationNodeStillEmpty(ctx context.Context, candidate *Candidate) error {
	node := &corev1.Node{}
	if err := s.kubeClient.Get(ctx, types.NamespacedName{Name: candidate.Node.Name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("getting reclamation candidate Node, %w", err)
	}
	if node.Spec.ProviderID != candidate.ProviderID() {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q changed provider ID", candidate.Name()))
	}
	if node.Annotations[v1.DoNotDisruptAnnotationKey] == "true" || node.Labels[v1.NodeInitializedLabelKey] != "true" {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is no longer eligible", candidate.Name()))
	}
	empty, err := reclamationEmpty(ctx, s.kubeClient, node)
	if err != nil {
		return fmt.Errorf("checking reclamation candidate emptiness, %w", err)
	}
	if !empty {
		return NewUnrecoverableError(fmt.Errorf("reclamation candidate %q is no longer empty", candidate.Name()))
	}
	return nil
}

func (s *ScoreBasedConsolidation) reclamationDeleteSucceeded(ctx context.Context, candidate *Candidate) error {
	if candidate == nil || candidate.NodePool == nil {
		return fmt.Errorf("reclamation candidate is missing its NodePool")
	}
	if err := s.updateLastReclamationRemoval(ctx, candidate.NodePool.Name); err != nil {
		return err
	}
	ScoreBasedReclamationNodeRemovalsTotal.Inc(map[string]string{metrics.NodePoolLabel: candidate.NodePool.Name})
	return nil
}

func (s *ScoreBasedConsolidation) updateLastReclamationRemoval(ctx context.Context, nodePoolName string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		nodePool := &v1.NodePool{}
		if err := s.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("getting NodePool to persist reclamation timestamp, %w", err)
		}
		stored := nodePool.DeepCopy()
		if nodePool.Annotations == nil {
			nodePool.Annotations = map[string]string{}
		}
		nodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey] = s.clock.Now().UTC().Format(time.RFC3339Nano)
		if err := s.kubeClient.Patch(ctx, nodePool, client.MergeFrom(stored)); err != nil {
			return fmt.Errorf("persisting NodePool reclamation timestamp, %w", err)
		}
		return nil
	})
}

// scoreBasedReclamationDue reports whether the pool's periodic empty-node removal is eligible.
// A missing last-removal timestamp makes the first reclamation pass immediately eligible.
func scoreBasedReclamationDue(nodePool *v1.NodePool, now time.Time) bool {
	if !scoreBasedReclamationConfigured(nodePool) {
		return false
	}
	if ExperimentalReclamationRemoveAllEmptyImmediately {
		return true
	}
	return scoreBasedReclamationDueByInterval(nodePool, now)
}

func scoreBasedReclamationDueByInterval(nodePool *v1.NodePool, now time.Time) bool {
	interval := scoreBasedReclamationInterval(nodePool)
	lastRemoval := nodePool.Annotations[v1.ScoreBasedLastReclamationAnnotationKey]
	if lastRemoval == "" {
		return true
	}
	last, err := time.Parse(time.RFC3339Nano, lastRemoval)
	if err != nil {
		log.FromContext(context.Background()).V(1).Info("ignoring invalid score-based reclamation timestamp", "NodePool", nodePool.Name, "annotation", v1.ScoreBasedLastReclamationAnnotationKey, "value", lastRemoval)
		return false
	}
	return now.Sub(last) > interval
}

// scoreBasedReclamationConfigured reports whether this pool is configured for score-based dynamic
// consolidation. Its policy gate matches compaction, while its cadence is controlled separately by
// the reclamation interval. consolidateAfter does not affect score-based mode.
func scoreBasedReclamationConfigured(nodePool *v1.NodePool) bool {
	return NodePoolUsesScoreBasedConsolidation(nodePool) &&
		nodePool.Spec.Replicas == nil &&
		nodePool.Spec.Disruption.ConsolidationPolicy == v1.ConsolidationPolicyWhenEmptyOrUnderutilized
}

func scoreBasedReclamationInterval(nodePool *v1.NodePool) time.Duration {
	if nodePool == nil || nodePool.Annotations == nil {
		return defaultScoreBasedReclamationInterval
	}
	value := nodePool.Annotations[v1.ScoreBasedReclamationIntervalAnnotationKey]
	if value == "" {
		return defaultScoreBasedReclamationInterval
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		log.FromContext(context.Background()).V(1).Info("using default score-based reclamation interval for invalid value", "NodePool", nodePool.Name, "annotation", v1.ScoreBasedReclamationIntervalAnnotationKey, "value", value)
		return defaultScoreBasedReclamationInterval
	}
	return interval
}

// reclamationEmpty reports whether a node has no non-DaemonSet pods. Terminating and terminal
// non-daemon pods still count while they remain bound to the node.
func reclamationEmpty(ctx context.Context, kubeClient client.Client, node *corev1.Node) (bool, error) {
	pods, err := nodeutils.GetPods(ctx, kubeClient, node)
	if err != nil {
		return false, err
	}
	for _, pod := range pods {
		if !podutils.IsOwnedByDaemonSet(pod) {
			return false, nil
		}
	}
	return true, nil
}

func reclamationRemovalCount(emptyNodeCount int) int {
	if ExperimentalReclamationRemoveAllEmptyImmediately {
		return emptyNodeCount
	}
	// Rounds the pool-wide half-batch upward.
	return (emptyNodeCount + 1) / 2
}

func reclamationCostPerVCPU(candidate *Candidate) float64 {
	if candidate == nil || candidate.instanceType == nil {
		return 0
	}
	cpuCapacity := candidate.instanceType.Capacity[corev1.ResourceCPU]
	cpu := cpuCapacity.AsApproximateFloat64()
	if cpu <= 0 {
		return 0
	}
	offerings := candidate.instanceType.Offerings.Compatible(scheduling.NewLabelRequirements(candidate.Labels()))
	if len(offerings) == 0 {
		return 0
	}
	return offerings.Cheapest().Price / cpu
}

func sortReclamationCandidates(candidates []*Candidate) {
	sort.Slice(candidates, func(i, j int) bool {
		left, right := reclamationCostPerVCPU(candidates[i]), reclamationCostPerVCPU(candidates[j])
		if left != right {
			return left > right
		}
		return candidates[i].Name() < candidates[j].Name()
	})
}

// isReclamationCandidateAvailable filters candidates that have become unsafe to remove since candidate collection.
func (s *ScoreBasedConsolidation) isReclamationCandidateAvailable(ctx context.Context, candidate *Candidate) bool {
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
	if currentNodeClaim.Labels[v1.NodePoolLabelKey] != candidate.NodePool.Name {
		return false
	}
	empty, err := reclamationEmpty(ctx, s.kubeClient, candidate.Node)
	return err == nil && empty
}

func (s *ScoreBasedConsolidation) reclamationCandidateEligible(candidate *Candidate) bool {
	if candidate == nil || candidate.NodePool == nil || candidate.NodeClaim == nil || candidate.Node == nil {
		return false
	}
	if standby.IsNodeClaimActivating(candidate.NodeClaim) {
		return false
	}
	if !scoreBasedReclamationConfigured(candidate.NodePool) {
		return false
	}
	return scoreBasedReclamationDue(candidate.NodePool, s.clock.Now())
}
