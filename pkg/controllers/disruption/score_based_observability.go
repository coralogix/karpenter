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
	"sync"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

var (
	ScoreBasedMoveSetEvaluationErrorsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "score_based_move_set_evaluation_errors_total",
			Help:      "Number of score-based consolidation move set evaluations that failed.",
		},
		[]string{},
	)
	ScoreBasedReclamationNodeRemovalsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "score_based_reclamation_node_removals_total",
			Help:      "Number of NodeClaim deletion requests successfully processed by score-based reclamation. Labeled by nodepool.",
		},
		[]string{metrics.NodePoolLabel},
	)
)

type moveSetSearchStats struct {
	mu         sync.Mutex
	count      int
	sum        time.Duration
	max        time.Duration
	errors     int
	firstError string
	noOpMoves  int
	noSavings  int
}

type scoreBasedNoMoveReason string

const (
	scoreBasedNoMoveAlreadyConsolidated      scoreBasedNoMoveReason = "cluster_already_marked_consolidated"
	scoreBasedNoMoveNoEligibleCandidates     scoreBasedNoMoveReason = "no_eligible_candidates"
	scoreBasedNoMoveAllCandidatesFiltered    scoreBasedNoMoveReason = "all_candidates_filtered"
	scoreBasedNoMoveBudgetOrPaceFiltered     scoreBasedNoMoveReason = "budget_or_pace_filtered"
	scoreBasedNoMoveSearchTimedOut           scoreBasedNoMoveReason = "search_timed_out"
	scoreBasedNoMoveNoPositiveFeasibleMove   scoreBasedNoMoveReason = "no_positive_or_feasible_move_after_completed_search"
	scoreBasedNoMoveAllValidationFailed      scoreBasedNoMoveReason = "all_top_moves_failed_validation"
	scoreBasedNoMoveValidationReturnedNoMove scoreBasedNoMoveReason = "validation_returned_no_command"
)

type scoreBasedNoMoveDetails struct {
	candidateCount                 int
	compactionCandidateCount       int
	budgetBlockedCandidateCount    int
	paceBlockedCandidateCount      int
	validCandidateCount            int
	budgetBlockedNodePools         map[string]struct{}
	paceBlockedNodePools           map[string]struct{}
	moveSetsEvaluated              int
	validMoveSetCount              int
	noOpMoveSetCount               int
	nonPositiveSavingsMoveSetCount int
	searchTimedOut                 bool
	moveSetEvaluationErrorCount    int
	firstMoveSetEvaluationError    string
	validationAttemptCount         int
}

func (s *moveSetSearchStats) record(d time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	s.sum += d
	if d > s.max {
		s.max = d
	}
	if err != nil {
		s.errors++
		if s.firstError == "" {
			s.firstError = err.Error()
		}
		ScoreBasedMoveSetEvaluationErrorsTotal.Inc(nil)
	}
}

func (s *moveSetSearchStats) avg() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count == 0 {
		return 0
	}
	return s.sum / time.Duration(s.count)
}

func (s *moveSetSearchStats) maxDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max
}

func (s *moveSetSearchStats) errorCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errors
}

func (s *moveSetSearchStats) firstErrorMessage() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstError
}

func (s *moveSetSearchStats) recordNoOpMoveSet() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.noOpMoves++
}

func (s *moveSetSearchStats) recordNonPositiveSavingsMoveSet() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.noSavings++
}

func (s *moveSetSearchStats) noOpMoveSetCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.noOpMoves
}

func (s *moveSetSearchStats) nonPositiveSavingsMoveSetCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.noSavings
}

func logMoveSetSearchComplete(
	ctx context.Context,
	moveSets int,
	evaluated int,
	valid int,
	timedOut bool,
	searchDuration time.Duration,
	stats *moveSetSearchStats,
) {
	log.FromContext(ctx).V(1).Info("score-based consolidation move set search complete",
		"moveSets", moveSets,
		"evaluated", evaluated,
		"valid", valid,
		"timedOut", timedOut,
		"searchDuration", searchDuration,
		"avgMoveSetEvalDuration", stats.avg(),
		"maxMoveSetEvalDuration", stats.maxDuration(),
		"computeErrors", stats.errorCount(),
		"firstComputeError", stats.firstErrorMessage(),
	)
}

func logScoreBasedCompactionMoveSelected(
	ctx context.Context,
	evals []*moveSetEvaluation,
	selectedEvalIdx int,
	candidatesEvaluated int,
	compactionCandidateCount int,
) {
	if selectedEvalIdx < 0 || selectedEvalIdx >= len(evals) {
		return
	}
	eval := evals[selectedEvalIdx]
	cmd := eval.Command
	candidate := cmd.Candidates[0]

	selectionReason := "highest priority score among move sets with positive simulated savings"
	if selectedEvalIdx > 0 {
		selectionReason = fmt.Sprintf(
			"validated fallback after %d higher-priority move set(s) failed post-simulation validation",
			selectedEvalIdx,
		)
	}

	log.FromContext(ctx).Info("score-based consolidation move selected",
		"selectionReason", selectionReason,
		"priorityScore", eval.Score,
		"estimatedSavingsUSD", cmd.EstimatedSavings(),
		"simulatedDecision", cmd.Decision(),
		"validMoveSetCount", len(evals),
		"priorityRank", selectedEvalIdx+1,
		"moveSetsEvaluated", candidatesEvaluated,
		"compactionCandidateCount", compactionCandidateCount,
		"node", candidate.Name(),
		"nodePool", candidate.NodePool.Name,
		"instanceType", candidate.Labels()[corev1.LabelInstanceTypeStable],
		"capacityType", candidate.Labels()[v1.CapacityTypeLabelKey],
		"replacementNodeCount", len(cmd.Replacements),
	)
}

func logScoreBasedCompactionNoMove(ctx context.Context, reason scoreBasedNoMoveReason, details scoreBasedNoMoveDetails) {
	fields := []any{
		"reason", reason,
		"candidateCount", details.candidateCount,
	}
	if details.compactionCandidateCount >= 0 {
		fields = append(fields, "compactionCandidateCount", details.compactionCandidateCount)
	}
	fields = append(fields,
		"budgetBlockedCandidateCount", details.budgetBlockedCandidateCount,
		"budgetBlockedNodePools", sortedScoreBasedNodePoolNames(details.budgetBlockedNodePools),
		"paceBlockedCandidateCount", details.paceBlockedCandidateCount,
		"paceBlockedNodePools", sortedScoreBasedNodePoolNames(details.paceBlockedNodePools),
		"validCandidateCount", details.validCandidateCount,
		"moveSetsEvaluated", details.moveSetsEvaluated,
		"validMoveSetCount", details.validMoveSetCount,
		"noOpMoveSetCount", details.noOpMoveSetCount,
		"nonPositiveSavingsMoveSetCount", details.nonPositiveSavingsMoveSetCount,
		"searchTimedOut", details.searchTimedOut,
		"moveSetEvaluationErrorCount", details.moveSetEvaluationErrorCount,
		"firstMoveSetEvaluationError", details.firstMoveSetEvaluationError,
		"validationAttemptCount", details.validationAttemptCount,
	)
	log.FromContext(ctx).Info("score-based consolidation made no move", fields...)
}

func logScoreBasedNoEligibleCandidates(ctx context.Context, disruption Method) {
	if _, ok := disruption.(*ScoreBasedConsolidation); !ok {
		return
	}
	logScoreBasedCompactionNoMove(ctx, scoreBasedNoMoveNoEligibleCandidates, scoreBasedNoMoveDetails{
		compactionCandidateCount: 0,
	})
}

func sortedScoreBasedNodePoolNames(nodePools map[string]struct{}) []string {
	names := make([]string, 0, len(nodePools))
	for name := range nodePools {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
