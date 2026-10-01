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
	mu                sync.Mutex
	count             int
	sum               time.Duration
	max               time.Duration
	errors            int
	firstError        string
	noOpMoves         int
	noSavings         int
	nonPositiveScores int
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
	nonPositiveScoreMoveSetCount   int
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

func (s *moveSetSearchStats) recordNonPositiveScoreMoveSet() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nonPositiveScores++
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

func (s *moveSetSearchStats) nonPositiveScoreMoveSetCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.nonPositiveScores
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
	evaluatedMoveSetCount int,
	eligibleCandidateCount int,
	timedOut bool,
) {
	if selectedEvalIdx < 0 || selectedEvalIdx >= len(evals) {
		return
	}
	eval := evals[selectedEvalIdx]
	cmd := eval.Command
	sourceNodes := scoreBasedSelectedSourceNodeLogs(eval)
	log.FromContext(ctx).Info("score-based consolidation move selected",
		"move", map[string]any{
			"searchPath":           eval.searchPath,
			"simulatedDecision":    cmd.Decision(),
			"sourceNodeCount":      len(cmd.Candidates),
			"replacementNodeCount": len(cmd.Replacements),
			"sourceNodes":          sourceNodes,
			"score": map[string]any{
				"rankingScore":            eval.Score,
				"baseScore":               eval.BaseScore,
				"method":                  scoreBasedMoveScoreMethod(eval),
				"normalizedCost":          eval.Cost,
				"costContribution":        eval.Cost.Contribution,
				"utilisationContribution": eval.UtilisationContribution,
				"memoryBalance":           eval.MemoryBalance,
				"memoryBalanceCorrection": eval.MemoryBalance.Correction,
			},
			"cost": map[string]any{
				"sourceUSDPerHour":               eval.Cost.SourceUSDPerHour,
				"estimatedReplacementUSDPerHour": eval.Cost.ReplacementUSDPerHour,
				"estimatedSavingsUSDPerHour":     eval.Cost.SavingsUSDPerHour,
			},
		},
		"search", map[string]any{
			"eligibleCandidateCount": eligibleCandidateCount,
			"evaluatedMoveSetCount":  evaluatedMoveSetCount,
			"retainedMoveSetCount":   len(evals),
			"timedOut":               timedOut,
		},
		"selection", map[string]any{
			"rank":                             selectedEvalIdx + 1,
			"rejectedHigherRankedMoveSetCount": selectedEvalIdx,
			"reason":                           scoreBasedSelectionReason(selectedEvalIdx),
			"validationStatus":                 selectedValidationStatus(eval),
			"validationReason":                 eval.validationReason,
		},
		"alternatives", scoreBasedAlternativeLogs(evals, selectedEvalIdx),
	)
}

func scoreBasedSelectedSourceNodeLogs(eval *moveSetEvaluation) []map[string]any {
	sourceNodes := make([]map[string]any, 0, len(eval.Command.Candidates))
	for i, candidate := range eval.Command.Candidates {
		var sourcePrice any
		if i < len(eval.SourceNodeScores) {
			sourcePrice = eval.SourceNodeScores[i].PriceUSDPerHour
		}
		sourceNode := map[string]any{
			"nodeName":       candidate.Name(),
			"nodeClaimName":  candidate.NodeClaim.Name,
			"nodePoolName":   candidate.NodePool.Name,
			"instanceType":   candidate.Labels()[corev1.LabelInstanceTypeStable],
			"capacityType":   candidate.Labels()[v1.CapacityTypeLabelKey],
			"costUSDPerHour": sourcePrice,
		}
		nodeScores := scoreBasedSourceNodeScoreLog(eval, i)
		if i < len(eval.sourceNodeSingleMoveScores) {
			nodeScores["singleNodeMoveScore"] = eval.sourceNodeSingleMoveScores[i]
		}
		sourceNode["score"] = nodeScores
		sourceNodes = append(sourceNodes, sourceNode)
	}
	return sourceNodes
}

func scoreBasedMoveScoreMethod(eval *moveSetEvaluation) string {
	if eval.searchPath != scoreBasedSearchPathBatch {
		return "joint_move_set_score"
	}
	if eval.MemoryBalance.Enabled {
		return "additive_base_plus_joint_memory_balance"
	}
	return "additive_single_node_scores"
}

func scoreBasedSelectionReason(selectedEvalIdx int) string {
	if selectedEvalIdx > 0 {
		return "fallback_after_validation"
	}
	return "highest_ranked_validated_move"
}

func scoreBasedAlternativeLogs(evals []*moveSetEvaluation, selectedEvalIdx int) []map[string]any {
	var alternatives []map[string]any
	for i, alternative := range evals {
		if i == selectedEvalIdx || len(alternatives) >= 3 {
			continue
		}
		alternatives = append(alternatives, scoreBasedAlternativeLog(alternative, i+1))
	}
	return alternatives
}

func scoreBasedSourceNodeScoreLog(eval *moveSetEvaluation, sourceIndex int) map[string]any {
	if eval == nil || sourceIndex >= len(eval.SourceNodeScores) {
		return map[string]any{}
	}
	score := eval.SourceNodeScores[sourceIndex]
	return map[string]any{
		"allocatableCPU":          score.AllocatableCPU,
		"requestedCPU":            score.RequestedCPU,
		"baselineUSDPerCPUHour":   score.BaselineUSDPerCPUHour,
		"normalizedSourceCost":    score.NormalizedSourceCost,
		"costContribution":        score.CostContribution,
		"cpuSlackFraction":        score.CPUSlackFraction,
		"utilisationWeight":       score.UtilisationWeight,
		"utilisationContribution": score.UtilisationContribution,
	}
}

func scoreBasedAlternativeLog(eval *moveSetEvaluation, rank int) map[string]any {
	sourceNodes := make([]map[string]any, 0, len(eval.Command.Candidates))
	for _, candidate := range eval.Command.Candidates {
		sourceNodes = append(sourceNodes, map[string]any{"nodeName": candidate.Name()})
	}
	return map[string]any{
		"rank":                       rank,
		"searchPath":                 eval.searchPath,
		"simulatedDecision":          eval.Command.Decision(),
		"sourceNodes":                sourceNodes,
		"estimatedSavingsUSDPerHour": eval.Cost.SavingsUSDPerHour,
		"score": map[string]any{
			"rankingScore":            eval.Score,
			"costContribution":        eval.Cost.Contribution,
			"utilisationContribution": eval.UtilisationContribution,
			"memoryBalanceCorrection": eval.MemoryBalance.Correction,
		},
		"memoryBalance": map[string]any{
			"status": eval.MemoryBalance.Status,
			"reason": eval.MemoryBalance.Reason,
		},
		"validation": map[string]any{
			"status": selectedValidationStatus(eval),
			"reason": eval.validationReason,
		},
	}
}

func selectedValidationStatus(eval *moveSetEvaluation) string {
	if eval == nil || eval.validationStatus == "" {
		return "not_attempted"
	}
	return eval.validationStatus
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
		"nonPositiveScoreMoveSetCount", details.nonPositiveScoreMoveSetCount,
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
