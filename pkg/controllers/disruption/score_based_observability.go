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
	"sigs.k8s.io/controller-runtime/pkg/log"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

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

type moveSetSearchStatsSnapshot struct {
	averageDuration                time.Duration
	maxDuration                    time.Duration
	errorCount                     int
	firstError                     string
	noOpMoveSetCount               int
	nonPositiveSavingsMoveSetCount int
	nonPositiveScoreMoveSetCount   int
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

func (details *scoreBasedNoMoveDetails) recordSearch(result moveSetSearchResult) {
	details.moveSetsEvaluated = result.evaluated
	details.searchTimedOut = result.timedOut
	if result.stats == nil {
		return
	}
	stats := result.stats.snapshot()
	details.noOpMoveSetCount = stats.noOpMoveSetCount
	details.nonPositiveSavingsMoveSetCount = stats.nonPositiveSavingsMoveSetCount
	details.nonPositiveScoreMoveSetCount = stats.nonPositiveScoreMoveSetCount
	details.moveSetEvaluationErrorCount = stats.errorCount
	details.firstMoveSetEvaluationError = stats.firstError
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
	statsSnapshot := stats.snapshot()
	log.FromContext(ctx).V(1).Info("score-based consolidation move set search complete",
		"moveSets", moveSets,
		"evaluated", evaluated,
		"valid", valid,
		"timedOut", timedOut,
		"searchDuration", searchDuration,
		"avgMoveSetEvalDuration", statsSnapshot.averageDuration,
		"maxMoveSetEvalDuration", statsSnapshot.maxDuration,
		"computeErrors", statsSnapshot.errorCount,
		"firstComputeError", statsSnapshot.firstError,
	)
}

func (s *moveSetSearchStats) snapshot() moveSetSearchStatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	averageDuration := time.Duration(0)
	if s.count > 0 {
		averageDuration = s.sum / time.Duration(s.count)
	}
	return moveSetSearchStatsSnapshot{
		averageDuration:                averageDuration,
		maxDuration:                    s.max,
		errorCount:                     s.errors,
		firstError:                     s.firstError,
		noOpMoveSetCount:               s.noOpMoves,
		nonPositiveSavingsMoveSetCount: s.noSavings,
		nonPositiveScoreMoveSetCount:   s.nonPositiveScores,
	}
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
			"score":                newScoreBasedMoveScoreLog(eval.Score, true),
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

type scoreBasedMoveScoreLog struct {
	Total   float64                       `json:"total"`
	Factors scoreBasedMoveScoreFactorsLog `json:"factors"`
}

type scoreBasedMoveScoreFactorsLog struct {
	Cost                      scoreBasedFactorLog[scoreBasedCostLogDetails]          `json:"cost"`
	Utilisation               scoreBasedFactorLog[scoreBasedUtilisationLogDetails]   `json:"utilisation"`
	MemoryBalance             scoreBasedFactorLog[scoreBasedMemoryLogDetails]        `json:"memoryBalance"`
	InstanceTypeConcentration scoreBasedFactorLog[scoreBasedConcentrationLogDetails] `json:"instanceTypeConcentration"`
}

type scoreBasedFactorLog[Details any] struct {
	Contribution float64           `json:"contribution"`
	Status       scoreFactorStatus `json:"status"`
	Reason       string            `json:"reason,omitempty"`
	Details      *Details          `json:"details,omitempty"`
}

type scoreBasedCostLogDetails struct {
	Enabled               *bool                                `json:"enabled,omitempty"`
	Valid                 *bool                                `json:"valid,omitempty"`
	SourceUSDPerHour      float64                              `json:"sourceUSDPerHour"`
	SavingsUSDPerHour     float64                              `json:"savingsUSDPerHour"`
	ReplacementUSDPerHour float64                              `json:"replacementUSDPerHour"`
	SavingsFraction       *float64                             `json:"savingsFraction,omitempty"`
	NormalizedSourceCost  *float64                             `json:"normalizedSourceCost,omitempty"`
	Groups                map[string]scoreBasedPriceGroupScore `json:"groups,omitempty"`
	Sources               []scoreBasedCostSourceLog            `json:"sources,omitempty"`
}

type scoreBasedCostSourceLog struct {
	NodeName              string  `json:"nodeName"`
	PriceUSDPerHour       float64 `json:"priceUSDPerHour"`
	AllocatableCPU        float64 `json:"allocatableCPU"`
	BaselineUSDPerCPUHour float64 `json:"baselineUSDPerCPUHour"`
	NormalizedSourceCost  float64 `json:"normalizedSourceCost"`
	Contribution          float64 `json:"contribution"`
}

type scoreBasedUtilisationLogDetails struct {
	Sources []scoreBasedUtilisationSourceLog `json:"sources,omitempty"`
}

type scoreBasedUtilisationSourceLog struct {
	NodeName       string  `json:"nodeName"`
	AllocatableCPU float64 `json:"allocatableCPU"`
	RequestedCPU   float64 `json:"requestedCPU"`
	SlackFraction  float64 `json:"slackFraction"`
	Weight         float64 `json:"weight"`
	Contribution   float64 `json:"contribution"`
}

type scoreBasedMemoryLogDetails struct {
	Enabled         bool                                 `json:"enabled"`
	RemovedCPU      float64                              `json:"removedCPU"`
	SourceNodeCount int                                  `json:"sourceNodeCount"`
	Pools           map[string]scoreBasedMemoryPoolScore `json:"pools,omitempty"`
}

type scoreBasedConcentrationLogDetails struct {
	Enabled bool                                                    `json:"enabled"`
	Pools   map[string]scoreBasedInstanceTypeConcentrationPoolScore `json:"pools,omitempty"`
	Sources []scoreBasedInstanceTypeConcentrationSourceScore        `json:"sources,omitempty"`
}

type scoreBasedSourceNodeLog struct {
	NodeName        string   `json:"nodeName"`
	NodeClaimName   string   `json:"nodeClaimName"`
	NodePoolName    string   `json:"nodePoolName"`
	InstanceType    string   `json:"instanceType"`
	CapacityType    string   `json:"capacityType"`
	SingleMoveScore *float64 `json:"singleNodeMoveScore,omitempty"`
}

func newScoreBasedMoveScoreLog(score moveSetScore, includeDetails bool) scoreBasedMoveScoreLog {
	result := scoreBasedMoveScoreLog{Total: score.Total}
	factors := score.Factors
	result.Factors = scoreBasedMoveScoreFactorsLog{
		Cost: scoreBasedFactorLog[scoreBasedCostLogDetails]{
			Contribution: factors.Cost.Contribution,
			Status:       factors.Cost.Status,
			Reason:       factors.Cost.Reason,
		},
		Utilisation: scoreBasedFactorLog[scoreBasedUtilisationLogDetails]{
			Contribution: factors.Utilisation.Contribution,
			Status:       factors.Utilisation.Status,
			Reason:       factors.Utilisation.Reason,
		},
		MemoryBalance: scoreBasedFactorLog[scoreBasedMemoryLogDetails]{
			Contribution: factors.MemoryBalance.Contribution,
			Status:       factors.MemoryBalance.Status,
			Reason:       factors.MemoryBalance.Reason,
		},
		InstanceTypeConcentration: scoreBasedFactorLog[scoreBasedConcentrationLogDetails]{
			Contribution: factors.InstanceTypeConcentration.Contribution,
			Status:       factors.InstanceTypeConcentration.Status,
			Reason:       factors.InstanceTypeConcentration.Reason,
		},
	}
	if !includeDetails {
		result.Factors.Cost.Details = &scoreBasedCostLogDetails{
			SourceUSDPerHour:      factors.Cost.SourceUSDPerHour,
			ReplacementUSDPerHour: factors.Cost.ReplacementUSDPerHour,
			SavingsUSDPerHour:     factors.Cost.SavingsUSDPerHour,
		}
		return result
	}

	costSources := make([]scoreBasedCostSourceLog, 0, len(score.Sources))
	utilisationSources := make([]scoreBasedUtilisationSourceLog, 0, len(score.Sources))
	for _, source := range score.Sources {
		costSources = append(costSources, scoreBasedCostSourceLog{
			NodeName:              source.NodeName,
			PriceUSDPerHour:       source.PriceUSDPerHour,
			AllocatableCPU:        source.AllocatableCPU,
			BaselineUSDPerCPUHour: source.BaselineUSDPerCPUHour,
			NormalizedSourceCost:  source.NormalizedSourceCost,
			Contribution:          source.CostContribution,
		})
		utilisationSources = append(utilisationSources, scoreBasedUtilisationSourceLog{
			NodeName:       source.NodeName,
			AllocatableCPU: source.AllocatableCPU,
			RequestedCPU:   source.RequestedCPU,
			SlackFraction:  source.CPUSlackFraction,
			Weight:         source.UtilisationWeight,
			Contribution:   source.UtilisationContribution,
		})
	}
	enabled := factors.Cost.Enabled
	valid := factors.Cost.Valid
	savingsFraction := factors.Cost.SavingsFraction
	normalizedSourceCost := factors.Cost.NormalizedSourceCost
	result.Factors.Cost.Details = &scoreBasedCostLogDetails{
		Enabled:               &enabled,
		Valid:                 &valid,
		SourceUSDPerHour:      factors.Cost.SourceUSDPerHour,
		SavingsUSDPerHour:     factors.Cost.SavingsUSDPerHour,
		ReplacementUSDPerHour: factors.Cost.ReplacementUSDPerHour,
		SavingsFraction:       &savingsFraction,
		NormalizedSourceCost:  &normalizedSourceCost,
		Groups:                factors.Cost.Groups,
		Sources:               costSources,
	}
	result.Factors.Utilisation.Details = &scoreBasedUtilisationLogDetails{Sources: utilisationSources}
	result.Factors.MemoryBalance.Details = &scoreBasedMemoryLogDetails{
		Enabled:         factors.MemoryBalance.Enabled,
		RemovedCPU:      factors.MemoryBalance.RemovedCPU,
		SourceNodeCount: factors.MemoryBalance.SourceNodeCount,
		Pools:           factors.MemoryBalance.Pools,
	}
	result.Factors.InstanceTypeConcentration.Details = &scoreBasedConcentrationLogDetails{
		Enabled: factors.InstanceTypeConcentration.Enabled,
		Pools:   factors.InstanceTypeConcentration.Pools,
		Sources: factors.InstanceTypeConcentration.SourceNodes,
	}
	return result
}

func scoreBasedSelectedSourceNodeLogs(eval *moveSetEvaluation) []scoreBasedSourceNodeLog {
	sourceNodes := make([]scoreBasedSourceNodeLog, 0, len(eval.Score.Sources))
	for i, source := range eval.Score.Sources {
		node := scoreBasedSourceNodeLog{
			NodeName:      source.NodeName,
			NodeClaimName: source.NodeClaimName,
			NodePoolName:  source.NodePoolName,
			InstanceType:  source.InstanceType,
			CapacityType:  source.CapacityType,
		}
		if i < len(eval.sourceNodeSingleMoveScores) {
			singleMoveScore := eval.sourceNodeSingleMoveScores[i]
			node.SingleMoveScore = &singleMoveScore
		}
		sourceNodes = append(sourceNodes, node)
	}
	return sourceNodes
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

func scoreBasedAlternativeLog(eval *moveSetEvaluation, rank int) map[string]any {
	sourceNodes := make([]scoreBasedSourceNodeLog, 0, len(eval.Score.Sources))
	for _, source := range eval.Score.Sources {
		sourceNodes = append(sourceNodes, scoreBasedSourceNodeLog{
			NodeName:      source.NodeName,
			NodeClaimName: source.NodeClaimName,
			NodePoolName:  source.NodePoolName,
			InstanceType:  source.InstanceType,
			CapacityType:  source.CapacityType,
		})
	}
	return map[string]any{
		"rank":              rank,
		"searchPath":        eval.searchPath,
		"simulatedDecision": eval.Command.Decision(),
		"sourceNodes":       sourceNodes,
		"score":             newScoreBasedMoveScoreLog(eval.Score, false),
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
