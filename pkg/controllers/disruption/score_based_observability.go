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
	"maps"
	"slices"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	moveEvaluationResultLabel      = "result"
	moveEvaluationFeasible         = "feasible"
	moveEvaluationNoOp             = "no_op"
	moveEvaluationNoSavings        = "no_savings"
	moveEvaluationNonPositiveScore = "non_positive_score"
	moveEvaluationError            = "error"

	moveKindLabel = "kind"
)

type moveKind string

const (
	moveKindSingle moveKind = "single"
	moveKindBatch  moveKind = "batch"
	moveKindPair   moveKind = "pair"
)

var (
	ScoreBasedMoveEvaluationsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "score_based_move_evaluations_total",
			Help:      "Number of moves simulated by score-based consolidation, by the search and by batch growth. Labeled by result.",
		},
		[]string{moveEvaluationResultLabel},
	)
	ScoreBasedMovesSelectedTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "score_based_moves_selected_total",
			Help:      "Number of moves chosen by score-based consolidation after validation. Labeled by move kind.",
		},
		[]string{moveKindLabel},
	)
)

func init() {
	for _, result := range []string{moveEvaluationFeasible, moveEvaluationNoOp, moveEvaluationNoSavings, moveEvaluationNonPositiveScore, moveEvaluationError} {
		ScoreBasedMoveEvaluationsTotal.Add(0, map[string]string{moveEvaluationResultLabel: result})
	}
	for _, kind := range []moveKind{moveKindSingle, moveKindBatch, moveKindPair} {
		ScoreBasedMovesSelectedTotal.Add(0, map[string]string{moveKindLabel: string(kind)})
	}
}

func countMoveEvaluation(result string) {
	ScoreBasedMoveEvaluationsTotal.Inc(map[string]string{moveEvaluationResultLabel: result})
}

func logScoreDecision(ctx context.Context, choice moveChoice, stats searchStats, candidates, eligible int, budgetBlockedPools, pacingBlockedPools []string, duration time.Duration) {
	fields := []interface{}{
		"eligibleCandidates", eligible, "evaluatedMoves", stats.Evaluated, "keptMoves", len(choice.Kept),
		"simulationErrors", stats.Errors, "noOp", stats.NoOp, "noSavings", stats.NoSavings, "nonPositiveScore", stats.NonPositiveScore,
		"pairsEvaluated", stats.PairsEvaluated, "deadlineHit", stats.TimedOut, "duration", duration,
		"budgetBlockedNodePools", budgetBlockedPools, "pacingBlockedNodePools", pacingBlockedPools,
	}
	failedValidation := 0
	for _, m := range choice.Kept {
		if m.validationRejected() {
			failedValidation++
		}
	}
	if choice.Chosen.Move != nil {
		alternatives := []map[string]interface{}{}
		for _, other := range choice.Kept {
			// The kept move a chosen batch move was built from is an alternative.
			if (other != choice.Chosen.Move || choice.Chosen.Kind == moveKindBatch) && len(alternatives) < 3 {
				alternatives = append(alternatives, moveLog(other))
			}
		}
		fields = append(fields, "move", chosenMoveLog(choice.Chosen), "rank", slices.Index(choice.Kept, choice.Chosen.Move)+1, "searchScore", choice.Chosen.Move.SearchScore.Total, "rescoredScore", choice.Chosen.Score.Total, "failedValidation", failedValidation, "alternatives", alternatives, "validationAttempts", len(choice.Kept))
		if choice.Chosen.Kind == moveKindBatch {
			fields = append(fields, "batchSimulations", choice.BatchMoves.JointSimulations, "batchesGrown", len(choice.BatchMoves.Moves))
		}
		log.FromContext(ctx).Info("score-based consolidation move selected", fields...)
		return
	}
	fields = append(fields, "reason", noMoveReason(len(choice.Kept), stats, candidates, eligible), "validationAttempts", len(choice.Kept))
	log.FromContext(ctx).Info("score-based consolidation found no move", fields...)
}

// noMoveReason assumes kept moves were all validated, since a pass that errors before validating them logs nothing.
func noMoveReason(kept int, stats searchStats, candidates, eligible int) string {
	switch {
	case stats.Errors > 0:
		return "simulation_error"
	case kept > 0:
		return "all_rejected_by_validation"
	case stats.TimedOut:
		return "timed_out"
	case candidates == 0:
		return "no_candidates"
	case eligible == 0:
		return "blocked_by_budget_or_pacing"
	case stats.NonPositiveScore > 0:
		return "no_positive_score_move"
	}
	return "no_saving_move"
}

func chosenMoveLog(chosen validatedMove) map[string]interface{} {
	if chosen.Kind != moveKindBatch {
		return moveLog(chosen.Move)
	}
	return commandLog(chosen.Kind, chosen.Command, chosen.Score, "passed")
}

func moveLog(m *move) map[string]interface{} {
	rescored := m.Validation != validationFailed
	entry := commandLog(m.kind(), m.Command, lo.Ternary(rescored, m.ValidationScore, m.SearchScore), m.validationText())
	if rescored && m.SearchScore.Total != m.ValidationScore.Total {
		entry["searchScore"] = m.SearchScore.Total
	}
	return entry
}

func commandLog(kind moveKind, cmd Command, score moveScore, validation string) map[string]interface{} {
	sources := []map[string]interface{}{}
	for _, c := range cmd.Candidates {
		sources = append(sources, map[string]interface{}{"node": c.Name(), "nodePool": c.NodePool.Name, "instanceType": c.Labels()[corev1.LabelInstanceTypeStable], "capacityType": c.Labels()[v1.CapacityTypeLabelKey], "price": c.Price})
	}
	destinations := []map[string]interface{}{}
	for _, name := range slices.Sorted(maps.Keys(score.StandbyPrices)) {
		destinations = append(destinations, map[string]interface{}{"node": name, "price": score.StandbyPrices[name]})
	}
	var newNode interface{}
	for _, n := range cmd.Results.NewNodeClaims {
		if len(n.InstanceTypeOptions) > 0 {
			newNode = map[string]interface{}{"instanceType": n.InstanceTypeOptions[0].Name, "price": score.NewNodePrice}
		}
	}
	return map[string]interface{}{"kind": kind, "sources": sources, "newNode": newNode, "reactivatedStandbyNodes": destinations, "score": score, "validation": validation}
}
