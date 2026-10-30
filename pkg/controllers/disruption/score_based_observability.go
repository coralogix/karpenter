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

	"github.com/samber/lo"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

func logScoreDecision(ctx context.Context, moves []*move, stats searchStats, candidates, eligible int, budgetBlockedPools, pacingBlockedPools []string, duration time.Duration) {
	fields := []interface{}{
		"eligibleCandidates", eligible, "evaluatedMoves", stats.Evaluated, "keptMoves", len(moves),
		"simulationErrors", stats.Errors, "noOp", stats.NoOp, "noSavings", stats.NoSavings,
		"deadlineHit", stats.TimedOut, "duration", duration,
		"budgetBlockedNodePools", budgetBlockedPools, "pacingBlockedNodePools", pacingBlockedPools,
	}
	failedValidation := 0
	for _, m := range moves {
		if m.validationFailedOrNoSavings() {
			failedValidation++
		}
	}
	for i, m := range moves {
		if !m.Selected {
			continue
		}
		alternatives := []map[string]interface{}{}
		for j, other := range moves {
			if j != i && len(alternatives) < 3 {
				alternatives = append(alternatives, moveLog(other))
			}
		}
		fields = append(fields, "move", moveLog(m), "rank", i+1, "searchScore", m.SearchScore.Savings, "rescoredScore", m.ValidationScore.Savings, "failedValidation", failedValidation, "alternatives", alternatives, "validationAttempts", len(moves))
		log.FromContext(ctx).Info("score-based consolidation move selected", fields...)
		return
	}
	fields = append(fields, "reason", noMoveReason(len(moves), stats, candidates, eligible), "validationAttempts", len(moves))
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
	}
	return "no_saving_move"
}

func moveLog(m *move) map[string]interface{} {
	cmd := m.Command
	rescored := m.Validation == validationPassed || m.Validation == validationNoSavings
	score := lo.Ternary(rescored, m.ValidationScore, m.SearchScore)
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
	entry := map[string]interface{}{"kind": "single", "sources": sources, "newNode": newNode, "reactivatedStandbyNodes": destinations, "savings": score.Savings, "validation": m.validationText()}
	if rescored && m.SearchScore.Savings != m.ValidationScore.Savings {
		entry["searchScore"] = m.SearchScore.Savings
	}
	return entry
}
