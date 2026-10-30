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
	"time"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

func logScoreDecision(ctx context.Context, evals []*moveSetEvaluation, stats searchStats, eligible int, blocked []string, duration time.Duration, reason string) {
	fields := []interface{}{"eligibleCandidates", eligible, "evaluatedMoves", stats.Evaluated, "keptMoves", len(evals), "simulationErrors", stats.Errors, "noOp", stats.NoOp, "noSavings", stats.NoSavings, "deadlineHit", stats.TimedOut, "duration", duration, "blockedNodePools", blocked}
	attempts := 0
	for i, eval := range evals {
		if eval.Validation != "not_attempted" {
			attempts++
		}
		if eval.Validation != "passed" {
			continue
		}
		alternatives := []map[string]interface{}{}
		for j, other := range evals {
			if j != i && len(alternatives) < 3 {
				alternatives = append(alternatives, moveLog(other))
			}
		}
		fields = append(fields, "move", moveLog(eval), "rank", i+1, "failedHigherRanked", i, "alternatives", alternatives, "validationAttempts", attempts)
		log.FromContext(ctx).Info("score-based consolidation move selected", fields...)
		return
	}
	if stats.Errors > 0 {
		reason = "simulation_error"
	}
	fields = append(fields, "reason", reason, "validationAttempts", attempts)
	log.FromContext(ctx).Info("score-based consolidation found no move", fields...)
}

func moveLog(eval *moveSetEvaluation) map[string]interface{} {
	cmd := eval.Command
	sources := []map[string]interface{}{}
	for _, c := range cmd.Candidates {
		sources = append(sources, map[string]interface{}{"node": c.Name(), "nodePool": c.NodePool.Name, "instanceType": c.Labels()[corev1.LabelInstanceTypeStable], "capacityType": c.Labels()[v1.CapacityTypeLabelKey], "price": c.Price})
	}
	destinations := []map[string]interface{}{}
	for _, name := range standbyDestinations(cmd.Results) {
		destinations = append(destinations, map[string]interface{}{"node": name, "price": eval.StandbyPrices[name]})
	}
	var newNode interface{}
	for _, n := range cmd.Results.NewNodeClaims {
		if len(n.InstanceTypeOptions) > 0 {
			// EstimatedSavings is the sources' price minus the new node's price.
			newNode = map[string]interface{}{"instanceType": n.InstanceTypeOptions[0].Name, "price": cmd.SourceCost() - cmd.EstimatedSavings()}
		}
	}
	return map[string]interface{}{"kind": "single", "sources": sources, "newNode": newNode, "reactivatedStandbyNodes": destinations, "savings": eval.Score, "validation": eval.Validation}
}
