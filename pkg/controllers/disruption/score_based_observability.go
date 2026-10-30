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
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/standby"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

type scoreMoveValidator struct {
	Validator
	pacing *DisruptionPacing
}

func (v scoreMoveValidator) Validate(ctx context.Context, cmd Command, delay time.Duration) (Command, error) {
	validated, err := v.Validator.Validate(ctx, cmd, delay)
	if err != nil {
		return Command{}, err
	}
	if !v.pacing.CommandAllowed(cmd) {
		return Command{}, NewValidationError(fmt.Errorf("whole-command pacing"))
	}
	return validated, nil
}

func (c Command) standbyCost() float64 {
	cost := 0.0
	for _, n := range c.Results.ExistingNodes {
		if len(n.Pods) > 0 && standby.HasStandbyTaint(n.Node) {
			cost += c.scoreBasedStandbyPrices[n.Name()]
		}
	}
	return cost
}

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
	sources := []map[string]interface{}{}
	for _, c := range eval.Command.Candidates {
		sources = append(sources, map[string]interface{}{"node": c.Name(), "nodePool": c.NodePool.Name, "instanceType": c.Labels()[corev1.LabelInstanceTypeStable], "capacityType": c.Labels()[v1.CapacityTypeLabelKey], "price": c.Price})
	}
	destinations := []map[string]interface{}{}
	for _, name := range standbyDestinations(eval.Command.Results) {
		destinations = append(destinations, map[string]interface{}{"node": name, "price": eval.Command.scoreBasedStandbyPrices[name]})
	}
	var newNode interface{}
	for _, n := range eval.Command.Results.NewNodeClaims {
		if len(n.InstanceTypeOptions) > 0 {
			price := 0.0
			offerings := n.InstanceTypeOptions[0].Offerings.Available().Compatible(n.Requirements)
			if len(offerings) > 0 {
				price = offerings.Cheapest().Price
			}
			newNode = map[string]interface{}{"instanceType": n.InstanceTypeOptions[0].Name, "price": price}
		}
	}
	return map[string]interface{}{"kind": "single", "sources": sources, "newNode": newNode, "reactivatedStandbyNodes": destinations, "savings": eval.Score, "validation": eval.Validation}
}

func validateStandbyDestinations(expected, actual pscheduling.Results) error {
	if !slices.Equal(standbyDestinations(expected), standbyDestinations(actual)) {
		return NewSchedulingValidationError(fmt.Errorf("standby destinations changed"))
	}
	return nil
}

func standbyDestinations(results pscheduling.Results) []string {
	names := []string{}
	for _, n := range results.ExistingNodes {
		if len(n.Pods) > 0 && standby.HasStandbyTaint(n.Node) {
			names = append(names, n.Name())
		}
	}
	sort.Strings(names)
	return names
}
