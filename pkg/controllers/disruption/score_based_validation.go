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
	"errors"
	"fmt"
	"slices"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/cxtracing"
	"sigs.k8s.io/karpenter/pkg/events"
)

var errWholeCommandPacing = errors.New("whole-command pacing")

const validationErrorReason = "error"

var scoreBasedKeptMovesLimit = 10

// validate rescores the kept moves on a fresh snapshot and returns the evaluator batch growth simulates with.
func (s *ScoreBasedConsolidation) validate(ctx context.Context, feasible, batchable []*move) (moveEvaluator, []*move, error) {
	ctx, endSpan := cxtracing.Start(ctx, scoreBasedSpanName+".validate")
	defer endSpan()
	evaluator, err := s.validationEvaluator(ctx, feasible, batchable)
	if err != nil {
		return moveEvaluator{}, nil, err
	}
	return evaluator, validateKeptMoves(ctx, evaluator, s.recorder, feasible), nil
}

func (s *ScoreBasedConsolidation) validationEvaluator(ctx context.Context, feasible, batchable []*move) (moveEvaluator, error) {
	sources := sets.New[string]()
	for _, m := range slices.Concat(keptMoves(feasible), batchable) {
		for _, c := range m.Command.Candidates {
			sources.Insert(c.Name())
		}
	}
	snapshot, err := s.prepare(ctx, sources)
	if err != nil {
		return moveEvaluator{}, err
	}
	compute, err := s.newComputer(ctx, snapshot.candidates)
	if err != nil {
		return moveEvaluator{}, err
	}
	checks := rescoreChecks{
		candidates:                 lo.KeyBy(snapshot.candidates, func(c *Candidate) string { return c.Name() }),
		disruptionBudgetByNodePool: snapshot.disruptionBudgetByNodePool,
		cluster:                    s.cluster,
		pacing:                     s.disruptionPacing,
	}
	return moveEvaluator{compute: checks.wrap(compute), inputs: snapshot.scoreInputs}, nil
}

// notSimulatedError marks a move the checks rejected before compute, so batch growth does not count it as a
// simulation.
type notSimulatedError struct {
	error
}

func (e notSimulatedError) Unwrap() error {
	return e.error
}

type rescoreChecks struct {
	candidates                 map[string]*Candidate
	disruptionBudgetByNodePool map[string]int
	cluster                    *state.Cluster
	pacing                     *DisruptionPacing
}

func (r rescoreChecks) wrap(compute consolidationComputer) consolidationComputer {
	return func(ctx context.Context, sources ...*Candidate) (Command, error) {
		fresh, err := r.beforeCompute(sources)
		if err != nil {
			return Command{}, notSimulatedError{error: err}
		}
		cmd, err := compute(ctx, fresh...)
		if err != nil {
			return Command{}, err
		}
		// Recheck nomination after simulating, see https://github.com/kubernetes-sigs/karpenter/issues/1167.
		if err := r.checkNotNominated(cmd.Candidates); err != nil {
			return Command{}, err
		}
		return cmd, nil
	}
}

// beforeCompute returns the fresh candidates for a move's sources, or the validation error for the first check the
// move fails: churn, budget or nomination, then whole-command pacing. None of them needs the simulation, so a move
// they reject costs no compute.
func (r rescoreChecks) beforeCompute(sources []*Candidate) ([]*Candidate, error) {
	fresh, err := r.freshSources(sources)
	if err != nil {
		return nil, err
	}
	if !r.pacing.CommandAllowed(Command{Candidates: fresh}) {
		return nil, NewValidationError(errWholeCommandPacing)
	}
	return fresh, nil
}

func (r rescoreChecks) freshSources(sources []*Candidate) ([]*Candidate, error) {
	fresh := make([]*Candidate, 0, len(sources))
	perPool := map[string]int{}
	for _, source := range sources {
		c, ok := r.candidates[source.Name()]
		if !ok {
			return nil, NewChurnValidationError(fmt.Errorf("node %s is no longer a candidate", source.Name()))
		}
		perPool[c.NodePool.Name]++
		if perPool[c.NodePool.Name] > r.disruptionBudgetByNodePool[c.NodePool.Name] {
			return nil, NewBudgetValidationError(fmt.Errorf("the move can no longer run without violating budgets"))
		}
		fresh = append(fresh, c)
	}
	return fresh, r.checkNotNominated(fresh)
}

func (r rescoreChecks) checkNotNominated(candidates []*Candidate) error {
	for _, c := range candidates {
		if r.cluster.IsNodeNominated(c.ProviderID()) {
			return NewBudgetValidationError(fmt.Errorf("a candidate was nominated during validation"))
		}
	}
	return nil
}

func validateKeptMoves(ctx context.Context, evaluator moveEvaluator, recorder events.Recorder, feasible []*move) []*move {
	kept := keptMoves(feasible)
	for _, m := range kept {
		rescore(ctx, evaluator, m)
		if m.failedValidation() {
			FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
			m.Command.EmitRejectedEvents(recorder, m.validationFailureReason())
		}
	}
	return kept
}

// keptMoves are the feasible moves validated after the delay. The limit only bounds validation work.
func keptMoves(feasible []*move) []*move {
	return feasible[:min(len(feasible), scoreBasedKeptMovesLimit)]
}

func rescore(ctx context.Context, evaluator moveEvaluator, m *move) {
	fresh, err := evaluator.evaluate(ctx, m.Command.Candidates)
	if err == nil && fresh == nil {
		err = NewSchedulingValidationError(errors.New("move no longer consolidates"))
	}
	if err != nil {
		if !IsValidationError(err) {
			log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
			m.Validation, m.FailureReason = validationFailed, validationErrorReason
			return
		}
		m.Validation, m.FailureReason, m.FailureMessage = validationFailed, scoreBasedValidationFailureReason(err), err.Error()
		return
	}
	m.Command, m.ValidationScore = fresh.Command, fresh.Score
	switch fresh.Score.evaluationResult() {
	case moveEvaluationNoSavings:
		m.Validation = validationNoSavings
	case moveEvaluationNonPositiveScore:
		m.Validation = validationNonPositiveScore
	default:
		m.Validation = validationPassed
	}
}

func scoreBasedValidationFailureReason(err error) string {
	var ve *ValidationError
	if errors.As(err, &ve) && errors.Is(ve.error, errWholeCommandPacing) {
		return "pacing"
	}
	return getValidationFailureReason(err)
}
