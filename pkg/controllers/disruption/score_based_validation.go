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

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

var errWholeCommandPacing = errors.New("whole-command pacing")

// rescoreKeptMoves repeats the search on a fresh snapshot of the kept moves' sources and returns the move with the
// highest positive rescored score, exactly as re-simulated.
func (s *ScoreBasedConsolidation) rescoreKeptMoves(ctx context.Context, moves []*move) (Command, error) {
	sources := sets.New[string]()
	for _, m := range moves {
		for _, c := range m.Command.Candidates {
			sources.Insert(c.Name())
		}
	}
	snapshot, err := s.prepare(ctx, sources)
	if err != nil {
		return Command{}, err
	}
	compute, err := s.newComputer(ctx, snapshot.candidates)
	if err != nil {
		return Command{}, err
	}
	checks := rescoreChecks{
		candidates:                 lo.KeyBy(snapshot.candidates, func(c *Candidate) string { return c.Name() }),
		disruptionBudgetByNodePool: snapshot.disruptionBudgetByNodePool,
		cluster:                    s.cluster,
		pacing:                     s.disruptionPacing,
	}
	return selectBestRescoredCommand(ctx, checks.wrap(compute), s.recorder, moves), nil
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
			return Command{}, err
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
			return nil, NewChurnValidationError(fmt.Errorf("candidate %s is no longer valid", source.Name()))
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

func selectBestRescoredCommand(ctx context.Context, compute consolidationComputer, recorder events.Recorder, moves []*move) Command {
	for _, m := range moves {
		rescore(ctx, compute, m)
	}
	passed := lo.Filter(moves, func(e *move, _ int) bool { return e.Validation == validationPassed })
	if len(passed) == 0 {
		if moves[0].Validation == validationFailed {
			moves[0].Command.EmitRejectedEvents(recorder, moves[0].FailureReason)
		}
		return Command{}
	}
	best := lo.MaxBy(passed, func(a, b *move) bool { return a.ValidationScore.Savings > b.ValidationScore.Savings })
	best.Selected = true
	return best.Command
}

func rescore(ctx context.Context, compute consolidationComputer, m *move) {
	fresh, err := evaluateMove(ctx, compute, m.Command.Candidates)
	if err == nil && fresh == nil {
		err = NewSchedulingValidationError(errors.New("move no longer consolidates"))
	}
	if err != nil {
		if !IsValidationError(err) {
			log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
			m.Validation, m.FailureReason = validationFailed, "error"
			return
		}
		FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
		m.Validation, m.FailureReason = validationFailed, scoreBasedValidationFailureReason(err)
		return
	}
	m.Command, m.ValidationScore = fresh.Command, fresh.Score
	m.Validation = lo.Ternary(fresh.Score.Savings > 0, validationPassed, validationNoSavings)
}

func scoreBasedValidationFailureReason(err error) string {
	var ve *ValidationError
	if errors.As(err, &ve) && errors.Is(ve.error, errWholeCommandPacing) {
		return "pacing"
	}
	return getValidationFailureReason(err)
}
