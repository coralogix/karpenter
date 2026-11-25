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
	"math/rand"
	"runtime"
	"slices"
	"sort"
	"sync"
	"time"

	"k8s.io/utils/clock"

	"github.com/destel/rill"
	"github.com/samber/lo"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

var scoreBasedSearchParallelism = runtime.GOMAXPROCS(0)

type move struct {
	Command         Command
	SearchScore     moveScore
	ValidationScore moveScore
	Validation      validationStatus
	FailureReason   string
}

type validationStatus int

const (
	// validationNotRun is for moves outside the kept moves, which only batch moves use.
	validationNotRun validationStatus = iota
	validationPassed
	// validationFailed means validation errored, so there is no rescored score.
	validationFailed
	validationNoSavings
	validationNonPositiveScore
)

func (m *move) validationRejected() bool {
	return m.Validation != validationNotRun && m.Validation != validationPassed
}

func (m *move) validationText() string {
	if m.validationRejected() {
		return "failed: " + m.validationFailureReason()
	}
	return "passed"
}

// failedValidation is a rejection by validation itself, not an error while simulating during it.
func (m *move) failedValidation() bool {
	return m.validationRejected() && m.FailureReason != validationErrorReason
}

func (m *move) validationFailureReason() string {
	switch m.Validation {
	case validationNoSavings:
		return moveEvaluationNoSavings
	case validationNonPositiveScore:
		return moveEvaluationNonPositiveScore
	}
	return m.FailureReason
}

type consolidationComputer func(ctx context.Context, candidates ...*Candidate) (Command, error)

// moveEvaluator scores moves with the inputs taken from the same snapshot as the computer simulating them.
type moveEvaluator struct {
	compute consolidationComputer
	inputs  scoreInputs
}

type searchStats struct {
	Evaluated        int
	Errors           int
	NoOp             int
	NoSavings        int
	NonPositiveScore int
	TimedOut         bool
}

// searchMoves simulates every move and returns the feasible ones, highest search score first.
func searchMoves(ctx context.Context, sources [][]*Candidate, deadline time.Time, evaluator moveEvaluator, random *rand.Rand, clk clock.Clock) ([]*move, searchStats) {
	sources = slices.Clone(sources)
	random.Shuffle(len(sources), func(i, j int) { sources[i], sources[j] = sources[j], sources[i] })
	var stats searchStats
	var mu sync.Mutex
	pending := rill.Generate(func(send func([]*Candidate), _ func(error)) {
		for _, nodes := range sources {
			if ctx.Err() != nil {
				return
			}
			if clk.Now().After(deadline) {
				mu.Lock()
				stats.TimedOut = true
				mu.Unlock()
				return
			}
			send(nodes)
		}
	})
	valid := rill.FilterMap(pending, scoreBasedSearchParallelism, func(nodes []*Candidate) (*move, bool, error) {
		s, err := evaluator.evaluate(ctx, nodes)
		mu.Lock()
		defer mu.Unlock()
		stats.Evaluated++
		result := moveEvaluationResult(s, err)
		countMoveEvaluation(result)
		switch result {
		case moveEvaluationError:
			stats.Errors++
			log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
		case moveEvaluationNoOp:
			stats.NoOp++
		case moveEvaluationNoSavings:
			stats.NoSavings++
		case moveEvaluationNonPositiveScore:
			stats.NonPositiveScore++
		default:
			return &move{Command: s.Command, SearchScore: s.Score}, true, nil
		}
		return nil, false, nil
	})
	return movesByScore(valid), stats
}

// scoredCommand is scored on whichever snapshot the evaluator simulates on, so each caller names the score by phase.
type scoredCommand struct {
	Command Command
	Score   moveScore
}

// evaluate is the per-move step shared by the search and the rescore: compute the command and score it.
// It returns nil when the move is a no-op or needs more than one new node.
func (e moveEvaluator) evaluate(ctx context.Context, nodes []*Candidate) (*scoredCommand, error) {
	cmd, err := e.compute(ctx, nodes...)
	if err != nil {
		return nil, err
	}
	if cmd.Decision() == NoOpDecision || len(cmd.Replacements) > 1 {
		return nil, nil
	}
	return &scoredCommand{Command: cmd, Score: scoreMove(cmd, e.inputs)}, nil
}

func moveEvaluationResult(s *scoredCommand, err error) string {
	switch {
	case err != nil:
		return moveEvaluationError
	case s == nil:
		return moveEvaluationNoOp
	}
	return s.Score.evaluationResult()
}

// evaluationResult is the rule for keeping a move, shared by the search and the rescore: a move is feasible only if it
// saves money and scores above zero.
func (s moveScore) evaluationResult() string {
	switch {
	case s.Savings <= 0:
		return moveEvaluationNoSavings
	case s.Total <= 0:
		return moveEvaluationNonPositiveScore
	}
	return moveEvaluationFeasible
}

func movesByScore(valid <-chan rill.Try[*move]) []*move {
	moves := []*move{}
	for a := range valid {
		moves = append(moves, a.Value)
	}
	sort.SliceStable(moves, func(i, j int) bool { return moves[i].SearchScore.Total > moves[j].SearchScore.Total })
	return moves
}

func batchableMoves(feasible []*move) []*move {
	return lo.Filter(feasible, func(e *move, _ int) bool { return len(e.Command.Replacements) == 0 })
}

func (s searchStats) canMarkConsolidated(eligible int, blocked bool) bool {
	return s.Evaluated == eligible && s.Errors == 0 && !blocked
}
