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

	"sigs.k8s.io/controller-runtime/pkg/log"
)

var scoreBasedValidEvaluationsTarget = 10

var scoreBasedSearchParallelism = runtime.GOMAXPROCS(0)

type move struct {
	Command         Command
	SearchScore     moveScore
	ValidationScore moveScore
	Validation      validationStatus
	FailureReason   string
	Selected        bool
}

type validationStatus int

const (
	validationPassed validationStatus = iota
	// validationFailed means validation errored, so there is no rescored score.
	validationFailed
	validationNoSavings
	validationNonPositiveScore
)

func (m *move) validationRejected() bool {
	return m.Validation != validationPassed
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

// searchMoves simulates every move and keeps the highest-scoring ones.
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
		switch {
		case err != nil:
			stats.Errors++
			countMoveEvaluation(moveEvaluationError)
			log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
			return nil, false, nil
		case s == nil:
			stats.NoOp++
			countMoveEvaluation(moveEvaluationNoOp)
			return nil, false, nil
		}
		result := s.Score.evaluationResult()
		countMoveEvaluation(result)
		switch result {
		case moveEvaluationNoSavings:
			stats.NoSavings++
			return nil, false, nil
		case moveEvaluationNonPositiveScore:
			stats.NonPositiveScore++
			return nil, false, nil
		}
		return &move{Command: s.Command, SearchScore: s.Score}, true, nil
	})
	moves := topMovesByScore(valid, scoreBasedValidEvaluationsTarget)
	return moves, stats
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

func topMovesByScore(valid <-chan rill.Try[*move], limit int) []*move {
	moves := []*move{}
	for a := range valid {
		moves = append(moves, a.Value)
	}
	sort.SliceStable(moves, func(i, j int) bool { return moves[i].SearchScore.Total > moves[j].SearchScore.Total })
	if len(moves) > limit {
		moves = moves[:limit]
	}
	return moves
}

func (s searchStats) canMarkConsolidated(eligible int, blocked bool) bool {
	return s.Evaluated == eligible && s.Errors == 0 && !blocked
}
