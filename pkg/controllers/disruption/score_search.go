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
	"math/rand"
	"runtime"
	"slices"
	"sort"
	"sync"
	"time"

	"k8s.io/utils/clock"

	"github.com/destel/rill"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/cxtracing"
)

var scoreBasedSearchParallelism = runtime.GOMAXPROCS(0)

const scoreBasedPairLimit = 256

type move struct {
	Command         Command
	SearchScore     moveScore
	ValidationScore moveScore
	Validation      validationStatus
	FailureReason   string
	FailureMessage  string
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
	switch {
	case !m.validationRejected():
		return "passed"
	case m.FailureMessage != "":
		return fmt.Sprintf("failed: %s (%s)", m.validationFailureReason(), m.FailureMessage)
	}
	return "failed: " + m.validationFailureReason()
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
	PairsEvaluated   int
}

type moveSearch struct {
	evaluator moveEvaluator
	budgets   map[string]int
	pacing    *DisruptionPacing
	deadline  time.Time
	random    *rand.Rand
	clk       clock.Clock
}

func (s moveSearch) run(ctx context.Context, candidates []*Candidate) ([]*move, searchStats) {
	singles := make([][]*Candidate, len(candidates))
	for i, c := range candidates {
		singles[i] = []*Candidate{c}
	}
	singleCtx, endSingle := startSearchPhaseSpan(ctx, "single", len(candidates))
	moves, stats := s.simulate(singleCtx, singles)
	endSingle(stats)
	if len(moves) > 0 || stats.TimedOut {
		return moves, stats
	}
	pairCtx, endPair := startSearchPhaseSpan(ctx, "pair", len(candidates))
	pairs, pairStats := s.simulate(pairCtx, s.choosePairs(candidates))
	endPair(pairStats)
	return pairs, stats.addPairPhase(pairStats)
}

func startSearchPhaseSpan(ctx context.Context, phase string, candidates int) (context.Context, func(searchStats)) {
	ctx, endSpan := cxtracing.Start(ctx, scoreBasedSpanName+".search."+phase, attribute.Int("candidates", candidates))
	return ctx, func(stats searchStats) {
		trace.SpanFromContext(ctx).SetAttributes(attribute.Int("evaluated_moves", stats.Evaluated), attribute.Bool("deadline_hit", stats.TimedOut))
		endSpan()
	}
}

// simulate simulates every move and returns the feasible ones, highest search score first.
func (s moveSearch) simulate(ctx context.Context, sources [][]*Candidate) ([]*move, searchStats) {
	sources = slices.Clone(sources)
	s.random.Shuffle(len(sources), func(i, j int) { sources[i], sources[j] = sources[j], sources[i] })
	var stats searchStats
	var mu sync.Mutex
	pending := rill.Generate(func(send func([]*Candidate), _ func(error)) {
		for _, nodes := range sources {
			if ctx.Err() != nil {
				return
			}
			if s.clk.Now().After(s.deadline) {
				mu.Lock()
				stats.TimedOut = true
				mu.Unlock()
				return
			}
			send(nodes)
		}
	})
	valid := rill.FilterMap(pending, scoreBasedSearchParallelism, func(nodes []*Candidate) (*move, bool, error) {
		scored, err := s.evaluator.evaluate(ctx, nodes)
		mu.Lock()
		defer mu.Unlock()
		stats.Evaluated++
		result := classifyEvaluation(scored, err)
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
			return &move{Command: scored.Command, SearchScore: scored.Score}, true, nil
		}
		return nil, false, nil
	})
	return collectMovesByScore(valid), stats
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
	score := scoreMove(cmd, e.inputs)
	cmd.scoreSavings = &score.Savings
	return &scoredCommand{Command: cmd, Score: score}, nil
}

func classifyEvaluation(s *scoredCommand, err error) string {
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

func collectMovesByScore(valid <-chan rill.Try[*move]) []*move {
	moves := []*move{}
	for a := range valid {
		moves = append(moves, a.Value)
	}
	sort.SliceStable(moves, func(i, j int) bool { return moves[i].SearchScore.Total > moves[j].SearchScore.Total })
	return moves
}

func (s searchStats) addPairPhase(pairs searchStats) searchStats {
	s.Evaluated += pairs.Evaluated
	s.Errors += pairs.Errors
	s.NoOp += pairs.NoOp
	s.NoSavings += pairs.NoSavings
	s.NonPositiveScore += pairs.NonPositiveScore
	s.TimedOut = pairs.TimedOut
	s.PairsEvaluated = pairs.Evaluated
	return s
}

// A pair phase that sampled does not prevent marking.
func (s searchStats) canMarkConsolidated(eligible int, blocked bool) bool {
	return s.Evaluated-s.PairsEvaluated == eligible && !s.TimedOut && s.Errors == 0 && !blocked
}

// Pairs share nodes, so only single-node moves join a batch.
func filterBatchable(feasible []*move) []*move {
	return lo.Filter(feasible, func(e *move, _ int) bool { return e.kind() == moveKindSingle && len(e.Command.Replacements) == 0 })
}
