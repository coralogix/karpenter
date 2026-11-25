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

	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var scoreBasedValidEvaluationsTarget = 10

var scoreBasedSearchParallelism = runtime.GOMAXPROCS(0)

type move struct {
	Command         Command
	SearchScore     moveScore
	ValidationScore moveScore
	Validation      validationStatus
	// FailureReason explains a failed validation.
	FailureReason string
	Selected      bool
}

type validationStatus int

const (
	validationPassed validationStatus = iota
	// validationFailed means validation errored, so there is no rescored score.
	validationFailed
	// validationNoSavings means validation re-simulated the move but it no longer saves money.
	validationNoSavings
)

func (m *move) validationFailedOrNoSavings() bool {
	return m.Validation == validationFailed || m.Validation == validationNoSavings
}

// validationText is the log form of the validation outcome.
func (m *move) validationText() string {
	switch m.Validation {
	case validationFailed:
		return "failed: " + m.FailureReason
	case validationNoSavings:
		return "failed: no_savings"
	}
	return "passed"
}

type consolidationComputer func(ctx context.Context, candidates ...*Candidate) (Command, error)

type searchStats struct {
	Evaluated int
	Errors    int
	NoOp      int
	NoSavings int
	TimedOut  bool
}

type moveScore struct {
	Savings       float64
	SourcePrice   float64
	NewNodePrice  float64
	StandbyPrices map[string]float64
}

// searchMoves simulates every move and keeps the most profitable ones.
func searchMoves(ctx context.Context, sources [][]*Candidate, deadline time.Time, compute consolidationComputer, random *rand.Rand, clk clock.Clock) ([]*move, searchStats) {
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
		s, err := evaluateMove(ctx, compute, nodes)
		mu.Lock()
		defer mu.Unlock()
		stats.Evaluated++
		switch {
		case err != nil:
			stats.Errors++
			log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
			return nil, false, nil
		case s == nil:
			stats.NoOp++
			return nil, false, nil
		case s.Score.Savings <= 0:
			stats.NoSavings++
			return nil, false, nil
		}
		return &move{Command: s.Command, SearchScore: s.Score}, true, nil
	})
	moves := topMovesBySavings(valid, scoreBasedValidEvaluationsTarget)
	return moves, stats
}

// scoredCommand is scored on whichever snapshot the evaluator simulates on, so each caller names the score by phase.
type scoredCommand struct {
	Command Command
	Score   moveScore
}

// evaluateMove is the per-move step shared by the search and the rescore: compute the command and score it.
// It returns nil when the move is a no-op or needs more than one new node.
func evaluateMove(ctx context.Context, compute consolidationComputer, nodes []*Candidate) (*scoredCommand, error) {
	cmd, err := compute(ctx, nodes...)
	if err != nil {
		return nil, err
	}
	if cmd.Decision() == NoOpDecision || len(cmd.Replacements) > 1 {
		return nil, nil
	}
	return &scoredCommand{Command: cmd, Score: scoreMove(cmd)}, nil
}

func scoreMove(cmd Command) moveScore {
	standbyPrices := map[string]float64{}
	for _, n := range cmd.Results.ExistingNodes {
		if len(n.Pods) > 0 && standby.HasStandbyTaint(n.Node) {
			standbyPrices[n.Name()] = n.Price
		}
	}
	score := moveScore{Savings: cmd.EstimatedSavings() - lo.Sum(lo.Values(standbyPrices)), SourcePrice: cmd.SourceCost(), StandbyPrices: standbyPrices}
	if len(cmd.Replacements) > 0 {
		score.NewNodePrice = score.SourcePrice - cmd.EstimatedSavings()
	}
	return score
}

func topMovesBySavings(valid <-chan rill.Try[*move], limit int) []*move {
	moves := []*move{}
	for a := range valid {
		moves = append(moves, a.Value)
	}
	sort.SliceStable(moves, func(i, j int) bool { return moves[i].SearchScore.Savings > moves[j].SearchScore.Savings })
	if len(moves) > limit {
		moves = moves[:limit]
	}
	return moves
}

func (s searchStats) canMarkConsolidated(eligible int, blocked bool) bool {
	return s.Evaluated == eligible && s.Errors == 0 && !blocked
}
