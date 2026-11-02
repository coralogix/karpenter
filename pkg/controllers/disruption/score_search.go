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
	"sort"
	"sync"
	"time"

	"k8s.io/utils/clock"

	"github.com/destel/rill"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

var scoreBasedValidEvaluationsTarget = 10

var scoreBasedMoveSetParallelism = runtime.GOMAXPROCS(0)

type moveSet struct {
	Nodes []*Candidate
}

type moveSetEvaluation struct {
	Command Command
	// Score is the total from the search simulation.
	Score float64
	// RescoredScore is the total after validation re-simulation.
	RescoredScore float64
	// StandbyPrices holds the hourly price of each standby node the move reactivates.
	Breakdown       scoreBreakdown
	SearchBreakdown scoreBreakdown
	StandbyPrices   map[string]float64
	Validation      string
	Selected        bool
}

type consolidationComputer func(ctx context.Context, candidates ...*Candidate) (Command, error)

type searchStats struct {
	Evaluated int
	Errors    int
	NoOp      int
	NoSavings int
	TimedOut  bool
}

// searchMoves simulates every move and keeps the highest-scoring ones. nodePrices gives the hourly price of each cluster node, which prices the standby nodes a move reactivates.
func searchMoves(ctx context.Context, moves []moveSet, deadline time.Time, compute consolidationComputer, inputs scoreInputs, random *rand.Rand, clk clock.Clock) ([]*moveSetEvaluation, searchStats) {
	return searchSingleMoves(ctx, moves, deadline, compute, inputs, random, clk, scoreBasedValidEvaluationsTarget)
}

//nolint:gocyclo // Batch growth keeps simulation, limits, and search accounting together.
func searchAllMoves(ctx context.Context, moves []moveSet, deadline time.Time, compute consolidationComputer, inputs scoreInputs, random *rand.Rand, clk clock.Clock, budgets map[string]int, pacing *DisruptionPacing) ([]*moveSetEvaluation, searchStats) {
	singles, stats := searchSingleMoves(ctx, moves, deadline, compute, inputs, random, clk, len(moves))
	if len(singles) == 0 {
		return singles, stats
	}
	evals := append([]*moveSetEvaluation(nil), singles...)
	ids := map[*Candidate]int{}
	for i, move := range moves {
		ids[move.Nodes[0]] = i
	}
	seen := map[string]bool{}
	seeds := []*moveSetEvaluation{}
	for _, single := range singles {
		if len(single.Command.Replacements) == 0 {
			seeds = append(seeds, single)
		}
	}
	for _, seed := range seeds[:min(3, len(seeds))] {
		batch := append([]*Candidate(nil), seed.Command.Candidates...)
		attempts := 0
		for _, single := range seeds {
			if ctx.Err() != nil || clk.Now().After(deadline) {
				stats.TimedOut = clk.Now().After(deadline)
				break
			}
			candidate := single.Command.Candidates[0]
			if candidate == batch[0] || attempts >= 256 {
				continue
			}
			proposed := append(append([]*Candidate(nil), batch...), candidate)
			counts := map[string]int{}
			allowed := true
			for _, c := range proposed {
				counts[c.NodePool.Name]++
				if budgets != nil && counts[c.NodePool.Name] > budgets[c.NodePool.Name] {
					allowed = false
				}
			}
			if !allowed || !pacing.CommandAllowed(Command{Candidates: proposed}) {
				continue
			}
			attempts++
			cmd, err := compute(ctx, proposed...)
			stats.Evaluated++
			result := "feasible"
			if err != nil {
				stats.Errors++
				result = "error"
				log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
			} else if cmd.Decision() == NoOpDecision || len(cmd.Replacements) != 0 {
				stats.NoOp++
				result = "no_op"
			} else {
				batch = proposed
				breakdown, prices := inputs.score(cmd, cmd.Results)
				if breakdown.Savings <= 0 || breakdown.Total <= 0 {
					stats.NoSavings++
					result = "no_savings"
					if breakdown.Savings > 0 {
						result = "non_positive_score"
					}
				} else {
					indices := make([]int, len(proposed))
					for i, c := range proposed {
						indices[i] = ids[c]
					}
					sort.Ints(indices)
					key := fmt.Sprint(indices)
					if !seen[key] {
						seen[key] = true
						evals = append(evals, &moveSetEvaluation{Command: cmd, Score: breakdown.Total, Breakdown: breakdown, SearchBreakdown: breakdown, StandbyPrices: prices, Validation: "not_attempted"})
					}
				}
			}
			ScoreBasedMoveEvaluationsTotal.Inc(map[string]string{"result": result})
		}
	}
	sort.SliceStable(evals, func(i, j int) bool { return evals[i].Score > evals[j].Score })
	if len(evals) > scoreBasedValidEvaluationsTarget {
		evals = evals[:scoreBasedValidEvaluationsTarget]
		found := false
		for _, eval := range evals {
			if eval == singles[0] {
				found = true
				break
			}
		}
		if !found {
			evals = append(evals, singles[0])
		}
	}
	return evals, stats
}

func searchSingleMoves(ctx context.Context, moves []moveSet, deadline time.Time, compute consolidationComputer, inputs scoreInputs, random *rand.Rand, clk clock.Clock, limit int) ([]*moveSetEvaluation, searchStats) {
	moves = append([]moveSet(nil), moves...)
	random.Shuffle(len(moves), func(i, j int) { moves[i], moves[j] = moves[j], moves[i] })
	var stats searchStats
	var mu sync.Mutex
	pending := rill.Generate(func(send func(moveSet), _ func(error)) {
		for _, move := range moves {
			if ctx.Err() != nil {
				return
			}
			if clk.Now().After(deadline) {
				mu.Lock()
				stats.TimedOut = true
				mu.Unlock()
				return
			}
			send(move)
		}
	})
	valid := rill.OrderedFilterMap(pending, scoreBasedMoveSetParallelism, func(move moveSet) (*moveSetEvaluation, bool, error) {
		cmd, err := compute(ctx, move.Nodes...)
		mu.Lock()
		defer mu.Unlock()
		stats.Evaluated++
		result := "feasible"
		defer func() { ScoreBasedMoveEvaluationsTotal.Inc(map[string]string{"result": result}) }()
		if err != nil {
			result = "error"
			stats.Errors++
			log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
			return nil, false, nil
		}
		if cmd.Decision() == NoOpDecision || len(cmd.Replacements) > 1 {
			result = "no_op"
			stats.NoOp++
			return nil, false, nil
		}
		breakdown, standbyPrices := inputs.score(cmd, cmd.Results)
		if breakdown.Savings <= 0 || breakdown.Total <= 0 {
			result = "no_savings"
			if breakdown.Savings > 0 {
				result = "non_positive_score"
			}
			stats.NoSavings++
			return nil, false, nil
		}
		return &moveSetEvaluation{Command: cmd, Score: breakdown.Total, Breakdown: breakdown, SearchBreakdown: breakdown, StandbyPrices: standbyPrices, Validation: "not_attempted"}, true, nil
	})
	evals := topMovesByScore(valid, limit)
	return evals, stats
}

func topMovesByScore(valid <-chan rill.Try[*moveSetEvaluation], limit int) []*moveSetEvaluation {
	evals := []*moveSetEvaluation{}
	for a := range valid {
		evals = append(evals, a.Value)
	}
	sort.SliceStable(evals, func(i, j int) bool { return evals[i].Score > evals[j].Score })
	if len(evals) > limit {
		evals = evals[:limit]
	}
	return evals
}

func (s searchStats) canMarkConsolidated(eligible int, blocked bool) bool {
	return s.Evaluated == eligible && s.Errors == 0 && !blocked
}
