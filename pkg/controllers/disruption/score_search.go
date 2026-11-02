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
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/utils/clock"

	"sigs.k8s.io/karpenter/pkg/cxtracing"

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
	Evaluated       int
	Errors          int
	NoOp            int
	NoSavings       int
	TimedOut        bool
	PositiveSavings bool
	Blocked         bool
}

//nolint:gocyclo // Batch growth keeps simulation, limits, and search accounting together.
func searchAllMoves(ctx context.Context, moves []moveSet, deadline time.Time, compute consolidationComputer, inputs scoreInputs, random *rand.Rand, clk clock.Clock, budgets map[string]int, pacing *DisruptionPacing) ([]*moveSetEvaluation, searchStats) {
	singleCtx, endSingle := cxtracing.Start(ctx, "single")
	singles, stats := searchSingleMoves(singleCtx, moves, deadline, compute, inputs, random, clk, len(moves))
	trace.SpanFromContext(singleCtx).SetAttributes(attribute.Int("candidate_count", len(moves)), attribute.Int("evaluated_move_count", stats.Evaluated), attribute.Bool("deadline_hit", stats.TimedOut))
	endSingle()
	if len(singles) == 0 {
		if !stats.PositiveSavings && !stats.TimedOut && ctx.Err() == nil {
			pairCtx, endPair := cxtracing.Start(ctx, "pair")
			before := stats.Evaluated
			evals, pairStats := searchPairMoves(pairCtx, moves, deadline, compute, inputs, random, clk, budgets, pacing, stats)
			trace.SpanFromContext(pairCtx).SetAttributes(attribute.Int("candidate_count", len(moves)), attribute.Int("evaluated_move_count", pairStats.Evaluated-before), attribute.Bool("deadline_hit", pairStats.TimedOut))
			endPair()
			return evals, pairStats
		}
		return singles, stats
	}
	batchCtx, endBatch := cxtracing.Start(ctx, "batch")
	before := stats.Evaluated
	defer func() {
		trace.SpanFromContext(batchCtx).SetAttributes(attribute.Int("candidate_count", len(moves)), attribute.Int("evaluated_move_count", stats.Evaluated-before), attribute.Bool("deadline_hit", stats.TimedOut))
		endBatch()
	}()
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
			if !scoreMoveAllowed(proposed, budgets, pacing) {
				stats.Blocked = true
				continue
			}
			attempts++
			cmd, err := compute(batchCtx, proposed...)
			stats.Evaluated++
			var result string
			if err != nil {
				stats.Errors++
				result = "error"
				log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
			} else if cmd.Decision() == NoOpDecision || len(cmd.Replacements) != 0 {
				stats.NoOp++
				result = "no_op"
			} else {
				batch = proposed
				eval, scoreResult := evaluateScore(cmd, inputs)
				result = scoreResult
				if eval == nil {
					stats.NoSavings++
				} else {
					indices := make([]int, len(proposed))
					for i, c := range proposed {
						indices[i] = ids[c]
					}
					sort.Ints(indices)
					key := fmt.Sprint(indices)
					if !seen[key] {
						seen[key] = true
						evals = append(evals, eval)
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

//nolint:gocyclo // Sampling, deadline checks, and simulation accounting form one search phase.
func searchPairMoves(ctx context.Context, moves []moveSet, deadline time.Time, compute consolidationComputer, inputs scoreInputs, random *rand.Rand, clk clock.Clock, budgets map[string]int, pacing *DisruptionPacing, stats searchStats) ([]*moveSetEvaluation, searchStats) {
	moves = append([]moveSet(nil), moves...)
	random.Shuffle(len(moves), func(i, j int) { moves[i], moves[j] = moves[j], moves[i] })
	pairs := [][2]int{}
	n := len(moves)
	if n*(n-1)/2 <= 256 {
		for i := 0; i < n; i++ {
			for j := i + 1; j < n; j++ {
				pairs = append(pairs, [2]int{i, j})
			}
		}
	} else {
		weights := make([]float64, n)
		total := 0.0
		for i, move := range moves {
			source := inputs.sources[move.Nodes[0]]
			utilization := 0.0
			if source.AllocatableCPU > 0 {
				utilization = source.RequestedCPU / source.AllocatableCPU
			}
			weights[i] = math.Max(0.05, 1-math.Max(0, utilization))
			total += weights[i]
		}
		draw := func() int {
			x := random.Float64() * total
			for i, weight := range weights {
				x -= weight
				if x < 0 {
					return i
				}
			}
			return n - 1
		}
		seen := map[[2]int]bool{}
		for len(pairs) < 256 {
			if ctx.Err() != nil || clk.Now().After(deadline) {
				stats.TimedOut = clk.Now().After(deadline)
				break
			}
			i, j := draw(), draw()
			if i == j {
				continue
			}
			if i > j {
				i, j = j, i
			}
			pair := [2]int{i, j}
			if !seen[pair] {
				seen[pair] = true
				pairs = append(pairs, pair)
			}
		}
	}
	evals := []*moveSetEvaluation{}
	for _, pair := range pairs {
		if ctx.Err() != nil || clk.Now().After(deadline) {
			stats.TimedOut = clk.Now().After(deadline)
			break
		}
		candidates := []*Candidate{moves[pair[0]].Nodes[0], moves[pair[1]].Nodes[0]}
		if !scoreMoveAllowed(candidates, budgets, pacing) {
			stats.Blocked = true
			continue
		}
		cmd, err := compute(ctx, candidates...)
		stats.Evaluated++
		result := "no_op"
		if err != nil {
			stats.Errors++
			result = "error"
			log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
		} else if cmd.Decision() == NoOpDecision || len(cmd.Replacements) != 1 {
			stats.NoOp++
		} else {
			eval, scoreResult := evaluateScore(cmd, inputs)
			result = scoreResult
			if eval == nil {
				stats.NoSavings++
			} else {
				evals = append(evals, eval)
			}
		}
		ScoreBasedMoveEvaluationsTotal.Inc(map[string]string{"result": result})
	}
	sort.SliceStable(evals, func(i, j int) bool { return evals[i].Score > evals[j].Score })
	if len(evals) > scoreBasedValidEvaluationsTarget {
		evals = evals[:scoreBasedValidEvaluationsTarget]
	}
	return evals, stats
}

func scoreMoveAllowed(candidates []*Candidate, budgets map[string]int, pacing *DisruptionPacing) bool {
	counts := map[string]int{}
	for _, c := range candidates {
		counts[c.NodePool.Name]++
		if budgets != nil && counts[c.NodePool.Name] > budgets[c.NodePool.Name] {
			return false
		}
	}
	return pacing.CommandAllowed(Command{Candidates: candidates})
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
		eval, scoreResult := evaluateScore(cmd, inputs)
		result = scoreResult
		if scoreResult == "feasible" || scoreResult == "non_positive_score" {
			stats.PositiveSavings = true
		}
		if eval == nil {
			stats.NoSavings++
			return nil, false, nil
		}
		return eval, true, nil
	})
	evals := topMovesByScore(valid, limit)
	return evals, stats
}

func evaluateScore(cmd Command, inputs scoreInputs) (*moveSetEvaluation, string) {
	breakdown, prices := inputs.score(cmd, cmd.Results)
	if breakdown.Savings <= 0 {
		return nil, "no_savings"
	}
	if breakdown.Total <= 0 {
		return nil, "non_positive_score"
	}
	return &moveSetEvaluation{Command: cmd, Score: breakdown.Total, Breakdown: breakdown, SearchBreakdown: breakdown, StandbyPrices: prices, Validation: "not_attempted"}, "feasible"
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
	return s.Evaluated >= eligible && s.Errors == 0 && !s.TimedOut && !s.Blocked && !blocked
}
