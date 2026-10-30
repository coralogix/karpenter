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
	"sort"
	"sync"
	"time"

	"k8s.io/utils/clock"

	"github.com/destel/rill"
	"github.com/samber/lo"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

var scoreBasedValidEvaluationsTarget = 10

var scoreBasedMoveSetParallelism = runtime.GOMAXPROCS(0)

type moveSet struct {
	Nodes []*Candidate
}

type moveSetEvaluation struct {
	Command Command
	Score   float64
	// StandbyPrices holds the hourly price of each standby node the move reactivates.
	StandbyPrices map[string]float64
	Validation    string
}

type consolidationComputer func(ctx context.Context, candidates ...*Candidate) (Command, error)

type searchStats struct {
	Evaluated int
	Errors    int
	NoOp      int
	NoSavings int
	TimedOut  bool
}

// searchMoves simulates every move and keeps the most profitable ones. nodePrices gives the hourly price of each cluster node, which prices the standby nodes a move reactivates.
func searchMoves(ctx context.Context, moves []moveSet, deadline time.Time, compute consolidationComputer, nodePrices map[string]float64, random *rand.Rand, clk clock.Clock) ([]*moveSetEvaluation, searchStats) {
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
		if err != nil {
			stats.Errors++
			log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
			return nil, false, nil
		}
		if cmd.Decision() == NoOpDecision || len(cmd.Replacements) > 1 {
			stats.NoOp++
			return nil, false, nil
		}
		standbyPrices := map[string]float64{}
		for _, name := range standbyDestinations(cmd.Results) {
			standbyPrices[name] = nodePrices[name]
		}
		savings := cmd.EstimatedSavings() - lo.Sum(lo.Values(standbyPrices))
		if savings <= 0 {
			stats.NoSavings++
			return nil, false, nil
		}
		return &moveSetEvaluation{Command: cmd, Score: savings, StandbyPrices: standbyPrices, Validation: "not_attempted"}, true, nil
	})
	evals := topMovesBySavings(valid, scoreBasedValidEvaluationsTarget)
	return evals, stats
}

func topMovesBySavings(valid <-chan rill.Try[*moveSetEvaluation], limit int) []*moveSetEvaluation {
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
