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
	"slices"
	"sort"
	"sync"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/cxtracing"
)

const scoreBasedBatchMoveSeeds = 3

var scoreBasedBatchSimulationsPerSeed = 32

type batchMovesBuildingResult struct {
	Moves            []validatedMove
	JointSimulations int
}

func buildBatchMoves(ctx context.Context, evaluator moveEvaluator, kept, batchable []*move) batchMovesBuildingResult {
	seeds := []*move{}
	for _, m := range kept {
		if m.Validation == validationPassed && len(m.Command.Replacements) == 0 {
			seeds = append(seeds, m)
		}
	}
	sort.SliceStable(seeds, func(i, j int) bool { return seeds[i].ValidationScore.Total > seeds[j].ValidationScore.Total })
	seeds = seeds[:min(len(seeds), scoreBasedBatchMoveSeeds)]
	ctx, endSpan := cxtracing.Start(ctx, scoreBasedSpanName+".batch", attribute.Int("seeds", len(seeds)))
	defer endSpan()
	grown := make([]validatedMove, len(seeds))
	simulations := make([]int, len(seeds))
	var wg sync.WaitGroup
	for i, seed := range seeds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			grown[i], simulations[i] = growBatchMove(ctx, evaluator, seed, batchable)
		}()
	}
	wg.Wait()
	jointSimulations := lo.Sum(simulations)
	trace.SpanFromContext(ctx).SetAttributes(attribute.Int("joint_simulations", jointSimulations))
	return batchMovesBuildingResult{
		Moves:            lo.Filter(grown, func(m validatedMove, _ int) bool { return m.Kind == moveKindBatch }),
		JointSimulations: jointSimulations,
	}
}

func growBatchMove(ctx context.Context, evaluator moveEvaluator, seed *move, batchable []*move) (validatedMove, int) {
	current := seed.validated()
	simulations := 0
	for _, addition := range batchable {
		if simulations == scoreBasedBatchSimulationsPerSeed {
			break
		}
		if addition == seed || addition.validationRejected() {
			continue
		}
		grown, err := evaluator.evaluate(ctx, slices.Concat(current.Command.Candidates, addition.Command.Candidates))
		var notSimulated notSimulatedError
		if errors.As(err, &notSimulated) {
			continue
		}
		simulations++
		// The nomination recheck after compute rejects churn, not a failed simulation, so it is not an evaluation.
		if IsValidationError(err) {
			continue
		}
		result := classifyEvaluation(grown, err)
		countMoveEvaluation(result)
		if err != nil {
			log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
		}
		// The last accepted batch move is what runs, so an addition must raise the score.
		if result != moveEvaluationFeasible || len(grown.Command.Replacements) > 0 || grown.Score.Total <= current.Score.Total {
			continue
		}
		current = validatedMove{Move: seed, Kind: moveKindBatch, Command: grown.Command, Score: grown.Score}
	}
	return current, simulations
}
