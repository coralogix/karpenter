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
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/destel/rill"

	"sigs.k8s.io/karpenter/pkg/events"
)

// scoreBasedPairSearchLimit bounds the pair fallback search while keeping small pair spaces exhaustive.
const scoreBasedPairSearchLimit = 256

// scoreBasedBatchSeedLimit bounds the number of greedy batch-growth passes.
const scoreBasedBatchSeedLimit = 3

type scoreBasedPair struct {
	first, second int
	priority      float64
}

//nolint:gocyclo // The bounded greedy growth loop keeps deadline, feasibility cache, and fallback retention in one place.
func growScoreBasedBatches(
	ctx context.Context,
	singletons []*moveSetEvaluation,
	budgets map[string]int,
	pace *UnderutilizedConsolidationPace,
	compute consolidationComputer,
	stats *moveSetSearchStats,
) ([]*moveSetEvaluation, int, bool, error) {
	if len(singletons) < 2 {
		return nil, 0, false, nil
	}
	sort.Slice(singletons, func(i, j int) bool { return singletons[i].Score > singletons[j].Score })
	seeds := make([]*moveSetEvaluation, 0, scoreBasedBatchSeedLimit)
	seedKeys := map[string]struct{}{}
	for _, singleton := range singletons {
		if len(seeds) >= scoreBasedBatchSeedLimit {
			break
		}
		key := candidateMoveSetKey(singleton.Command.Candidates)
		if _, found := seedKeys[key]; found {
			continue
		}
		seedKeys[key] = struct{}{}
		seeds = append(seeds, singleton)
	}
	if len(seeds) == 0 {
		return nil, 0, false, nil
	}

	scoreByCandidate := make(map[string]float64, len(singletons))
	for _, singleton := range singletons {
		if len(singleton.Command.Candidates) == 1 {
			scoreByCandidate[candidateMoveSetKey(singleton.Command.Candidates)] = singleton.Score
		}
	}
	var bestBatches []*moveSetEvaluation
	var smallestBatch *moveSetEvaluation
	seenSets := map[string]bool{}
	knownSets := map[string]struct{}{}
	retainedSets := map[string]struct{}{}
	attempted := 0
	const perSeedAttemptLimit = 256
	searchStart := time.Now()

	retainBatch := func(eval *moveSetEvaluation) {
		if eval == nil {
			return
		}
		key := candidateMoveSetKey(eval.Command.Candidates)
		if _, found := retainedSets[key]; found {
			return
		}
		retainedSets[key] = struct{}{}
		if smallestBatch == nil || len(eval.Command.Candidates) < len(smallestBatch.Command.Candidates) ||
			(len(eval.Command.Candidates) == len(smallestBatch.Command.Candidates) && eval.Score > smallestBatch.Score) {
			smallestBatch = eval
		}
		bestBatches = append(bestBatches, eval)
		sort.Slice(bestBatches, func(i, j int) bool { return bestBatches[i].Score > bestBatches[j].Score })
		if len(bestBatches) > scoreBasedMoveSetResultLimit {
			bestBatches = bestBatches[:scoreBasedMoveSetResultLimit]
		}
	}

	for _, seed := range seeds {
		if ctx.Err() != nil {
			break
		}
		current := append([]*Candidate(nil), seed.Command.Candidates...)
		remaining := make([]*moveSetEvaluation, 0, len(singletons)-1)
		for _, singleton := range singletons {
			if !containsCandidate(current, singleton.Command.Candidates[0]) {
				remaining = append(remaining, singleton)
			}
		}
		seedAttempts := 0
		for len(remaining) > 0 && seedAttempts < perSeedAttemptLimit {
			grew := false
			nextRemaining := make([]*moveSetEvaluation, 0, len(remaining))
			for _, singleton := range remaining {
				if ctx.Err() != nil || seedAttempts >= perSeedAttemptLimit {
					break
				}
				candidate := singleton.Command.Candidates[0]
				if !scoreBasedMoveSetCanAdd(current, candidate, budgets, pace) {
					continue
				}
				trial := append(append([]*Candidate(nil), current...), candidate)
				key := candidateMoveSetKey(trial)
				feasible := seenSets[key]
				_, known := knownSets[key]
				var eval *moveSetEvaluation
				if !known {
					seedAttempts++
					attempted++
					moveEval := evaluateMoveSetWith(ctx, moveSet{Nodes: trial}, compute, stats,
						func(cmd Command) bool { return len(cmd.Candidates) == len(trial) && len(cmd.Replacements) == 0 },
						func(Command) float64 {
							var total float64
							for _, member := range trial {
								total += scoreByCandidate[candidateMoveSetKey([]*Candidate{member})]
							}
							return total
						},
					)
					feasible = moveEval != nil
					seenSets[key] = feasible
					knownSets[key] = struct{}{}
					eval = moveEval
					retainBatch(eval)
					if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
						break
					}
				}
				if feasible {
					current = trial
					grew = true
				} else {
					nextRemaining = append(nextRemaining, singleton)
				}
			}
			if ctx.Err() != nil || !grew {
				break
			}
			remaining = nextRemaining
		}
	}
	if smallestBatch != nil {
		found := false
		for _, eval := range bestBatches {
			if candidateMoveSetKey(eval.Command.Candidates) == candidateMoveSetKey(smallestBatch.Command.Candidates) {
				found = true
				break
			}
		}
		if !found {
			bestBatches = append(bestBatches, smallestBatch)
		}
	}
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	if ctx.Err() != nil && !timedOut {
		return nil, attempted, false, ctx.Err()
	}
	logMoveSetSearchComplete(ctx, attempted, attempted, len(bestBatches), timedOut, time.Since(searchStart), stats)
	return bestBatches, attempted, timedOut, nil
}

//nolint:gocyclo // Result selection reserves independent fallbacks before filling the bounded score-ranked result set.
func retainScoreBasedMoveSetResults(singles, batches []*moveSetEvaluation, limit int) []*moveSetEvaluation {
	if limit <= 0 {
		return append(singles, batches...)
	}
	var smallestBatch *moveSetEvaluation
	for _, batch := range batches {
		if len(batch.Command.Candidates) < 2 {
			continue
		}
		if smallestBatch == nil || len(batch.Command.Candidates) < len(smallestBatch.Command.Candidates) ||
			(len(batch.Command.Candidates) == len(smallestBatch.Command.Candidates) && batch.Score > smallestBatch.Score) {
			smallestBatch = batch
		}
	}
	var bestSingleton *moveSetEvaluation
	batchEvalCandidates := make([]*moveSetEvaluation, 0, len(batches))
	for _, batch := range batches {
		if len(batch.Command.Candidates) >= 2 {
			batchEvalCandidates = append(batchEvalCandidates, batch)
		}
	}
	for _, single := range singles {
		if len(single.Command.Candidates) != 1 {
			continue
		}
		if bestSingleton == nil || single.Score > bestSingleton.Score {
			bestSingleton = single
		}
	}
	var independentSingleton *moveSetEvaluation
	bestIndependentOccurrences := int(^uint(0) >> 1)
	for _, single := range singles {
		if len(single.Command.Candidates) != 1 || (bestSingleton != nil && candidateMoveSetKey(single.Command.Candidates) == candidateMoveSetKey(bestSingleton.Command.Candidates)) {
			continue
		}
		occurrences := 0
		for _, batch := range batchEvalCandidates {
			if containsCandidate(batch.Command.Candidates, single.Command.Candidates[0]) {
				occurrences++
			}
		}
		if occurrences < bestIndependentOccurrences || (occurrences == bestIndependentOccurrences && (independentSingleton == nil || single.Score > independentSingleton.Score)) {
			bestIndependentOccurrences = occurrences
			independentSingleton = single
		}
	}

	selected := make([]*moveSetEvaluation, 0, limit)
	add := func(eval *moveSetEvaluation) {
		if eval == nil {
			return
		}
		for _, existing := range selected {
			if candidateMoveSetKey(existing.Command.Candidates) == candidateMoveSetKey(eval.Command.Candidates) {
				return
			}
		}
		selected = append(selected, eval)
	}
	add(bestSingleton)
	add(independentSingleton)
	add(smallestBatch)
	if len(selected) > limit {
		reserved := append([]*moveSetEvaluation(nil), selected...)
		selected = selected[:0]
		add(bestSingleton)
		sort.Slice(reserved, func(i, j int) bool { return reserved[i].Score > reserved[j].Score })
		for _, eval := range reserved {
			if len(selected) >= limit {
				break
			}
			add(eval)
		}
	}
	combined := append(append([]*moveSetEvaluation(nil), singles...), batches...)
	sort.Slice(combined, func(i, j int) bool { return combined[i].Score > combined[j].Score })
	for _, eval := range combined {
		if len(selected) >= limit {
			break
		}
		add(eval)
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Score > selected[j].Score })
	return selected
}

//nolint:gocyclo // Exhaustive small-space and bounded weighted sampling share deduplication and budget checks.
func scoreBasedPairMoveSets(
	ctx context.Context,
	candidates []*Candidate,
	budgets map[string]int,
	pace *UnderutilizedConsolidationPace,
	limit int,
	rng *rand.Rand,
) ([]moveSet, bool, bool) {
	if limit <= 0 {
		return nil, false, false
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // Random order only guides this non-security-sensitive search.
	}
	n := len(candidates)
	totalPairs := int64(n) * int64(n-1) / 2
	exhaustive := totalPairs <= int64(limit)
	weights := make([]float64, n)
	prefixWeights := make([]float64, n)
	for i, candidate := range candidates {
		if ctx.Err() != nil {
			return nil, false, false
		}
		utilization := 1.0
		allocatable := candidateAllocatableCPU(candidate)
		if allocatable > 0 {
			utilization = candidateRequestedCPU(candidate) / allocatable
		}
		weights[i] = scoreBasedPairSamplingWeight(utilization)
		prefixWeights[i] = weights[i]
		if i > 0 {
			prefixWeights[i] += prefixWeights[i-1]
		}
	}

	pairs := make([]scoreBasedPair, 0, int(min(totalPairs, int64(limit))))
	seen := map[[2]int]struct{}{}
	constrained := false
	addPair := func(first, second int, priority float64) {
		if first > second {
			first, second = second, first
		}
		key := [2]int{first, second}
		if _, found := seen[key]; found {
			return
		}
		seen[key] = struct{}{}
		allowed, blocked := scoreBasedMoveSetCanAddWithReason(nil, candidates[first], budgets, pace)
		if allowed {
			var blockedNext bool
			allowed, blockedNext = scoreBasedMoveSetCanAddWithReason([]*Candidate{candidates[first]}, candidates[second], budgets, pace)
			blocked = blocked || blockedNext
		}
		if !allowed {
			constrained = constrained || blocked
			return
		}
		pairs = append(pairs, scoreBasedPair{first: first, second: second, priority: priority})
	}

	if exhaustive {
		for first := 0; first < n; first++ {
			for second := first + 1; second < n; second++ {
				if ctx.Err() != nil {
					return moveSetsFromPairs(candidates, pairs), false, constrained
				}
				u := 1 - rng.Float64()
				if u <= 0 {
					u = math.SmallestNonzeroFloat64
				}
				priority := -math.Log(u) / (weights[first] * weights[second])
				addPair(first, second, priority)
			}
		}
		sort.Slice(pairs, func(i, j int) bool { return pairs[i].priority < pairs[j].priority })
		return moveSetsFromPairs(candidates, pairs), true, constrained
	}

	maxAttempts := limit * 50
	for attempt := 0; attempt < maxAttempts && len(pairs) < limit; attempt++ {
		if ctx.Err() != nil {
			return moveSetsFromPairs(candidates, pairs), false, constrained
		}
		first := weightedCandidateIndex(rng, weights, prefixWeights, -1)
		second := weightedCandidateIndex(rng, weights, prefixWeights, first)
		if first < 0 || second < 0 {
			break
		}
		addPair(first, second, float64(attempt))
	}
	return moveSetsFromPairs(candidates, pairs), false, constrained
}

func moveSetsFromPairs(candidates []*Candidate, pairs []scoreBasedPair) []moveSet {
	moveSets := make([]moveSet, 0, len(pairs))
	for _, pair := range pairs {
		moveSets = append(moveSets, moveSet{Nodes: []*Candidate{candidates[pair.first], candidates[pair.second]}})
	}
	return moveSets
}

func weightedCandidateIndex(rng *rand.Rand, weights, prefixWeights []float64, excluded int) int {
	if len(prefixWeights) == 0 {
		return -1
	}
	total := prefixWeights[len(prefixWeights)-1]
	if excluded >= 0 {
		total -= weights[excluded]
	}
	if total <= 0 {
		return -1
	}
	target := rng.Float64() * total
	if excluded >= 0 && target >= prefixWeights[excluded]-weights[excluded] {
		target += weights[excluded]
	}
	index := sort.Search(len(prefixWeights), func(i int) bool { return prefixWeights[i] > target })
	if index == excluded {
		index++
	}
	if index >= len(weights) {
		index = len(weights) - 1
		if index == excluded {
			index--
		}
	}
	return index
}

func scoreBasedPairSamplingWeight(utilization float64) float64 {
	return math.Max(0.05, 1-math.Max(0, math.Min(1, utilization)))
}

func scoreBasedMoveSetCanAdd(current []*Candidate, next *Candidate, budgets map[string]int, pace *UnderutilizedConsolidationPace) bool {
	allowed, _ := scoreBasedMoveSetCanAddWithReason(current, next, budgets, pace)
	return allowed
}

func scoreBasedMoveSetCanAddWithReason(current []*Candidate, next *Candidate, budgets map[string]int, pace *UnderutilizedConsolidationPace) (bool, bool) {
	if next == nil || next.NodePool == nil {
		return false, false
	}
	poolCount, workCount := 0, 0
	for _, candidate := range current {
		if candidate == nil || candidate.NodePool == nil || candidate.NodePool.Name != next.NodePool.Name {
			continue
		}
		poolCount++
		if len(candidate.reschedulablePods) > 0 {
			workCount++
		}
	}
	if poolCount+1 > budgets[next.NodePool.Name] {
		return false, true
	}
	if len(next.reschedulablePods) > 0 && !pace.candidateAllowed(next.NodePool, workCount) {
		return false, true
	}
	return true, false
}

func candidateMoveSetKey(candidates []*Candidate) string {
	keys := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		key := ""
		if candidate.NodeClaim != nil && candidate.NodeClaim.UID != "" {
			key = "uid:" + string(candidate.NodeClaim.UID)
		} else if candidate.ProviderID() != "" {
			key = "provider:" + candidate.ProviderID()
		} else if candidate.NodeClaim != nil {
			key = "claim:" + candidate.NodeClaim.Name
		} else if candidate.Node != nil {
			key = "node:" + candidate.Node.Name
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

func containsCandidate(candidates []*Candidate, next *Candidate) bool {
	key := candidateMoveSetKey([]*Candidate{next})
	for _, candidate := range candidates {
		if candidateMoveSetKey([]*Candidate{candidate}) == key {
			return true
		}
	}
	return false
}

type scoreBasedSearchEventRecorder struct {
	recorder events.Recorder
}

func (r scoreBasedSearchEventRecorder) Publish(evts ...events.Event) {
	forward := make([]events.Event, 0, len(evts))
	for _, evt := range evts {
		if evt.Reason != events.ConsolidationCandidate {
			forward = append(forward, evt)
		}
	}
	if len(forward) > 0 {
		r.recorder.Publish(forward...)
	}
}

func evaluateMoveSetsPar(
	ctx context.Context,
	moveSets []moveSet,
	deadline time.Time,
	compute consolidationComputer,
) ([]*moveSetEvaluation, int, bool, *moveSetSearchStats, error) {
	return evaluateMoveSetsParWithStats(ctx, moveSets, deadline, compute, scoreBasedMoveSetResultLimit, nil, nil, nil)
}

func evaluateMoveSetsParWithStats(
	ctx context.Context,
	moveSets []moveSet,
	deadline time.Time,
	compute consolidationComputer,
	resultLimit int,
	accept func(Command) bool,
	score func(Command) float64,
	stats *moveSetSearchStats,
) ([]*moveSetEvaluation, int, bool, *moveSetSearchStats, error) {
	searchStart := time.Now()
	if stats == nil {
		stats = &moveSetSearchStats{}
	}

	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	var evaluated atomic.Int64

	// Stop feeding move sets once the deadline passes so in-flight work is drained but new work is not started.
	pending := rill.Generate(func(send func(moveSet), _ func(error)) {
		for _, moveSet := range moveSets {
			if ctx.Err() != nil {
				return
			}
			send(moveSet)
		}
	})

	valid := rill.OrderedFilterMap(pending, scoreBasedMoveSetParallelism, func(moveSet moveSet) (*moveSetEvaluation, bool, error) {
		eval := evaluateMoveSetWith(ctx, moveSet, compute, stats, accept, score)
		if ctx.Err() == nil {
			evaluated.Add(1)
			return eval, eval != nil, nil
		}
		return nil, false, nil
	})

	evals, err := collectMoveSetEvaluations(valid, resultLimit)
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	logMoveSetSearchComplete(ctx, len(moveSets), int(evaluated.Load()), len(evals), timedOut, time.Since(searchStart), stats)
	if err != nil {
		return nil, int(evaluated.Load()), timedOut, stats, err
	}
	if ctx.Err() != nil && !timedOut {
		return nil, int(evaluated.Load()), false, stats, ctx.Err()
	}
	if timedOut && len(evals) == 0 {
		ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
	}
	return evals, int(evaluated.Load()), timedOut, stats, nil
}

func evaluateMoveSetWith(
	ctx context.Context,
	moveSet moveSet,
	compute consolidationComputer,
	stats *moveSetSearchStats,
	accept func(Command) bool,
	score func(Command) float64,
) *moveSetEvaluation {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	start := time.Now()
	cmd, err := compute(ctx, moveSet.Nodes...)
	stats.record(time.Since(start), err)
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	if err != nil {
		return nil
	}
	if cmd.Decision() == NoOpDecision {
		stats.recordNoOpMoveSet()
		return nil
	}
	if cmd.EstimatedSavings() <= 0 {
		stats.recordNonPositiveSavingsMoveSet()
		return nil
	}
	if accept != nil && !accept(cmd) {
		return nil
	}
	moveSetScore := moveSetPriorityScore(cmd)
	if score != nil {
		moveSetScore = score(cmd)
	}
	return &moveSetEvaluation{
		Command: cmd,
		Score:   moveSetScore,
	}
}

func collectMoveSetEvaluations(valid <-chan rill.Try[*moveSetEvaluation], limit int) ([]*moveSetEvaluation, error) {
	defer rill.Discard(valid)

	var evals []*moveSetEvaluation
	for a := range valid {
		if a.Error != nil {
			return nil, a.Error
		}
		evals = append(evals, a.Value)
	}
	sort.Slice(evals, func(i, j int) bool {
		return evals[i].Score > evals[j].Score
	})
	if limit > 0 && len(evals) > limit {
		evals = evals[:limit]
	}
	return evals, nil
}
