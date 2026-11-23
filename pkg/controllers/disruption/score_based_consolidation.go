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
	"slices"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/cxtracing"
	"sigs.k8s.io/karpenter/pkg/events"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

var ScoreBasedConsolidationTimeoutDuration = 15 * time.Second

const ScoreBasedConsolidationType = "score-based"

const scoreBasedSpanName = "karpenter.disruption.score_based_consolidation"

func NodePoolUsesScoreBasedConsolidation(np *v1.NodePool) bool {
	if np == nil {
		return false
	}
	_, ok := np.Annotations[v1.ScoreBasedConsolidationAnnotationKey]
	return ok
}

type ScoreBasedConsolidation struct {
	consolidation
}

func NewScoreBasedConsolidation(c consolidation) *ScoreBasedConsolidation {
	return &ScoreBasedConsolidation{consolidation: c}
}

func (s *ScoreBasedConsolidation) ShouldDisrupt(ctx context.Context, cn *Candidate) bool {
	if !NodePoolUsesScoreBasedConsolidation(cn.NodePool) {
		return false
	}
	for _, pod := range cn.reschedulablePods {
		if podutils.ToleratesDisruptedNoScheduleTaint(pod) {
			s.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("Pod %s/%s tolerates the disruption taint", pod.Namespace, pod.Name))...)
			return false
		}
	}
	return s.consolidation.ShouldDisrupt(ctx, cn)
}

// ComputeCommands builds its candidates and budgets from its own snapshot, so it ignores the controller's.
//
// nolint:gocyclo
func (s *ScoreBasedConsolidation) ComputeCommands(ctx context.Context, _ map[string]int, _ ...*Candidate) (_ []Command, err error) {
	ctx, endSpan := cxtracing.Start(ctx, scoreBasedSpanName)
	defer endSpan()
	if s.IsConsolidated() {
		return []Command{}, nil
	}

	start := s.clock.Now()
	deadline := start.Add(ScoreBasedConsolidationTimeoutDuration)
	snapshot, err := s.prepare(ctx, nil)
	if err != nil {
		return []Command{}, err
	}
	validCandidates, budgetBlockedPools, pacingBlockedPools := s.filterCandidates(snapshot)
	var choice moveChoice
	var stats searchStats
	eligible := len(validCandidates)
	var searchDuration time.Duration
	defer func() {
		// A failed pass writes no decision log; the controller reports the error.
		if err == nil {
			logScoreDecision(ctx, choice, stats, len(snapshot.candidates), eligible, budgetBlockedPools, pacingBlockedPools, searchDuration)
		}
	}()

	var feasible []*move
	feasible, stats, err = s.searchForMoves(ctx, validCandidates, snapshot, deadline)
	searchDuration = s.clock.Since(start)
	if err != nil {
		return []Command{}, err
	}
	if stats.TimedOut {
		ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
	}
	if len(feasible) == 0 {
		if stats.canMarkConsolidated(eligible, len(budgetBlockedPools) > 0 || len(pacingBlockedPools) > 0) {
			s.markConsolidated()
		}
		return []Command{}, nil
	}

	if err := s.waitForValidation(ctx, start); err != nil {
		return []Command{}, err
	}
	batchable := filterBatchable(feasible)
	var evaluator moveEvaluator
	evaluator, choice.Kept, err = s.validate(ctx, feasible, batchable)
	if err != nil {
		return []Command{}, err
	}
	choice.BatchMoves = buildBatchMoves(ctx, evaluator, choice.Kept, batchable)
	choice.Chosen = selectMove(s.recorder, choice.Kept, choice.BatchMoves.Moves)
	if choice.Chosen.Move == nil {
		return []Command{}, nil
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.Int("rank", choice.rank()), attribute.String("kind", string(choice.Chosen.Kind)))
	return []Command{choice.Chosen.Command}, nil
}

func (s *ScoreBasedConsolidation) filterCandidates(snapshot clusterSnapshot) ([]*Candidate, []string, []string) {
	var validCandidates []*Candidate
	nodePools := map[string]*v1.NodePool{}
	budgetBlockedPools := sets.New[string]()
	pacingBlockedPools := sets.New[string]()
	poolsWithValidCandidates := sets.New[string]()
	for _, candidate := range snapshot.candidates {
		pool := candidate.NodePool.Name
		nodePools[pool] = candidate.NodePool
		if snapshot.disruptionBudgetByNodePool[pool] == 0 {
			budgetBlockedPools.Insert(pool)
			continue
		}
		if !s.disruptionPacing.CandidateAllowed(candidate.NodePool, 0) {
			pacingBlockedPools.Insert(pool)
			if len(candidate.reschedulablePods) > 0 {
				continue
			}
		}
		poolsWithValidCandidates.Insert(pool)
		validCandidates = append(validCandidates, candidate)
	}
	for pool := range budgetBlockedPools {
		if inProgress := snapshot.budgetUsedUpByDisruptionsInProgress[pool]; inProgress > 0 {
			s.recorder.Publish(disruptionevents.NodePoolBudgetUsedByDisruptionsInProgress(nodePools[pool], s.Reason(), inProgress))
		}
	}
	for pool := range pacingBlockedPools.Difference(poolsWithValidCandidates) {
		s.recorder.Publish(disruptionevents.NodePoolHeldBackByPacing(nodePools[pool], s.Reason()))
	}
	return validCandidates, sets.List(budgetBlockedPools), sets.List(pacingBlockedPools)
}

func (s *ScoreBasedConsolidation) waitForValidation(ctx context.Context, start time.Time) error {
	_, endSpan := cxtracing.Start(ctx, scoreBasedSpanName+".validation_wait")
	defer endSpan()
	remainingValidationDelay := commandValidationDelay - s.clock.Since(start)
	if remainingValidationDelay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.clock.After(remainingValidationDelay):
		}
	}
	return nil
}

func (s *ScoreBasedConsolidation) searchForMoves(ctx context.Context, candidates []*Candidate, snapshot clusterSnapshot, deadline time.Time) ([]*move, searchStats, error) {
	ctx, endSpan := cxtracing.Start(ctx, scoreBasedSpanName+".search")
	defer endSpan()
	if len(candidates) == 0 {
		return nil, searchStats{}, nil
	}
	compute, err := s.newComputer(ctx, candidates)
	if err != nil {
		return nil, searchStats{}, err
	}
	search := moveSearch{
		evaluator: moveEvaluator{compute: compute, inputs: snapshot.scoreInputs},
		budgets:   snapshot.disruptionBudgetByNodePool,
		pacing:    s.disruptionPacing,
		deadline:  deadline,
		random:    rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec
		clk:       s.clock,
	}
	moves, stats := search.run(ctx, candidates)
	return moves, stats, nil
}

func (s *ScoreBasedConsolidation) newComputer(ctx context.Context, candidates []*Candidate) (consolidationComputer, error) {
	simulator, err := NewConsolidationSchedulingSimulator(ctx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder, candidates...)
	if err != nil {
		return nil, err
	}
	// Simulation announces every move it computes, but only the chosen move's sources get ConsolidationCandidate.
	consolidationWithoutCandidateEvents := s.consolidation
	consolidationWithoutCandidateEvents.recorder = withoutCandidateEvents{Recorder: s.recorder}
	return func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		return consolidationWithoutCandidateEvents.computeConsolidation(ctx, simulator, candidates...)
	}, nil
}

type withoutCandidateEvents struct {
	events.Recorder
}

func (r withoutCandidateEvents) Publish(evts ...events.Event) {
	r.Recorder.Publish(lo.Reject(evts, func(evt events.Event, _ int) bool { return evt.Reason == events.ConsolidationCandidate })...)
}

type moveChoice struct {
	Chosen     validatedMove
	Kept       []*move
	BatchMoves batchMovesBuildingResult
}

// selectMove keeps search order among equal scores, ranking a batch move right after the kept move it was built from.
func selectMove(recorder events.Recorder, kept []*move, batchMoves []validatedMove) validatedMove {
	var moves []validatedMove
	for _, m := range kept {
		if m.Validation != validationPassed {
			continue
		}
		moves = append(moves, m.validated())
		moves = append(moves, lo.Filter(batchMoves, func(b validatedMove, _ int) bool { return b.Move == m })...)
	}
	best := lo.MaxBy(moves, func(a, b validatedMove) bool { return a.Score.Total > b.Score.Total })
	if best.Move != nil {
		best.Command.EmitCandidateEvents(recorder)
		ScoreBasedMovesSelectedTotal.Inc(map[string]string{moveKindLabel: string(best.Kind)})
	}
	return best
}

// rank is the chosen move's position by search score. A batch move ranks as the kept move it was built from.
func (c moveChoice) rank() int {
	return slices.Index(c.Kept, c.Chosen.Move) + 1
}

// validatedMove is a kept move that passed validation, or a batch move built from it. The zero value is no move.
type validatedMove struct {
	Move    *move
	Kind    moveKind
	Command Command
	Score   moveScore
}

func (m *move) validated() validatedMove {
	return validatedMove{Move: m, Kind: m.kind(), Command: m.Command, Score: m.ValidationScore}
}

func (m *move) kind() moveKind {
	return lo.Ternary(len(m.Command.Candidates) == 2, moveKindPair, moveKindSingle)
}

func (s *ScoreBasedConsolidation) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonUnderutilized
}

func (s *ScoreBasedConsolidation) Class() string {
	return GracefulDisruptionClass
}

func (s *ScoreBasedConsolidation) ConsolidationType() string {
	return ScoreBasedConsolidationType
}
