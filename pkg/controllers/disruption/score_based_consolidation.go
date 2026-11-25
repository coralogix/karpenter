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
	"maps"
	"math/rand"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

var ScoreBasedConsolidationTimeoutDuration = 15 * time.Second

const ScoreBasedConsolidationType = "score-based"

func NodePoolUsesScoreBasedConsolidation(np *v1.NodePool) bool {
	if np == nil {
		return false
	}
	_, ok := np.Annotations[v1.ScoreBasedConsolidationAnnotationKey]
	return ok
}

type ScoreBasedConsolidation struct {
	consolidation
	validator Validator
}

func NewScoreBasedConsolidation(c consolidation, opts ...option.Function[MethodOptions]) *ScoreBasedConsolidation {
	o := option.Resolve(append([]option.Function[MethodOptions]{WithValidator(NewScoreBasedConsolidationValidator(c))}, opts...)...)
	return &ScoreBasedConsolidation{
		consolidation: c,
		validator:     o.validator,
	}
}

func NewScoreBasedConsolidationValidator(c consolidation) *ConsolidationValidator {
	s := &ScoreBasedConsolidation{consolidation: c}
	return &ConsolidationValidator{
		validation: validation{
			clock:         c.clock,
			cluster:       c.cluster,
			kubeClient:    c.kubeClient,
			provisioner:   c.provisioner,
			cloudProvider: c.cloudProvider,
			recorder:      c.recorder,
			queue:         c.queue,
			reason:        v1.DisruptionReasonUnderutilized,
		},
		filter:         s.ShouldDisrupt,
		validationType: s.ConsolidationType(),
	}
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

// nolint:gocyclo
func (s *ScoreBasedConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) (_ []Command, err error) {
	if s.IsConsolidated() {
		return []Command{}, nil
	}

	start := s.clock.Now()
	deadline := start.Add(ScoreBasedConsolidationTimeoutDuration)
	validCandidates, budgetBlockedPools, pacingBlockedPools := s.filterCandidates(disruptionBudgetMapping, candidates)
	var moves []*move
	var stats searchStats
	eligible := len(validCandidates)
	var searchDuration time.Duration
	defer func() {
		// A failed pass writes no decision log; the controller reports the error.
		if err == nil {
			logScoreDecision(ctx, moves, stats, len(candidates), eligible, budgetBlockedPools, pacingBlockedPools, searchDuration)
		}
	}()

	moves, stats, err = s.searchForMoves(ctx, validCandidates, deadline)
	searchDuration = s.clock.Since(start)
	if err != nil {
		return []Command{}, err
	}
	if stats.TimedOut {
		ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
	}
	if len(moves) == 0 {
		if stats.canMarkConsolidated(eligible, len(budgetBlockedPools) > 0 || len(pacingBlockedPools) > 0) {
			s.markConsolidated()
		}
		return []Command{}, nil
	}

	if err := s.waitForValidation(ctx, start); err != nil {
		return []Command{}, err
	}
	cmd, err := selectBestRescoredCommand(ctx, scoreMoveValidator{s.validator, s.disruptionPacing}, s.recorder, moves)
	if err != nil {
		return []Command{}, err
	}
	if len(cmd.Candidates) == 0 {
		return []Command{}, nil
	}
	return []Command{cmd}, nil
}

func (s *ScoreBasedConsolidation) filterCandidates(disruptionBudgetMapping map[string]int, candidates []*Candidate) ([]*Candidate, []string, []string) {
	var validCandidates []*Candidate
	budgetBlockedPools := sets.New[string]()
	pacingBlockedPools := sets.New[string]()
	for _, candidate := range candidates {
		pool := candidate.NodePool.Name
		if disruptionBudgetMapping[pool] == 0 {
			budgetBlockedPools.Insert(pool)
			continue
		}
		if !s.disruptionPacing.CandidateAllowed(candidate.NodePool, 0) {
			pacingBlockedPools.Insert(pool)
			if len(candidate.reschedulablePods) > 0 {
				continue
			}
		}
		validCandidates = append(validCandidates, candidate)
	}
	return validCandidates, slices.Sorted(maps.Keys(budgetBlockedPools)), slices.Sorted(maps.Keys(pacingBlockedPools))
}

func (s *ScoreBasedConsolidation) waitForValidation(ctx context.Context, start time.Time) error {
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

func (s *ScoreBasedConsolidation) searchForMoves(ctx context.Context, candidates []*Candidate, deadline time.Time) ([]*move, searchStats, error) {
	if len(candidates) == 0 {
		return nil, searchStats{}, nil
	}
	simulator, err := NewConsolidationSchedulingSimulator(ctx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder, candidates...)
	if err != nil {
		return nil, searchStats{}, err
	}
	sources := make([][]*Candidate, len(candidates))
	for i, c := range candidates {
		sources[i] = []*Candidate{c}
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	moves, stats := searchMoves(ctx, sources, deadline, func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		return s.computeConsolidation(ctx, simulator, candidates...)
	}, rng, s.clock)
	return moves, stats, nil
}

func selectBestRescoredCommand(ctx context.Context, validator Validator, recorder events.Recorder, moves []*move) (Command, error) {
	for _, m := range moves {
		if err := rescore(ctx, validator, m); err != nil {
			return Command{}, err
		}
	}
	passed := lo.Filter(moves, func(e *move, _ int) bool { return e.Validation == validationPassed })
	if len(passed) == 0 {
		if moves[0].Validation == validationFailed {
			moves[0].Command.EmitRejectedEvents(recorder, moves[0].FailureReason)
		}
		return Command{}, nil
	}
	best := lo.MaxBy(passed, func(a, b *move) bool { return a.ValidationScore.Savings > b.ValidationScore.Savings })
	best.Selected = true
	return best.Command, nil
}

// rescore validates the move and records the outcome on m. Only errors other than validation failures are returned.
func rescore(ctx context.Context, validator Validator, m *move) error {
	validated, err := validator.Validate(ctx, m.Command, 0)
	if err != nil {
		if !IsValidationError(err) {
			return fmt.Errorf("validating score-based consolidation, %w", err)
		}
		m.Validation, m.FailureReason = validationFailed, scoreBasedValidationFailureReason(err)
		return nil
	}
	m.Command = validated
	m.ValidationScore = scoreMove(validated)
	m.Validation = lo.Ternary(m.ValidationScore.Savings > 0, validationPassed, validationNoSavings)
	return nil
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
