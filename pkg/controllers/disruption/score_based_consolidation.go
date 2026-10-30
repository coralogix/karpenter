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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/awslabs/operatorpkg/option"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

var ScoreBasedConsolidationTimeoutDuration = 15 * time.Second

const ScoreBasedConsolidationType = "score-based"

func NodePoolUsesScoreBasedConsolidation(np *v1.NodePool) bool {
	if np == nil || np.Annotations == nil {
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

			validateResults: validateStandbyDestinations,
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

func (s *ScoreBasedConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) (_ []Command, err error) {
	if s.IsConsolidated() {
		return []Command{}, nil
	}

	start := s.clock.Now()
	deadline := start.Add(ScoreBasedConsolidationTimeoutDuration)
	validCandidates, blocked := s.filterCandidates(disruptionBudgetMapping, candidates)
	var reason string
	var evals []*moveSetEvaluation
	var stats searchStats
	eligible := len(validCandidates)
	var searchDuration time.Duration
	defer func() {
		// A failed pass writes no decision log; the controller reports the error.
		if err == nil {
			logScoreDecision(ctx, evals, stats, eligible, blocked, searchDuration, reason)
		}
	}()

	evals, stats, err = s.searchForMoveSets(ctx, validCandidates, deadline)
	searchDuration = s.clock.Since(start)
	if err != nil {
		return []Command{}, err
	}
	reason = searchOutcomeReason(len(candidates), eligible, stats.TimedOut)
	if stats.TimedOut {
		ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
	}
	if len(evals) == 0 {
		if stats.canMarkConsolidated(eligible, len(blocked) > 0) {
			s.markConsolidated()
		}
		return []Command{}, nil
	}

	if err := s.waitForValidation(ctx, start); err != nil {
		return []Command{}, err
	}
	reason = "all_rejected_by_validation"
	cmd, err := selectFirstStillValidCommand(ctx, scoreMoveValidator{s.validator, s.disruptionPacing}, s.recorder, evals)
	if err != nil {
		return []Command{}, err
	}
	if len(cmd.Candidates) == 0 {
		return []Command{}, nil
	}
	return []Command{cmd}, nil
}

// searchOutcomeReason is the no-move reason when the search found nothing.
func searchOutcomeReason(candidates, eligible int, timedOut bool) string {
	switch {
	case timedOut:
		return "timed_out"
	case candidates == 0:
		return "no_candidates"
	case eligible == 0:
		return "blocked_by_budget_or_pacing"
	}
	return "no_saving_move"
}

func (s *ScoreBasedConsolidation) filterCandidates(disruptionBudgetMapping map[string]int, candidates []*Candidate) ([]*Candidate, []string) {
	var validCandidates []*Candidate
	blocked := sets.New[string]()
	for _, candidate := range candidates {
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			blocked.Insert(candidate.NodePool.Name)
		} else if len(candidate.reschedulablePods) > 0 && !s.disruptionPacing.CandidateAllowed(candidate.NodePool, 0) {
			blocked.Insert(candidate.NodePool.Name)
		} else {
			validCandidates = append(validCandidates, candidate)
		}
	}
	return validCandidates, slices.Sorted(maps.Keys(blocked))
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

func (s *ScoreBasedConsolidation) searchForMoveSets(ctx context.Context, candidates []*Candidate, deadline time.Time) ([]*moveSetEvaluation, searchStats, error) {
	if len(candidates) == 0 {
		return nil, searchStats{}, nil
	}
	simulator, err := NewConsolidationSchedulingSimulator(ctx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder, candidates...)
	if err != nil {
		return nil, searchStats{}, err
	}
	prices, err := s.nodePrices(ctx)
	if err != nil {
		return nil, searchStats{}, err
	}
	moves := make([]moveSet, len(candidates))
	for i, c := range candidates {
		moves[i] = moveSet{Nodes: []*Candidate{c}}
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	evals, stats := searchMoves(ctx, moves, deadline, func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		return s.computeConsolidation(ctx, simulator, candidates...)
	}, prices, rng, s.clock)
	return evals, stats, nil
}

func (s *ScoreBasedConsolidation) nodePrices(ctx context.Context) (map[string]float64, error) {
	_, instanceTypes, err := BuildNodePoolMap(ctx, s.kubeClient, s.cloudProvider)
	if err != nil {
		return nil, err
	}
	prices := map[string]float64{}
	for node := range s.cluster.Nodes() {
		poolName := node.Labels()[v1.NodePoolLabelKey]
		it := instanceTypes[poolName][node.Labels()[corev1.LabelInstanceTypeStable]]
		if it == nil {
			continue
		}
		prices[node.Name()] = resolveNodePrice(node, it)
	}
	return prices, nil
}

func selectFirstStillValidCommand(ctx context.Context, validator Validator, recorder events.Recorder, evals []*moveSetEvaluation) (Command, error) {
	var firstValidationReason string

	for i, eval := range evals {
		if _, err := validator.Validate(ctx, eval.Command, 0); err != nil {
			reason := scoreBasedValidationFailureReason(err)
			eval.Validation = "failed: " + reason
			if IsValidationError(err) {
				if i == 0 {
					firstValidationReason = reason
				}
				continue
			}
			return Command{}, fmt.Errorf("validating score-based consolidation, %w", err)
		}
		eval.Validation = "passed"
		return eval.Command, nil
	}
	if firstValidationReason != "" {
		evals[0].Command.EmitRejectedEvents(recorder, firstValidationReason)
	}
	return Command{}, nil
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
