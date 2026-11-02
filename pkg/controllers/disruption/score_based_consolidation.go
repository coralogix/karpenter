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
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/awslabs/operatorpkg/option"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/cxtracing"
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

//nolint:gocyclo // Pass orchestration keeps search, validation, and tracing together.
func (s *ScoreBasedConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) (_ []Command, err error) {
	ctx, endPass := cxtracing.Start(ctx, "karpenter.disruption.score_based_consolidation")
	defer endPass()
	if s.IsConsolidated() {
		return []Command{}, nil
	}
	start := s.clock.Now()
	deadline := start.Add(ScoreBasedConsolidationTimeoutDuration)
	validCandidates, blocked := s.filterCandidates(disruptionBudgetMapping, candidates)
	var reason string
	var evals []*moveSetEvaluation
	var inputs scoreInputs
	var stats searchStats
	eligible := len(validCandidates)
	var searchDuration time.Duration
	defer func() {
		// A failed pass writes no decision log; the controller reports the error.
		if err == nil {
			logScoreDecision(ctx, evals, stats, eligible, blocked, searchDuration, reason)
		}
	}()

	evals, inputs, stats, err = s.searchForMoveSets(ctx, validCandidates, deadline, disruptionBudgetMapping)
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

	waitCtx, endWait := cxtracing.Start(ctx, "validation_wait")
	waitErr := s.waitForValidation(waitCtx, start)
	endWait()
	if waitErr != nil {
		return []Command{}, waitErr
	}
	reason = "all_rejected_by_validation"
	validateCtx, endValidate := cxtracing.Start(ctx, "validate")
	cmd, err := selectBestRescoredCommand(validateCtx, scoreMoveValidator{s.validator, s.disruptionPacing}, s.recorder, evals, inputs)
	for i, eval := range evals {
		if err == nil && eval.Selected {
			trace.SpanFromContext(validateCtx).SetAttributes(attribute.Int("rank", i+1), attribute.String("kind", scoreMoveKind(cmd)))
			break
		}
	}
	endValidate()
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
	paced := map[string]*v1.NodePool{}
	allowed := sets.New[string]()
	for _, candidate := range candidates {
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			blocked.Insert(candidate.NodePool.Name)
		} else if len(candidate.reschedulablePods) > 0 && !s.disruptionPacing.CandidateAllowed(candidate.NodePool, 0) {
			blocked.Insert(candidate.NodePool.Name)
			paced[candidate.NodePool.Name] = candidate.NodePool
		} else {
			allowed.Insert(candidate.NodePool.Name)
			validCandidates = append(validCandidates, candidate)
		}
	}
	for name, pool := range paced {
		if !allowed.Has(name) {
			s.recorder.Publish(disruptionevents.NodePoolBlockedByPacing(pool))
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

func (s *ScoreBasedConsolidation) searchForMoveSets(ctx context.Context, candidates []*Candidate, deadline time.Time, budgets map[string]int) ([]*moveSetEvaluation, scoreInputs, searchStats, error) {
	ctx, endSearch := cxtracing.Start(ctx, "search")
	defer endSearch()
	if len(candidates) == 0 {
		return nil, scoreInputs{}, searchStats{}, nil
	}
	simulator, err := NewConsolidationSchedulingSimulator(ctx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder, candidates...)
	if err != nil {
		return nil, scoreInputs{}, searchStats{}, err
	}
	prices, err := s.nodePrices(ctx, simulator.nodes)
	if err != nil {
		return nil, scoreInputs{}, searchStats{}, err
	}
	moves := make([]moveSet, len(candidates))
	for i, c := range candidates {
		moves[i] = moveSet{Nodes: []*Candidate{c}}
	}
	inputs := newScoreInputs(simulator.nodes, candidates, prices, s.recorder)
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	searchConsolidation := s.consolidation
	searchConsolidation.recorder = candidateEventFilter{s.recorder}
	evals, stats := searchAllMoves(ctx, moves, deadline, func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		return searchConsolidation.computeConsolidation(ctx, simulator, candidates...)
	}, inputs, rng, s.clock, budgets, s.disruptionPacing)
	return evals, inputs, stats, nil
}

func (s *ScoreBasedConsolidation) nodePrices(ctx context.Context, nodes state.StateNodes) (map[string]float64, error) {
	_, instanceTypes, err := BuildNodePoolMap(ctx, s.kubeClient, s.cloudProvider)
	if err != nil {
		return nil, err
	}
	prices := map[string]float64{}
	for _, node := range nodes {
		poolName := node.Labels()[v1.NodePoolLabelKey]
		it := instanceTypes[poolName][node.Labels()[corev1.LabelInstanceTypeStable]]
		if it == nil {
			continue
		}
		prices[node.Name()] = resolveNodePrice(node, it)
	}
	return prices, nil
}

//nolint:gocyclo // Validation errors, rescoring, and selection are handled in one pass.
func selectBestRescoredCommand(ctx context.Context, validator Validator, recorder events.Recorder, evals []*moveSetEvaluation, inputs scoreInputs) (Command, error) {
	var bestCmd Command
	var bestScore float64
	var bestEval *moveSetEvaluation

	for _, eval := range evals {
		validated, err := validator.Validate(ctx, eval.Command, 0)
		if err != nil {
			reason := scoreBasedValidationFailureReason(err)
			eval.Validation = "failed: " + reason
			if IsValidationError(err) {
				FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
				continue
			}
			return Command{}, fmt.Errorf("validating score-based consolidation, %w", err)
		}
		breakdown, standbyPrices := inputs.score(eval.Command, validated.Results)
		score := breakdown.Total
		eval.Breakdown = breakdown
		eval.RescoredScore = score
		eval.StandbyPrices = standbyPrices
		if breakdown.Savings <= 0 || score <= 0 {
			eval.Validation = "failed: no_savings"
			if breakdown.Savings > 0 {
				eval.Validation = "failed: non_positive_score"
			}
			FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
			continue
		}
		eval.Validation = "passed"
		if bestEval == nil || score > bestScore {
			if bestEval != nil {
				bestEval.Selected = false
			}
			bestScore = score
			bestCmd = validated
			bestEval = eval
			eval.Selected = true
		}
	}
	for _, eval := range evals {
		if eval == bestEval {
			break
		}
		if reason, failed := strings.CutPrefix(eval.Validation, "failed: "); failed {
			eval.Command.EmitRejectedEvents(recorder, reason)
		}
	}
	if bestEval == nil {
		return Command{}, nil
	}
	bestCmd.EmitCandidateEvents(recorder)
	ScoreBasedMovesSelectedTotal.Inc(map[string]string{"kind": scoreMoveKind(bestCmd)})
	return bestCmd, nil
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

// candidateEventFilter preserves simulation blockers while suppressing speculative candidates.
type candidateEventFilter struct{ events.Recorder }

func (r candidateEventFilter) Publish(evts ...events.Event) {
	for _, event := range evts {
		if event.Reason != events.ConsolidationCandidate {
			r.Recorder.Publish(event)
		}
	}
}
