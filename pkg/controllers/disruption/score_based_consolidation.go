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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	"github.com/awslabs/operatorpkg/option"
	"github.com/destel/rill"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

var ScoreBasedConsolidationTimeoutDuration = 15 * time.Second

var scoreBasedValidEvaluationsTarget = 10

var scoreBasedMoveSetParallelism = runtime.GOMAXPROCS(0)

const ScoreBasedConsolidationType = "score-based"

type moveSet struct {
	Nodes []*Candidate
}

type moveSetEvaluation struct {
	Command    Command
	Score      float64
	Validation string
}

type consolidationComputer func(ctx context.Context, candidates ...*Candidate) (Command, error)

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

func (s *ScoreBasedConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	if s.IsConsolidated() {
		return []Command{}, nil
	}

	start := s.clock.Now()
	deadline := start.Add(ScoreBasedConsolidationTimeoutDuration)
	validCandidates, blocked := s.filterCandidates(disruptionBudgetMapping, candidates)
	reason := "no_saving_move"
	var evals []*moveSetEvaluation
	var stats searchStats
	eligible := len(validCandidates)
	var searchDuration time.Duration
	defer func() {
		logScoreDecision(ctx, evals, stats, eligible, blocked, searchDuration, reason)
	}()

	var err error
	evals, stats, err = s.searchForMoveSets(ctx, validCandidates, deadline)
	searchDuration = s.clock.Since(start)
	if err != nil {
		reason = "simulation_error"
		return []Command{}, err
	}
	if len(candidates) == 0 {
		reason = "no_candidates"
	} else if len(validCandidates) == 0 {
		reason = "blocked_by_budget_or_pacing"
	}
	if stats.TimedOut {
		ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
		reason = "timed_out"
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

func (s *ScoreBasedConsolidation) filterCandidates(disruptionBudgetMapping map[string]int, candidates []*Candidate) ([]*Candidate, []string) {
	var validCandidates []*Candidate
	blocked := map[string]bool{}
	for _, candidate := range candidates {
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			blocked[candidate.NodePool.Name] = true
			continue
		}
		if len(candidate.reschedulablePods) > 0 && !s.disruptionPacing.CandidateAllowed(candidate.NodePool, 0) {
			blocked[candidate.NodePool.Name] = true
			continue
		}
		validCandidates = append(validCandidates, candidate)
	}
	names := []string{}
	for name := range blocked {
		names = append(names, name)
	}
	sort.Strings(names)
	return validCandidates, names
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
	evals, stats := searchMoves(ctx, moves, deadline, func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		cmd, err := s.computeConsolidation(ctx, simulator, candidates...)
		if err == nil {
			cmd.scoreBasedStandbyPrices = prices
		}
		return cmd, err
	}, rand.New(rand.NewSource(time.Now().UnixNano())), s.clock) //nolint:gosec
	return evals, stats, nil
}

func (s *ScoreBasedConsolidation) nodePrices(ctx context.Context) (map[string]float64, error) {
	_, instanceTypes, err := BuildNodePoolMap(ctx, s.kubeClient, s.cloudProvider)
	if err != nil {
		return nil, err
	}
	prices := map[string]float64{}
	for _, node := range s.cluster.DeepCopyNodes() {
		poolName := node.Labels()[v1.NodePoolLabelKey]
		it := instanceTypes[poolName][node.Labels()[corev1.LabelInstanceTypeStable]]
		if it == nil {
			continue
		}
		prices[node.Name()] = resolveNodePrice(node, it)
	}
	return prices, nil
}

type searchStats struct {
	Evaluated int
	Errors    int
	NoOp      int
	NoSavings int
	TimedOut  bool
}

func searchMoves(ctx context.Context, moves []moveSet, deadline time.Time, compute consolidationComputer, random *rand.Rand, clk clock.Clock) ([]*moveSetEvaluation, searchStats) {
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
		savings := cmd.EstimatedSavings() - cmd.standbyCost()
		if savings <= 0 {
			stats.NoSavings++
			return nil, false, nil
		}
		return &moveSetEvaluation{Command: cmd, Score: savings, Validation: "not_attempted"}, true, nil
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

func (s searchStats) canMarkConsolidated(eligible int, blocked bool) bool {
	return s.Evaluated == eligible && s.Errors == 0 && !blocked
}
