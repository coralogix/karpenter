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
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/destel/rill"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var ScoreBasedConsolidationTimeoutDuration = 20 * time.Second

var scoreBasedValidEvaluationsTarget = 10

var scoreBasedMoveSetParallelism = runtime.GOMAXPROCS(0)

const ScoreBasedConsolidationType = "score-based"

type moveSet struct {
	Nodes []*Candidate
}

type moveSetEvaluation struct {
	Command Command
	Score   float64
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
		filter:         s.shouldCompact,
		validationType: s.ConsolidationType(),
	}
}

func (s *ScoreBasedConsolidation) ShouldDisrupt(ctx context.Context, cn *Candidate) bool {
	if cn == nil || !NodePoolUsesScoreBasedConsolidation(cn.NodePool) {
		return false
	}
	if standby.IsNodeClaimActivating(cn.NodeClaim) {
		return false
	}
	if standby.IsNodeClaimStandby(cn.NodeClaim) || standby.HasNodeTaint(cn.Node) {
		return s.ShouldReclaim(ctx, cn)
	}
	if s.shouldCompact(ctx, cn) {
		return true
	}
	return s.ShouldReclaim(ctx, cn)
}

// shouldCompact admits only eligible, non-empty active nodes into normal compaction.
// It deliberately excludes empty nodes even when the combined method admits them for reclamation.
func (s *ScoreBasedConsolidation) shouldCompact(ctx context.Context, cn *Candidate) bool {
	if !s.compactionCandidateEligible(ctx, cn) {
		return false
	}
	return s.compactionCandidateNonEmpty(ctx, cn)
}

func (s *ScoreBasedConsolidation) compactionCandidateEligible(ctx context.Context, cn *Candidate) bool {
	if cn == nil || cn.Node == nil || cn.NodeClaim == nil || cn.NodePool == nil {
		return false
	}
	if !NodePoolUsesScoreBasedConsolidation(cn.NodePool) {
		return false
	}
	if standby.IsNodeClaimActivating(cn.NodeClaim) || standby.IsNodeClaimStandby(cn.NodeClaim) || standby.HasNodeTaint(cn.Node) {
		return false
	}
	return s.consolidation.ShouldDisrupt(ctx, cn)
}

func (s *ScoreBasedConsolidation) compactionCandidateNonEmpty(ctx context.Context, cn *Candidate) bool {
	// A reschedulable non-daemon pod proves the node is non-empty. Check the API when the
	// candidate has no reschedulable pods to include terminal and terminating bound pods.
	if len(cn.reschedulablePods) > 0 {
		return true
	}
	empty, err := reclamationEmpty(ctx, s.kubeClient, cn.Node)
	return err == nil && !empty
}

//nolint:gocyclo
func (s *ScoreBasedConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	reclamationCommand, reclamationPools, err := s.computeReclamationCommand(ctx, candidates)
	if err != nil {
		return []Command{}, err
	}
	reclamationCommands := []Command{}
	if reclamationCommand != nil {
		reclamationCommands = append(reclamationCommands, *reclamationCommand)
	}
	if s.IsConsolidated() && len(reclamationPools) == 0 {
		return reclamationCommands, nil
	}

	// A pool with due, empty capacity is handled only by reclamation in this pass.
	// Keep normal compaction available for other pools.
	normalCandidates := make([]*Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil || candidate.NodePool == nil || reclamationPools[candidate.NodePool.Name] || standby.IsNodeClaimActivating(candidate.NodeClaim) || standby.IsNodeClaimStandby(candidate.NodeClaim) || standby.HasNodeTaint(candidate.Node) {
			continue
		}
		if s.shouldCompact(ctx, candidate) {
			normalCandidates = append(normalCandidates, candidate)
		}
	}
	candidates = normalCandidates
	if len(candidates) == 0 {
		return reclamationCommands, nil
	}

	start := s.clock.Now()
	deadline := start.Add(ScoreBasedConsolidationTimeoutDuration)
	constrainedByBudgets := false
	constrainedByPace := false

	candidates = s.SortCandidates(candidates)
	var validCandidates []*Candidate
	for _, candidate := range candidates {
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			constrainedByBudgets = true
			continue
		}
		if len(candidate.reschedulablePods) > 0 && !s.underutilizedPace.candidateAllowed(candidate.NodePool, 0) {
			constrainedByPace = true
			continue
		}
		validCandidates = append(validCandidates, candidate)
	}

	evals, evaluated, err := s.searchForMoveSets(ctx, validCandidates, deadline)
	if err != nil {
		return reclamationCommands, err
	}
	if len(evals) == 0 {
		timedOut := evaluated < len(validCandidates)
		if timedOut {
			log.FromContext(ctx).V(1).Info(fmt.Sprintf("abandoning score-based consolidation due to timeout after evaluating %d candidates", evaluated))
		}
		if !timedOut && !constrainedByBudgets && !constrainedByPace {
			s.markConsolidated()
		}
		return reclamationCommands, nil
	}

	sort.Slice(evals, func(i, j int) bool {
		return evals[i].Score > evals[j].Score
	})

	remainingValidationDelay := commandValidationDelay - s.clock.Since(start)
	if remainingValidationDelay > 0 {
		select {
		case <-ctx.Done():
			return reclamationCommands, ctx.Err()
		case <-s.clock.After(remainingValidationDelay):
		}
	}
	cmd, err := selectFirstStillValidCommand(ctx, s.validator, s.recorder, evals)
	if err != nil {
		return reclamationCommands, err
	}
	if len(cmd.Candidates) == 0 {
		return reclamationCommands, nil
	}
	cmd.Action = EvacuateAction
	return append(reclamationCommands, cmd), nil
}

func (s *ScoreBasedConsolidation) searchForMoveSets(ctx context.Context, validCandidates []*Candidate, deadline time.Time) ([]*moveSetEvaluation, int, error) {
	moveSets := make([]moveSet, len(validCandidates))
	for i, candidate := range validCandidates {
		moveSets[i] = moveSet{Nodes: []*Candidate{candidate}}
	}
	simulator, err := NewConsolidationSchedulingSimulator(ctx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder, validCandidates...)
	if err != nil {
		return nil, 0, err
	}
	compute := func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		return s.computeConsolidation(ctx, simulator, candidates...)
	}
	return evaluateMoveSetsPar(ctx, moveSets, deadline, compute)
}

func evaluateMoveSetsPar(
	ctx context.Context,
	moveSets []moveSet,
	deadline time.Time,
	compute consolidationComputer,
) ([]*moveSetEvaluation, int, error) {
	searchStart := time.Now()
	stats := &moveSetSearchStats{}

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
		eval := evaluateMoveSet(ctx, moveSet, compute, stats)
		if ctx.Err() == nil {
			evaluated.Add(1)
			return eval, eval != nil, nil
		}
		return nil, false, nil
	})

	evals, err := collectMoveSetEvaluations(valid, scoreBasedValidEvaluationsTarget)
	timedOut := ctx.Err() != nil && len(evals) == 0
	logMoveSetSearchComplete(ctx, len(moveSets), int(evaluated.Load()), len(evals), timedOut, time.Since(searchStart), stats)
	if err != nil {
		return nil, int(evaluated.Load()), err
	}
	if len(evals) == 0 {
		if timedOut {
			ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: ScoreBasedConsolidationType})
		}
		return nil, int(evaluated.Load()), nil
	}
	return evals, int(evaluated.Load()), nil
}

func evaluateMoveSet(ctx context.Context, moveSet moveSet, compute consolidationComputer, stats *moveSetSearchStats) *moveSetEvaluation {
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
		return nil
	}
	if cmd.EstimatedSavings() <= 0 {
		return nil
	}
	return &moveSetEvaluation{
		Command: cmd,
		Score:   moveSetPriorityScore(moveSet),
	}
}

func collectMoveSetEvaluations(valid <-chan rill.Try[*moveSetEvaluation], limit int) ([]*moveSetEvaluation, error) {
	defer rill.Discard(valid)

	evals := make([]*moveSetEvaluation, 0, limit)
	for a := range valid {
		if a.Error != nil {
			return nil, a.Error
		}
		evals = append(evals, a.Value)
		if len(evals) >= limit {
			break
		}
	}
	return evals, nil
}

func moveSetPriorityScore(moveSet moveSet) float64 {
	var maxScore float64
	for _, node := range moveSet.Nodes {
		if score := nodePriorityScore(node); score > maxScore {
			maxScore = score
		}
	}
	return maxScore
}

func selectFirstStillValidCommand(ctx context.Context, validator Validator, recorder events.Recorder, evals []*moveSetEvaluation) (Command, error) {
	var firstValidationReason string

	for i, eval := range evals {
		if _, err := validator.Validate(ctx, eval.Command, 0); err != nil {
			if IsValidationError(err) {
				if i == 0 {
					firstValidationReason = getValidationFailureReason(err)
				}
				continue
			}
			return Command{}, fmt.Errorf("validating score-based consolidation, %w", err)
		}
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

// SortCandidates orders candidates by nodePriority (highest first).
func (s *ScoreBasedConsolidation) SortCandidates(candidates []*Candidate) []*Candidate {
	sort.Slice(candidates, func(i, j int) bool {
		return nodePriorityScore(candidates[i]) > nodePriorityScore(candidates[j])
	})
	return candidates
}
