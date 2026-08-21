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
)

var ScoreBasedConsolidationTimeoutDuration = 20 * time.Second

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

// NodePoolUsesScoreBasedConsolidation reports whether the NodePool opts into score-based consolidation.
func NodePoolUsesScoreBasedConsolidation(np *v1.NodePool) bool {
	if np == nil || np.Annotations == nil {
		return false
	}
	_, ok := np.Annotations[v1.ScoreBasedConsolidationAnnotationKey]
	return ok
}

// ScoreBasedConsolidation is a disruption method for NodePools that opt in via annotation.
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
	return s.consolidation.ShouldDisrupt(ctx, cn)
}

// ComputeCommands generates a disruption command given candidates.
//
//nolint:gocyclo
func (s *ScoreBasedConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	if s.IsConsolidated() {
		return []Command{}, nil
	}
	candidates = s.SortCandidates(candidates)

	deadline := s.clock.Now().Add(ScoreBasedConsolidationTimeoutDuration)
	constrainedByBudgets := false
	constrainedByPace := false

	var moveSets []moveSet
	for _, candidate := range candidates {
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			constrainedByBudgets = true
			continue
		}
		if len(candidate.reschedulablePods) > 0 && !s.underutilizedPace.candidateAllowed(candidate.NodePool, 0) {
			constrainedByPace = true
			continue
		}
		moveSets = append(moveSets, moveSet{Nodes: []*Candidate{candidate}})
	}

	eval, evaluated, err := evaluateMoveSetsPar(ctx, moveSets, deadline, s.computeConsolidation, ScoreBasedConsolidationType)
	if err != nil {
		return []Command{}, err
	}
	if eval == nil {
		timedOut := evaluated < len(moveSets)
		if timedOut {
			log.FromContext(ctx).V(1).Info(fmt.Sprintf("abandoning score-based consolidation due to timeout after evaluating %d candidates", evaluated))
		}
		if !timedOut && !constrainedByBudgets && !constrainedByPace {
			s.markConsolidated()
		}
		return []Command{}, nil
	}

	if _, err = s.validator.Validate(ctx, eval.Command, consolidationTTL); err != nil {
		if IsValidationError(err) {
			reason := getValidationFailureReason(err)
			eval.Command.EmitRejectedEvents(s.recorder, reason)
			return []Command{}, nil
		}
		return []Command{}, fmt.Errorf("validating score-based consolidation, %w", err)
	}
	return []Command{eval.Command}, nil
}

func evaluateMoveSetsPar(
	ctx context.Context,
	moveSets []moveSet,
	deadline time.Time,
	compute consolidationComputer,
	consolidationType string,
) (*moveSetEvaluation, int, error) {
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
		eval := evaluateMoveSet(ctx, moveSet, compute)
		if ctx.Err() == nil {
			evaluated.Add(1)
			return eval, eval != nil, nil
		}
		return nil, false, nil
	})

	winner, found, err := rill.First(valid)
	if err != nil {
		return nil, int(evaluated.Load()), err
	}
	if !found {
		if ctx.Err() != nil {
			ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: consolidationType})
		}
		return nil, int(evaluated.Load()), nil
	}
	return winner, int(evaluated.Load()), nil
}

func evaluateMoveSet(ctx context.Context, moveSet moveSet, compute consolidationComputer) *moveSetEvaluation {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	cmd, err := compute(ctx, moveSet.Nodes...)
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	if err != nil {
		log.FromContext(ctx).Error(err, "failed computing score-based consolidation")
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

func moveSetPriorityScore(moveSet moveSet) float64 {
	var maxScore float64
	for _, node := range moveSet.Nodes {
		if score := nodePriorityScore(node); score > maxScore {
			maxScore = score
		}
	}
	return maxScore
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
