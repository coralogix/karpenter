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
	"fmt"
	"runtime"
	"sort"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/destel/rill"
	"go.opentelemetry.io/otel/attribute"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cxtracing"
	"sigs.k8s.io/karpenter/pkg/events"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const (
	scoreBasedConsolidationSpan              = "karpenter.disruption.score_based_consolidation"
	scoreBasedConsolidationPrepareSpan       = "karpenter.disruption.score_based_consolidation.prepare_compaction_candidates"
	scoreBasedConsolidationFilterSpan        = "karpenter.disruption.score_based_consolidation.filter_valid_candidates"
	scoreBasedConsolidationMoveSetSearchSpan = "karpenter.disruption.score_based_consolidation.move_set_search"
	scoreBasedConsolidationNewSimulatorSpan  = "karpenter.disruption.score_based_consolidation.new_scheduling_simulator"
	scoreBasedConsolidationTTLWaitSpan       = "karpenter.disruption.score_based_consolidation.consolidation_ttl_wait"
	scoreBasedConsolidationValidateSpan      = "karpenter.disruption.score_based_consolidation.validate_command"
)

var ScoreBasedConsolidationTimeoutDuration = 15 * time.Second

// scoreBasedMoveSetResultLimit is the number of highest-scoring move sets returned after search completes.
var scoreBasedMoveSetResultLimit = 10

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
	return s.shouldCompact(ctx, cn)
}

// shouldCompact admits only eligible, non-empty active nodes into normal compaction.
// It deliberately excludes empty nodes even when the combined method admits them for reclamation.
func (s *ScoreBasedConsolidation) shouldCompact(ctx context.Context, cn *Candidate) bool {
	if !s.compactionCandidateEligible(cn) {
		return false
	}
	return s.compactionCandidateNonEmpty(ctx, cn)
}

func (s *ScoreBasedConsolidation) compactionCandidateEligible(cn *Candidate) bool {
	if cn == nil || cn.Node == nil || cn.NodeClaim == nil || cn.NodePool == nil {
		return false
	}
	if !NodePoolUsesScoreBasedConsolidation(cn.NodePool) {
		return false
	}
	if standby.IsNodeClaimActivating(cn.NodeClaim) || standby.IsNodeClaimStandby(cn.NodeClaim) || standby.HasNodeTaint(cn.Node) {
		return false
	}
	return s.shouldDisruptIgnoringConsolidateAfter(cn)
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
	if s.IsConsolidated() {
		logScoreBasedCompactionNoMove(ctx, scoreBasedNoMoveAlreadyConsolidated, scoreBasedNoMoveDetails{
			candidateCount:           len(candidates),
			compactionCandidateCount: -1,
		})
		return []Command{}, nil
	}

	prepareCtx, stopPrepare := cxtracing.Measure(ctx, nil, scoreBasedConsolidationPrepareSpan)
	inputCandidateCount := len(candidates)
	normalCandidates := make([]*Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil || candidate.NodePool == nil || standby.IsNodeClaimActivating(candidate.NodeClaim) || standby.IsNodeClaimStandby(candidate.NodeClaim) || standby.HasNodeTaint(candidate.Node) {
			continue
		}
		if s.shouldCompact(prepareCtx, candidate) {
			normalCandidates = append(normalCandidates, candidate)
		}
	}
	stopPrepare()
	candidates = normalCandidates
	if len(candidates) == 0 {
		reason := scoreBasedNoMoveAllCandidatesFiltered
		if inputCandidateCount == 0 {
			reason = scoreBasedNoMoveNoEligibleCandidates
		}
		logScoreBasedCompactionNoMove(ctx, reason, scoreBasedNoMoveDetails{
			candidateCount:           inputCandidateCount,
			compactionCandidateCount: 0,
		})
		return []Command{}, nil
	}

	start := s.clock.Now()
	deadline := start.Add(ScoreBasedConsolidationTimeoutDuration)
	budgetBlockedCandidateCount := 0
	paceBlockedCandidateCount := 0
	budgetBlockedNodePools := map[string]struct{}{}
	paceBlockedNodePools := map[string]struct{}{}

	_, stopFilter := cxtracing.Measure(ctx, nil, scoreBasedConsolidationFilterSpan,
		attribute.Int("compaction_candidate_count", len(candidates)),
	)
	candidates = s.SortCandidates(candidates)
	var validCandidates []*Candidate
	for _, candidate := range candidates {
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			budgetBlockedCandidateCount++
			if _, checked := budgetBlockedNodePools[candidate.NodePool.Name]; !checked {
				budgetBlockedNodePools[candidate.NodePool.Name] = struct{}{}
				if s.budgetExhaustedByInFlightDisruptions(candidate.NodePool) {
					s.recorder.Publish(scoreBasedNodePoolBlockedByInFlightDisruptions(candidate.NodePool))
				}
			}
			continue
		}
		if len(candidate.reschedulablePods) > 0 && !s.underutilizedPace.candidateAllowed(candidate.NodePool, 0) {
			paceBlockedCandidateCount++
			if _, reported := paceBlockedNodePools[candidate.NodePool.Name]; !reported {
				s.recorder.Publish(scoreBasedNodePoolBlockedByPace(candidate.NodePool))
				paceBlockedNodePools[candidate.NodePool.Name] = struct{}{}
			}
			continue
		}
		validCandidates = append(validCandidates, candidate)
	}
	stopFilter()
	if len(validCandidates) == 0 {
		logScoreBasedCompactionNoMove(ctx, scoreBasedNoMoveBudgetOrPaceFiltered, scoreBasedNoMoveDetails{
			candidateCount:              inputCandidateCount,
			compactionCandidateCount:    len(candidates),
			budgetBlockedCandidateCount: budgetBlockedCandidateCount,
			paceBlockedCandidateCount:   paceBlockedCandidateCount,
			budgetBlockedNodePools:      budgetBlockedNodePools,
			paceBlockedNodePools:        paceBlockedNodePools,
		})
		return []Command{}, nil
	}

	searchCtx, stopSearch := cxtracing.Measure(ctx, nil, scoreBasedConsolidationMoveSetSearchSpan,
		attribute.Int("valid_candidate_count", len(validCandidates)),
	)
	evals, evaluated, timedOut, searchStats, err := s.searchForMoveSets(searchCtx, validCandidates, deadline)
	stopSearch()
	if err != nil {
		return []Command{}, err
	}
	if len(evals) == 0 {
		if timedOut {
			log.FromContext(ctx).V(1).Info(fmt.Sprintf("abandoning score-based consolidation due to timeout after evaluating %d candidates", evaluated))
		}
		if !timedOut && budgetBlockedCandidateCount == 0 && paceBlockedCandidateCount == 0 {
			s.markConsolidated()
		}
		reason := scoreBasedNoMoveNoPositiveFeasibleMove
		if timedOut {
			reason = scoreBasedNoMoveSearchTimedOut
		}
		logScoreBasedCompactionNoMove(ctx, reason, scoreBasedNoMoveDetails{
			candidateCount:                 inputCandidateCount,
			compactionCandidateCount:       len(candidates),
			budgetBlockedCandidateCount:    budgetBlockedCandidateCount,
			paceBlockedCandidateCount:      paceBlockedCandidateCount,
			budgetBlockedNodePools:         budgetBlockedNodePools,
			paceBlockedNodePools:           paceBlockedNodePools,
			validCandidateCount:            len(validCandidates),
			moveSetsEvaluated:              evaluated,
			noOpMoveSetCount:               searchStats.noOpMoveSetCount(),
			nonPositiveSavingsMoveSetCount: searchStats.nonPositiveSavingsMoveSetCount(),
			searchTimedOut:                 timedOut,
			moveSetEvaluationErrorCount:    searchStats.errorCount(),
			firstMoveSetEvaluationError:    searchStats.firstErrorMessage(),
		})
		return []Command{}, nil
	}

	sort.Slice(evals, func(i, j int) bool {
		return evals[i].Score > evals[j].Score
	})

	remainingValidationDelay := commandValidationDelay - s.clock.Since(start)
	if remainingValidationDelay > 0 {
		_, stopTTLWait := cxtracing.Start(ctx, scoreBasedConsolidationTTLWaitSpan,
			attribute.String("remaining_delay", remainingValidationDelay.String()),
		)
		select {
		case <-ctx.Done():
			stopTTLWait()
			return []Command{}, ctx.Err()
		case <-s.clock.After(remainingValidationDelay):
		}
		stopTTLWait()
	}
	validateCtx, stopValidate := cxtracing.Measure(ctx, nil, scoreBasedConsolidationValidateSpan,
		attribute.Int("move_set_eval_count", len(evals)),
	)
	cmd, selectedEvalIdx, err := selectFirstStillValidCommand(validateCtx, s.validator, s.recorder, evals)
	stopValidate()
	if err != nil {
		return []Command{}, err
	}
	if len(cmd.Candidates) == 0 {
		reason := scoreBasedNoMoveValidationReturnedNoMove
		if selectedEvalIdx < 0 {
			reason = scoreBasedNoMoveAllValidationFailed
		}
		logScoreBasedCompactionNoMove(ctx, reason, scoreBasedNoMoveDetails{
			candidateCount:                 inputCandidateCount,
			compactionCandidateCount:       len(candidates),
			budgetBlockedCandidateCount:    budgetBlockedCandidateCount,
			paceBlockedCandidateCount:      paceBlockedCandidateCount,
			budgetBlockedNodePools:         budgetBlockedNodePools,
			paceBlockedNodePools:           paceBlockedNodePools,
			validCandidateCount:            len(validCandidates),
			moveSetsEvaluated:              evaluated,
			validMoveSetCount:              len(evals),
			noOpMoveSetCount:               searchStats.noOpMoveSetCount(),
			nonPositiveSavingsMoveSetCount: searchStats.nonPositiveSavingsMoveSetCount(),
			searchTimedOut:                 timedOut,
			moveSetEvaluationErrorCount:    searchStats.errorCount(),
			firstMoveSetEvaluationError:    searchStats.firstErrorMessage(),
			validationAttemptCount:         len(evals),
		})
		return []Command{}, nil
	}
	logScoreBasedCompactionMoveSelected(ctx, evals, selectedEvalIdx, evaluated, len(validCandidates))
	cmd.EmitCandidateEvents(s.recorder)
	cmd.Action = EvacuateAction
	return []Command{cmd}, nil
}

func (s *ScoreBasedConsolidation) searchForMoveSets(ctx context.Context, validCandidates []*Candidate, deadline time.Time) ([]*moveSetEvaluation, int, bool, *moveSetSearchStats, error) {
	moveSets := make([]moveSet, len(validCandidates))
	for i, candidate := range validCandidates {
		moveSets[i] = moveSet{Nodes: []*Candidate{candidate}}
	}
	simulatorCtx, stopSimulator := cxtracing.Measure(ctx, nil, scoreBasedConsolidationNewSimulatorSpan,
		attribute.Int("valid_candidate_count", len(validCandidates)),
	)
	simulator, err := NewConsolidationSchedulingSimulator(simulatorCtx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder, validCandidates...)
	stopSimulator()
	if err != nil {
		return nil, 0, false, nil, err
	}
	simulationCtx := cxtracing.WithoutSpan(ctx)
	searchConsolidation := s.consolidation
	searchConsolidation.recorder = scoreBasedSearchEventRecorder{recorder: s.recorder}
	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		cmd, err := searchConsolidation.computeConsolidation(simulationCtx, simulator, candidates...)
		publishScoreBasedNoSavingsEvent(s.recorder, cmd, err)
		return cmd, err
	}
	return evaluateMoveSetsPar(ctx, moveSets, deadline, compute)
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

	evals, err := collectMoveSetEvaluations(valid, scoreBasedMoveSetResultLimit)
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
		stats.recordNoOpMoveSet()
		return nil
	}
	if cmd.EstimatedSavings() <= 0 {
		stats.recordNonPositiveSavingsMoveSet()
		return nil
	}
	return &moveSetEvaluation{
		Command: cmd,
		Score:   moveSetPriorityScore(cmd),
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
	if len(evals) > limit {
		evals = evals[:limit]
	}
	return evals, nil
}

func selectFirstStillValidCommand(ctx context.Context, validator Validator, recorder events.Recorder, evals []*moveSetEvaluation) (Command, int, error) {
	for i, eval := range evals {
		if _, err := validator.Validate(ctx, eval.Command, 0); err != nil {
			if IsValidationError(err) {
				eval.Command.EmitRejectedEvents(recorder, getValidationFailureReason(err))
				continue
			}
			return Command{}, -1, fmt.Errorf("validating score-based consolidation, %w", err)
		}
		return eval.Command, i, nil
	}
	return Command{}, -1, nil
}

func (s *ScoreBasedConsolidation) budgetExhaustedByInFlightDisruptions(nodePool *v1.NodePool) bool {
	if nodePool == nil || s.cluster == nil {
		return false
	}
	numNodes, disrupting := 0, 0
	for _, node := range s.cluster.DeepCopyNodes() {
		if !node.Managed() || !node.Initialized() || node.NodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() {
			continue
		}
		if node.Labels()[v1.NodePoolLabelKey] != nodePool.Name {
			continue
		}
		numNodes++
		if node.MarkedForDeletion() || nodeutils.GetCondition(node.Node, corev1.NodeReady).Status != corev1.ConditionTrue {
			disrupting++
		}
	}
	allowed := nodePool.MustGetAllowedDisruptions(s.clock, numNodes, v1.DisruptionReasonUnderutilized)
	return allowed > 0 && disrupting >= allowed
}

func scoreBasedNoPositiveSavingsEvents(candidates []*Candidate) []events.Event {
	const message = "Score-based consolidation found no positive simulated savings"
	var evts []events.Event
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		if candidate.Node != nil {
			evts = append(evts, events.Event{
				InvolvedObject: candidate.Node,
				Type:           corev1.EventTypeNormal,
				Reason:         events.Unconsolidatable,
				Message:        message,
				DedupeValues:   []string{string(candidate.Node.UID), "score-based-no-positive-savings"},
				DedupeTimeout:  15 * time.Minute,
			})
		}
		if candidate.NodeClaim != nil {
			evts = append(evts, events.Event{
				InvolvedObject: candidate.NodeClaim,
				Type:           corev1.EventTypeNormal,
				Reason:         events.Unconsolidatable,
				Message:        message,
				DedupeValues:   []string{string(candidate.NodeClaim.UID), "score-based-no-positive-savings"},
				DedupeTimeout:  15 * time.Minute,
			})
		}
	}
	return evts
}

func publishScoreBasedNoSavingsEvent(recorder events.Recorder, cmd Command, err error) {
	if err == nil && cmd.Decision() != NoOpDecision && cmd.EstimatedSavings() <= 0 {
		recorder.Publish(scoreBasedNoPositiveSavingsEvents(cmd.Candidates)...)
	}
}

func scoreBasedNodePoolBlockedByInFlightDisruptions(nodePool *v1.NodePool) events.Event {
	return events.Event{
		InvolvedObject: nodePool,
		Type:           corev1.EventTypeNormal,
		Reason:         events.DisruptionBlocked,
		Message:        "No allowed Underutilized disruptions because the NodePool's allowed disruptions are already in flight",
		DedupeValues:   []string{string(nodePool.UID), "score-based-underutilized-budget-in-flight"},
		DedupeTimeout:  time.Minute,
	}
}

func scoreBasedNodePoolBlockedByPace(nodePool *v1.NodePool) events.Event {
	paceLimit := nodePool.Annotations[v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey]
	return events.Event{
		InvolvedObject: nodePool,
		Type:           corev1.EventTypeNormal,
		Reason:         events.DisruptionBlocked,
		Message:        fmt.Sprintf("Underutilized consolidation is waiting for NodePool pace limit %s=%s disruptions per minute", v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey, paceLimit),
		DedupeValues:   []string{string(nodePool.UID), "score-based-underutilized-pace"},
		DedupeTimeout:  time.Minute,
	}
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
