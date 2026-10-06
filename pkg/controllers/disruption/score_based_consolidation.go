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
	"math/rand"
	"runtime"
	"sort"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cxtracing"
	"sigs.k8s.io/karpenter/pkg/events"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const (
	scoreBasedConsolidationSpan               = "karpenter.disruption.score_based_consolidation"
	scoreBasedConsolidationPrepareSpan        = "karpenter.disruption.score_based_consolidation.prepare_compaction_candidates"
	scoreBasedConsolidationFilterSpan         = "karpenter.disruption.score_based_consolidation.filter_valid_candidates"
	scoreBasedConsolidationMoveSetSearchSpan  = "karpenter.disruption.score_based_consolidation.move_set_search"
	scoreBasedConsolidationMemorySnapshotSpan = "karpenter.disruption.score_based_consolidation.memory_balance_snapshot"
	scoreBasedConsolidationNewSimulatorSpan   = "karpenter.disruption.score_based_consolidation.new_scheduling_simulator"
	scoreBasedConsolidationTTLWaitSpan        = "karpenter.disruption.score_based_consolidation.consolidation_ttl_wait"
	scoreBasedConsolidationValidateSpan       = "karpenter.disruption.score_based_consolidation.validate_command"
)

var ScoreBasedConsolidationTimeoutDuration = 15 * time.Second

// scoreBasedMoveSetResultLimit is the number of highest-scoring move sets returned after search completes.
var scoreBasedMoveSetResultLimit = 10

var scoreBasedMoveSetParallelism = runtime.GOMAXPROCS(0)

const ScoreBasedConsolidationType = "score-based"

type moveSet struct {
	Nodes []*Candidate
}

type scoreBasedMoveSetSearchPath string

const (
	scoreBasedSearchPathSingleNode scoreBasedMoveSetSearchPath = "single_node"
	scoreBasedSearchPathBatch      scoreBasedMoveSetSearchPath = "batch"
	scoreBasedSearchPathPair2To1   scoreBasedMoveSetSearchPath = "pair_2_to_1"
)

type moveSetEvaluation struct {
	Command                    Command
	Score                      moveSetScore
	searchPath                 scoreBasedMoveSetSearchPath
	sourceNodeSingleMoveScores []float64
	validationStatus           string
	validationReason           string
}

type moveSetSearchResult struct {
	evals       []*moveSetEvaluation
	evaluated   int
	timedOut    bool
	stats       *moveSetSearchStats
	complete    bool
	constrained bool
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
	validator                    Validator
	memoryConfigSignature        map[string]string
	concentrationConfigSignature map[string]string
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
func (s *ScoreBasedConsolidation) shouldCompact(_ context.Context, cn *Candidate) bool {
	if !s.compactionCandidateEligible(cn) {
		return false
	}
	// A terminating-only node is still occupied, but it has no movable workload for a
	// compaction command. Leave it active until the pod leaves or becomes terminal; standby
	// marking will then own it without leaving an evacuation waiting on a pod Karpenter cannot evict.
	return lo.SomeBy(cn.reschedulablePods, func(pod *corev1.Pod) bool {
		return podutils.IsActive(pod) && podutils.IsReschedulable(pod)
	})
}

func (s *ScoreBasedConsolidation) compactionCandidateEligible(cn *Candidate) bool {
	if cn == nil || cn.Node == nil || cn.NodeClaim == nil || cn.NodePool == nil {
		return false
	}
	if !NodePoolUsesScoreBasedConsolidation(cn.NodePool) {
		return false
	}
	if cn.NodePool.Spec.Disruption.ConsolidationPolicy != v1.ConsolidationPolicyWhenEmptyOrUnderutilized {
		return false
	}
	if standby.IsNodeClaimActivating(cn.NodeClaim) || standby.IsNodeClaimStandby(cn.NodeClaim) || standby.HasNodeTaint(cn.Node) {
		return false
	}
	return s.shouldDisruptIgnoringConsolidateAfter(cn)
}

func (s *ScoreBasedConsolidation) apiReader() client.Reader {
	if s.queue != nil && s.queue.apiReader != nil {
		return s.queue.apiReader
	}
	return s.kubeClient
}

//nolint:gocyclo
func (s *ScoreBasedConsolidation) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	currentMemoryConfigSignature, memoryConfigChanged := scoreBasedMemoryConfigChanged(s.memoryConfigSignature, candidates)
	s.memoryConfigSignature = currentMemoryConfigSignature
	currentConcentrationConfigSignature, concentrationConfigChanged := scoreBasedInstanceTypeConcentrationConfigChanged(s.concentrationConfigSignature, candidates)
	s.concentrationConfigSignature = currentConcentrationConfigSignature
	if memoryConfigChanged || concentrationConfigChanged {
		s.lastConsolidationState = time.Time{}
	}
	if s.IsConsolidated() && !memoryConfigChanged && !concentrationConfigChanged {
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
		if !s.underutilizedPace.candidateAllowed(candidate.NodePool, 0) {
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
	searchResult, err := s.searchForMoveSets(searchCtx, validCandidates, disruptionBudgetMapping, deadline)
	stopSearch()
	if err != nil {
		return []Command{}, err
	}
	evals := searchResult.evals
	evaluated := searchResult.evaluated
	timedOut := searchResult.timedOut
	searchStats := searchResult.stats
	if len(evals) == 0 {
		if timedOut {
			log.FromContext(ctx).V(1).Info(fmt.Sprintf("abandoning score-based consolidation due to timeout after evaluating %d candidates", evaluated))
		}
		if searchResult.complete && !searchResult.constrained && !timedOut && budgetBlockedCandidateCount == 0 && paceBlockedCandidateCount == 0 {
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
			nonPositiveScoreMoveSetCount:   searchStats.nonPositiveScoreMoveSetCount(),
			searchTimedOut:                 timedOut,
			moveSetEvaluationErrorCount:    searchStats.errorCount(),
			firstMoveSetEvaluationError:    searchStats.firstErrorMessage(),
		})
		return []Command{}, nil
	}

	sort.Slice(evals, func(i, j int) bool {
		return evals[i].Score.Total > evals[j].Score.Total
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
	cmd, selectedEvalIdx, err := selectFirstStillValidCommand(validateCtx, s.validator, s.recorder, evals, s.validateCommandPaceAndCap)
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
			nonPositiveScoreMoveSetCount:   searchStats.nonPositiveScoreMoveSetCount(),
			searchTimedOut:                 timedOut,
			moveSetEvaluationErrorCount:    searchStats.errorCount(),
			firstMoveSetEvaluationError:    searchStats.firstErrorMessage(),
			validationAttemptCount:         len(evals),
		})
		return []Command{}, nil
	}
	logScoreBasedCompactionMoveSelected(ctx, evals, selectedEvalIdx, evaluated, len(validCandidates), timedOut)
	cmd.EmitCandidateEvents(s.recorder)
	cmd.Action = EvacuateAction
	return []Command{cmd}, nil
}

//nolint:gocyclo // Keep the single-first decision, bounded fallback branch, and shared deadline handling together.
func (s *ScoreBasedConsolidation) searchForMoveSets(ctx context.Context, validCandidates []*Candidate, budgets map[string]int, deadline time.Time) (moveSetSearchResult, error) {
	result := moveSetSearchResult{stats: &moveSetSearchStats{}}
	searchCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	searchCandidates := append([]*Candidate(nil), validCandidates...)
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // Random order only guides this non-security-sensitive search.
	rng.Shuffle(len(searchCandidates), func(i, j int) {
		searchCandidates[i], searchCandidates[j] = searchCandidates[j], searchCandidates[i]
	})
	singleNodeMoveSets := make([]moveSet, len(validCandidates))
	for i, candidate := range searchCandidates {
		singleNodeMoveSets[i] = moveSet{Nodes: []*Candidate{candidate}}
	}
	scoringConfigs := scoreBasedScoringConfigForCandidates(validCandidates)
	simulatorCtx, stopSimulator := cxtracing.Measure(searchCtx, nil, scoreBasedConsolidationNewSimulatorSpan,
		attribute.Int("valid_candidate_count", len(validCandidates)),
	)
	simulator, err := NewConsolidationSchedulingSimulator(simulatorCtx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder, validCandidates...)
	stopSimulator()
	if err != nil {
		if errors.Is(searchCtx.Err(), context.DeadlineExceeded) {
			result.timedOut = true
			return result, nil
		}
		if searchCtx.Err() != nil {
			return result, searchCtx.Err()
		}
		return result, err
	}
	nodeSnapshot := simulator.StateNodesSnapshot()
	priceSnapshot := newScoreBasedPriceSnapshot(nodeSnapshot, validCandidates)
	concentrationSnapshot := newScoreBasedInstanceTypeConcentrationSnapshot(nodeSnapshot, scoringConfigs)
	memorySnapshotCtx, stopMemorySnapshot := cxtracing.Measure(searchCtx, nil, scoreBasedConsolidationMemorySnapshotSpan,
		attribute.Bool("enabled", hasScoreBasedMemoryBalanceWeight(scoringConfigs)),
	)
	var memorySnapshot *scoreBasedMemorySnapshot
	if hasScoreBasedMemoryBalanceWeight(scoringConfigs) {
		memorySnapshot = newScoreBasedMemorySnapshot(nodeSnapshot, scoringConfigs)
		trace.SpanFromContext(memorySnapshotCtx).SetAttributes(memorySnapshot.spanAttributes(len(nodeSnapshot))...)
	}
	stopMemorySnapshot()
	searchConsolidation := s.consolidation
	searchConsolidation.recorder = scoreBasedSearchEventRecorder{recorder: s.recorder}
	compute := func(computeCtx context.Context, candidates ...*Candidate) (Command, error) {
		cmd, err := searchConsolidation.computeConsolidation(cxtracing.WithoutSpan(computeCtx), simulator, candidates...)
		publishScoreBasedNoSavingsEvent(s.recorder, cmd, err)
		return cmd, err
	}
	scorer := newMoveSetScorer(priceSnapshot, scoringConfigs, memorySnapshot, concentrationSnapshot)
	scoreMoveSet := scorer.Score

	// Search every singleton before deciding whether to batch successes or explore pairs.
	singles, evaluated, timedOut, stats, err := evaluateMoveSetsParWithStats(searchCtx, singleNodeMoveSets, deadline, compute, 0, nil, scoreMoveSet, result.stats)
	result.evals = singles
	result.evaluated += evaluated
	result.timedOut = timedOut
	if err != nil {
		return result, err
	}
	for _, eval := range singles {
		eval.searchPath = scoreBasedSearchPathSingleNode
		eval.sourceNodeSingleMoveScores = []float64{eval.Score.Total}
	}
	if timedOut {
		result.evals = retainScoreBasedMoveSetResults(singles, nil, scoreBasedMoveSetResultLimit)
		return result, nil
	}

	if len(singles) > 0 {
		zeroReplacementSingles := make([]*moveSetEvaluation, 0, len(singles))
		for _, single := range singles {
			if len(single.Command.Candidates) == 1 && len(single.Command.Replacements) == 0 {
				zeroReplacementSingles = append(zeroReplacementSingles, single)
			}
		}
		batches, attempted, batchTimedOut, batchErr := growScoreBasedBatches(searchCtx, zeroReplacementSingles, budgets, s.underutilizedPace, compute, result.stats, scoreMoveSet)
		if batchErr != nil {
			return result, batchErr
		}
		result.evals = retainScoreBasedMoveSetResults(singles, batches, scoreBasedMoveSetResultLimit)
		result.evaluated += attempted
		result.timedOut = batchTimedOut
		result.complete = !batchTimedOut
		return result, nil
	}

	// If every singleton completed without a positive feasible move, sample pairs with higher
	// probability for lower-utilization nodes. Pair generation is bounded and deadline-aware.
	pairs, exhaustive, constrained := scoreBasedPairMoveSets(searchCtx, searchCandidates, budgets, s.underutilizedPace, scoreBasedPairSearchLimit, rng)
	result.constrained = constrained
	if searchCtx.Err() != nil {
		if !errors.Is(searchCtx.Err(), context.DeadlineExceeded) {
			return result, searchCtx.Err()
		}
		result.timedOut = true
		return result, nil
	}
	pairEvals, pairEvaluated, pairTimedOut, _, err := evaluateMoveSetsParWithStats(searchCtx, pairs, deadline, compute, scoreBasedMoveSetResultLimit,
		func(cmd Command) bool { return len(cmd.Candidates) == 2 && len(cmd.Replacements) == 1 }, scoreMoveSet, stats)
	if err != nil {
		return result, err
	}
	for _, eval := range pairEvals {
		eval.searchPath = scoreBasedSearchPathPair2To1
	}
	result.evals = pairEvals
	result.evaluated += pairEvaluated
	result.timedOut = pairTimedOut
	result.complete = exhaustive && !pairTimedOut
	return result, nil
}

//nolint:gocyclo // Ranked fallback, validation reporting, and optional final checks form one command selection pass.
func selectFirstStillValidCommand(ctx context.Context, validator Validator, recorder events.Recorder, evals []*moveSetEvaluation, checks ...func(context.Context, *Command) error) (Command, int, error) {
	for _, eval := range evals {
		eval.validationStatus = "not_attempted"
		eval.validationReason = ""
	}
	for i, eval := range evals {
		validated, err := validator.Validate(ctx, eval.Command, 0)
		if err == nil && len(checks) > 0 {
			validated.Candidates = append([]*Candidate(nil), validated.Candidates...)
			for candidateIdx, candidate := range validated.Candidates {
				if candidate != nil {
					candidateCopy := *candidate
					validated.Candidates[candidateIdx] = &candidateCopy
				}
			}
			for _, check := range checks {
				if checkErr := check(ctx, &validated); checkErr != nil {
					err = checkErr
					break
				}
			}
		}
		if err != nil {
			if IsValidationError(err) {
				eval.validationStatus = "failed"
				eval.validationReason = getValidationFailureReason(err)
				eval.Command.EmitRejectedEvents(recorder, eval.validationReason)
				continue
			}
			return Command{}, -1, fmt.Errorf("validating score-based consolidation, %w", err)
		}
		eval.validationStatus = "validated"
		if len(checks) > 0 {
			return validated, i, nil
		}
		return eval.Command, i, nil
	}
	return Command{}, -1, nil
}

//nolint:gocyclo // This final command gate checks current pool policy, cap, pace, and every selected source.
func (s *ScoreBasedConsolidation) validateCommandPaceAndCap(ctx context.Context, cmd *Command) error {
	counts := map[string]int{}
	for _, candidate := range cmd.Candidates {
		if candidate == nil || candidate.Node == nil || candidate.NodeClaim == nil || candidate.NodePool == nil {
			return NewChurnValidationError(fmt.Errorf("score-based compaction candidate is missing its Node, NodeClaim, or NodePool"))
		}
		counts[candidate.NodePool.Name]++
	}
	reader := s.apiReader()
	if reader == nil {
		return fmt.Errorf("score-based compaction requires an API reader for final validation")
	}
	for name, count := range counts {
		liveNodePool := &v1.NodePool{}
		if err := reader.Get(ctx, types.NamespacedName{Name: name}, liveNodePool); err != nil {
			if apierrors.IsNotFound(err) {
				return NewChurnValidationError(fmt.Errorf("NodePool %q no longer exists", name))
			}
			return fmt.Errorf("getting live NodePool %q for compaction pace validation, %w", name, err)
		}
		nodePool := liveNodePool
		if !NodePoolUsesScoreBasedConsolidation(nodePool) || nodePool.Spec.Disruption.ConsolidationPolicy != v1.ConsolidationPolicyWhenEmptyOrUnderutilized {
			return NewChurnValidationError(fmt.Errorf("NodePool %q no longer permits score-based compaction", name))
		}
		config := underutilizedPaceConfigFor(nodePool)
		if config.maxNodesPerCommand > 0 && count > config.maxNodesPerCommand {
			return NewBudgetValidationError(fmt.Errorf("score-based compaction has %d candidates for NodePool %q, exceeding its limit of %d", count, name, config.maxNodesPerCommand))
		}
		for _, candidate := range cmd.Candidates {
			if candidate.NodePool.Name != name {
				continue
			}
			if err := s.validateLiveCompactionCandidate(ctx, reader, candidate, name); err != nil {
				return err
			}
			candidate.NodePool = nodePool
		}
		if !s.underutilizedPace.candidateAllowed(nodePool, count-1) {
			if s.recorder != nil {
				s.recorder.Publish(scoreBasedNodePoolBlockedByPace(nodePool))
			}
			return NewBudgetValidationError(fmt.Errorf("score-based compaction is blocked by the pace limit for NodePool %q", name))
		}
	}
	return nil
}

//nolint:gocyclo // Identity, deletion, standby, activation, and occupancy checks jointly protect one live source.
func (s *ScoreBasedConsolidation) validateLiveCompactionCandidate(ctx context.Context, reader client.Reader, candidate *Candidate, nodePoolName string) error {
	liveNode := &corev1.Node{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(candidate.Node), liveNode); err != nil {
		if apierrors.IsNotFound(err) {
			return NewChurnValidationError(fmt.Errorf("score-based compaction candidate Node %q no longer exists", candidate.Node.Name))
		}
		return fmt.Errorf("getting live Node %q for score-based compaction validation, %w", candidate.Node.Name, err)
	}
	if liveNode.UID != candidate.Node.UID || liveNode.Spec.ProviderID != candidate.ProviderID() || !liveNode.DeletionTimestamp.IsZero() ||
		standby.HasNodeTaint(liveNode) || liveNode.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" ||
		liveNode.Labels[v1.NodePoolLabelKey] != nodePoolName {
		return NewChurnValidationError(fmt.Errorf("score-based compaction candidate Node %q changed identity or state", candidate.Node.Name))
	}
	liveNodeClaim := &v1.NodeClaim{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), liveNodeClaim); err != nil {
		if apierrors.IsNotFound(err) {
			return NewChurnValidationError(fmt.Errorf("score-based compaction candidate NodeClaim %q no longer exists", candidate.NodeClaim.Name))
		}
		return fmt.Errorf("getting live NodeClaim %q for score-based compaction validation, %w", candidate.NodeClaim.Name, err)
	}
	if liveNodeClaim.UID != candidate.NodeClaim.UID || !liveNodeClaim.DeletionTimestamp.IsZero() ||
		liveNodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() ||
		standby.IsNodeClaimStandby(liveNodeClaim) || standby.IsNodeClaimActivating(liveNodeClaim) ||
		liveNodeClaim.Labels[v1.NodePoolLabelKey] != nodePoolName {
		return NewChurnValidationError(fmt.Errorf("score-based compaction candidate NodeClaim %q changed state", candidate.NodeClaim.Name))
	}
	livePods, err := scoreBasedNodeOccupyingPods(ctx, reader, liveNode)
	if err != nil {
		return fmt.Errorf("checking live occupancy for score-based compaction Node %q, %w", liveNode.Name, err)
	}
	if lo.NoneBy(livePods, func(pod *corev1.Pod) bool {
		return podutils.IsActive(pod) && podutils.IsReschedulable(pod)
	}) {
		return NewChurnValidationError(fmt.Errorf("score-based compaction candidate Node %q no longer has reschedulable workload", liveNode.Name))
	}
	return nil
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
