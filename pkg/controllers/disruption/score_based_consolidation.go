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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
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
	var moves []*move
	var stats searchStats
	eligible := len(validCandidates)
	var searchDuration time.Duration
	defer func() {
		// A failed pass writes no decision log; the controller reports the error.
		if err == nil {
			logScoreDecision(ctx, moves, stats, len(snapshot.candidates), eligible, budgetBlockedPools, pacingBlockedPools, searchDuration)
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
	cmd, err := s.rescoreKeptMoves(ctx, moves)
	if err != nil {
		return []Command{}, err
	}
	if len(cmd.Candidates) == 0 {
		return []Command{}, nil
	}
	return []Command{cmd}, nil
}

func (s *ScoreBasedConsolidation) filterCandidates(snapshot clusterSnapshot) ([]*Candidate, []string, []string) {
	var validCandidates []*Candidate
	budgetBlockedPools := sets.New[string]()
	pacingBlockedPools := sets.New[string]()
	for _, candidate := range snapshot.candidates {
		pool := candidate.NodePool.Name
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
	compute, err := s.newComputer(ctx, candidates)
	if err != nil {
		return nil, searchStats{}, err
	}
	sources := make([][]*Candidate, len(candidates))
	for i, c := range candidates {
		sources[i] = []*Candidate{c}
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec
	moves, stats := searchMoves(ctx, sources, deadline, compute, rng, s.clock)
	return moves, stats, nil
}

func (s *ScoreBasedConsolidation) newComputer(ctx context.Context, candidates []*Candidate) (consolidationComputer, error) {
	simulator, err := NewConsolidationSchedulingSimulator(ctx, s.kubeClient, s.cluster, s.provisioner, s.clock, s.recorder, candidates...)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		return s.computeConsolidation(ctx, simulator, candidates...)
	}, nil
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
