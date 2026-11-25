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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"

	"github.com/samber/lo"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

func TestSearchMovesRejectsUnprofitableAndMultipleReplacements(t *testing.T) {
	sources := [][]*Candidate{
		[]*Candidate{candidateWithPrice(t, 0)},
		[]*Candidate{candidateWithPrice(t, 2)},
		[]*Candidate{candidateWithPrice(t, 3)},
	}
	moves, stats := newTestSearch(moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		cmd := Command{Candidates: c}
		if c[0].Price == 2 {
			cmd.Replacements = []*Replacement{{}, {}}
		}
		return cmd, nil
	}, inputs: testScoreInputs(sources)}).simulate(context.Background(), sources)
	if len(moves) != 1 || moves[0].SearchScore.Total != 4 || stats.Evaluated != 3 {
		t.Fatalf("got %v, stats %+v", moves, stats)
	}
}

func TestSearchMovesPreservesSuccessOnSimulationError(t *testing.T) {
	sources := [][]*Candidate{[]*Candidate{candidateWithPrice(t, 1)}, []*Candidate{candidateWithPrice(t, 2)}}
	moves, stats := newTestSearch(moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		if c[0].Price == 2 {
			return Command{}, fmt.Errorf("simulation failed")
		}
		return Command{Candidates: c}, nil
	}, inputs: testScoreInputs(sources)}).simulate(context.Background(), sources)
	if len(moves) != 1 || moves[0].SearchScore.Total != 2 || stats.Errors != 1 || stats.Evaluated != 2 {
		t.Fatalf("got %v, stats %+v", moves, stats)
	}
}

func TestSearchMovesErrorPreventsConsolidatedState(t *testing.T) {
	_, stats := newTestSearch(moveEvaluator{compute: func(context.Context, ...*Candidate) (Command, error) {
		return Command{}, fmt.Errorf("simulation failed")
	}}).simulate(context.Background(), [][]*Candidate{[]*Candidate{candidateWithPrice(t, 1)}})
	if stats.canMarkConsolidated(1, false) {
		t.Fatal("failed search must not mark cluster consolidated")
	}
}

func TestSearchMovesFiltersMoveWhenStandbyCostExceedsSavings(t *testing.T) {
	expensive := candidateWithPrice(t, 10)
	cheap := candidateWithPrice(t, 5)
	sources := [][]*Candidate{[]*Candidate{expensive}, []*Candidate{cheap}}
	moves, stats := newTestSearch(moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		if c[0].Price == 10 {
			cmd := Command{Candidates: c}
			cmd.Results.ExistingNodes = standbyReactivationResults("standby-a", 12)
			return cmd, nil
		}
		return Command{Candidates: c}, nil
	}, inputs: testScoreInputs(sources)}).simulate(context.Background(), sources)
	if len(moves) != 1 || moves[0].SearchScore.Total != 6 || stats.NoSavings != 1 {
		t.Fatalf("got %v, stats %+v", moves, stats)
	}
}

func standbyReactivationResults(standbyName string, price float64) []*pscheduling.ExistingNode {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: standbyName},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{v1.StandbyNoScheduleTaint}},
	}
	sn := state.NewNode()
	sn.Node = node
	return []*pscheduling.ExistingNode{{StateNode: sn, Pods: []*corev1.Pod{{}}, Price: price}}
}

func TestSearchMovesRejectsPositiveSavingsWithZeroScore(t *testing.T) {
	candidate := candidateWithPrice(t, 1)
	sources := [][]*Candidate{[]*Candidate{candidate}}
	inputs := testScoreInputs(sources)
	candidate.Node.Status.Allocatable = nil
	moves, stats := newTestSearch(moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		return Command{Candidates: c}, nil
	}, inputs: inputs}).simulate(context.Background(), sources)
	if len(moves) != 0 || stats.NonPositiveScore != 1 || stats.NoSavings != 0 {
		t.Fatalf("kept %d moves, stats %+v, want none counted as non-positive score", len(moves), stats)
	}
}

func TestInvalidUtilizationWeightWarnsOnce(t *testing.T) {
	fakeRecorder := record.NewFakeRecorder(10)
	recorder := events.NewRecorder(fakeRecorder)
	pool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool", UID: "pool", Annotations: map[string]string{v1.UtilizationWeightAnnotationKey: "NaN"}}}
	candidate := candidateWithPrice(t, 1)
	candidate.NodePool = pool
	candidate.NodeClaim = &v1.NodeClaim{}
	candidate.Node.Labels[v1.NodePoolLabelKey] = pool.Name
	candidate.Node.Labels[v1.NodeInitializedLabelKey] = "true"
	candidate.Node.Labels[v1.NodeRegisteredLabelKey] = "true"
	candidate.Node.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
	instanceTypes := instanceTypesByNodePool{pool.Name: {candidate.instanceType.Name: candidate.instanceType}}
	sources := [][]*Candidate{[]*Candidate{candidate}}
	for range 2 {
		inputs := newScoreInputs(state.StateNodes{candidate.StateNode}, instanceTypes, []*Candidate{candidate}, recorder)
		moves, _ := newTestSearch(moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
			return Command{Candidates: c}, nil
		}, inputs: inputs}).simulate(context.Background(), sources)
		if len(moves) != 1 || moves[0].SearchScore.Total != 1+defaultUtilizationWeight {
			t.Fatalf("evaluations = %+v, want score %g using default weight", moves, 1+defaultUtilizationWeight)
		}
	}

	warnings := collectFakeRecorderEvents(fakeRecorder)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "Warning InvalidUtilizationWeight") {
		t.Fatalf("events = %v, want one invalid-weight warning", warnings)
	}
}

func TestSearchMovesCountsEvaluationResults(t *testing.T) {
	sources := [][]*Candidate{
		[]*Candidate{candidateWithPrice(t, 1)},
		[]*Candidate{candidateWithPrice(t, 2)},
		[]*Candidate{candidateWithPrice(t, 3)},
		[]*Candidate{candidateWithPrice(t, 4)},
		[]*Candidate{candidateWithPrice(t, 5)},
	}
	inputs := testScoreInputs(sources)
	sources[3][0].Node.Status.Allocatable = nil
	before := moveEvaluationCounts(t)

	newTestSearch(moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		switch c[0].Price {
		case 1:
			return Command{}, nil
		case 2:
			return Command{Candidates: c, Results: pscheduling.Results{ExistingNodes: standbyReactivationResults("standby-a", 3)}}, nil
		case 3:
			return Command{}, fmt.Errorf("simulation failed")
		}
		return Command{Candidates: c}, nil
	}, inputs: inputs}).simulate(context.Background(), sources)

	got := moveEvaluationCounts(t)
	for result, want := range map[string]float64{"feasible": 1, "no_op": 1, "no_savings": 1, "non_positive_score": 1, "error": 1} {
		if delta := got[result] - before[result]; delta != want {
			t.Errorf("%s evaluations = %v, want %v", result, delta, want)
		}
	}
}

func moveEvaluationCounts(t *testing.T) map[string]float64 {
	t.Helper()
	return counterValuesByLabel(t, "karpenter_voluntary_disruption_score_based_move_evaluations_total", "result")
}

func newTestSearch(evaluator moveEvaluator) moveSearch {
	return moveSearch{evaluator: evaluator, deadline: time.Now().Add(time.Second), random: rand.New(rand.NewSource(1)), clk: clock.RealClock{}} //nolint:gosec
}

func TestSearch_skipsPairsWhenASingleNodeMoveIsFeasible(t *testing.T) {
	candidates, inputs := pairTestCandidates(t, 2)

	_, stats, pairsComputed := runSearchWith(candidates, inputs, func(_ context.Context, c ...*Candidate) (Command, error) {
		return Command{Candidates: c}, nil
	}, budgetsForAll(candidates))

	if pairsComputed != 0 || stats.PairsEvaluated != 0 {
		t.Fatalf("simulated %d pairs, stats %+v, want no pair phase", pairsComputed, stats)
	}
}

func TestSearch_triesPairsWhenSingleNodeMovesSaveMoneyButNoneScoresAboveZero(t *testing.T) {
	candidates, inputs := pairTestCandidates(t, 2)
	for _, c := range candidates {
		// Without allocatable CPU every term is 0, so a move saves money but scores 0.
		c.Node.Status.Allocatable = nil
	}

	_, stats, pairsComputed := runSearchWith(candidates, inputs, func(_ context.Context, c ...*Candidate) (Command, error) {
		return Command{Candidates: c}, nil
	}, budgetsForAll(candidates))

	if stats.NonPositiveScore != 3 || pairsComputed != 1 || stats.PairsEvaluated != 1 {
		t.Fatalf("simulated %d pairs, stats %+v, want the one pair tried after two non-positive single-node moves, and non-positive too", pairsComputed, stats)
	}
}

func TestSearch_keepsAPairOntoAtMostOneNewNode(t *testing.T) {
	for _, tc := range []struct {
		newNodes int
		kept     bool
	}{{newNodes: 0, kept: true}, {newNodes: 1, kept: true}, {newNodes: 2}} {
		t.Run(fmt.Sprintf("%d new nodes", tc.newNodes), func(t *testing.T) {
			candidates, inputs := pairTestCandidates(t, 2)

			moves, stats, _ := runSearchWith(candidates, inputs, pairsOnto(tc.newNodes), budgetsForAll(candidates))

			if kept := len(moves) == 1 && len(moves[0].Command.Candidates) == 2; kept != tc.kept || stats.NoOp != 3-len(moves) {
				t.Fatalf("kept %d moves, stats %+v, want pair kept %v and every other move counted as no-op", len(moves), stats, tc.kept)
			}
		})
	}
}

func TestSearch_simulatesOnlyPairsWithinDisruptionBudgets(t *testing.T) {
	candidates, inputs := pairTestCandidates(t, 4, "a", "a", "b", "b")
	oneNodePerPool := map[string]int{"a": 1, "b": 1}

	_, stats, pairsComputed := runSearchWith(candidates, inputs, pairsOnto(1), oneNodePerPool)

	if pairsComputed != 4 || stats.PairsEvaluated != 4 {
		t.Fatalf("simulated %d pairs, stats %+v, want only the 4 mixed-pool pairs", pairsComputed, stats)
	}
}

func TestSearch_samplesAtMost256Pairs(t *testing.T) {
	// 24 candidates in one pool make 276 admissible pairs.
	candidates, inputs := pairTestCandidates(t, 24)

	_, stats, pairsComputed := runSearchWith(candidates, inputs, pairsOnto(0), budgetsForAll(candidates))

	if pairsComputed != 256 || stats.PairsEvaluated != 256 {
		t.Fatalf("simulated %d pairs, stats %+v, want 256 sampled pairs", pairsComputed, stats)
	}
}

func TestSearch_sampledPairPhaseThatFindsNothingMarksTheClusterConsolidated(t *testing.T) {
	candidates, inputs := pairTestCandidates(t, 24)

	moves, stats, _ := runSearchWith(candidates, inputs, pairsOnto(2), budgetsForAll(candidates))

	if len(moves) != 0 || stats.PairsEvaluated != 256 || !stats.canMarkConsolidated(len(candidates), false) {
		t.Fatalf("kept %d moves, stats %+v, want a sampled pair phase that keeps nothing to mark the cluster consolidated", len(moves), stats)
	}
}

func TestChooseMove_pairRescoredOntoNoNewNodeRunsAsPair(t *testing.T) {
	pair := keptPair(t)

	chosen := chooseMove(moveEvaluator{compute: pairsOnto(0), inputs: testScoreInputs(sourcesOf([]*move{pair}))}, events.NewRecorder(record.NewFakeRecorder(10)), []*move{pair}, nil)

	if chosen.Move != pair || chosen.Kind != moveKindPair || len(chosen.Command.Replacements) != 0 {
		t.Fatalf("chose %+v, want the pair as kind pair on no new node", chosen)
	}
}

func TestChooseMove_pairRescoredOntoTwoNewNodesFailsWithScheduling(t *testing.T) {
	pair := keptPair(t)

	chosen := chooseMove(moveEvaluator{compute: pairsOnto(2), inputs: testScoreInputs(sourcesOf([]*move{pair}))}, events.NewRecorder(record.NewFakeRecorder(10)), []*move{pair}, nil)

	if chosen.Move != nil || pair.validationFailureReason() != "scheduling" {
		t.Fatalf("chose %+v, validation failure %q, want no move and scheduling", chosen, pair.validationFailureReason())
	}
}

func keptPair(t *testing.T) *move {
	t.Helper()
	candidates, _ := pairTestCandidates(t, 2)
	return &move{Command: Command{Candidates: candidates, Replacements: make([]*Replacement, 1)}, SearchScore: moveScore{Total: 2}}
}

// pairsOnto makes every single-node move a no-op and every pair land on newNodes new nodes.
func pairsOnto(newNodes int) consolidationComputer {
	return func(_ context.Context, c ...*Candidate) (Command, error) {
		if len(c) == 1 {
			return Command{}, nil
		}
		return Command{Candidates: c, Replacements: make([]*Replacement, newNodes)}, nil
	}
}

// The i-th candidate is in pools[i], or all share one pool when no pools are given.
func pairTestCandidates(t *testing.T, n int, pools ...string) ([]*Candidate, scoreInputs) {
	t.Helper()
	candidates := make([]*Candidate, n)
	singles := make([][]*Candidate, n)
	for i := range candidates {
		candidates[i] = namedCandidate(t, fmt.Sprintf("node-%d", i), 1)
		if len(pools) > 0 {
			candidates[i].NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: pools[i]}}
		}
		singles[i] = []*Candidate{candidates[i]}
	}
	return candidates, testScoreInputs(singles)
}

func budgetsForAll(candidates []*Candidate) map[string]int {
	return lo.SliceToMap(candidates, func(c *Candidate) (string, int) { return c.NodePool.Name, len(candidates) })
}

// runSearchWith runs the search with every move simulated by compute and returns how many pairs were simulated.
func runSearchWith(candidates []*Candidate, inputs scoreInputs, compute consolidationComputer, budgets map[string]int) ([]*move, searchStats, int) {
	var pairsComputed atomic.Int32
	counting := func(ctx context.Context, c ...*Candidate) (Command, error) {
		if len(c) == 2 {
			pairsComputed.Add(1)
		}
		return compute(ctx, c...)
	}
	search := newTestSearch(moveEvaluator{compute: counting, inputs: inputs})
	search.budgets = budgets
	moves, stats := search.run(context.Background(), candidates)
	return moves, stats, int(pairsComputed.Load())
}
