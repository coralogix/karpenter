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
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

func TestChooseMove_growsChosenMoveIntoBatchMoveThatOutscoresIt(t *testing.T) {
	a, b := namedCandidate(t, "a", 1), namedCandidate(t, "b", 1)
	kept := []*move{singleMove(a, 3)}
	batchable := []*move{kept[0], singleMove(b, 2)}
	selectedBefore := counterValuesByLabel(t, "karpenter_voluntary_disruption_score_based_moves_selected_total", "kind")

	cmd := expectBatchMoveSelection(t, jointlyResimulatedTo(map[string]pscheduling.Results{"a": {}, "a+b": {}}, nil), kept, batchable)

	expectSources(t, cmd, "a", "b")
	selected := counterValuesByLabel(t, "karpenter_voluntary_disruption_score_based_moves_selected_total", "kind")
	if delta := selected[string(moveKindBatch)] - selectedBefore[string(moveKindBatch)]; delta != 1 {
		t.Errorf("selected batch moves = %v, want 1", delta)
	}
}

func TestChooseMove_skipsAdditionNeedingNewNodeAndKeepsGrowing(t *testing.T) {
	a, b, c := namedCandidate(t, "a", 1), namedCandidate(t, "b", 1), namedCandidate(t, "c", 1)
	kept := []*move{singleMove(a, 3)}
	batchable := []*move{kept[0], singleMove(b, 2), singleMove(c, 2)}

	cmd := expectBatchMoveSelection(t, jointlyResimulatedTo(map[string]pscheduling.Results{"a": {}, "a+c": {}}, []string{"a+b"}), kept, batchable)

	expectSources(t, cmd, "a", "c")
}

func TestChooseMove_skipsAdditionThatLowersScore(t *testing.T) {
	a, b, c := namedCandidate(t, "a", 10), namedCandidate(t, "b", 1), namedCandidate(t, "c", 1)
	kept := []*move{singleMove(a, 11)}
	batchable := []*move{kept[0], singleMove(b, 2), singleMove(c, 2)}
	fits := map[string]pscheduling.Results{
		"a": {},
		// Reactivating a standby node almost as expensive as both sources leaves little savings.
		"a+b": {ExistingNodes: standbyReactivationResults("standby-costly", 10)},
		"a+c": {},
	}

	cmd := expectBatchMoveSelection(t, jointlyResimulatedTo(fits, nil), kept, batchable)

	expectSources(t, cmd, "a", "c")
}

func TestChooseMove_growthStopsAtPacingPerBatchCap(t *testing.T) {
	pool := paceTestNodePool("1", "2")
	clk := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clk)
	pacing.Refresh([]*v1.NodePool{pool}, nil)
	sources := candidatesInPool(t, pool, "a", "b", "c")
	kept := []*move{singleMove(sources[0], 3)}
	batchable := []*move{kept[0], singleMove(sources[1], 2), singleMove(sources[2], 2)}
	checks := rescoreChecksFor(sources, map[string]int{pool.Name: 3}, pacing)
	simulated := recordSimulatedMoves(jointlyResimulatedTo(map[string]pscheduling.Results{"a": {}, "a+b": {}, "a+b+c": {}}, nil))

	cmd := expectBatchMoveSelection(t, checks.wrap(simulated.compute), kept, batchable)

	expectSources(t, cmd, "a", "b")
	if slices.Contains(simulated.moves, "a+b+c") {
		t.Error("simulated a batch over the pacing per-batch cap")
	}
}

func TestChooseMove_growthStopsAtNodePoolBudget(t *testing.T) {
	pool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "budgeted"}}
	sources := candidatesInPool(t, pool, "a", "b", "c")
	kept := []*move{singleMove(sources[0], 3)}
	batchable := []*move{kept[0], singleMove(sources[1], 2), singleMove(sources[2], 2)}
	checks := rescoreChecksFor(sources, map[string]int{pool.Name: 2}, nil)
	simulated := recordSimulatedMoves(jointlyResimulatedTo(map[string]pscheduling.Results{"a": {}, "a+b": {}, "a+b+c": {}}, nil))

	cmd := expectBatchMoveSelection(t, checks.wrap(simulated.compute), kept, batchable)

	expectSources(t, cmd, "a", "b")
	if slices.Contains(simulated.moves, "a+b+c") {
		t.Error("simulated a batch over the NodePool's budget")
	}
}

func TestChooseMove_neverAddsKeptMoveThatFailedValidation(t *testing.T) {
	a, b, c := namedCandidate(t, "a", 1), namedCandidate(t, "b", 1), namedCandidate(t, "c", 1)
	kept := []*move{singleMove(a, 3), singleMove(b, 2.5)}
	batchable := []*move{kept[0], kept[1], singleMove(c, 2)}

	// b alone no longer consolidates, so it fails validation, although it would still fit together with a.
	cmd := expectBatchMoveSelection(t, jointlyResimulatedTo(map[string]pscheduling.Results{"a": {}, "a+b": {}, "a+c": {}}, nil), kept, batchable)

	expectSources(t, cmd, "a", "c")
}

func TestChooseMove_doesNotGrowMoveNeedingNewNode(t *testing.T) {
	a, b := namedCandidate(t, "a", 1), namedCandidate(t, "b", 1)
	kept := []*move{singleMove(a, 3)}
	batchable := []*move{kept[0], singleMove(b, 2)}

	cmd := expectBatchMoveSelection(t, jointlyResimulatedTo(map[string]pscheduling.Results{"a+b": {}}, []string{"a"}), kept, batchable)

	expectSources(t, cmd, "a")
	if len(cmd.Replacements) != 1 {
		t.Fatalf("replacements = %d, want the validated move's new node", len(cmd.Replacements))
	}
}

func TestChooseMove_runsHigherScoringBatchMoveOfTwoSeeds(t *testing.T) {
	a, b, c, d := namedCandidate(t, "a", 1), namedCandidate(t, "b", 1), namedCandidate(t, "c", 1), namedCandidate(t, "d", 3)
	kept := []*move{singleMove(a, 5), singleMove(b, 4)}
	batchable := []*move{kept[0], kept[1], singleMove(c, 2), singleMove(d, 2)}
	fits := map[string]pscheduling.Results{"a": {}, "b": {}, "a+c": {}, "b+d": {}}

	cmd := expectBatchMoveSelection(t, jointlyResimulatedTo(fits, nil), kept, batchable)

	expectSources(t, cmd, "b", "d")
}

func candidatesInPool(t *testing.T, pool *v1.NodePool, names ...string) []*Candidate {
	t.Helper()
	return lo.Map(names, func(name string, _ int) *Candidate {
		c := namedCandidate(t, name, 1)
		c.NodePool, c.reschedulablePods = pool, []*corev1.Pod{{}}
		return c
	})
}

func rescoreChecksFor(candidates []*Candidate, budgets map[string]int, pacing *DisruptionPacing) rescoreChecks {
	return rescoreChecks{
		candidates:                 lo.KeyBy(candidates, func(c *Candidate) string { return c.Name() }),
		disruptionBudgetByNodePool: budgets,
		cluster:                    state.NewCluster(clocktesting.NewFakeClock(time.Now()), nil, nil),
		pacing:                     pacing,
	}
}

type simulatedMoves struct {
	mu    sync.Mutex
	moves []string
	inner consolidationComputer
}

func recordSimulatedMoves(compute consolidationComputer) *simulatedMoves {
	return &simulatedMoves{inner: compute}
}

func (s *simulatedMoves) compute(ctx context.Context, candidates ...*Candidate) (Command, error) {
	s.mu.Lock()
	s.moves = append(s.moves, moveKey(candidates))
	s.mu.Unlock()
	return s.inner(ctx, candidates...)
}

func moveKey(candidates []*Candidate) string {
	names := lo.Map(candidates, func(c *Candidate, _ int) string { return c.Name() })
	sort.Strings(names)
	return strings.Join(names, "+")
}

func singleMove(source *Candidate, searchScore float64) *move {
	return &move{Command: Command{Candidates: []*Candidate{source}}, SearchScore: moveScore{Total: searchScore}}
}

// jointlyResimulatedTo resimulates a move whose sorted source names, joined by "+", are a key of fits, and gives a move
// in needsNewNode one new node. Any other move is a no-op.
func jointlyResimulatedTo(fits map[string]pscheduling.Results, needsNewNode []string) consolidationComputer {
	return func(_ context.Context, candidates ...*Candidate) (Command, error) {
		key := moveKey(candidates)
		if slices.Contains(needsNewNode, key) {
			return Command{Candidates: candidates, Replacements: []*Replacement{{NodeClaim: &pscheduling.NodeClaim{}}}}, nil
		}
		results, ok := fits[key]
		if !ok {
			return Command{}, nil
		}
		return Command{Candidates: candidates, Results: results}, nil
	}
}

func expectBatchMoveSelection(t *testing.T, compute consolidationComputer, kept, batchable []*move) Command {
	t.Helper()
	moves := sourcesOf(slices.Concat(kept, batchable))
	return chooseMove(moveEvaluator{compute: compute, inputs: testScoreInputs(moves)}, events.NewRecorder(record.NewFakeRecorder(100)), kept, batchable).Command
}

func expectSources(t *testing.T, cmd Command, want ...string) {
	t.Helper()
	got := lo.Map(cmd.Candidates, func(c *Candidate, _ int) string { return c.Name() })
	sort.Strings(got)
	if !slices.Equal(got, want) {
		t.Fatalf("command sources = %v, want %v", got, want)
	}
}
