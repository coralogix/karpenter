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
	"slices"
	"strconv"

	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

func TestNodePoolUsesScoreBasedConsolidation(t *testing.T) {
	cases := []struct {
		name string
		np   *v1.NodePool
		want bool
	}{
		{name: "nil pool", np: nil, want: false},
		{name: "no annotations", np: &v1.NodePool{}, want: false},
		{name: "other annotation", np: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{"other": "true"}}}, want: false},
		{name: "score-based consolidation", np: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{v1.ScoreBasedConsolidationAnnotationKey: ""}}}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NodePoolUsesScoreBasedConsolidation(tc.np); got != tc.want {
				t.Fatalf("NodePoolUsesScoreBasedConsolidation() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestScoreBasedConsolidationCandidateFiltering(t *testing.T) {
	ctx := context.Background()
	c := MakeConsolidation(nil, nil, nil, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	emptiness := NewEmptiness(c)
	singleNode := NewSingleNodeConsolidation(c)
	multiNode := NewMultiNodeConsolidation(c)
	scoreBased := NewScoreBasedConsolidation(c)

	makeCandidate := scoreBasedTestCandidate

	t.Run("annotated pool", func(t *testing.T) {
		candidate := makeCandidate(true)
		if !scoreBased.ShouldDisrupt(ctx, candidate) {
			t.Fatal("expected score-based consolidation to accept annotated pool")
		}
		if singleNode.ShouldDisrupt(ctx, candidate) {
			t.Fatal("expected single-node consolidation to reject annotated pool")
		}
		if multiNode.ShouldDisrupt(ctx, candidate) {
			t.Fatal("expected multi-node consolidation to reject annotated pool")
		}
		if emptiness.ShouldDisrupt(ctx, candidate) {
			t.Fatal("expected emptiness to reject annotated pool")
		}
	})

	t.Run("unannotated pool", func(t *testing.T) {
		candidate := makeCandidate(false)
		if scoreBased.ShouldDisrupt(ctx, candidate) {
			t.Fatal("expected score-based consolidation to reject unannotated pool")
		}
		if !singleNode.ShouldDisrupt(ctx, candidate) {
			t.Fatal("expected single-node consolidation to accept unannotated pool")
		}
		if !multiNode.ShouldDisrupt(ctx, candidate) {
			t.Fatal("expected multi-node consolidation to accept unannotated pool")
		}
	})
}

func TestScoreBasedConsolidationShouldDisruptOnlyScoreBasedPools(t *testing.T) {
	ctx := context.Background()
	c := MakeConsolidation(nil, nil, nil, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	candidate := scoreBasedTestCandidate(true)

	scoreBased := &ScoreBasedConsolidation{consolidation: c}
	singleNode := &SingleNodeConsolidation{consolidation: c}

	if singleNode.ShouldDisrupt(ctx, candidate) {
		t.Fatal("single-node ShouldDisrupt must reject score-based pools")
	}
	if !scoreBased.ShouldDisrupt(ctx, candidate) {
		t.Fatal("score-based ShouldDisrupt must accept score-based pools")
	}
}

func TestSearchMoves_ranksScore(t *testing.T) {
	ctx := context.Background()
	sources := make([][]*Candidate, 2)
	delays := []time.Duration{50 * time.Millisecond, 5 * time.Millisecond}
	for i := range sources {
		candidate := candidateWithPrice(t, float64(i+1))
		candidate.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i)}}
		sources[i] = []*Candidate{candidate}
	}

	compute := func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		idx, err := strconv.Atoi(candidates[0].NodePool.Name)
		if err != nil {
			t.Fatalf("unexpected move set index: %v", err)
		}
		select {
		case <-time.After(delays[idx]):
		case <-ctx.Done():
			return Command{}, ctx.Err()
		}
		return Command{Candidates: candidates}, nil
	}

	winner, stats := newTestSearch(moveEvaluator{compute: compute, inputs: testScoreInputs(sources)}).simulate(ctx, sources)
	evaluated := stats.Evaluated
	if len(winner) != 2 {
		t.Fatalf("evaluations = %d, want 2", len(winner))
	}
	if winner[0].Command.Candidates[0].NodePool.Name != "1" {
		t.Fatalf("first evaluation index = %q, want %q", winner[0].Command.Candidates[0].NodePool.Name, "1")
	}
	if winner[1].Command.Candidates[0].NodePool.Name != "0" {
		t.Fatalf("second evaluation index = %q, want %q", winner[1].Command.Candidates[0].NodePool.Name, "0")
	}
	if evaluated != 2 {
		t.Fatalf("evaluated = %d, want 2", evaluated)
	}
}

func TestSearchMoves_stopsOnDeadline(t *testing.T) {
	ctx := context.Background()
	candidate := candidateWithPrice(t, 0.10)
	sources := [][]*Candidate{[]*Candidate{candidate}, []*Candidate{candidate}}

	compute := func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		select {
		case <-time.After(time.Second):
			return Command{Candidates: candidates}, nil
		case <-ctx.Done():
			return Command{}, ctx.Err()
		}
	}

	winner, stats := moveSearch{evaluator: moveEvaluator{compute: compute}, deadline: time.Now().Add(-time.Millisecond), random: rand.New(rand.NewSource(1)), clk: clock.RealClock{}}.simulate(ctx, sources) //nolint:gosec
	evaluated := stats.Evaluated
	if len(winner) != 0 {
		t.Fatalf("evaluations = %d, want 0", len(winner))
	}
	if evaluated != 0 {
		t.Fatalf("evaluated = %d, want 0", evaluated)
	}
	if !stats.TimedOut {
		t.Fatal("expected stats.TimedOut")
	}
}

func TestSearchMoves_keepsTenBest(t *testing.T) {
	ctx := context.Background()
	sources := make([][]*Candidate, 12)
	for i := range sources {
		candidate := candidateWithPrice(t, float64(i+1))
		candidate.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i)}}
		sources[i] = []*Candidate{candidate}
	}

	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		return Command{Candidates: candidates}, nil
	}

	feasible, stats := newTestSearch(moveEvaluator{compute: compute, inputs: testScoreInputs(sources)}).simulate(ctx, sources)
	moves := keptMoves(feasible)
	evaluated := stats.Evaluated
	if len(moves) != scoreBasedKeptMovesLimit {
		t.Fatalf("evaluations = %d, want %d", len(moves), scoreBasedKeptMovesLimit)
	}
	for i, m := range moves {
		want := strconv.Itoa(11 - i)
		if m.Command.Candidates[0].NodePool.Name != want {
			t.Fatalf("evaluation[%d] index = %q, want %q", i, m.Command.Candidates[0].NodePool.Name, want)
		}
	}
	if evaluated < scoreBasedKeptMovesLimit {
		t.Fatalf("evaluated = %d, want at least %d", evaluated, scoreBasedKeptMovesLimit)
	}
}

func TestSearchMoves_returnsPartialOnTimeout(t *testing.T) {
	orig := scoreBasedSearchParallelism
	scoreBasedSearchParallelism = 1
	t.Cleanup(func() { scoreBasedSearchParallelism = orig })

	fakeClock := clocktesting.NewFakeClock(time.Now())
	deadline := fakeClock.Now().Add(100 * time.Millisecond)
	ctx := context.Background()
	sources := make([][]*Candidate, 3)
	for i := range sources {
		candidate := candidateWithPrice(t, float64(i+1))
		candidate.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i)}}
		sources[i] = []*Candidate{candidate}
	}

	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		if fakeClock.Now().Before(deadline) {
			fakeClock.Step(200 * time.Millisecond)
		}
		return Command{Candidates: candidates}, nil
	}

	moves, stats := moveSearch{evaluator: moveEvaluator{compute: compute, inputs: testScoreInputs(sources)}, deadline: deadline, random: rand.New(rand.NewSource(1)), clk: fakeClock}.simulate(ctx, sources) //nolint:gosec
	if !stats.TimedOut {
		t.Fatal("expected stats.TimedOut")
	}
	if stats.Evaluated == 0 || stats.Evaluated >= len(sources) {
		t.Fatalf("evaluated = %d, want partial progress on %d moves", stats.Evaluated, len(sources))
	}
	if len(moves) == 0 {
		t.Fatal("expected at least one kept evaluation")
	}
}

func TestChooseMove_runsHigherRescoredMoveAsResimulated(t *testing.T) {
	highSearch, lowSearch := namedCandidate(t, "high-search", 10), namedCandidate(t, "low-search", 5)
	moves := []*move{
		{Command: Command{Candidates: []*Candidate{highSearch}, Results: pscheduling.Results{ExistingNodes: standbyReactivationResults("standby-expensive", 1)}}, SearchScore: moveScore{Total: 9}},
		{Command: Command{Candidates: []*Candidate{lowSearch}}, SearchScore: moveScore{Total: 5}},
	}
	compute := resimulatedTo(map[string]pscheduling.Results{
		"high-search": {ExistingNodes: standbyReactivationResults("standby-costly", 6)},
		"low-search":  {ExistingNodes: standbyReactivationResults("standby-cheap", 0.01)},
	})

	cmd := expectRescore(t, compute, testScoreInputs(sourcesOf(moves)), moves)

	if cmd.Candidates[0].Name() != "low-search" {
		t.Fatalf("chosen candidate = %q, want low-search", cmd.Candidates[0].Name())
	}
	if got := cmd.Results.ExistingNodes[0].Name(); got != "standby-cheap" {
		t.Fatalf("executed standby = %q, want standby-cheap", got)
	}
}

func TestChooseMove_rejectsRescoredMoveWithoutSavings(t *testing.T) {
	source := namedCandidate(t, "source", 0.10)
	results := pscheduling.Results{ExistingNodes: standbyReactivationResults("standby-a", 0.10)}
	moves := []*move{{Command: Command{Candidates: []*Candidate{source}, Results: results}, SearchScore: moveScore{Total: 0.10}}}

	cmd := expectRescore(t, resimulatedTo(map[string]pscheduling.Results{"source": results}), testScoreInputs(sourcesOf(moves)), moves)

	if len(cmd.Candidates) != 0 {
		t.Fatalf("command candidates = %d, want 0", len(cmd.Candidates))
	}
	if moves[0].validationText() != "failed: no_savings" {
		t.Fatalf("validation = %q, want failed: no_savings", moves[0].validationText())
	}
}

func TestChooseMove_rejectsPositiveSavingsWithNonPositiveRescoredScore(t *testing.T) {
	source := namedCandidate(t, "source", 1)
	moves := []*move{{Command: Command{Candidates: []*Candidate{source}}, SearchScore: moveScore{Total: 2}}}
	inputs := testScoreInputs(sourcesOf(moves))
	source.Node.Status.Allocatable = nil

	cmd := expectRescore(t, resimulatedTo(map[string]pscheduling.Results{"source": {}}), inputs, moves)

	if len(cmd.Candidates) != 0 {
		t.Fatalf("command candidates = %d, want 0", len(cmd.Candidates))
	}
	if moves[0].ValidationScore.Savings <= 0 || moves[0].validationText() != "failed: non_positive_score" {
		t.Fatalf("rescored savings %v, validation = %q, want positive savings and failed: non_positive_score", moves[0].ValidationScore.Savings, moves[0].validationText())
	}
}

func TestChooseMove_noOpFailsWithSchedulingAndNextMoveRuns(t *testing.T) {
	high, low := namedCandidate(t, "high-score", 0.50), namedCandidate(t, "low-score", 0.10)
	moves := []*move{
		{Command: Command{Candidates: []*Candidate{high}}, SearchScore: moveScore{Total: 0.50}},
		{Command: Command{Candidates: []*Candidate{low}}, SearchScore: moveScore{Total: 0.10}},
	}

	cmd := expectRescore(t, resimulatedTo(map[string]pscheduling.Results{"low-score": {}}), testScoreInputs(sourcesOf(moves)), moves)

	if len(cmd.Candidates) != 1 || cmd.Candidates[0] != low {
		t.Fatalf("command candidates = %v, want low-score", cmd.Candidates)
	}
	if moves[0].validationFailureReason() != "scheduling" {
		t.Fatalf("validation failure = %q, want scheduling", moves[0].validationFailureReason())
	}
}

func TestChooseMove_candidateEventsOnlyOnChosenMoveAndRejectedEventsOnEveryFailedMove(t *testing.T) {
	recorder := test.NewEventRecorder()
	moves := []*move{
		{Command: Command{Candidates: []*Candidate{namedCandidate(t, "fails-first", 0.50)}}, SearchScore: moveScore{Total: 0.50}},
		{Command: Command{Candidates: []*Candidate{namedCandidate(t, "chosen", 0.30)}}, SearchScore: moveScore{Total: 0.30}},
		{Command: Command{Candidates: []*Candidate{namedCandidate(t, "fails-last", 0.10)}}, SearchScore: moveScore{Total: 0.10}},
	}

	chooseMove(moveEvaluator{compute: resimulatedTo(map[string]pscheduling.Results{"chosen": {}}), inputs: testScoreInputs(sourcesOf(moves))}, recorder, moves, nil)

	if got := involvedObjectNames(recorder, events.ConsolidationCandidate); !slices.Equal(got, []string{"chosen"}) {
		t.Errorf("ConsolidationCandidate events on %v, want only the chosen move's source", got)
	}
	if got := involvedObjectNames(recorder, events.ConsolidationRejected); !slices.Equal(got, []string{"fails-first", "fails-last"}) {
		t.Errorf("ConsolidationRejected events on %v, want every failed move", got)
	}
}

func TestChooseMove_countsSelectedKindAndEveryFailedValidation(t *testing.T) {
	moves := []*move{
		{Command: Command{Candidates: []*Candidate{namedCandidate(t, "no-longer-fits", 0.50)}}, SearchScore: moveScore{Total: 0.50}},
		{Command: Command{Candidates: []*Candidate{namedCandidate(t, "chosen", 0.30)}}, SearchScore: moveScore{Total: 0.30}},
		{Command: Command{Candidates: []*Candidate{namedCandidate(t, "no-longer-saves", 0.10)}}, SearchScore: moveScore{Total: 0.10}},
		{Command: Command{Candidates: []*Candidate{namedCandidate(t, "simulation-errors", 0.05)}}, SearchScore: moveScore{Total: 0.05}},
	}
	resimulate := resimulatedTo(map[string]pscheduling.Results{
		"chosen":          {},
		"no-longer-saves": {ExistingNodes: standbyReactivationResults("standby-costly", 1)},
	})
	compute := func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		if candidates[0].Name() == "simulation-errors" {
			return Command{}, fmt.Errorf("simulation failed")
		}
		return resimulate(ctx, candidates...)
	}
	selectedBefore := counterValuesByLabel(t, "karpenter_voluntary_disruption_score_based_moves_selected_total", "kind")
	failedBefore := counterValuesByLabel(t, "karpenter_voluntary_disruption_failed_validations_total", ConsolidationTypeLabel)

	expectRescore(t, compute, testScoreInputs(sourcesOf(moves)), moves)

	selected := counterValuesByLabel(t, "karpenter_voluntary_disruption_score_based_moves_selected_total", "kind")
	if delta := selected[string(moveKindSingle)] - selectedBefore[string(moveKindSingle)]; delta != 1 {
		t.Errorf("selected single moves = %v, want 1", delta)
	}
	failed := counterValuesByLabel(t, "karpenter_voluntary_disruption_failed_validations_total", ConsolidationTypeLabel)
	if delta := failed[ScoreBasedConsolidationType] - failedBefore[ScoreBasedConsolidationType]; delta != 2 {
		t.Errorf("score-based failed validations = %v, want 2", delta)
	}
}

func TestChooseMove_rejectsMoveByWholeCommandPacing(t *testing.T) {
	pool := paceTestNodePool("1", "1")
	clk := clocktesting.NewFakeClock(time.Now())
	pacing := NewDisruptionPacing(clk)
	pacing.Refresh([]*v1.NodePool{pool}, nil)
	a, b := namedCandidate(t, "a", 1), namedCandidate(t, "b", 1)
	for _, c := range []*Candidate{a, b} {
		c.NodePool, c.reschedulablePods = pool, []*corev1.Pod{{}}
	}
	checks := rescoreChecks{
		candidates:                 map[string]*Candidate{"a": a, "b": b},
		disruptionBudgetByNodePool: map[string]int{pool.Name: 2},
		cluster:                    state.NewCluster(clk, nil, nil),
		pacing:                     pacing,
	}
	moves := []*move{{Command: Command{Candidates: []*Candidate{a, b}}, SearchScore: moveScore{Total: 2}}}

	computed := false
	compute := func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		computed = true
		return resimulatedTo(map[string]pscheduling.Results{"a": {}})(ctx, candidates...)
	}

	cmd := expectRescore(t, checks.wrap(compute), testScoreInputs(sourcesOf(moves)), moves)

	if len(cmd.Candidates) != 0 {
		t.Fatalf("command candidates = %d, want 0", len(cmd.Candidates))
	}
	if moves[0].validationFailureReason() != "pacing" {
		t.Fatalf("validation failure = %q, want pacing", moves[0].validationFailureReason())
	}
	if computed {
		t.Fatal("compute was called for a move rejected by pacing")
	}
}

func TestFilterCandidates_poolHeldBackByPacingExplainsItself(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	paced, unpaced := paceTestNodePool("1", ""), &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "unpaced", UID: "unpaced"}}
	paced.Name, paced.UID = "paced", "paced"
	pacing := NewDisruptionPacing(clk)
	pacing.Refresh([]*v1.NodePool{paced, unpaced}, nil)
	pacing.ChargePass([]DisruptionPacingSuccessfulStart{{Command: &Command{Candidates: []*Candidate{{NodePool: paced, reschedulablePods: []*corev1.Pod{{}}}}}, StartTime: clk.Now()}})
	held, kept := namedCandidate(t, "held", 1), namedCandidate(t, "kept", 1)
	held.NodePool, kept.NodePool = paced, unpaced
	held.reschedulablePods, kept.reschedulablePods = []*corev1.Pod{{}}, []*corev1.Pod{{}}
	recorder := test.NewEventRecorder()
	s := NewScoreBasedConsolidation(consolidation{recorder: recorder, disruptionPacing: pacing})

	s.filterCandidates(clusterSnapshot{candidates: []*Candidate{held, kept}, disruptionBudgetByNodePool: map[string]int{"paced": 1, "unpaced": 1}})

	if got := involvedObjectNames(recorder, events.DisruptionBlocked); !slices.Equal(got, []string{"paced"}) {
		t.Errorf("DisruptionBlocked events on %v, want only the paced pool", got)
	}
}

func TestFilterCandidates_poolWhoseBudgetIsUsedUpByDisruptionsInProgressExplainsItself(t *testing.T) {
	busy, closed := namedCandidate(t, "busy-node", 1), namedCandidate(t, "closed-node", 1)
	busy.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "busy", UID: "busy"}}
	// A pool whose budget allows no disruptions at all is reported by the budget mapping, not here.
	closed.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "closed", UID: "closed"}}
	recorder := test.NewEventRecorder()
	s := NewScoreBasedConsolidation(consolidation{recorder: recorder})

	s.filterCandidates(clusterSnapshot{
		candidates:                          []*Candidate{busy, closed},
		disruptionBudgetByNodePool:          map[string]int{"busy": 0, "closed": 0},
		budgetUsedUpByDisruptionsInProgress: map[string]int{"busy": 1},
	})

	if got := involvedObjectNames(recorder, events.DisruptionBlocked); !slices.Equal(got, []string{"busy"}) {
		t.Errorf("DisruptionBlocked events on %v, want only the pool with disruptions in progress", got)
	}
}

func resimulatedTo(byFirstSource map[string]pscheduling.Results) consolidationComputer {
	return func(_ context.Context, candidates ...*Candidate) (Command, error) {
		results, ok := byFirstSource[candidates[0].Name()]
		if !ok {
			return Command{}, nil
		}
		return Command{Candidates: candidates, Results: results}, nil
	}
}

func expectRescore(t *testing.T, compute consolidationComputer, inputs scoreInputs, moves []*move) Command {
	t.Helper()
	return chooseMove(moveEvaluator{compute: compute, inputs: inputs}, events.NewRecorder(record.NewFakeRecorder(10)), moves, nil).Command
}

// chooseMove runs validation, batch move building and selection as ComputeCommands does.
func chooseMove(evaluator moveEvaluator, recorder events.Recorder, feasible, batchable []*move) validatedMove {
	kept := validateKeptMoves(context.Background(), evaluator, recorder, feasible)
	return selectMove(recorder, kept, buildBatchMoves(context.Background(), evaluator, kept, batchable).Moves)
}

func namedCandidate(t *testing.T, name string, price float64) *Candidate {
	t.Helper()
	c := candidateWithPrice(t, price)
	c.Node.Name = name
	c.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
	return c
}

func involvedObjectNames(recorder *test.EventRecorder, reason string) []string {
	names := sets.New[string]()
	recorder.ForEachEvent(func(evt events.Event) {
		if evt.Reason == reason {
			names.Insert(evt.InvolvedObject.(client.Object).GetName())
		}
	})
	return sets.List(names)
}

func counterValuesByLabel(t *testing.T, name, label string) map[string]float64 {
	t.Helper()
	families, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]float64{}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, pair := range m.GetLabel() {
				if pair.GetName() == label {
					values[pair.GetValue()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	return values
}

func collectFakeRecorderEvents(fakeRecorder *record.FakeRecorder) []string {
	var events []string
	for len(fakeRecorder.Events) > 0 {
		events = append(events, <-fakeRecorder.Events)
	}
	return events
}

func candidateWithPrice(t *testing.T, price float64) *Candidate {
	t.Helper()
	offering := &cloudprovider.Offering{
		Price: price,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
			corev1.LabelTopologyZone: "us-east-1a",
		}),
	}
	instanceType := &cloudprovider.InstanceType{
		Name:      "m5.large",
		Offerings: cloudprovider.Offerings{offering},
	}
	node := state.NewNode()
	node.Node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				corev1.LabelInstanceTypeStable: instanceType.Name,
				v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			},
		},
		Spec: corev1.NodeSpec{ProviderID: "provider-1"},
	}
	return &Candidate{
		StateNode:    node,
		NodePool:     &v1.NodePool{},
		instanceType: instanceType,
		Price:        price,
	}
}

// testScoreInputs gives every source one CPU, a baseline of 1 and a utilization weight of 1, so a move without requests
// scores its savings plus 1 per source.
func testScoreInputs(sources [][]*Candidate) scoreInputs {
	inputs := scoreInputs{
		pricePerCPUByCapacityKey:    map[capacityKey]float64{},
		utilizationWeightByNodePool: map[string]float64{},
	}
	for _, nodes := range sources {
		for _, c := range nodes {
			c.Node.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
			inputs.pricePerCPUByCapacityKey[capacityKeyOf(c.StateNode)] = 1
			inputs.utilizationWeightByNodePool[c.NodePool.Name] = 1
		}
	}
	return inputs
}

func sourcesOf(moves []*move) [][]*Candidate {
	return lo.Map(moves, func(m *move, _ int) []*Candidate { return m.Command.Candidates })
}

func scoreBasedTestCandidate(scoreBasedConsolidation bool) *Candidate {
	annotations := map[string]string{}
	if scoreBasedConsolidation {
		annotations[v1.ScoreBasedConsolidationAnnotationKey] = ""
	}
	nodeClaim := &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "nodeclaim-1",
			Labels: map[string]string{
				corev1.LabelInstanceTypeStable: "m5.large",
				v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone:       "us-east-1a",
			},
		},
	}
	nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)

	node := state.NewNode()
	node.Node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				corev1.LabelInstanceTypeStable: "m5.large",
				v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
				corev1.LabelTopologyZone:       "us-east-1a",
			},
		},
	}
	node.NodeClaim = nodeClaim

	return &Candidate{
		StateNode: node,
		NodePool: &v1.NodePool{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "default",
				Annotations: annotations,
			},
			Spec: v1.NodePoolSpec{
				Disruption: v1.Disruption{
					ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
					ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
				},
			},
		},
		instanceType:             &cloudprovider.InstanceType{Name: "m5.large"},
		reschedulablePods:        []*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "pod"}}},
		DisruptionCost:           PerNodeBaseDisruptionCost + 1,
		RescheduleDisruptionCost: PerNodeBaseDisruptionCost + 1,
	}
}
