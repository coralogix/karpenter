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
	"math/rand"
	"strconv"
	"strings"

	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/scheduling"
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

func TestSearchMoves_ranksSavings(t *testing.T) {
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

	winner, stats := searchMoves(ctx, sources, time.Now().Add(time.Second), compute, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
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

	winner, stats := searchMoves(ctx, sources, time.Now().Add(-time.Millisecond), compute, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
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

	moves, stats := searchMoves(ctx, sources, time.Now().Add(time.Second), compute, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
	evaluated := stats.Evaluated
	if len(moves) != scoreBasedValidEvaluationsTarget {
		t.Fatalf("evaluations = %d, want %d", len(moves), scoreBasedValidEvaluationsTarget)
	}
	for i, m := range moves {
		want := strconv.Itoa(11 - i)
		if m.Command.Candidates[0].NodePool.Name != want {
			t.Fatalf("evaluation[%d] index = %q, want %q", i, m.Command.Candidates[0].NodePool.Name, want)
		}
	}
	if evaluated < scoreBasedValidEvaluationsTarget {
		t.Fatalf("evaluated = %d, want at least %d", evaluated, scoreBasedValidEvaluationsTarget)
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

	moves, stats := searchMoves(ctx, sources, deadline, compute, rand.New(rand.NewSource(1)), fakeClock) //nolint:gosec
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

func TestSelectBestRescoredCommand_runsHigherRescoredMoveAsResimulated(t *testing.T) {
	highSearch, lowSearch := namedCandidate(t, "high-search", 10), namedCandidate(t, "low-search", 5)
	moves := []*move{
		{Command: Command{Candidates: []*Candidate{highSearch}, Results: pscheduling.Results{ExistingNodes: standbyReactivationResults("standby-expensive", 1)}}, SearchScore: moveScore{Savings: 9}},
		{Command: Command{Candidates: []*Candidate{lowSearch}}, SearchScore: moveScore{Savings: 5}},
	}
	compute := resimulatedTo(map[string]pscheduling.Results{
		"high-search": {ExistingNodes: standbyReactivationResults("standby-costly", 6)},
		"low-search":  {ExistingNodes: standbyReactivationResults("standby-cheap", 0.01)},
	})

	cmd := expectRescore(t, compute, moves)

	if cmd.Candidates[0].Name() != "low-search" {
		t.Fatalf("chosen candidate = %q, want low-search", cmd.Candidates[0].Name())
	}
	if got := cmd.Results.ExistingNodes[0].Name(); got != "standby-cheap" {
		t.Fatalf("executed standby = %q, want standby-cheap", got)
	}
}

func TestSelectBestRescoredCommand_rejectsNonPositiveRescoredScore(t *testing.T) {
	source := namedCandidate(t, "source", 0.10)
	results := pscheduling.Results{ExistingNodes: standbyReactivationResults("standby-a", 0.10)}
	moves := []*move{{Command: Command{Candidates: []*Candidate{source}, Results: results}, SearchScore: moveScore{Savings: 0.10}}}

	cmd := expectRescore(t, resimulatedTo(map[string]pscheduling.Results{"source": results}), moves)

	if len(cmd.Candidates) != 0 {
		t.Fatalf("command candidates = %d, want 0", len(cmd.Candidates))
	}
	if moves[0].validationText() != "failed: no_savings" {
		t.Fatalf("validation = %q, want failed: no_savings", moves[0].validationText())
	}
}

func TestSelectBestRescoredCommand_noOpFailsWithSchedulingAndNextMoveRuns(t *testing.T) {
	high, low := namedCandidate(t, "high-score", 0.50), namedCandidate(t, "low-score", 0.10)
	moves := []*move{
		{Command: Command{Candidates: []*Candidate{high}}, SearchScore: moveScore{Savings: 0.50}},
		{Command: Command{Candidates: []*Candidate{low}}, SearchScore: moveScore{Savings: 0.10}},
	}

	cmd := expectRescore(t, resimulatedTo(map[string]pscheduling.Results{"low-score": {}}), moves)

	if len(cmd.Candidates) != 1 || cmd.Candidates[0] != low {
		t.Fatalf("command candidates = %v, want low-score", cmd.Candidates)
	}
	if moves[0].validationText() != "failed: scheduling" {
		t.Fatalf("validation = %q, want failed: scheduling", moves[0].validationText())
	}
}

func TestSelectBestRescoredCommand_emitsRejectedEventOnlyForFirstMoveWhenAllFail(t *testing.T) {
	fakeRecorder := record.NewFakeRecorder(10)
	moves := []*move{
		{Command: Command{Candidates: []*Candidate{namedCandidate(t, "high-score", 0.50)}}, SearchScore: moveScore{Savings: 0.50}},
		{Command: Command{Candidates: []*Candidate{namedCandidate(t, "low-score", 0.10)}}, SearchScore: moveScore{Savings: 0.10}},
	}

	cmd := selectBestRescoredCommand(context.Background(), resimulatedTo(nil), events.NewRecorder(fakeRecorder), moves)

	if len(cmd.Candidates) != 0 {
		t.Fatalf("command candidates = %d, want 0", len(cmd.Candidates))
	}
	rejectedEvents := collectFakeRecorderEvents(fakeRecorder)
	if eventsMentioningNode(rejectedEvents, "low-score") || !eventsMentioningNode(rejectedEvents, "high-score") {
		t.Fatalf("rejected events = %v, want only high-score", rejectedEvents)
	}
}

func TestSelectBestRescoredCommand_rejectsMoveByWholeCommandPacing(t *testing.T) {
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
	moves := []*move{{Command: Command{Candidates: []*Candidate{a, b}}, SearchScore: moveScore{Savings: 2}}}

	cmd := expectRescore(t, checks.wrap(resimulatedTo(map[string]pscheduling.Results{"a": {}})), moves)

	if len(cmd.Candidates) != 0 {
		t.Fatalf("command candidates = %d, want 0", len(cmd.Candidates))
	}
	if moves[0].validationText() != "failed: pacing" {
		t.Fatalf("validation = %q, want failed: pacing", moves[0].validationText())
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

func expectRescore(t *testing.T, compute consolidationComputer, moves []*move) Command {
	t.Helper()
	return selectBestRescoredCommand(context.Background(), compute, events.NewRecorder(record.NewFakeRecorder(10)), moves)
}

func namedCandidate(t *testing.T, name string, price float64) *Candidate {
	t.Helper()
	c := candidateWithPrice(t, price)
	c.Node.Name = name
	c.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
	return c
}

func collectFakeRecorderEvents(fakeRecorder *record.FakeRecorder) []string {
	var events []string
	for len(fakeRecorder.Events) > 0 {
		events = append(events, <-fakeRecorder.Events)
	}
	return events
}

func eventsMentioningNode(events []string, nodeName string) bool {
	for _, event := range events {
		if strings.Contains(event, nodeName) {
			return true
		}
	}
	return false
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
		instanceType: instanceType,
		Price:        price,
	}
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
