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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/controller-runtime/pkg/log"

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

func TestLogScoreBasedCompactionNoMoveSummarizesBlockedPools(t *testing.T) {
	var output string
	ctx := log.IntoContext(context.Background(), funcr.New(func(_, args string) {
		output = args
	}, funcr.Options{}))
	logScoreBasedCompactionNoMove(ctx, scoreBasedNoMoveBudgetOrPaceFiltered, scoreBasedNoMoveDetails{
		candidateCount:                 6,
		compactionCandidateCount:       6,
		budgetBlockedCandidateCount:    3,
		budgetBlockedNodePools:         map[string]struct{}{"zeta": {}, "alpha": {}},
		paceBlockedCandidateCount:      2,
		paceBlockedNodePools:           map[string]struct{}{"gamma": {}, "beta": {}},
		validCandidateCount:            1,
		moveSetsEvaluated:              1,
		noOpMoveSetCount:               2,
		nonPositiveSavingsMoveSetCount: 3,
	})

	for _, field := range []string{
		`"reason"="budget_or_pace_filtered"`,
		`"candidateCount"=6`,
		`"budgetBlockedCandidateCount"=3`,
		`"paceBlockedCandidateCount"=2`,
		`"validCandidateCount"=1`,
		`"noOpMoveSetCount"=2`,
		`"nonPositiveSavingsMoveSetCount"=3`,
	} {
		if !strings.Contains(output, field) {
			t.Errorf("log output %q does not contain %q", output, field)
		}
	}
	if strings.Index(output, "alpha") > strings.Index(output, "zeta") || strings.Index(output, "beta") > strings.Index(output, "gamma") {
		t.Fatalf("blocked NodePool names are not sorted in log output: %s", output)
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

func TestScoreBasedCompactionIgnoresConsolidateAfter(t *testing.T) {
	ctx := context.Background()
	c := MakeConsolidation(nil, nil, nil, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	scoreBased := NewScoreBasedConsolidation(c)
	validator := scoreBased.validator.(*ConsolidationValidator)

	t.Run("annotated pool with Never", func(t *testing.T) {
		candidate := scoreBasedTestCandidate(true)
		candidate.NodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration(v1.Never)

		if !scoreBased.shouldCompact(ctx, candidate) {
			t.Fatal("score-based compaction must ignore consolidateAfter: Never")
		}
		if !validator.filter(ctx, candidate) {
			t.Fatal("score-based compaction validator must admit an annotated Never pool")
		}
	})

	t.Run("annotated pool with long duration and unset Consolidatable condition", func(t *testing.T) {
		candidate := scoreBasedTestCandidate(true)
		candidate.NodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("24h")
		if err := candidate.NodeClaim.StatusConditions().Clear(v1.ConditionTypeConsolidatable); err != nil {
			t.Fatalf("clearing Consolidatable condition: %v", err)
		}

		if !scoreBased.shouldCompact(ctx, candidate) {
			t.Fatal("score-based compaction must not depend on consolidateAfter or the Consolidatable condition")
		}
		if !validator.filter(ctx, candidate) {
			t.Fatal("score-based compaction validator must admit an annotated pool with Consolidatable unset")
		}
	})

	t.Run("annotated pool retains the consolidation policy gate", func(t *testing.T) {
		candidate := scoreBasedTestCandidate(true)
		candidate.NodePool.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmpty
		if scoreBased.shouldCompact(ctx, candidate) {
			t.Fatal("score-based compaction must require WhenEmptyOrUnderutilized policy")
		}
	})

	t.Run("unannotated pool with Never remains disabled", func(t *testing.T) {
		candidate := scoreBasedTestCandidate(false)
		candidate.NodePool.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration(v1.Never)

		if scoreBased.ShouldDisrupt(ctx, candidate) {
			t.Fatal("score-based compaction must reject an unannotated pool")
		}
		if c.ShouldDisrupt(ctx, candidate) {
			t.Fatal("upstream consolidation must remain disabled for an unannotated Never pool")
		}
	})
}

func TestScoreBasedConsolidationValidatorFilter(t *testing.T) {
	ctx := context.Background()
	c := MakeConsolidation(nil, nil, nil, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil)
	candidate := scoreBasedTestCandidate(true)

	scoreBased := &ScoreBasedConsolidation{consolidation: c}
	singleNode := &SingleNodeConsolidation{consolidation: c}
	scoreValidator := NewScoreBasedConsolidationValidator(c)
	defaultValidator := NewScoreBasedConsolidation(c).validator.(*ConsolidationValidator)

	if singleNode.ShouldDisrupt(ctx, candidate) {
		t.Fatal("single-node ShouldDisrupt must reject score-based pools")
	}
	if !scoreBased.ShouldDisrupt(ctx, candidate) {
		t.Fatal("score-based ShouldDisrupt must accept score-based pools")
	}
	if scoreValidator.validationType != ScoreBasedConsolidationType {
		t.Fatalf("score validator type = %q, want %q", scoreValidator.validationType, ScoreBasedConsolidationType)
	}
	if defaultValidator.validationType != ScoreBasedConsolidationType {
		t.Fatalf("default score-based validator type = %q, want %q", defaultValidator.validationType, ScoreBasedConsolidationType)
	}
	if !scoreValidator.filter(ctx, candidate) {
		t.Fatal("score-based validator filter must accept score-based pools")
	}
	if defaultValidator.filter(ctx, candidate) != scoreBased.shouldCompact(ctx, candidate) {
		t.Fatal("score-based consolidation must wire its compaction-only predicate into its validator")
	}
}

func TestMoveSetSearchStats(t *testing.T) {
	stats := &moveSetSearchStats{}

	stats.record(100*time.Millisecond, nil)
	stats.record(300*time.Millisecond, fmt.Errorf("compute failed"))
	stats.record(200*time.Millisecond, nil)

	if got := stats.avg(); got != 200*time.Millisecond {
		t.Fatalf("avg = %v, want %v", got, 200*time.Millisecond)
	}
	if got := stats.maxDuration(); got != 300*time.Millisecond {
		t.Fatalf("max = %v, want %v", got, 300*time.Millisecond)
	}
	if got := stats.errorCount(); got != 1 {
		t.Fatalf("errors = %d, want 1", got)
	}
	if got := stats.firstErrorMessage(); got != "compute failed" {
		t.Fatalf("first error = %q, want %q", got, "compute failed")
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(d time.Duration) {
			defer wg.Done()
			stats.record(d, nil)
		}(time.Duration(i+1) * time.Millisecond)
	}
	wg.Wait()

	if got := stats.maxDuration(); got != 300*time.Millisecond {
		t.Fatalf("max after concurrent records = %v, want %v", got, 300*time.Millisecond)
	}
	if got := stats.errorCount(); got != 1 {
		t.Fatalf("errors after concurrent records = %d, want 1", got)
	}
}

func TestMoveSetPriorityScore(t *testing.T) {
	lowPriceCandidate := candidateWithPrice(t, 0.10)
	highPriceCandidate := candidateWithPrice(t, 0.50)

	lowCmd := Command{Candidates: []*Candidate{lowPriceCandidate}}
	if got := moveSetPriorityScore(lowCmd); got != 51 {
		t.Fatalf("single-node move set score = %v, want 51", got)
	}
	multiCmd := Command{Candidates: []*Candidate{lowPriceCandidate, highPriceCandidate}}
	if got := moveSetPriorityScore(multiCmd); got != 152 {
		t.Fatalf("multi-node move set score = %v, want 152", got)
	}
}

func TestScoreBasedPairSamplingWeightsFavorLowUtilizationWithoutExcludingHigh(t *testing.T) {
	if low, high := scoreBasedPairSamplingWeight(0.1), scoreBasedPairSamplingWeight(0.95); low <= high || high <= 0 {
		t.Fatalf("pair weights for low/high utilization = %v/%v, want low > high > 0", low, high)
	}

	weights := []float64{1, 9, 1, 10}
	prefix := []float64{1, 10, 11, 21}
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // A fixed seed makes this sampler-distribution test repeatable.
	counts := make([]int, len(weights))
	const samples = 120_000
	for range samples {
		index := weightedCandidateIndex(rng, weights, prefix, 1)
		if index == 1 {
			t.Fatal("weighted sampling selected the excluded candidate")
		}
		counts[index]++
	}
	for index, want := range map[int]float64{0: 1.0 / 12, 2: 1.0 / 12, 3: 10.0 / 12} {
		got := float64(counts[index]) / samples
		if delta := got - want; delta < -0.01 || delta > 0.01 {
			t.Errorf("candidate %d sampled at %.3f, want approximately %.3f", index, got, want)
		}
	}
}

func TestGrowScoreBasedBatchesUsesAdditiveSingleScoresAndRequiresZeroReplacement(t *testing.T) {
	first := scoreBasedBatchTestCandidate("first", "pool")
	second := scoreBasedBatchTestCandidate("second", "pool")
	third := scoreBasedBatchTestCandidate("third", "pool")
	singletons := []*moveSetEvaluation{
		{Command: Command{Candidates: []*Candidate{first}}, Score: 4},
		{Command: Command{Candidates: []*Candidate{second}}, Score: 3},
		{Command: Command{Candidates: []*Candidate{third}}, Score: 2},
	}
	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		if len(candidates) == 1 {
			return Command{Candidates: candidates}, nil
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "result-marker", Namespace: "default"}}
		return Command{Candidates: candidates, Results: pscheduling.Results{PodErrors: map[*corev1.Pod]error{pod: fmt.Errorf("retained simulation result")}}}, nil
	}
	batches, _, timedOut, err := growScoreBasedBatches(context.Background(), singletons, map[string]int{"pool": 3}, nil, compute, &moveSetSearchStats{})
	if err != nil || timedOut {
		t.Fatalf("growScoreBasedBatches() error/timedOut = %v/%v, want nil/false", err, timedOut)
	}
	assertAdditiveBatchResults(t, batches, map[string]float64{
		candidateMoveSetKey([]*Candidate{first}):  4,
		candidateMoveSetKey([]*Candidate{second}): 3,
		candidateMoveSetKey([]*Candidate{third}):  2,
	})

	replacementCompute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		if len(candidates) > 1 {
			return Command{Candidates: candidates, Replacements: []*Replacement{{Name: "replacement"}}}, nil
		}
		return Command{Candidates: candidates}, nil
	}
	withoutZeroReplacement, _, _, err := growScoreBasedBatches(context.Background(), singletons[:2], map[string]int{"pool": 2}, nil, replacementCompute, &moveSetSearchStats{})
	if err != nil {
		t.Fatalf("growScoreBasedBatches() with replacement error = %v", err)
	}
	if len(withoutZeroReplacement) != 0 {
		t.Fatalf("replacement batches retained = %d, want none", len(withoutZeroReplacement))
	}
}

func assertAdditiveBatchResults(t *testing.T, batches []*moveSetEvaluation, scoreByCandidate map[string]float64) {
	t.Helper()
	var foundTwoNodeBatch, foundThreeNodeBatch bool
	for _, batch := range batches {
		var wantScore float64
		for _, candidate := range batch.Command.Candidates {
			wantScore += scoreByCandidate[candidateMoveSetKey([]*Candidate{candidate})]
		}
		if batch.Score != wantScore {
			t.Errorf("batch score = %v, want sum of singleton scores %v", batch.Score, wantScore)
		}
		if len(batch.Command.Results.PodErrors) != 1 {
			t.Errorf("batch scheduling result PodErrors = %v, want retained simulation result", batch.Command.Results.PodErrors)
		}
		switch len(batch.Command.Candidates) {
		case 2:
			foundTwoNodeBatch = true
		case 3:
			foundThreeNodeBatch = true
		}
	}
	if !foundTwoNodeBatch || !foundThreeNodeBatch {
		t.Fatalf("batch sizes retained = %v, want feasible two- and three-node fallbacks", batchSizes(batches))
	}
}

func TestScoreBasedBatchPlanningEnforcesBudgetsPacingAndDeadline(t *testing.T) {
	first := scoreBasedBatchTestCandidate("first", "pool")
	second := scoreBasedBatchTestCandidate("second", "pool")
	singletons := []*moveSetEvaluation{
		{Command: Command{Candidates: []*Candidate{first}}, Score: 2},
		{Command: Command{Candidates: []*Candidate{second}}, Score: 1},
	}
	computeCalls := 0
	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		computeCalls++
		return Command{Candidates: candidates}, nil
	}

	batches, attempted, _, err := growScoreBasedBatches(context.Background(), singletons, map[string]int{"pool": 1}, nil, compute, &moveSetSearchStats{})
	if err != nil {
		t.Fatalf("budget-limited batch search error = %v", err)
	}
	if len(batches) != 0 || attempted != 0 || computeCalls != 0 {
		t.Fatalf("budget-limited search returned %d batches after %d simulations (%d compute calls), want all zero", len(batches), attempted, computeCalls)
	}

	first.NodePool.Annotations = map[string]string{
		v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey: "5",
		v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey:    "1",
	}
	second.NodePool = first.NodePool
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	batches, attempted, _, err = growScoreBasedBatches(context.Background(), singletons, map[string]int{"pool": 2}, pace, compute, &moveSetSearchStats{})
	if err != nil {
		t.Fatalf("pace-limited batch search error = %v", err)
	}
	if len(batches) != 0 || attempted != 0 || computeCalls != 0 {
		t.Fatalf("pace-limited search returned %d batches after %d simulations (%d compute calls), want all zero", len(batches), attempted, computeCalls)
	}

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, _, timedOut, err := growScoreBasedBatches(expired, singletons, map[string]int{"pool": 2}, nil, compute, &moveSetSearchStats{})
	if err != nil || !timedOut {
		t.Fatalf("expired batch search error/timedOut = %v/%v, want nil/true", err, timedOut)
	}
}

func TestScoreBasedPairSearchSamplesUniqueBoundedPairs(t *testing.T) {
	candidates := []*Candidate{
		scoreBasedBatchTestCandidate("one", "pool"),
		scoreBasedBatchTestCandidate("two", "pool"),
		scoreBasedBatchTestCandidate("three", "pool"),
		scoreBasedBatchTestCandidate("four", "pool"),
	}
	pairs, exhaustive, constrained := scoreBasedPairMoveSets(context.Background(), candidates, map[string]int{"pool": 4}, nil, 3, rand.New(rand.NewSource(1))) //nolint:gosec // Fixed seed makes pair-set sampling deterministic.
	if exhaustive || constrained {
		t.Fatalf("pair search exhaustive/constrained = %v/%v, want false/false", exhaustive, constrained)
	}
	if len(pairs) != 3 {
		t.Fatalf("sampled pairs = %d, want limit 3", len(pairs))
	}
	seen := map[string]struct{}{}
	for _, pair := range pairs {
		if len(pair.Nodes) != 2 {
			t.Fatalf("pair candidate count = %d, want 2", len(pair.Nodes))
		}
		key := candidateMoveSetKey(pair.Nodes)
		if _, found := seen[key]; found {
			t.Fatalf("duplicate sampled pair %q", key)
		}
		seen[key] = struct{}{}
	}
}

func TestRetainScoreBasedMoveSetResultsKeepsBestSingletonAndSmallBatchWithinLimit(t *testing.T) {
	single := scoreBasedBatchTestCandidate("best-single", "pool")
	singles := []*moveSetEvaluation{{Command: Command{Candidates: []*Candidate{single}}, Score: 100}}
	var batches []*moveSetEvaluation
	for i := 0; i < 12; i++ {
		candidates := make([]*Candidate, 0, i+2)
		for j := 0; j < i+2; j++ {
			candidates = append(candidates, scoreBasedBatchTestCandidate(fmt.Sprintf("batch-%d-node-%d", i, j), fmt.Sprintf("pool-%d", i)))
		}
		batches = append(batches, &moveSetEvaluation{Command: Command{Candidates: candidates}, Score: float64(1000 + i)})
	}
	retained := retainScoreBasedMoveSetResults(singles, batches, 10)
	if len(retained) > 10 {
		t.Fatalf("retained results = %d, want at most 10", len(retained))
	}
	bestSingletonRetained, smallestBatchRetained := false, false
	for _, eval := range retained {
		if candidateMoveSetKey(eval.Command.Candidates) == candidateMoveSetKey([]*Candidate{single}) {
			bestSingletonRetained = true
		}
		if len(eval.Command.Candidates) == 2 {
			smallestBatchRetained = true
		}
	}
	if !bestSingletonRetained || !smallestBatchRetained {
		t.Fatalf("best singleton/smallest batch retained = %v/%v, want true/true", bestSingletonRetained, smallestBatchRetained)
	}
}

func scoreBasedBatchTestCandidate(name, pool string) *Candidate {
	offering := &cloudprovider.Offering{
		Price: 0.2,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
			corev1.LabelTopologyZone: "us-east-1a",
		}),
	}
	instanceType := &cloudprovider.InstanceType{
		Name: "m5.large",
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("2"),
		},
		Offerings: cloudprovider.Offerings{offering},
	}
	claim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: name,
		Labels: map[string]string{
			corev1.LabelInstanceTypeStable: instanceType.Name,
			v1.CapacityTypeLabelKey:        v1.CapacityTypeOnDemand,
			corev1.LabelTopologyZone:       "us-east-1a",
		},
	}, Spec: corev1.NodeSpec{ProviderID: name}}
	stateNode := state.NewNode()
	stateNode.Node = node
	stateNode.NodeClaim = claim
	return &Candidate{
		StateNode:         stateNode,
		instanceType:      instanceType,
		NodePool:          &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: pool}},
		capacityType:      v1.CapacityTypeOnDemand,
		zone:              "us-east-1a",
		reschedulablePods: []*corev1.Pod{{}},
	}
}

func batchSizes(evals []*moveSetEvaluation) []int {
	sizes := make([]int, 0, len(evals))
	for _, eval := range evals {
		sizes = append(sizes, len(eval.Command.Candidates))
	}
	return sizes
}

func TestEvaluateMoveSetsPar_respectsOrder(t *testing.T) {
	ctx := context.Background()
	moveSets := make([]moveSet, 2)
	delays := []time.Duration{50 * time.Millisecond, 5 * time.Millisecond}
	for i := range moveSets {
		candidate := candidateWithPrice(t, 0.10)
		candidate.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i)}}
		moveSets[i] = moveSet{Nodes: []*Candidate{candidate}}
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

	winner, evaluated, _, _, err := evaluateMoveSetsPar(ctx, moveSets, time.Now().Add(time.Second), compute)
	if err != nil {
		t.Fatalf("evaluateMoveSetsPar() error = %v", err)
	}
	if len(winner) != 2 {
		t.Fatalf("evaluations = %d, want 2", len(winner))
	}
	if winner[0].Command.Candidates[0].NodePool.Name != "0" {
		t.Fatalf("first evaluation index = %q, want %q", winner[0].Command.Candidates[0].NodePool.Name, "0")
	}
	if winner[1].Command.Candidates[0].NodePool.Name != "1" {
		t.Fatalf("second evaluation index = %q, want %q", winner[1].Command.Candidates[0].NodePool.Name, "1")
	}
	if evaluated != 2 {
		t.Fatalf("evaluated = %d, want 2", evaluated)
	}
}

func TestEvaluateMoveSetsPar_stopsOnDeadline(t *testing.T) {
	ctx := context.Background()
	candidate := candidateWithPrice(t, 0.10)
	moveSets := []moveSet{{Nodes: []*Candidate{candidate}}, {Nodes: []*Candidate{candidate}}}

	compute := func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		select {
		case <-time.After(time.Second):
			return Command{Candidates: candidates}, nil
		case <-ctx.Done():
			return Command{}, ctx.Err()
		}
	}

	winner, evaluated, timedOut, _, err := evaluateMoveSetsPar(ctx, moveSets, time.Now().Add(-time.Millisecond), compute)
	if err != nil {
		t.Fatalf("evaluateMoveSetsPar() error = %v", err)
	}
	if len(winner) != 0 {
		t.Fatalf("evaluations = %d, want 0", len(winner))
	}
	if evaluated != 0 {
		t.Fatalf("evaluated = %d, want 0", evaluated)
	}
	if !timedOut {
		t.Fatal("timedOut = false, want true")
	}
}

func TestEvaluateMoveSetsPar_returnsTopResultsAfterSearch(t *testing.T) {
	ctx := context.Background()
	moveSets := make([]moveSet, 12)
	for i := range moveSets {
		price := 0.10 + float64(i)*0.01
		candidate := candidateWithPrice(t, price)
		candidate.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i)}}
		moveSets[i] = moveSet{Nodes: []*Candidate{candidate}}
	}

	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		return Command{Candidates: candidates}, nil
	}

	evals, evaluated, _, _, err := evaluateMoveSetsPar(ctx, moveSets, time.Now().Add(time.Second), compute)
	if err != nil {
		t.Fatalf("evaluateMoveSetsPar() error = %v", err)
	}
	if len(evals) != scoreBasedMoveSetResultLimit {
		t.Fatalf("evaluations = %d, want %d", len(evals), scoreBasedMoveSetResultLimit)
	}
	if evaluated != len(moveSets) {
		t.Fatalf("evaluated = %d, want %d", evaluated, len(moveSets))
	}
	for i, eval := range evals {
		want := strconv.Itoa(len(moveSets) - 1 - i)
		if eval.Command.Candidates[0].NodePool.Name != want {
			t.Fatalf("evaluation[%d] index = %q, want %q", i, eval.Command.Candidates[0].NodePool.Name, want)
		}
	}
}

func TestEvaluateMoveSetsPar_returnsPartialOnTimeout(t *testing.T) {
	ctx := context.Background()
	moveSets := make([]moveSet, 3)
	for i := range moveSets {
		candidate := candidateWithPrice(t, 0.10)
		candidate.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i)}}
		moveSets[i] = moveSet{Nodes: []*Candidate{candidate}}
	}

	compute := func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		idx, err := strconv.Atoi(candidates[0].NodePool.Name)
		if err != nil {
			t.Fatalf("unexpected move set index: %v", err)
		}
		delay := time.Millisecond
		if idx > 0 {
			delay = time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return Command{}, ctx.Err()
		}
		return Command{Candidates: candidates}, nil
	}

	evals, evaluated, timedOut, _, err := evaluateMoveSetsPar(ctx, moveSets, time.Now().Add(100*time.Millisecond), compute)
	if err != nil {
		t.Fatalf("evaluateMoveSetsPar() error = %v", err)
	}
	if len(evals) != 1 {
		t.Fatalf("evaluations = %d, want 1", len(evals))
	}
	if evals[0].Command.Candidates[0].NodePool.Name != "0" {
		t.Fatalf("evaluation index = %q, want %q", evals[0].Command.Candidates[0].NodePool.Name, "0")
	}
	if evaluated != 1 {
		t.Fatalf("evaluated = %d, want 1", evaluated)
	}
	if !timedOut {
		t.Fatal("timedOut = false, want true")
	}
}

func TestSelectFirstValidatedCommand_validationFallbackReportsRejectedAttempt(t *testing.T) {
	ctx := context.Background()
	fakeRecorder := record.NewFakeRecorder(10)
	recorder := events.NewRecorder(fakeRecorder)
	highScoreCandidate := candidateWithPrice(t, 0.50)
	highScoreCandidate.Node.Name = "high-score"
	highScoreCandidate.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "high-score"}}
	lowScoreCandidate := candidateWithPrice(t, 0.10)
	lowScoreCandidate.Node.Name = "low-score"
	lowScoreCandidate.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "low-score"}}
	evals := []*moveSetEvaluation{
		{Command: Command{Candidates: []*Candidate{highScoreCandidate}}, Score: 0.50},
		{Command: Command{Candidates: []*Candidate{lowScoreCandidate}}, Score: 0.10},
	}
	validator := rejectFirstCommandValidator{}

	cmd, selectedIdx, err := selectFirstStillValidCommand(ctx, validator, recorder, evals)
	if err != nil {
		t.Fatalf("selectFirstValidatedCommand() error = %v", err)
	}
	if selectedIdx != 1 {
		t.Fatalf("selected eval index = %d, want 1", selectedIdx)
	}
	if len(cmd.Candidates) != 1 {
		t.Fatalf("command candidates = %d, want 1", len(cmd.Candidates))
	}
	if cmd.Candidates[0] != lowScoreCandidate {
		t.Fatal("expected fallback to lower-scored command after validation rejection")
	}
	rejectedEvents := collectFakeRecorderEvents(fakeRecorder)
	if !eventsMentioningNode(rejectedEvents, "high-score") {
		t.Fatal("expected rejected events for the failed high-score command")
	}
	if eventsMentioningNode(rejectedEvents, "low-score") {
		t.Fatal("did not expect rejected events for the validated fallback command")
	}
}

func TestSelectFirstValidatedCommand_emitsRejectedEventForEachAttemptWhenAllFail(t *testing.T) {
	ctx := context.Background()
	fakeRecorder := record.NewFakeRecorder(10)
	recorder := events.NewRecorder(fakeRecorder)
	highScoreCandidate := candidateWithPrice(t, 0.50)
	highScoreCandidate.Node.Name = "high-score"
	highScoreCandidate.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "high-score"}}
	lowScoreCandidate := candidateWithPrice(t, 0.10)
	lowScoreCandidate.Node.Name = "low-score"
	lowScoreCandidate.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "low-score"}}
	evals := []*moveSetEvaluation{
		{Command: Command{Candidates: []*Candidate{highScoreCandidate}}, Score: 0.50},
		{Command: Command{Candidates: []*Candidate{lowScoreCandidate}}, Score: 0.10},
	}

	cmd, selectedIdx, err := selectFirstStillValidCommand(ctx, rejectAllCommandValidator{}, recorder, evals)
	if err != nil {
		t.Fatalf("selectFirstValidatedCommand() error = %v", err)
	}
	if selectedIdx != -1 {
		t.Fatalf("selected eval index = %d, want -1", selectedIdx)
	}
	if len(cmd.Candidates) != 0 {
		t.Fatalf("command candidates = %d, want 0", len(cmd.Candidates))
	}

	rejectedEvents := collectFakeRecorderEvents(fakeRecorder)
	if !eventsMentioningNode(rejectedEvents, "low-score") || !eventsMentioningNode(rejectedEvents, "high-score") {
		t.Fatal("expected rejected events for each command that failed validation")
	}
}

func TestScoreBasedSearchEventRecorderSuppressesOnlyCandidateEvents(t *testing.T) {
	capture := &capturedEventRecorder{}
	recorder := scoreBasedSearchEventRecorder{recorder: capture}
	recorder.Publish(
		events.Event{Reason: events.ConsolidationCandidate},
		events.Event{Reason: events.Unconsolidatable},
	)
	if len(capture.events) != 1 || capture.events[0].Reason != events.Unconsolidatable {
		t.Fatalf("forwarded events = %#v, want only the Unconsolidatable blocker event", capture.events)
	}
}

func TestPublishScoreBasedNoSavingsEventIsDedupeable(t *testing.T) {
	fakeRecorder := record.NewFakeRecorder(10)
	recorder := events.NewRecorder(fakeRecorder)
	candidate := candidateWithPrice(t, 0)
	candidate.Node.UID = "node-uid"
	candidate.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim", UID: "nodeclaim-uid"}}
	cmd := Command{Candidates: []*Candidate{candidate}}

	publishScoreBasedNoSavingsEvent(recorder, cmd, nil)
	publishScoreBasedNoSavingsEvent(recorder, cmd, nil)
	recorded := collectFakeRecorderEvents(fakeRecorder)
	if len(recorded) != 2 {
		t.Fatalf("recorded Unconsolidatable events = %d, want one per Node and NodeClaim after deduplication", len(recorded))
	}
}

type capturedEventRecorder struct {
	events []events.Event
}

func (r *capturedEventRecorder) Publish(evts ...events.Event) {
	r.events = append(r.events, evts...)
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

type rejectFirstCommandValidator struct{}

func (rejectFirstCommandValidator) Validate(_ context.Context, cmd Command, _ time.Duration) (Command, error) {
	if len(cmd.Candidates) > 0 && cmd.Candidates[0].Name() == "high-score" {
		return Command{}, NewSchedulingValidationError(fmt.Errorf("rejected high-score command"))
	}
	return cmd, nil
}

type rejectAllCommandValidator struct{}

func (rejectAllCommandValidator) Validate(_ context.Context, _ Command, _ time.Duration) (Command, error) {
	return Command{}, NewSchedulingValidationError(fmt.Errorf("rejected"))
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
		Name: "m5.large",
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("2"),
		},
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
		instanceType:      &cloudprovider.InstanceType{Name: "m5.large"},
		reschedulablePods: []*corev1.Pod{{}},
	}
}
