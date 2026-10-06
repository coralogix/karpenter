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
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/funcr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
	searchStats := &moveSetSearchStats{}
	searchStats.record(time.Millisecond, fmt.Errorf("compute failed"))
	searchStats.recordNoOpMoveSet()
	searchStats.recordNonPositiveSavingsMoveSet()
	searchStats.recordNonPositiveScoreMoveSet()
	details := scoreBasedNoMoveDetails{
		candidateCount:              6,
		compactionCandidateCount:    6,
		budgetBlockedCandidateCount: 3,
		budgetBlockedNodePools:      map[string]struct{}{"zeta": {}, "alpha": {}},
		paceBlockedCandidateCount:   2,
		paceBlockedNodePools:        map[string]struct{}{"gamma": {}, "beta": {}},
		validCandidateCount:         1,
	}
	details.recordSearch(moveSetSearchResult{evaluated: 5, timedOut: true, stats: searchStats})
	details.validMoveSetCount = 2
	details.validationAttemptCount = 2
	logScoreBasedCompactionNoMove(ctx, scoreBasedNoMoveBudgetOrPaceFiltered, details)

	for _, field := range []string{
		`"reason"="budget_or_pace_filtered"`,
		`"candidateCount"=6`,
		`"budgetBlockedCandidateCount"=3`,
		`"paceBlockedCandidateCount"=2`,
		`"validCandidateCount"=1`,
		`"moveSetsEvaluated"=5`,
		`"validMoveSetCount"=2`,
		`"noOpMoveSetCount"=1`,
		`"nonPositiveSavingsMoveSetCount"=1`,
		`"nonPositiveScoreMoveSetCount"=1`,
		`"searchTimedOut"=true`,
		`"moveSetEvaluationErrorCount"=1`,
		`"firstMoveSetEvaluationError"="compute failed"`,
		`"validationAttemptCount"=2`,
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

	t.Run("balanced score-based pool", func(t *testing.T) {
		candidate := makeCandidate(true)
		candidate.NodePool.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyBalanced
		if scoreBased.ShouldDisrupt(ctx, candidate) {
			t.Fatal("expected score-based consolidation to reject Balanced policy")
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

func TestScoreBasedCompactionWaitsForStatefulSetTermination(t *testing.T) {
	ctx := context.Background()
	scoreBased := NewScoreBasedConsolidation(MakeConsolidation(nil, nil, nil, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil))
	cases := []struct {
		name  string
		phase corev1.PodPhase
	}{
		{name: "terminating StatefulSet workload", phase: corev1.PodRunning},
		{name: "terminal StatefulSet workload with finalizer", phase: corev1.PodSucceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidate := scoreBasedTestCandidate(true)
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "stateful-workload",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
					Finalizers:        []string{"test.finalizer"},
					OwnerReferences:   []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "workload"}},
				},
				Status: corev1.PodStatus{Phase: tc.phase},
			}
			candidate.reschedulablePods = []*corev1.Pod{pod}
			if scoreBased.shouldCompact(ctx, candidate) {
				t.Fatal("score-based compaction must wait for terminating or terminal StatefulSet pods to leave before admitting the node")
			}
		})
	}
}

func TestScoreBasedFinalValidationUsesLiveOccupancyAndPaceConfig(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	testClock := clocktesting.NewFakeClock(now)
	pace := NewUnderutilizedConsolidationPace(testClock)
	nodePool := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "default", Annotations: map[string]string{
			v1.ScoreBasedConsolidationAnnotationKey:                  "",
			v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey: "4",
			v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey:    "2",
		}},
		Spec: v1.NodePoolSpec{Disruption: v1.Disruption{ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized}},
	}
	makeCandidate := func(name string) (*Candidate, *corev1.Pod) {
		candidate := scoreBasedTestCandidate(true)
		candidate.NodePool = nodePool.DeepCopy()
		candidate.Node.Name = name
		candidate.Node.UID = types.UID(name + "-uid")
		candidate.Node.Spec.ProviderID = "provider-" + name
		candidate.Node.Labels[v1.NodePoolLabelKey] = nodePool.Name
		candidate.NodeClaim.Name = "claim-" + name
		candidate.NodeClaim.UID = types.UID("claim-" + name + "-uid")
		candidate.NodeClaim.Labels[v1.NodePoolLabelKey] = nodePool.Name
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "workload-" + name, Namespace: "default"}, Spec: corev1.PodSpec{NodeName: name}}
		candidate.reschedulablePods = []*corev1.Pod{pod.DeepCopy()}
		return candidate, pod
	}
	first, firstPod := makeCandidate("node-1")
	second, secondPod := makeCandidate("node-2")
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(nodePool, first.Node, first.NodeClaim, firstPod, second.Node, second.NodeClaim, secondPod).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).Build()
	recorder := events.NewRecorder(record.NewFakeRecorder(10))
	scoreBased := &ScoreBasedConsolidation{consolidation: MakeConsolidation(testClock, nil, kubeClient, nil, nil, recorder, nil, pace)}
	cmd := Command{Method: scoreBased, Candidates: []*Candidate{first, second}}

	// The live pool lowered the per-command cap after search planned this two-node batch.
	liveNodePool := nodePool.DeepCopy()
	liveNodePool.Annotations[v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey] = "1"
	if err := kubeClient.Update(ctx, liveNodePool); err != nil {
		t.Fatal(err)
	}
	if err := scoreBased.validateCommandPaceAndCap(ctx, &cmd); !IsValidationError(err) {
		t.Fatalf("validation error = %v, want a validation failure after the live cap was lowered", err)
	}

	// With the live cap restored and rate lowered, validation refreshes the candidate pools so
	// the ensuing charge uses the same live rate that admitted the command.
	liveNodePool.Annotations[v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey] = "2"
	liveNodePool.Annotations[v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey] = "2"
	if err := kubeClient.Update(ctx, liveNodePool); err != nil {
		t.Fatal(err)
	}
	cmd.Candidates = []*Candidate{{StateNode: first.StateNode, NodePool: first.NodePool, reschedulablePods: first.reschedulablePods}, {StateNode: second.StateNode, NodePool: second.NodePool, reschedulablePods: second.reschedulablePods}}
	if err := scoreBased.validateCommandPaceAndCap(ctx, &cmd); err != nil {
		t.Fatalf("validation with the current live pace config: %v", err)
	}
	cmd.Method = scoreBased
	pace.Charge(&cmd)
	testClock.Step(59 * time.Second)
	if pace.candidateAllowed(liveNodePool, 0) {
		t.Fatal("pace charge used the stale rate instead of the live rate refreshed during validation")
	}

	// A selected node that became empty according to the live API must fall back to standby.
	terminalPod := &corev1.Pod{}
	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(firstPod), terminalPod); err != nil {
		t.Fatal(err)
	}
	terminalPod.Status.Phase = corev1.PodSucceeded
	if err := kubeClient.Status().Update(ctx, terminalPod); err != nil {
		t.Fatal(err)
	}
	cmd.Candidates = cmd.Candidates[:1]
	if err := scoreBased.validateCommandPaceAndCap(ctx, &cmd); !IsValidationError(err) || getValidationFailureReason(err) != "churn" {
		t.Fatalf("validation for a now-empty candidate = %v, want churn validation failure", err)
	}
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

	snapshot := stats.snapshot()
	if got := snapshot.averageDuration; got != 200*time.Millisecond {
		t.Fatalf("avg = %v, want %v", got, 200*time.Millisecond)
	}
	if got := snapshot.maxDuration; got != 300*time.Millisecond {
		t.Fatalf("max = %v, want %v", got, 300*time.Millisecond)
	}
	if got := snapshot.errorCount; got != 1 {
		t.Fatalf("errors = %d, want 1", got)
	}
	if got := snapshot.firstError; got != "compute failed" {
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

	snapshot = stats.snapshot()
	if got := snapshot.maxDuration; got != 300*time.Millisecond {
		t.Fatalf("max after concurrent records = %v, want %v", got, 300*time.Millisecond)
	}
	if got := snapshot.errorCount; got != 1 {
		t.Fatalf("errors after concurrent records = %d, want 1", got)
	}
}

func TestMoveSetScorerCombinesNormalizedCostAndUtilisation(t *testing.T) {
	lowPriceCandidate := candidateWithPrice(t, 0.10)
	highPriceCandidate := candidateWithPrice(t, 0.50)
	lowPriceCandidate.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool"}}
	highPriceCandidate.NodePool = lowPriceCandidate.NodePool
	lowPriceCandidate.Node.Name = "node-low"
	highPriceCandidate.Node.Name = "node-high"
	key := scoreBasedPriceGroupKey{nodePool: "pool", capacityType: v1.CapacityTypeOnDemand}
	prices := &scoreBasedPriceSnapshot{
		groups: map[scoreBasedPriceGroupKey]*scoreBasedPriceGroup{
			key: {totalPrice: 0.60, totalCPU: 4, nodeCount: 2, known: true, baseline: 0.15},
		},
		nodes: map[string]scoreBasedNodePrice{
			"node-low":  {group: key, cpu: 2, price: 0.10, known: true},
			"node-high": {group: key, cpu: 2, price: 0.50, known: true},
		},
	}

	lowCmd := Command{Candidates: []*Candidate{lowPriceCandidate}}
	configs := scoreBasedScoringConfig{"pool": {utilisationWeight: defaultScoreBasedUtilisationWeight}}
	scorer := newMoveSetScorer(prices, configs, nil, newScoreBasedInstanceTypeConcentrationSnapshot(nil, configs))
	if got := scorer.Score(lowCmd).Total; math.Abs(got-(1+1.0/3.0)) > 1e-9 {
		t.Fatalf("single-node move set score = %v, want normalized cost 1/3 plus utilization 1", got)
	}
	multiCmd := Command{Candidates: []*Candidate{lowPriceCandidate, highPriceCandidate}}
	if got := scorer.Score(multiCmd).Total; got != 4 {
		t.Fatalf("multi-node move set score = %v, want normalized cost 2 plus utilization 2", got)
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
		{Command: Command{Candidates: []*Candidate{first}}, Score: moveSetScore{Total: 4}},
		{Command: Command{Candidates: []*Candidate{second}}, Score: moveSetScore{Total: 3}},
		{Command: Command{Candidates: []*Candidate{third}}, Score: moveSetScore{Total: 2}},
	}
	singletonScores := map[string]float64{
		candidateMoveSetKey([]*Candidate{first}):  4,
		candidateMoveSetKey([]*Candidate{second}): 3,
		candidateMoveSetKey([]*Candidate{third}):  2,
	}
	scoreAdditiveFixture := func(command Command) moveSetScore {
		var total float64
		for _, candidate := range command.Candidates {
			total += singletonScores[candidateMoveSetKey([]*Candidate{candidate})]
		}
		return moveSetScore{Total: total, Factors: moveSetScoreFactors{
			Cost:                      scoreBasedCostScore{Enabled: true, Valid: true, Status: scoreFactorApplied, Contribution: total},
			Utilisation:               scoreBasedUtilisationScore{Status: scoreFactorDisabled, Reason: "test_score_not_calculated"},
			MemoryBalance:             scoreBasedMemoryScore{Status: scoreFactorDisabled, Reason: "test_score_not_calculated"},
			InstanceTypeConcentration: scoreBasedInstanceTypeConcentrationScore{Status: scoreFactorDisabled, Reason: "test_score_not_calculated"},
		}}
	}
	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		if len(candidates) == 1 {
			return Command{Candidates: candidates}, nil
		}
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "result-marker", Namespace: "default"}}
		return Command{Candidates: candidates, Results: pscheduling.Results{PodErrors: map[*corev1.Pod]error{pod: fmt.Errorf("retained simulation result")}}}, nil
	}
	batchOutcome, err := growScoreBasedBatches(context.Background(), singletons, map[string]int{"pool": 3}, nil, compute, &moveSetSearchStats{}, scoreAdditiveFixture)
	if err != nil || batchOutcome.timedOut || !batchOutcome.complete {
		t.Fatalf("growScoreBasedBatches() error/timedOut/complete = %v/%v/%v, want nil/false/true", err, batchOutcome.timedOut, batchOutcome.complete)
	}
	assertAdditiveBatchResults(t, batchOutcome.evals, map[string]float64{
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
	withoutZeroReplacement, err := growScoreBasedBatches(context.Background(), singletons[:2], map[string]int{"pool": 2}, nil, replacementCompute, &moveSetSearchStats{}, scoreAdditiveFixture)
	if err != nil {
		t.Fatalf("growScoreBasedBatches() with replacement error = %v", err)
	}
	if len(withoutZeroReplacement.evals) != 0 {
		t.Fatalf("replacement batches retained = %d, want none", len(withoutZeroReplacement.evals))
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
		if batch.Score.Total != wantScore {
			t.Errorf("batch score = %v, want sum of singleton scores %v", batch.Score.Total, wantScore)
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
		{Command: Command{Candidates: []*Candidate{first}}, Score: moveSetScore{Total: 2}},
		{Command: Command{Candidates: []*Candidate{second}}, Score: moveSetScore{Total: 1}},
	}
	computeCalls := 0
	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		computeCalls++
		return Command{Candidates: candidates}, nil
	}

	batchOutcome, err := growScoreBasedBatches(context.Background(), singletons, map[string]int{"pool": 1}, nil, compute, &moveSetSearchStats{}, sourcePriceSearchTestScore)
	require.NoError(t, err)
	require.Empty(t, batchOutcome.evals)
	require.Zero(t, batchOutcome.evaluated)
	require.Zero(t, computeCalls)
	require.True(t, batchOutcome.complete)

	first.NodePool.Annotations = map[string]string{
		v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey: "5",
		v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey:    "1",
	}
	first.reschedulablePods = nil
	second.reschedulablePods = nil
	second.NodePool = first.NodePool
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	batchOutcome, err = growScoreBasedBatches(context.Background(), singletons, map[string]int{"pool": 2}, pace, compute, &moveSetSearchStats{}, sourcePriceSearchTestScore)
	require.NoError(t, err)
	require.Empty(t, batchOutcome.evals)
	require.Zero(t, batchOutcome.evaluated)
	require.Zero(t, computeCalls)
	require.True(t, batchOutcome.complete)

	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	batchOutcome, err = growScoreBasedBatches(expired, singletons, map[string]int{"pool": 2}, nil, compute, &moveSetSearchStats{}, sourcePriceSearchTestScore)
	require.NoError(t, err)
	require.True(t, batchOutcome.timedOut)
	require.False(t, batchOutcome.complete)
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
	singles := []*moveSetEvaluation{{Command: Command{Candidates: []*Candidate{single}}, Score: moveSetScore{Total: 100}}}
	var batches []*moveSetEvaluation
	for i := 0; i < 12; i++ {
		candidates := make([]*Candidate, 0, i+2)
		for j := 0; j < i+2; j++ {
			candidates = append(candidates, scoreBasedBatchTestCandidate(fmt.Sprintf("batch-%d-node-%d", i, j), fmt.Sprintf("pool-%d", i)))
		}
		batches = append(batches, &moveSetEvaluation{Command: Command{Candidates: candidates}, Score: moveSetScore{Total: float64(1000 + i)}})
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
		StateNode:                stateNode,
		instanceType:             instanceType,
		Price:                    0.2,
		NodePool:                 &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: pool}},
		capacityType:             v1.CapacityTypeOnDemand,
		zone:                     "us-east-1a",
		reschedulablePods:        []*corev1.Pod{{}},
		RescheduleDisruptionCost: PerNodeBaseDisruptionCost + 1,
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
	for i := range moveSets {
		candidate := candidateWithPrice(t, 0.10)
		candidate.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i)}}
		moveSets[i] = moveSet{Nodes: []*Candidate{candidate}}
	}

	compute := func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		delay := 5 * time.Millisecond
		if candidates[0].NodePool.Name == "0" {
			delay = 50 * time.Millisecond
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return Command{}, ctx.Err()
		}
		return Command{Candidates: candidates}, nil
	}

	outcome, err := evaluateMoveSetsParWithStats(ctx, moveSets, time.Now().Add(time.Second), compute, scoreBasedMoveSetResultLimit, nil, sourcePriceSearchTestScore, nil)
	require.NoError(t, err)
	require.Len(t, outcome.evals, 2)
	require.Equal(t, []string{"0", "1"}, []string{
		outcome.evals[0].Command.Candidates[0].NodePool.Name,
		outcome.evals[1].Command.Candidates[0].NodePool.Name,
	})
	require.Equal(t, 2, outcome.evaluated)
	require.True(t, outcome.complete)
	require.False(t, outcome.timedOut)
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

	outcome, err := evaluateMoveSetsParWithStats(ctx, moveSets, time.Now().Add(-time.Millisecond), compute, scoreBasedMoveSetResultLimit, nil, sourcePriceSearchTestScore, nil)
	if err != nil {
		t.Fatalf("evaluateMoveSetsParWithStats() error = %v", err)
	}
	if len(outcome.evals) != 0 {
		t.Fatalf("evaluations = %d, want 0", len(outcome.evals))
	}
	if outcome.evaluated != 0 {
		t.Fatalf("evaluated = %d, want 0", outcome.evaluated)
	}
	if !outcome.timedOut || outcome.complete {
		t.Fatalf("timedOut/complete = %v/%v, want true/false", outcome.timedOut, outcome.complete)
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

	outcome, err := evaluateMoveSetsParWithStats(ctx, moveSets, time.Now().Add(time.Second), compute, scoreBasedMoveSetResultLimit, nil, sourcePriceSearchTestScore, nil)
	if err != nil {
		t.Fatalf("evaluateMoveSetsParWithStats() error = %v", err)
	}
	if len(outcome.evals) != scoreBasedMoveSetResultLimit {
		t.Fatalf("evaluations = %d, want %d", len(outcome.evals), scoreBasedMoveSetResultLimit)
	}
	if outcome.evaluated != len(moveSets) {
		t.Fatalf("evaluated = %d, want %d", outcome.evaluated, len(moveSets))
	}
	if !outcome.complete || outcome.timedOut {
		t.Fatalf("complete/timedOut = %v/%v, want true/false", outcome.complete, outcome.timedOut)
	}
	for i, eval := range outcome.evals {
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
		delay := time.Millisecond
		if candidates[0].NodePool.Name != "0" {
			delay = time.Second
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return Command{}, ctx.Err()
		}
		return Command{Candidates: candidates}, nil
	}

	outcome, err := evaluateMoveSetsParWithStats(ctx, moveSets, time.Now().Add(100*time.Millisecond), compute, scoreBasedMoveSetResultLimit, nil, sourcePriceSearchTestScore, nil)
	require.NoError(t, err)
	require.Len(t, outcome.evals, 1)
	require.Equal(t, "0", outcome.evals[0].Command.Candidates[0].NodePool.Name)
	require.Equal(t, 1, outcome.evaluated)
	require.True(t, outcome.timedOut)
	require.False(t, outcome.complete)
}

func TestEvaluateMoveSetsPar_returnsCanceledErrorWithoutCompleting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	candidate := candidateWithPrice(t, 0.10)
	compute := func(ctx context.Context, candidates ...*Candidate) (Command, error) {
		return Command{Candidates: candidates}, nil
	}

	outcome, err := evaluateMoveSetsParWithStats(ctx, []moveSet{{Nodes: []*Candidate{candidate}}}, time.Now().Add(time.Second), compute, scoreBasedMoveSetResultLimit, nil, sourcePriceSearchTestScore, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("evaluateMoveSetsParWithStats() error = %v, want %v", err, context.Canceled)
	}
	if outcome.complete || outcome.timedOut || len(outcome.evals) != 0 {
		t.Fatalf("canceled outcome = complete %v, timedOut %v, evaluations %d; want false/false/0", outcome.complete, outcome.timedOut, len(outcome.evals))
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
		{Command: Command{Candidates: []*Candidate{highScoreCandidate}}, Score: moveSetScore{Total: 0.50}},
		{Command: Command{Candidates: []*Candidate{lowScoreCandidate}}, Score: moveSetScore{Total: 0.10}},
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
	if evals[0].validationStatus != "failed" || evals[0].validationReason != "scheduling" || evals[1].validationStatus != "validated" {
		t.Fatalf("validation outcomes = %s/%s then %s/%s, want failed/scheduling then validated", evals[0].validationStatus, evals[0].validationReason, evals[1].validationStatus, evals[1].validationReason)
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
		{Command: Command{Candidates: []*Candidate{highScoreCandidate}}, Score: moveSetScore{Total: 0.50}},
		{Command: Command{Candidates: []*Candidate{lowScoreCandidate}}, Score: moveSetScore{Total: 0.10}},
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
	for _, eval := range evals {
		if eval.validationStatus != "failed" || eval.validationReason != "scheduling" {
			t.Errorf("validation outcome = %s/%s, want failed/scheduling", eval.validationStatus, eval.validationReason)
		}
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
		Price:        price,
	}
}

// sourcePriceSearchTestScore makes the parallel-search tests independent of the
// production price-baseline scorer, whose inputs come from an immutable fleet snapshot.
func sourcePriceSearchTestScore(command Command) moveSetScore {
	score := sumCandidatePrices(command.Candidates)
	return moveSetScore{Total: score, Factors: moveSetScoreFactors{
		Cost:                      scoreBasedCostScore{Enabled: true, Valid: true, Status: scoreFactorApplied, Contribution: score},
		Utilisation:               scoreBasedUtilisationScore{Status: scoreFactorDisabled, Reason: "test_score_not_calculated"},
		MemoryBalance:             scoreBasedMemoryScore{Status: scoreFactorDisabled, Reason: "test_score_not_calculated"},
		InstanceTypeConcentration: scoreBasedInstanceTypeConcentrationScore{Status: scoreFactorDisabled, Reason: "test_score_not_calculated"},
	}}
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
		StateNode:                node,
		RescheduleDisruptionCost: PerNodeBaseDisruptionCost + 1,
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
