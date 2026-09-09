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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
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
	if defaultValidator.filter(ctx, candidate) != scoreBased.ShouldDisrupt(ctx, candidate) {
		t.Fatal("score-based consolidation must wire ScoreBasedConsolidation.ShouldDisrupt into its validator")
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

	if got := moveSetPriorityScore(moveSet{Nodes: []*Candidate{lowPriceCandidate}}); got != 0.10 {
		t.Fatalf("single-node move set score = %v, want 0.10", got)
	}
	if got := moveSetPriorityScore(moveSet{Nodes: []*Candidate{lowPriceCandidate, highPriceCandidate}}); got != 0.50 {
		t.Fatalf("multi-node move set score = %v, want 0.50", got)
	}
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

	winner, evaluated, err := evaluateMoveSetsPar(ctx, moveSets, time.Now().Add(time.Second), compute)
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

	winner, evaluated, err := evaluateMoveSetsPar(ctx, moveSets, time.Now().Add(-time.Millisecond), compute)
	if err != nil {
		t.Fatalf("evaluateMoveSetsPar() error = %v", err)
	}
	if len(winner) != 0 {
		t.Fatalf("evaluations = %d, want 0", len(winner))
	}
	if evaluated != 0 {
		t.Fatalf("evaluated = %d, want 0", evaluated)
	}
}

func TestEvaluateMoveSetsPar_stopsAfterNValidEvaluations(t *testing.T) {
	ctx := context.Background()
	moveSets := make([]moveSet, 12)
	for i := range moveSets {
		candidate := candidateWithPrice(t, 0.10)
		candidate.NodePool = &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: strconv.Itoa(i)}}
		moveSets[i] = moveSet{Nodes: []*Candidate{candidate}}
	}

	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		return Command{Candidates: candidates}, nil
	}

	evals, evaluated, err := evaluateMoveSetsPar(ctx, moveSets, time.Now().Add(time.Second), compute)
	if err != nil {
		t.Fatalf("evaluateMoveSetsPar() error = %v", err)
	}
	if len(evals) != scoreBasedValidEvaluationsTarget {
		t.Fatalf("evaluations = %d, want %d", len(evals), scoreBasedValidEvaluationsTarget)
	}
	for i, eval := range evals {
		want := strconv.Itoa(i)
		if eval.Command.Candidates[0].NodePool.Name != want {
			t.Fatalf("evaluation[%d] index = %q, want %q", i, eval.Command.Candidates[0].NodePool.Name, want)
		}
	}
	if evaluated < scoreBasedValidEvaluationsTarget {
		t.Fatalf("evaluated = %d, want at least %d", evaluated, scoreBasedValidEvaluationsTarget)
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

	evals, evaluated, err := evaluateMoveSetsPar(ctx, moveSets, time.Now().Add(100*time.Millisecond), compute)
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
}

func TestSelectFirstValidatedCommand_validationFallback(t *testing.T) {
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

	cmd, err := selectFirstStillValidCommand(ctx, validator, recorder, evals)
	if err != nil {
		t.Fatalf("selectFirstValidatedCommand() error = %v", err)
	}
	if len(cmd.Candidates) != 1 {
		t.Fatalf("command candidates = %d, want 1", len(cmd.Candidates))
	}
	if cmd.Candidates[0] != lowScoreCandidate {
		t.Fatal("expected fallback to lower-scored command after validation rejection")
	}
	if got := len(collectFakeRecorderEvents(fakeRecorder)); got != 0 {
		t.Fatalf("rejected events = %d, want 0 when a fallback command passes validation", got)
	}
}

func TestSelectFirstValidatedCommand_emitsRejectedEventOnlyForFirstEvalWhenAllFail(t *testing.T) {
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

	cmd, err := selectFirstStillValidCommand(ctx, rejectAllCommandValidator{}, recorder, evals)
	if err != nil {
		t.Fatalf("selectFirstValidatedCommand() error = %v", err)
	}
	if len(cmd.Candidates) != 0 {
		t.Fatalf("command candidates = %d, want 0", len(cmd.Candidates))
	}

	rejectedEvents := collectFakeRecorderEvents(fakeRecorder)
	if eventsMentioningNode(rejectedEvents, "low-score") {
		t.Fatal("did not expect rejected events for fallback eval")
	}
	if !eventsMentioningNode(rejectedEvents, "high-score") {
		t.Fatal("expected rejected events for the first eval when all validations fail")
	}
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
		instanceType: &cloudprovider.InstanceType{Name: "m5.large"},
	}
}
