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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"

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
	moves, stats := searchMoves(context.Background(), sources, time.Now().Add(time.Second), moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		cmd := Command{Candidates: c}
		if c[0].Price == 2 {
			cmd.Replacements = []*Replacement{{}, {}}
		}
		return cmd, nil
	}, inputs: testScoreInputs(sources)}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
	if len(moves) != 1 || moves[0].SearchScore.Total != 4 || stats.Evaluated != 3 {
		t.Fatalf("got %v, stats %+v", moves, stats)
	}
}

func TestSearchMovesPreservesSuccessOnSimulationError(t *testing.T) {
	sources := [][]*Candidate{[]*Candidate{candidateWithPrice(t, 1)}, []*Candidate{candidateWithPrice(t, 2)}}
	moves, stats := searchMoves(context.Background(), sources, time.Now().Add(time.Second), moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		if c[0].Price == 2 {
			return Command{}, fmt.Errorf("simulation failed")
		}
		return Command{Candidates: c}, nil
	}, inputs: testScoreInputs(sources)}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
	if len(moves) != 1 || moves[0].SearchScore.Total != 2 || stats.Errors != 1 || stats.Evaluated != 2 {
		t.Fatalf("got %v, stats %+v", moves, stats)
	}
}

func TestSearchMovesErrorPreventsConsolidatedState(t *testing.T) {
	_, stats := searchMoves(context.Background(), [][]*Candidate{[]*Candidate{candidateWithPrice(t, 1)}}, time.Now().Add(time.Second), moveEvaluator{compute: func(context.Context, ...*Candidate) (Command, error) {
		return Command{}, fmt.Errorf("simulation failed")
	}}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
	if stats.canMarkConsolidated(1, false) {
		t.Fatal("failed search must not mark cluster consolidated")
	}
}

func TestSearchMovesFiltersMoveWhenStandbyCostExceedsSavings(t *testing.T) {
	expensive := candidateWithPrice(t, 10)
	cheap := candidateWithPrice(t, 5)
	sources := [][]*Candidate{[]*Candidate{expensive}, []*Candidate{cheap}}
	moves, stats := searchMoves(context.Background(), sources, time.Now().Add(time.Second), moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		if c[0].Price == 10 {
			cmd := Command{Candidates: c}
			cmd.Results.ExistingNodes = standbyReactivationResults("standby-a", 12)
			return cmd, nil
		}
		return Command{Candidates: c}, nil
	}, inputs: testScoreInputs(sources)}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
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
	moves, stats := searchMoves(context.Background(), sources, time.Now().Add(time.Second), moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		return Command{Candidates: c}, nil
	}, inputs: inputs}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
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
		moves, _ := searchMoves(context.Background(), sources, time.Now().Add(time.Second), moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
			return Command{Candidates: c}, nil
		}, inputs: inputs}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
		if len(moves) != 1 || moves[0].SearchScore.Total != 2 {
			t.Fatalf("evaluations = %+v, want score 2 using default weight", moves)
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

	searchMoves(context.Background(), sources, time.Now().Add(time.Second), moveEvaluator{compute: func(_ context.Context, c ...*Candidate) (Command, error) {
		switch c[0].Price {
		case 1:
			return Command{}, nil
		case 2:
			return Command{Candidates: c, Results: pscheduling.Results{ExistingNodes: standbyReactivationResults("standby-a", 3)}}, nil
		case 3:
			return Command{}, fmt.Errorf("simulation failed")
		}
		return Command{Candidates: c}, nil
	}, inputs: inputs}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec

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
