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
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

func TestSearchMovesRejectsUnprofitableAndMultipleReplacements(t *testing.T) {
	moves := []moveSet{
		{Nodes: []*Candidate{candidateWithPrice(t, 0)}},
		{Nodes: []*Candidate{candidateWithPrice(t, 2)}},
		{Nodes: []*Candidate{candidateWithPrice(t, 3)}},
	}
	evals, stats := searchSingleMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		cmd := Command{Candidates: c}
		if c[0].Price == 2 {
			cmd.Replacements = []*Replacement{{}, {}}
		}
		return cmd, nil
	}, testScoreInputs(moves, nil), rand.New(rand.NewSource(1)), clock.RealClock{}, scoreBasedValidEvaluationsTarget) //nolint:gosec
	if len(evals) != 1 || evals[0].Score != 3 || stats.Evaluated != 3 {
		t.Fatalf("got %v, stats %+v", evals, stats)
	}
}

func TestSearchMovesPreservesSuccessOnSimulationError(t *testing.T) {
	moves := []moveSet{{Nodes: []*Candidate{candidateWithPrice(t, 1)}}, {Nodes: []*Candidate{candidateWithPrice(t, 2)}}}
	evals, stats := searchSingleMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		if c[0].Price == 2 {
			return Command{}, fmt.Errorf("simulation failed")
		}
		return Command{Candidates: c}, nil
	}, testScoreInputs(moves, nil), rand.New(rand.NewSource(1)), clock.RealClock{}, scoreBasedValidEvaluationsTarget) //nolint:gosec
	if len(evals) != 1 || evals[0].Score != 1 || stats.Errors != 1 || stats.Evaluated != 2 {
		t.Fatalf("got %v, stats %+v", evals, stats)
	}
}

func TestSelectionRechecksWholeCommandPacing(t *testing.T) {
	pool := paceTestNodePool("1", "1")
	pacing := NewDisruptionPacing(clocktesting.NewFakeClock(time.Now()))
	pacing.Refresh([]*v1.NodePool{pool}, nil)
	cmd := *paceCommandWithPods(pool, 2)
	validator := scoreMoveValidator{Validator: acceptScoreCommand{}, pacing: pacing}
	if _, err := validator.Validate(context.Background(), cmd, 0); !IsValidationError(err) {
		t.Fatalf("expected whole-command pacing rejection, got %v", err)
	}
}

type acceptScoreCommand struct{}

func (acceptScoreCommand) Validate(_ context.Context, cmd Command, _ time.Duration) (Command, error) {
	return cmd, nil
}

func TestSearchMovesErrorPreventsConsolidatedState(t *testing.T) {
	_, stats := searchSingleMoves(context.Background(), []moveSet{{Nodes: []*Candidate{candidateWithPrice(t, 1)}}}, time.Now().Add(time.Second), func(context.Context, ...*Candidate) (Command, error) {
		return Command{}, fmt.Errorf("simulation failed")
	}, scoreInputs{}, rand.New(rand.NewSource(1)), clock.RealClock{}, scoreBasedValidEvaluationsTarget) //nolint:gosec
	if stats.canMarkConsolidated(1, false) {
		t.Fatal("failed search must not mark cluster consolidated")
	}
}

func TestSearchMovesFiltersMoveWhenStandbyCostExceedsSavings(t *testing.T) {
	expensive := candidateWithPrice(t, 10)
	cheap := candidateWithPrice(t, 5)
	moves := []moveSet{{Nodes: []*Candidate{expensive}}, {Nodes: []*Candidate{cheap}}}
	evals, stats := searchSingleMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		if c[0].Price == 10 {
			cmd := Command{Candidates: c}
			cmd.Results.ExistingNodes = standbyReactivationResults("standby-a")
			return cmd, nil
		}
		return Command{Candidates: c}, nil
	}, testScoreInputs(moves, map[string]float64{"standby-a": 12}), rand.New(rand.NewSource(1)), clock.RealClock{}, scoreBasedValidEvaluationsTarget) //nolint:gosec
	if len(evals) != 1 || evals[0].Score != 5 || stats.NoSavings != 1 {
		t.Fatalf("got %v, stats %+v", evals, stats)
	}
}

func standbyReactivationResults(standbyName string) []*pscheduling.ExistingNode {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: standbyName},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{v1.StandbyNoScheduleTaint}},
	}
	sn := state.NewNode()
	sn.Node = node
	return []*pscheduling.ExistingNode{{StateNode: sn, Pods: []*corev1.Pod{{}}}}
}

func TestSearchMovesRejectsPositiveSavingsWithZeroScore(t *testing.T) {
	candidate := candidateWithPrice(t, 1)
	moves := []moveSet{{Nodes: []*Candidate{candidate}}}
	inputs := scoreInputs{sources: map[*Candidate]scoreSource{candidate: {Price: 1, AllocatableCPU: 1, RequestedCPU: 1, Weight: 1}}}
	evals, _ := searchSingleMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		return Command{Candidates: c}, nil
	}, inputs, rand.New(rand.NewSource(1)), clock.RealClock{}, scoreBasedValidEvaluationsTarget) //nolint:gosec
	if len(evals) != 0 {
		t.Fatalf("kept %d moves, want none", len(evals))
	}
}

func TestInvalidUtilizationWeightWarnsOnce(t *testing.T) {
	fakeRecorder := record.NewFakeRecorder(10)
	recorder := events.NewRecorder(fakeRecorder)
	pool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool", UID: "pool", Annotations: map[string]string{"karpenter.coralogix.net/utilization-weight": "NaN"}}}
	candidate := candidateWithPrice(t, 1)
	candidate.NodePool = pool
	candidate.NodeClaim = &v1.NodeClaim{}
	candidate.Node.Labels[v1.NodePoolLabelKey] = pool.Name
	candidate.Node.Labels[v1.NodeInitializedLabelKey] = "true"
	candidate.Node.Labels[v1.NodeRegisteredLabelKey] = "true"
	candidate.Node.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
	moves := []moveSet{{Nodes: []*Candidate{candidate}}}
	for range 2 {
		inputs := newScoreInputs(state.StateNodes{candidate.StateNode}, []*Candidate{candidate}, map[string]float64{candidate.Name(): 1}, recorder)
		evals, _ := searchSingleMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
			return Command{Candidates: c}, nil
		}, inputs, rand.New(rand.NewSource(1)), clock.RealClock{}, scoreBasedValidEvaluationsTarget) //nolint:gosec
		if len(evals) != 1 || evals[0].Score != 2 {
			t.Fatalf("evaluations = %+v, want score 2 using default weight", evals)
		}
	}

	warnings := collectFakeRecorderEvents(fakeRecorder)
	if len(warnings) != 1 || !strings.Contains(warnings[0], "Warning InvalidUtilizationWeight") {
		t.Fatalf("events = %v, want one invalid-weight warning", warnings)
	}
}
