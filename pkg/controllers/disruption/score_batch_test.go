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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestBatchSearchGrowsHigherScoringMove(t *testing.T) {
	moves := []moveSet{{Nodes: []*Candidate{candidateWithPrice(t, 1)}}, {Nodes: []*Candidate{candidateWithPrice(t, 2)}}}
	for _, move := range moves {
		move.Nodes[0].NodePool = &v1.NodePool{}
	}
	evals, _ := searchAllMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		return Command{Candidates: c}, nil
	}, testScoreInputs(moves, nil), rand.New(rand.NewSource(1)), clock.RealClock{}, nil, nil) //nolint:gosec
	if len(evals) == 0 || len(evals[0].Command.Candidates) != 2 {
		t.Fatalf("expected highest scoring move to evacuate both nodes, got %+v", evals)
	}
}

func TestBatchSearchRejectsMovesNeedingNewNodes(t *testing.T) {
	moves := batchTestMoves(t, 2, &v1.NodePool{})
	evals, _ := searchAllMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		cmd := Command{Candidates: c}
		if len(c) > 1 {
			cmd.Replacements = []*Replacement{{}}
		}
		return cmd, nil
	}, testScoreInputs(moves, nil), rand.New(rand.NewSource(1)), clock.RealClock{}, nil, nil) //nolint:gosec
	for _, eval := range evals {
		if len(eval.Command.Candidates) != 1 {
			t.Fatalf("kept batch requiring new node: %+v", eval)
		}
	}
}

func TestBatchSearchRespectsPacingCap(t *testing.T) {
	pool := paceTestNodePool("100", "2")
	moves := batchTestMoves(t, 3, pool)
	pacing := NewDisruptionPacing(clock.RealClock{})
	pacing.Refresh([]*v1.NodePool{pool}, nil)
	evals, _ := searchAllMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) { return Command{Candidates: c}, nil }, testScoreInputs(moves, nil), rand.New(rand.NewSource(1)), clock.RealClock{}, nil, pacing) //nolint:gosec
	if len(evals[0].Command.Candidates) != 2 {
		t.Fatalf("expected batch of two, got %d", len(evals[0].Command.Candidates))
	}
	for _, eval := range evals {
		if len(eval.Command.Candidates) > 2 {
			t.Fatalf("batch exceeds pacing cap: %+v", eval)
		}
	}
}

func TestBatchSearchKeepsBestSingleFallback(t *testing.T) {
	moves := batchTestMoves(t, 12, &v1.NodePool{})
	evals, _ := searchAllMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) { return Command{Candidates: c}, nil }, testScoreInputs(moves, nil), rand.New(rand.NewSource(1)), clock.RealClock{}, nil, nil) //nolint:gosec
	if len(evals) != 11 {
		t.Fatalf("expected ten moves and single fallback, got %d", len(evals))
	}
	if len(evals[10].Command.Candidates) != 1 || evals[10].Command.Candidates[0] != moves[11].Nodes[0] {
		t.Fatal("best single missing from fallback")
	}
}

func batchTestMoves(t *testing.T, n int, pool *v1.NodePool) []moveSet {
	t.Helper()
	moves := make([]moveSet, n)
	for i := range moves {
		c := candidateWithPrice(t, float64(i+1))
		c.NodePool = pool
		c.reschedulablePods = []*corev1.Pod{{}}
		moves[i] = moveSet{Nodes: []*Candidate{c}}
	}
	return moves
}
