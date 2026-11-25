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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func TestSearchMovesRejectsUnprofitableAndMultipleReplacements(t *testing.T) {
	sources := [][]*Candidate{
		[]*Candidate{candidateWithPrice(t, 0)},
		[]*Candidate{candidateWithPrice(t, 2)},
		[]*Candidate{candidateWithPrice(t, 3)},
	}
	moves, stats := searchMoves(context.Background(), sources, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		cmd := Command{Candidates: c}
		if c[0].Price == 2 {
			cmd.Replacements = []*Replacement{{}, {}}
		}
		return cmd, nil
	}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
	if len(moves) != 1 || moves[0].SearchScore.Savings != 3 || stats.Evaluated != 3 {
		t.Fatalf("got %v, stats %+v", moves, stats)
	}
}

func TestSearchMovesPreservesSuccessOnSimulationError(t *testing.T) {
	sources := [][]*Candidate{[]*Candidate{candidateWithPrice(t, 1)}, []*Candidate{candidateWithPrice(t, 2)}}
	moves, stats := searchMoves(context.Background(), sources, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		if c[0].Price == 2 {
			return Command{}, fmt.Errorf("simulation failed")
		}
		return Command{Candidates: c}, nil
	}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
	if len(moves) != 1 || moves[0].SearchScore.Savings != 1 || stats.Errors != 1 || stats.Evaluated != 2 {
		t.Fatalf("got %v, stats %+v", moves, stats)
	}
}

func TestSearchMovesErrorPreventsConsolidatedState(t *testing.T) {
	_, stats := searchMoves(context.Background(), [][]*Candidate{[]*Candidate{candidateWithPrice(t, 1)}}, time.Now().Add(time.Second), func(context.Context, ...*Candidate) (Command, error) {
		return Command{}, fmt.Errorf("simulation failed")
	}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
	if stats.canMarkConsolidated(1, false) {
		t.Fatal("failed search must not mark cluster consolidated")
	}
}

func TestSearchMovesFiltersMoveWhenStandbyCostExceedsSavings(t *testing.T) {
	expensive := candidateWithPrice(t, 10)
	cheap := candidateWithPrice(t, 5)
	sources := [][]*Candidate{[]*Candidate{expensive}, []*Candidate{cheap}}
	moves, stats := searchMoves(context.Background(), sources, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		if c[0].Price == 10 {
			cmd := Command{Candidates: c}
			cmd.Results.ExistingNodes = standbyReactivationResults("standby-a", 12)
			return cmd, nil
		}
		return Command{Candidates: c}, nil
	}, rand.New(rand.NewSource(1)), clock.RealClock{}) //nolint:gosec
	if len(moves) != 1 || moves[0].SearchScore.Savings != 5 || stats.NoSavings != 1 {
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
