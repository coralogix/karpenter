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
	"testing"
	"time"

	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestPairSearchKeepsExactlyOneReplacement(t *testing.T) {
	for _, replacements := range []int{0, 1, 2} {
		t.Run(strconv.Itoa(replacements), func(t *testing.T) {
			moves := batchTestMoves(t, 2, &v1.NodePool{})
			evals, _ := searchAllMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
				if len(c) == 1 {
					return Command{}, nil
				}
				cmd := Command{Candidates: c}
				for range replacements {
					cmd.Replacements = append(cmd.Replacements, &Replacement{})
				}
				return cmd, nil
			}, testScoreInputs(moves, nil), rand.New(rand.NewSource(1)), clock.RealClock{}, nil, nil) //nolint:gosec
			if (len(evals) == 1) != (replacements == 1) {
				t.Fatalf("replacements %d: got %d moves", replacements, len(evals))
			}
		})
	}
}

func TestPairSearchSkipsWhenSingleSaves(t *testing.T) {
	moves := batchTestMoves(t, 2, &v1.NodePool{})
	inputs := testScoreInputs(moves, nil)
	for _, move := range moves {
		source := inputs.sources[move.Nodes[0]]
		source.Baseline, source.RequestedCPU = 0, source.AllocatableCPU
		inputs.sources[move.Nodes[0]] = source
	}
	pairs := 0
	_, _ = searchAllMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		if len(c) == 2 {
			pairs++
		}
		return Command{Candidates: c}, nil
	}, inputs, rand.New(rand.NewSource(1)), clock.RealClock{}, nil, nil) //nolint:gosec
	if pairs != 0 {
		t.Fatalf("simulated %d pairs despite positive single-node savings", pairs)
	}
}
