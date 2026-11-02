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

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/test"
)

func TestScoreBasedSelectionEvents(t *testing.T) {
	rejected := candidateWithPrice(t, 2)
	rejected.Node.Name = "high-score"
	rejected.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "high-score"}}
	chosen := candidateWithPrice(t, 1)
	chosen.Node.Name = "chosen"
	chosen.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "chosen"}}
	moves := []moveSet{{Nodes: []*Candidate{rejected}}, {Nodes: []*Candidate{chosen}}}
	inputs := testScoreInputs(moves, nil)
	recorder := test.NewEventRecorder()
	searchRecorder := candidateEventFilter{recorder}
	evals, _ := searchSingleMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, candidates ...*Candidate) (Command, error) {
		cmd := Command{Candidates: candidates}
		cmd.EmitCandidateEvents(searchRecorder)
		searchRecorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, "blocker")...)
		return cmd, nil
	}, inputs, rand.New(rand.NewSource(1)), clock.RealClock{}, scoreBasedValidEvaluationsTarget) //nolint:gosec
	cmd, err := selectBestRescoredCommand(context.Background(), rejectFirstCommandValidator{}, recorder, evals, inputs)
	if err != nil || len(cmd.Candidates) != 1 || cmd.Candidates[0] != chosen {
		t.Fatalf("selection = %+v, error = %v", cmd, err)
	}
	assertScoreSelectionEvents(t, recorder, rejected, chosen)
}

func assertScoreSelectionEvents(t *testing.T, recorder *test.EventRecorder, rejected, chosen *Candidate) {
	t.Helper()
	for _, event := range recorder.Events() {
		switch event.Reason {
		case events.ConsolidationCandidate:
			if event.InvolvedObject != chosen.Node && event.InvolvedObject != chosen.NodeClaim {
				t.Errorf("candidate event on %v", event.InvolvedObject)
			}
		case events.ConsolidationRejected:
			if event.InvolvedObject != rejected.Node && event.InvolvedObject != rejected.NodeClaim {
				t.Errorf("rejection event on %v", event.InvolvedObject)
			}
		}
	}
	if recorder.Calls(events.ConsolidationCandidate) != 2 || recorder.Calls(events.ConsolidationRejected) != 2 || recorder.Calls(events.Unconsolidatable) != 4 {
		t.Fatalf("events = %+v", recorder.Events())
	}
}

func TestScoreBasedPacingBlockedEvent(t *testing.T) {
	pool := paceTestNodePool("1", "")
	pool.UID = "pool"
	pacing := NewDisruptionPacing(clocktesting.NewFakeClock(time.Now()))
	pacing.Refresh([]*v1.NodePool{pool}, nil)
	cmd := paceCommandWithPods(pool, 1)
	if !completePacingCommand(pacing, cmd) {
		t.Fatal("failed to consume pacing allowance")
	}
	recorder := test.NewEventRecorder()
	s := &ScoreBasedConsolidation{consolidation: consolidation{disruptionPacing: pacing, recorder: recorder}}
	candidates, _ := s.filterCandidates(map[string]int{pool.Name: 1}, cmd.Candidates)
	if len(candidates) != 0 || recorder.Calls(events.DisruptionBlocked) != 1 {
		t.Fatalf("candidates = %v, events = %v", candidates, recorder.Events())
	}
	event := recorder.Events()[0]
	if event.InvolvedObject != pool || len(event.DedupeValues) == 0 || event.DedupeTimeout == 0 {
		t.Fatalf("event = %+v", event)
	}
}

func TestScoreBasedSearchMetrics(t *testing.T) {
	ScoreBasedMoveEvaluationsTotal.Reset()
	ScoreBasedMovesSelectedTotal.Reset()
	moves := make([]moveSet, 5)
	for i := range moves {
		moves[i] = moveSet{Nodes: []*Candidate{candidateWithPrice(t, float64(i))}}
	}
	inputs := testScoreInputs(moves, nil)
	inputs.sources[moves[3].Nodes[0]] = scoreSource{Price: 3, AllocatableCPU: 1, RequestedCPU: 1, Weight: 1}
	evals, _ := searchSingleMoves(context.Background(), moves, time.Now().Add(time.Second), func(_ context.Context, c ...*Candidate) (Command, error) {
		switch c[0].Price {
		case 1:
			return Command{}, nil
		case 2:
			return Command{}, fmt.Errorf("simulation error")
		default:
			return Command{Candidates: c}, nil
		}
	}, inputs, rand.New(rand.NewSource(1)), clock.RealClock{}, scoreBasedValidEvaluationsTarget) //nolint:gosec
	for _, result := range []string{"feasible", "no_op", "no_savings", "non_positive_score", "error"} {
		counter := ScoreBasedMoveEvaluationsTotal.(*opmetrics.PrometheusCounter).WithLabelValues(result)
		metric := &dto.Metric{}
		if err := counter.Write(metric); err != nil {
			t.Fatal(err)
		}
		if got := metric.GetCounter().GetValue(); got != 1 {
			t.Errorf("%s = %v, want 1", result, got)
		}
	}
	chosen := evals[0].Command.Candidates[0]
	chosen.NodeClaim = &v1.NodeClaim{}
	_, err := selectBestRescoredCommand(context.Background(), acceptScoreCommand{}, test.NewEventRecorder(), evals, inputs)
	if err != nil {
		t.Fatal(err)
	}
	selected := &dto.Metric{}
	if err := ScoreBasedMovesSelectedTotal.(*opmetrics.PrometheusCounter).WithLabelValues("single").Write(selected); err != nil {
		t.Fatal(err)
	}
	if got := selected.GetCounter().GetValue(); got != 1 {
		t.Errorf("selected single = %v, want 1", got)
	}
}
