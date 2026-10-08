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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	clocktesting "k8s.io/utils/clock/testing"
)

func TestSuccessfulStartAccountingUsesHandedOffCommand(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Now())
	nodePool := paceTestNodePool("1", "")
	command := Command{Candidates: []*Candidate{
		{NodePool: nodePool, reschedulablePods: []*corev1.Pod{{}}},
		{NodePool: nodePool, reschedulablePods: []*corev1.Pod{{}}},
	}}
	controller := &Controller{clock: clk}
	var handedOff *Command
	results := controller.startCommands(context.Background(), nil, []Command{command}, []int{0}, func(_ context.Context, cmd *Command) error {
		handedOff = cmd
		// Queue.StartCommand can shrink Candidates after accepting only some deletes.
		cmd.Candidates = cmd.Candidates[:1]
		return nil
	})
	if handedOff == nil {
		t.Fatal("StartCommand was not called")
	}
	if results[0].command != handedOff {
		t.Fatal("start result did not retain the command pointer handed to StartCommand")
	}

	successfulStarts, errs, started := controller.collectCommandResults(nil, results)
	if started != 1 || len(errs) != 1 || errs[0] != nil {
		t.Fatalf("collectCommandResults() = started %d, errs %v; want one successful start", started, errs)
	}
	if len(successfulStarts) != 1 || successfulStarts[0].Command != handedOff {
		t.Fatal("successful-start accounting did not use the handed-off command pointer")
	}

	pacing := NewDisruptionPacing(clk)
	pacing.ChargePass(successfulStarts)
	if pacing.CandidateAllowed(nodePool, 0) {
		t.Fatal("expected the successful non-empty start to charge pacing")
	}
	clk.Step(time.Minute + time.Second)
	if !pacing.CandidateAllowed(nodePool, 0) {
		t.Fatal("expected pacing to count the one candidate remaining on the handed-off command")
	}
}
