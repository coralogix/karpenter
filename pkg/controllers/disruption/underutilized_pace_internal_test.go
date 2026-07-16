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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	controllerruntimefake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/test"
)

func paceTestNodePool(rate, maxNodes string) *v1.NodePool {
	return &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
			UID:  "test-nodepool-uid",
			Annotations: map[string]string{
				v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey: rate,
				v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey:    maxNodes,
			},
		},
	}
}

func expectPaceMisconfiguredEvent(t *testing.T, recorder *test.EventRecorder, np *v1.NodePool) {
	t.Helper()
	if recorder.Calls(events.DisruptionBlocked) != 1 {
		t.Fatalf("expected 1 DisruptionBlocked event, got %d", recorder.Calls(events.DisruptionBlocked))
	}
	evts := recorder.Events()
	if len(evts) != 1 {
		t.Fatalf("expected 1 recorded event, got %d", len(evts))
	}
	evt := evts[0]
	if evt.InvolvedObject != np {
		t.Fatal("expected event involved object to be the NodePool")
	}
	if evt.Type != corev1.EventTypeNormal {
		t.Fatalf("expected event type %q, got %q", corev1.EventTypeNormal, evt.Type)
	}
	if evt.Reason != events.DisruptionBlocked {
		t.Fatalf("expected event reason %q, got %q", events.DisruptionBlocked, evt.Reason)
	}
	if evt.Message != "Invalid underutilized consolidation pace annotations; underutilized consolidation is paused for this NodePool" {
		t.Fatalf("unexpected event message: %q", evt.Message)
	}
	if len(evt.DedupeValues) != 2 || evt.DedupeValues[0] != string(np.UID) || evt.DedupeValues[1] != "underutilized-pace-misconfigured" {
		t.Fatalf("unexpected dedupe values: %v", evt.DedupeValues)
	}
}

func TestCandidateAllowedUnsetConfiguration(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()), nil)
	np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected unset configuration to allow candidates")
	}
}

func TestCandidateAllowedInvalidConfiguration(t *testing.T) {
	recorder := test.NewEventRecorder()
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()), recorder)
	np := paceTestNodePool("0", "1")
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected invalid configuration to block candidates")
	}
	expectPaceMisconfiguredEvent(t, recorder, np)
}

func TestCandidateAllowedPartialConfiguration(t *testing.T) {
	recorder := test.NewEventRecorder()
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()), recorder)
	np := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
			UID:  "test-nodepool-uid",
			Annotations: map[string]string{
				v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey: "1",
			},
		},
	}
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected partial configuration to block candidates")
	}
	expectPaceMisconfiguredEvent(t, recorder, np)
}

func TestCandidateAllowedBatchCapBoundary(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()), nil)
	np := paceTestNodePool("100", "2")
	if !pace.candidateAllowed(np, 1) {
		t.Fatal("expected selectedCount == max-1 to allow another candidate")
	}
	if pace.candidateAllowed(np, 2) {
		t.Fatal("expected selectedCount == max to block candidates")
	}
}

func TestCandidateAllowedRateCooldown(t *testing.T) {
	testClock := clocktesting.NewFakeClock(time.Now())
	pace := NewUnderutilizedConsolidationPace(testClock, nil)
	np := paceTestNodePool("1", "100")
	cmd := &Command{Candidates: []*Candidate{{NodePool: np}}}
	if _, ok := pace.TryAdmitCommand(cmd); !ok {
		t.Fatal("expected first admission")
	}
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected rate cooldown to block candidates")
	}
	testClock.Step(61 * time.Second)
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected candidates after cooldown elapsed")
	}
}

func TestCandidateAllowedNilPaceDisablesChecks(t *testing.T) {
	var pace *UnderutilizedConsolidationPace
	np := paceTestNodePool("0", "1")
	if !pace.candidateAllowed(np, 100) {
		t.Fatal("expected nil pace to disable planning-time checks")
	}
}

func TestTryAdmitCommandInvalidConfigurationEmitsEvent(t *testing.T) {
	recorder := test.NewEventRecorder()
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()), recorder)
	np := paceTestNodePool("0", "1")
	cmd := &Command{Candidates: []*Candidate{{NodePool: np}}}
	if _, ok := pace.TryAdmitCommand(cmd); ok {
		t.Fatal("expected invalid configuration to block admission")
	}
	expectPaceMisconfiguredEvent(t, recorder, np)
}

func TestReleasePreservesLaterCharge(t *testing.T) {
	testClock := clocktesting.NewFakeClock(time.Now())
	pace := NewUnderutilizedConsolidationPace(testClock, nil)
	np := paceTestNodePool("1", "100")
	cmdA := &Command{Candidates: []*Candidate{{NodePool: np}}}
	chargeA, ok := pace.TryAdmitCommand(cmdA)
	if !ok {
		t.Fatal("expected first admission")
	}
	testClock.Step(61 * time.Second)
	cmdB := &Command{Candidates: []*Candidate{{NodePool: np}}}
	if _, ok := pace.TryAdmitCommand(cmdB); !ok {
		t.Fatal("expected second admission after first cooldown elapsed")
	}
	pace.Release(chargeA)
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected later charge to remain active after releasing earlier charge")
	}
	testClock.Step(61 * time.Second)
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected admission after second charge cooldown elapsed")
	}
}

func TestStartCommandsNoStartedWhenPaceRejects(t *testing.T) {
	ctx := context.Background()
	testClock := clocktesting.NewFakeClock(time.Now())
	cloudProvider := fake.NewCloudProvider()
	kubeClient := controllerruntimefake.NewClientBuilder().Build()
	cluster := state.NewCluster(testClock, kubeClient, cloudProvider)
	recorder := test.NewEventRecorder()
	prov := provisioning.NewProvisioner(kubeClient, recorder, cloudProvider, cluster, testClock)
	queue := NewQueue(kubeClient, recorder, cluster, testClock, prov)
	controller := NewController(testClock, kubeClient, prov, cloudProvider, recorder, cluster, queue)

	np := paceTestNodePool("1", "100")
	cmd := Command{Candidates: []*Candidate{{NodePool: np}}}
	if _, ok := controller.underutilizedPace.TryAdmitCommand(&cmd); !ok {
		t.Fatal("expected first admission")
	}

	method := NewSingleNodeConsolidation(MakeConsolidation(testClock, cluster, kubeClient, prov, cloudProvider, recorder, queue, controller.underutilizedPace))
	started, err := controller.startCommands(ctx, method, []Command{cmd})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if started != 0 {
		t.Fatalf("expected 0 started commands when pace rejects, got %d", started)
	}
	if len(queue.GetCommands()) != 0 {
		t.Fatalf("expected no commands in queue, got %d", len(queue.GetCommands()))
	}
}
