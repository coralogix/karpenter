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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	controllerruntimefake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
)

func paceTestNodePool(rate, maxNodes string) *v1.NodePool {
	return &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
			Annotations: map[string]string{
				v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey: rate,
				v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey:    maxNodes,
			},
		},
	}
}

func TestCandidateAllowedUnsetConfiguration(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	if !pace.candidateAllowed(np, 0) {
		t.Fatal("expected unset configuration to allow candidates")
	}
}

func TestCandidateAllowedInvalidConfiguration(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := paceTestNodePool("0", "1")
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected invalid configuration to block candidates")
	}
}

func TestCandidateAllowedPartialConfiguration(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
	np := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default",
			Annotations: map[string]string{
				v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey: "1",
			},
		},
	}
	if pace.candidateAllowed(np, 0) {
		t.Fatal("expected partial configuration to block candidates")
	}
}

func TestCandidateAllowedBatchCapBoundary(t *testing.T) {
	pace := NewUnderutilizedConsolidationPace(clocktesting.NewFakeClock(time.Now()))
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
	pace := NewUnderutilizedConsolidationPace(testClock)
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
