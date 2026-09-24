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

package scheduling

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func schedulerBaselineTestContext() context.Context {
	return karpopts.ToContext(context.Background(), &karpopts.Options{})
}

func TestNewSchedulerBaselineCopiesDaemonPods(t *testing.T) {
	inputPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "daemon",
			Labels:    map[string]string{"app": "daemon"},
		},
	}
	inputs := &NodePoolInputs{}
	baseline := NewSchedulerBaseline(schedulerBaselineTestContext(), inputs, []*corev1.Pod{inputPod})

	baseline.daemonSetPods[0].Labels["app"] = "changed"
	if inputPod.Labels["app"] != "daemon" {
		t.Fatalf("expected baseline daemon pod copy to isolate input pod, got %q", inputPod.Labels["app"])
	}
}

func TestNewSchedulerBaselineDoesNotRelaxDaemonPodCatalog(t *testing.T) {
	inputPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "daemon"},
		Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "example.com/daemon", Operator: corev1.NodeSelectorOpIn, Values: []string{"first"}}}},
					{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "example.com/daemon", Operator: corev1.NodeSelectorOpIn, Values: []string{"second"}}}},
				},
			},
		}}},
	}
	inputs := &NodePoolInputs{nodeClaimTemplates: []*NodeClaimTemplate{{
		Requirements: scheduling.NewRequirements(scheduling.NewRequirement("example.com/daemon", corev1.NodeSelectorOpIn, "different")),
	}}}

	baseline := NewSchedulerBaseline(schedulerBaselineTestContext(), inputs, []*corev1.Pod{inputPod})
	if got := len(inputPod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms); got != 2 {
		t.Fatalf("expected source daemon pod affinity to remain intact, got %d terms", got)
	}
	if got := len(baseline.daemonSetPods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms); got != 2 {
		t.Fatalf("expected baseline daemon pod catalog to remain intact, got %d terms", got)
	}
}

func TestNewSchedulerFromBaselineOwnsAttemptState(t *testing.T) {
	template := &NodeClaimTemplate{
		NodePoolName: "pool",
		Requirements: scheduling.NewRequirements(),
	}
	inputs := &NodePoolInputs{
		nodePools:          []*v1.NodePool{{ObjectMeta: metav1.ObjectMeta{Name: "pool"}}},
		nodeClaimTemplates: []*NodeClaimTemplate{template},
	}
	daemonPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "daemon"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Ports: []corev1.ContainerPort{{ContainerPort: 80, HostPort: 8080}}}}},
	}
	testContext := schedulerBaselineTestContext()
	baseline := NewSchedulerBaseline(testContext, inputs, []*corev1.Pod{daemonPod})

	first, err := NewSchedulerFromBaseline(testContext, nil, baseline, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("creating first scheduler: %v", err)
	}
	second, err := NewSchedulerFromBaseline(testContext, nil, baseline, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("creating second scheduler: %v", err)
	}

	firstUsage := first.daemonHostPortUsage[template]
	secondUsage := second.daemonHostPortUsage[template]
	if firstUsage == secondUsage {
		t.Fatal("expected each scheduler attempt to own its host-port usage")
	}
	otherPod := daemonPod.DeepCopy()
	otherPod.Name = "other"
	otherPod.Spec.Containers[0].Ports[0].HostPort = 9090
	firstUsage.Add(otherPod, scheduling.GetHostPorts(otherPod))
	otherPod2 := otherPod.DeepCopy()
	otherPod2.Name = "other-2"
	if err := secondUsage.Conflicts(otherPod2, scheduling.GetHostPorts(otherPod2)); err != nil {
		t.Fatalf("expected host-port mutation in one attempt to stay isolated: %v", err)
	}

	if first.reservationManager == second.reservationManager {
		t.Fatal("expected each scheduler attempt to own its reservation manager")
	}
}

func TestNewSchedulerFromBaselineRejectsDRAContextMismatch(t *testing.T) {
	baselineContext := karpopts.ToContext(context.Background(), &karpopts.Options{IgnoreDRARequests: true})
	attemptContext := karpopts.ToContext(context.Background(), &karpopts.Options{IgnoreDRARequests: false})
	baseline := NewSchedulerBaseline(baselineContext, &NodePoolInputs{}, nil)

	if _, err := NewSchedulerFromBaseline(attemptContext, nil, baseline, nil, nil, nil, nil, nil, nil); err == nil {
		t.Fatal("expected scheduler attempt to reject a mismatched IgnoreDRARequests context")
	}
}
