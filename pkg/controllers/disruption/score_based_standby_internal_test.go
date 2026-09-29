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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestScoreBasedStandbyFiltersToEligibleEmptyActiveNodes(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		standby   bool
		occupied  bool
		annotated bool
		want      bool
	}{
		{name: "eligible active empty node", annotated: true, want: true},
		{name: "occupied node", annotated: true, occupied: true},
		{name: "already standby node", annotated: true, standby: true},
		{name: "unconfigured pool", annotated: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate, kubeClient := scoreBasedStandbyTestCandidate(tc.annotated, tc.standby, tc.occupied)
			method := NewScoreBasedStandby(consolidation{kubeClient: kubeClient})
			if got := method.ShouldDisrupt(ctx, candidate); got != tc.want {
				t.Fatalf("ShouldDisrupt() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestScoreBasedStandbyCommandIgnoresBudgetsAndHasNoSavings(t *testing.T) {
	method := NewScoreBasedStandby(consolidation{})
	candidates := []*Candidate{{}, {}}
	commands, err := method.ComputeCommands(context.Background(), map[string]int{"pool": 0}, candidates...)
	if err != nil {
		t.Fatalf("ComputeCommands() error = %v", err)
	}
	if len(commands) != 1 {
		t.Fatalf("ComputeCommands() returned %d commands, want 1", len(commands))
	}
	if commands[0].Action != StandbyAction || commands[0].Decision() != StandbyDecision {
		t.Fatalf("standby command action/decision = %q/%q, want %q/%q", commands[0].Action, commands[0].Decision(), StandbyAction, StandbyDecision)
	}
	if got := commands[0].EstimatedSavings(); got != 0 {
		t.Fatalf("standby command estimated savings = %v, want 0", got)
	}
}

func scoreBasedStandbyTestCandidate(annotated, markStandby, occupied bool) (*Candidate, client.Client) {
	poolAnnotations := map[string]string{}
	if annotated {
		poolAnnotations[v1.ScoreBasedConsolidationAnnotationKey] = ""
	}
	nodePool := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Annotations: poolAnnotations},
		Spec:       v1.NodePoolSpec{Disruption: v1.Disruption{ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized}},
	}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:   "claim",
		Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name},
	}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node",
		Labels: map[string]string{
			v1.NodePoolLabelKey:        nodePool.Name,
			v1.NodeInitializedLabelKey: "true",
		},
	}}
	node.Spec.ProviderID = "provider-id"
	if markStandby {
		standby.SetNodeClaimStandby(nodeClaim, true)
		standby.SetNodeTaint(node, true)
	}
	objects := []client.Object{nodePool, nodeClaim, node}
	if occupied {
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name}})
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(object client.Object) []string {
			pod := object.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		WithObjects(objects...).Build()
	stateNode := &state.StateNode{Node: node, NodeClaim: nodeClaim}
	return &Candidate{StateNode: stateNode, NodePool: nodePool}, kubeClient
}
