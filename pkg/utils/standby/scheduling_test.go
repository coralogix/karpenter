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

package standby

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func TestPlanningTaintsRemovesStandbyTaintForEligibleNode(t *testing.T) {
	since := time.Now().UTC()
	other := corev1.Taint{Key: "example.com/keep", Effect: corev1.TaintEffectNoSchedule}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodeInitializedLabelKey: "true",
			v1.NodeRegisteredLabelKey:  "true",
			v1.NodePoolLabelKey:        "pool",
		}},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{NodeTaint(), other}},
	}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Labels:      map[string]string{v1.NodePoolLabelKey: "pool"},
		Annotations: map[string]string{NodeClaimAnnotationKey: since.Format(time.RFC3339Nano)},
	}}
	stateNode := &state.StateNode{Node: node, NodeClaim: nodeClaim}

	original := append([]corev1.Taint(nil), node.Spec.Taints...)
	planning := PlanningTaints(stateNode)
	if !HasNodeTaint(node) {
		t.Fatal("planning must not mutate the node taints")
	}
	if len(planning) != 1 || planning[0] != other {
		t.Fatalf("planning taints = %v, want only unrelated taint %v", planning, other)
	}
	if len(original) != 2 {
		t.Fatalf("original taints changed: %v", node.Spec.Taints)
	}
}

func TestPlanningTaintsPreservesStandbyTaintWhenIneligible(t *testing.T) {
	since := time.Now().UTC()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
			v1.NodeInitializedLabelKey: "true",
			v1.NodeRegisteredLabelKey:  "true",
			v1.NodePoolLabelKey:        "pool",
		}},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{NodeTaint(), v1.DisruptedNoScheduleTaint}},
	}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Labels:      map[string]string{v1.NodePoolLabelKey: "pool"},
		Annotations: map[string]string{NodeClaimAnnotationKey: since.Format(time.RFC3339Nano)},
	}}
	stateNode := &state.StateNode{Node: node, NodeClaim: nodeClaim}

	planning := PlanningTaints(stateNode)
	if !HasNodeTaint(&corev1.Node{Spec: corev1.NodeSpec{Taints: planning}}) {
		t.Fatal("disruption-reserved standby node must keep standby taint in planning view")
	}
}
