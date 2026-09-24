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

package provisioning

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func TestSimulationPodCatalogReservesRealUIDsForSyntheticIDs(t *testing.T) {
	catalog, err := newSimulationPodCatalog([]*corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "empty-uid"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "real", UID: types.UID("simulation-pod-0")}},
	})
	if err != nil {
		t.Fatalf("creating simulation pod catalog: %v", err)
	}
	if got, want := catalog.pods[0].UID, types.UID("simulation-pod-1"); got != want {
		t.Fatalf("synthetic UID = %q, want %q", got, want)
	}
}

func TestSimulationPodCatalogOwnsPodCopies(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Labels: map[string]string{"source": "original"}}}
	catalog, err := newSimulationPodCatalog([]*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("creating simulation pod catalog: %v", err)
	}
	pod.Labels["source"] = "mutated"
	if got := catalog.pods[0].Labels["source"]; got != "original" {
		t.Fatalf("catalog pod label = %q, want original", got)
	}
}

func TestCloneStateNodesOwnsNodeCopies(t *testing.T) {
	node := state.NewNode()
	node.Node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{"source": "original"}}}
	cloned := cloneStateNodes([]*state.StateNode{node})
	node.Node.Labels["source"] = "mutated"
	if got := cloned[0].Node.Labels["source"]; got != "original" {
		t.Fatalf("cloned node label = %q, want original", got)
	}
}

func TestSimulationPodCatalogRejectsConflictingDuplicate(t *testing.T) {
	_, err := newSimulationPodCatalog([]*corev1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod"}},
		{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod"}, Spec: corev1.PodSpec{NodeName: "different"}},
	})
	if err == nil || !strings.Contains(err.Error(), "differing contents") {
		t.Fatalf("expected conflicting duplicate error, got %v", err)
	}
}

func TestSimulationPodIDCannotCrossCatalogs(t *testing.T) {
	first, err := newSimulationPodCatalog([]*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "first"}}})
	if err != nil {
		t.Fatalf("creating first catalog: %v", err)
	}
	second, err := newSimulationPodCatalog([]*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "second"}}})
	if err != nil {
		t.Fatalf("creating second catalog: %v", err)
	}
	inputs := &PreparedSimulationInputs{catalog: first, stateNodes: nil}
	_, err = inputs.NewRun(context.Background(), Scenario{PodIDs: []SimulationPodID{{catalog: second, index: 0}}})
	if err == nil || !strings.Contains(err.Error(), "outside simulation pod catalog") {
		t.Fatalf("expected foreign catalog ID error, got %v", err)
	}
}
