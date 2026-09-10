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
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
)

func TestNewForTopologiesDoesNotMutateMatchLabelKeys(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Labels: map[string]string{"workload": "blue"}},
		Spec: corev1.PodSpec{TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{
			TopologyKey:       corev1.LabelTopologyZone,
			MaxSkew:           1,
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			MatchLabelKeys:    []string{"workload"},
		}}},
	}

	first := newForTopologies(pod, PreferencePolicyRespect, nil)
	second := newForTopologies(pod, PreferencePolicyRespect, nil)
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("expected one topology group per call, got %d and %d", len(first), len(second))
	}
	if got := len(pod.Spec.TopologySpreadConstraints[0].LabelSelector.MatchExpressions); got != 0 {
		t.Fatalf("expected input selector to remain unchanged, got %d expressions", got)
	}
	if first[0].Hash() != second[0].Hash() {
		t.Fatalf("expected repeated rule construction to produce the same group hash, got %d and %d", first[0].Hash(), second[0].Hash())
	}
	selector, err := metav1.LabelSelectorAsSelector(first[0].rawSelector)
	if err != nil {
		t.Fatalf("parsing constructed selector: %v", err)
	}
	if !selector.Matches(labels.Set{"app": "web", "workload": "blue"}) || selector.Matches(labels.Set{"app": "web", "workload": "green"}) {
		t.Fatalf("constructed selector did not include the pod's MatchLabelKeys value: %v", first[0].rawSelector)
	}
}

func TestNewForAffinitiesUsesInjectedNamespaceResolver(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
		Spec: corev1.PodSpec{Affinity: &corev1.Affinity{
			PodAffinity: &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey:       corev1.LabelTopologyZone,
				Namespaces:        []string{"required"},
				NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"team": "blue"}},
			}}},
			PodAntiAffinity: &corev1.PodAntiAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight: 1,
				PodAffinityTerm: corev1.PodAffinityTerm{
					TopologyKey: corev1.LabelTopologyZone,
					Namespaces:  []string{"preferred"},
				},
			}}},
		}},
	}

	var calls int
	resolve := func(_ context.Context, namespace string, namespaces []string, selector *metav1.LabelSelector) (sets.Set[string], error) {
		calls++
		if namespace != pod.Namespace || len(namespaces) == 0 {
			t.Fatalf("unexpected namespace resolver arguments: namespace=%q namespaces=%v selector=%v", namespace, namespaces, selector)
		}
		return sets.New[string]("resolved"), nil
	}

	groups, err := newForAffinities(context.Background(), pod, PreferencePolicyRespect, nil, resolve)
	if err != nil {
		t.Fatalf("constructing affinity groups: %v", err)
	}
	if calls != 2 || len(groups) != 2 {
		t.Fatalf("expected resolver and group construction for required and preferred terms, calls=%d groups=%d", calls, len(groups))
	}
	for _, group := range groups {
		if !group.namespaces.Has("resolved") {
			t.Fatalf("expected injected namespace resolver result, got %v", group.namespaces)
		}
	}
}

func TestNewForInverseAntiAffinityUsesInjectedNamespaceResolver(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default"},
		Spec: corev1.PodSpec{Affinity: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey: corev1.LabelTopologyZone,
				Namespaces:  []string{"selected"},
			}},
		}}},
	}
	resolve := func(_ context.Context, _ string, _ []string, _ *metav1.LabelSelector) (sets.Set[string], error) {
		return sets.New[string]("resolved"), nil
	}

	groups, err := newForInverseAntiAffinity(context.Background(), pod, nil, resolve)
	if err != nil {
		t.Fatalf("constructing inverse anti-affinity groups: %v", err)
	}
	if len(groups) != 1 || !groups[0].namespaces.Has("resolved") {
		t.Fatalf("expected one inverse group from injected namespace resolver, got %#v", groups)
	}
}
