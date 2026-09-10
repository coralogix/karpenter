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

package scheduling_test

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

func TestNewNodeClaimAnnotationsAreIsolated(t *testing.T) {
	template := &scheduling.NodeClaimTemplate{
		NodeClaim: v1.NodeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Annotations: map[string]string{"existing": "value"},
			},
		},
	}

	claim1 := scheduling.NewNodeClaim(template, nil, nil, nil, nil, nil, scheduling.ReservedOfferingModeFallback)
	claim2 := scheduling.NewNodeClaim(template, nil, nil, nil, nil, nil, scheduling.ReservedOfferingModeFallback)

	claim1.Annotations["claim1"] = "true"
	claim2.Annotations["claim2"] = "true"

	if got := claim1.Annotations["existing"]; got != "value" {
		t.Fatalf("claim1 lost the existing annotation, got %q", got)
	}
	if got := claim2.Annotations["existing"]; got != "value" {
		t.Fatalf("claim2 lost the existing annotation, got %q", got)
	}
	if got := template.Annotations["existing"]; got != "value" {
		t.Fatalf("template lost the existing annotation, got %q", got)
	}
	if _, ok := claim1.Annotations["claim2"]; ok {
		t.Fatal("claim1 annotations were modified through claim2")
	}
	if _, ok := claim2.Annotations["claim1"]; ok {
		t.Fatal("claim2 annotations were modified through claim1")
	}
	if _, ok := template.Annotations["claim1"]; ok {
		t.Fatal("template annotations were modified through claim1")
	}
	if _, ok := template.Annotations["claim2"]; ok {
		t.Fatal("template annotations were modified through claim2")
	}
}
