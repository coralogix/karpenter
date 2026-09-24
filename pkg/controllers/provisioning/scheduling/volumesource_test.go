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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	karpscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestCapturedVolumeSourceDistinguishesMissingAndEmpty(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pod", UID: types.UID("pod-1")}}
	source := NewCapturedVolumeSource(map[types.UID]VolumeData{
		pod.UID: {Volumes: karpscheduling.Volumes{}},
	})

	volumes, err := source.Volumes(context.Background(), pod)
	if err != nil {
		t.Fatalf("getting known empty volume data: %v", err)
	}
	if volumes == nil {
		t.Fatal("expected a known empty volume result, got nil")
	}

	missing := pod.DeepCopy()
	missing.UID = "missing"
	if _, err := source.Volumes(context.Background(), missing); err == nil || !strings.Contains(err.Error(), "was not captured") {
		t.Fatalf("expected missing volume data error, got %v", err)
	}
}

func TestCapturedVolumeSourceOwnsVolumeData(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID("pod-1")}}
	requirements := karpscheduling.NewRequirements(karpscheduling.NewRequirement("topology.kubernetes.io/zone", corev1.NodeSelectorOpIn, "a"))
	volumes := karpscheduling.Volumes{}
	volumes.Add("ebs.csi.aws.com", "pvc-1")
	source := NewCapturedVolumeSource(map[types.UID]VolumeData{
		pod.UID: {Requirements: requirements, Volumes: volumes},
	})
	requirements.Add(karpscheduling.NewRequirement("example.com/mutated", corev1.NodeSelectorOpIn, "true"))
	volumes.Add("ebs.csi.aws.com", "pvc-2")

	if _, ok := source.Requirements(pod)["example.com/mutated"]; ok {
		t.Fatal("captured requirements aliased caller data")
	}
	got, err := source.Volumes(context.Background(), pod)
	if err != nil {
		t.Fatalf("getting captured volume data: %v", err)
	}
	if got["ebs.csi.aws.com"].Has("pvc-2") {
		t.Fatal("captured volumes aliased caller data")
	}
}
