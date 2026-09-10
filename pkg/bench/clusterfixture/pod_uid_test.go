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

package clusterfixture

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestDecodePodPreservesUID(t *testing.T) {
	data := []byte(`apiVersion: v1
kind: Pod
metadata:
  name: app
  namespace: default
  uid: dumped-pod-uid
spec:
  containers:
  - name: app
    image: nginx
`)

	pods, err := decodeYAMLDocuments[*corev1.Pod](data)
	if err != nil {
		t.Fatalf("decodeYAMLDocuments() error = %v", err)
	}
	if len(pods) != 1 {
		t.Fatalf("pods = %d, want 1", len(pods))
	}
	if pods[0].UID != types.UID("dumped-pod-uid") {
		t.Fatalf("pod UID = %q, want dumped-pod-uid", pods[0].UID)
	}
}

func TestEnsurePodUIDs(t *testing.T) {
	newPods := func() []*corev1.Pod {
		return []*corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Name: "preserved", Namespace: "default", UID: "dumped-uid"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "generated-a", Namespace: "default"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "generated-b", Namespace: "default"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "duplicate", Namespace: "default", UID: "dumped-uid"}},
		}
	}

	first := newPods()
	ensurePodUIDs(first)
	if first[0].UID != types.UID("dumped-uid") {
		t.Fatalf("preserved UID = %q, want dumped-uid", first[0].UID)
	}
	seen := map[types.UID]struct{}{}
	for _, pod := range first {
		if pod.UID == "" {
			t.Fatalf("pod %s has an empty UID", pod.Name)
		}
		if _, ok := seen[pod.UID]; ok {
			t.Fatalf("duplicate UID %q", pod.UID)
		}
		seen[pod.UID] = struct{}{}
	}

	second := newPods()
	ensurePodUIDs(second)
	for i := range first {
		if first[i].UID != second[i].UID {
			t.Fatalf("UID at index %d changed between runs: %q != %q", i, first[i].UID, second[i].UID)
		}
	}
}
