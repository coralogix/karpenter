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
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestObjectIndexListPods(t *testing.T) {
	idx := newObjectIndex(&Fixture{
		Pods: []*corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "default", Labels: map[string]string{"app": "web"}},
				Spec:       corev1.PodSpec{},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "scheduled", Namespace: "default", Labels: map[string]string{"app": "api"}},
				Spec:       corev1.PodSpec{NodeName: "node-a"},
			},
			{
				ObjectMeta: metav1.ObjectMeta{Name: "other-ns", Namespace: "kube-system"},
				Spec:       corev1.PodSpec{NodeName: "node-b"},
			},
		},
	})

	t.Run("field selector", func(t *testing.T) {
		podList := &corev1.PodList{}
		if err := idx.list(podList, client.ListOptions{
			FieldSelector: fieldsOne("spec.nodeName", ""),
		}); err != nil {
			t.Fatalf("list pending pods: %v", err)
		}
		if len(podList.Items) != 1 || podList.Items[0].Name != "pending" {
			t.Fatalf("pending pods = %#v, want only pending", podList.Items)
		}
	})

	t.Run("namespace and label selector", func(t *testing.T) {
		selector := labels.Set{"app": "api"}.AsSelector()
		podList := &corev1.PodList{}
		if err := idx.list(podList, client.ListOptions{
			Namespace:     "default",
			LabelSelector: selector,
		}); err != nil {
			t.Fatalf("list pods: %v", err)
		}
		if len(podList.Items) != 1 || podList.Items[0].Name != "scheduled" {
			t.Fatalf("filtered pods = %#v, want scheduled", podList.Items)
		}
	})
}

func fieldsOne(key, value string) fields.Selector {
	sel, err := fields.ParseSelector(key + "=" + value)
	if err != nil {
		panic(err)
	}
	return sel
}
