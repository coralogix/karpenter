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

	// The shared make test target supplies Ginkgo flags to every package.
	_ "github.com/onsi/ginkgo/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestStandbyMarkers(t *testing.T) {
	since := time.Date(2026, time.October, 9, 12, 0, 0, 0, time.UTC)
	nodeClaim := &v1.NodeClaim{}
	SetNodeClaimStandby(nodeClaim, true, since)
	SetNodeClaimActivating(nodeClaim, true)
	if !IsNodeClaimStandby(nodeClaim) || !IsNodeClaimActivating(nodeClaim) {
		t.Fatal("expected standby and activation markers to be set")
	}
	gotSince, ok := NodeClaimStandbySince(nodeClaim)
	if !ok || !gotSince.Equal(since) {
		t.Fatalf("standby since = (%v, %v), want (%v, true)", gotSince, ok, since)
	}
	SetNodeClaimActivating(nodeClaim, false)
	SetNodeClaimStandby(nodeClaim, false, time.Time{})
	if IsNodeClaimStandby(nodeClaim) || IsNodeClaimActivating(nodeClaim) {
		t.Fatal("expected standby markers to be cleared")
	}
}

func TestLegacyStandbyMarker(t *testing.T) {
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{NodeClaimAnnotationKey: legacyStandbyMarker}}}
	if !IsNodeClaimStandby(nodeClaim) {
		t.Fatal("expected legacy standby marker to be recognized")
	}
	if _, ok := NodeClaimStandbySince(nodeClaim); ok {
		t.Fatal("legacy standby marker should not provide a soak timestamp")
	}
}

func TestSetNodeTaintPreservesOtherTaints(t *testing.T) {
	other := corev1.Taint{Key: "example.com/keep", Effect: corev1.TaintEffectNoSchedule}
	node := &corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{other}}}
	SetNodeTaint(node, true)
	if !HasNodeTaint(node) || len(node.Spec.Taints) != 2 {
		t.Fatal("expected standby taint to be added alongside unrelated taint")
	}
	SetNodeTaint(node, false)
	if HasNodeTaint(node) || len(node.Spec.Taints) != 1 || node.Spec.Taints[0] != other {
		t.Fatal("expected standby taint removal to preserve unrelated taints")
	}
}
