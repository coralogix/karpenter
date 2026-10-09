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
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// HasDisruptionNoScheduleTaint reports whether the node is reserved for in-flight disruption.
func HasDisruptionNoScheduleTaint(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	for _, taint := range node.Spec.Taints {
		if taint.MatchTaint(&v1.DisruptedNoScheduleTaint) {
			return true
		}
	}
	return false
}

// EligibleForSchedulingActivation reports whether retained standby capacity may be treated as
// schedulable during planning. Activating nodes are excluded; callers resume them before planning.
func EligibleForSchedulingActivation(node *state.StateNode) bool {
	if node == nil || node.Node == nil || node.NodeClaim == nil {
		return false
	}
	if node.MarkedForDeletion() || IsNodeClaimActivating(node.NodeClaim) || HasNodeActivationMarker(node.Node) {
		return false
	}
	if HasDisruptionNoScheduleTaint(node.Node) {
		return false
	}
	return IsNodeClaimStandby(node.NodeClaim)
}

// PlanningTaints returns the taint set scheduling should use for admission, daemon overhead, and topology.
// It never mutates cluster objects.
func PlanningTaints(node *state.StateNode) []corev1.Taint {
	taints := node.Taints()
	if !EligibleForSchedulingActivation(node) {
		return append([]corev1.Taint(nil), taints...)
	}
	standbyTaint := NodeTaint()
	filtered := make([]corev1.Taint, 0, len(taints))
	for _, taint := range taints {
		if taint.Key != standbyTaint.Key {
			filtered = append(filtered, taint)
		}
	}
	return filtered
}
