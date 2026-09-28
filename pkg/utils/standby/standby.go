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

// Package standby contains shared representation helpers for retained nodes.
package standby

import (
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

const (
	NodeClaimAnnotationKey                 = "karpenter.coralogix.net/standby"
	NodeClaimActivatingAnnotationKey       = "karpenter.coralogix.net/standby-activating"
	NodeClaimActivationSourceAnnotationKey = "karpenter.coralogix.net/standby-activation-source"
	NodeTaintKey                           = "karpenter.coralogix.net/standby"
	NodeTaintValue                         = "true"
)

type ActivationSource string

const (
	ActivationSourceProvisioning ActivationSource = "provisioning"
	ActivationSourceCompaction   ActivationSource = "compaction"
	ActivationSourceRecovery     ActivationSource = "recovery"
)

// NodeTaint returns the taint that prevents ordinary pods from scheduling onto standby capacity.
func NodeTaint() corev1.Taint {
	return corev1.Taint{Key: NodeTaintKey, Value: NodeTaintValue, Effect: corev1.TaintEffectNoSchedule}
}

// IsNodeClaimActivating reports whether activation of this NodeClaim was interrupted and needs resuming.
func IsNodeClaimActivating(nodeClaim *v1.NodeClaim) bool {
	return nodeClaim != nil && nodeClaim.Annotations[NodeClaimActivatingAnnotationKey] == "true"
}

// SetNodeClaimActivating updates the persistent activation-in-progress marker on a NodeClaim in memory.
func SetNodeClaimActivating(nodeClaim *v1.NodeClaim, activating bool) {
	if nodeClaim == nil {
		return
	}
	if activating {
		if nodeClaim.Annotations == nil {
			nodeClaim.Annotations = map[string]string{}
		}
		nodeClaim.Annotations[NodeClaimActivatingAnnotationKey] = "true"
		return
	}
	if nodeClaim.Annotations != nil {
		delete(nodeClaim.Annotations, NodeClaimActivatingAnnotationKey)
	}
}

// NodeClaimActivationSource returns the source persisted with an in-progress activation.
func NodeClaimActivationSource(nodeClaim *v1.NodeClaim) ActivationSource {
	if nodeClaim == nil {
		return ""
	}
	return ActivationSource(nodeClaim.Annotations[NodeClaimActivationSourceAnnotationKey])
}

// SetNodeClaimActivationSource updates the source persisted with an in-progress activation.
func SetNodeClaimActivationSource(nodeClaim *v1.NodeClaim, source ActivationSource) {
	if nodeClaim == nil {
		return
	}
	if source == "" {
		if nodeClaim.Annotations != nil {
			delete(nodeClaim.Annotations, NodeClaimActivationSourceAnnotationKey)
		}
		return
	}
	if nodeClaim.Annotations == nil {
		nodeClaim.Annotations = map[string]string{}
	}
	nodeClaim.Annotations[NodeClaimActivationSourceAnnotationKey] = string(source)
}

// IsNodeClaimStandby reports whether a NodeClaim carries the persistent standby marker.
func IsNodeClaimStandby(nodeClaim *v1.NodeClaim) bool {
	return nodeClaim != nil && nodeClaim.Annotations[NodeClaimAnnotationKey] == "true"
}

// SetNodeClaimStandby updates the persistent standby marker on a NodeClaim in memory.
func SetNodeClaimStandby(nodeClaim *v1.NodeClaim, standby bool) {
	if nodeClaim == nil {
		return
	}
	if standby {
		if nodeClaim.Annotations == nil {
			nodeClaim.Annotations = map[string]string{}
		}
		nodeClaim.Annotations[NodeClaimAnnotationKey] = "true"
		return
	}
	if nodeClaim.Annotations != nil {
		delete(nodeClaim.Annotations, NodeClaimAnnotationKey)
	}
}

// HasNodeTaint reports whether a node has the standby NoSchedule taint.
func HasNodeTaint(node *corev1.Node) bool {
	if node == nil {
		return false
	}
	taint := NodeTaint()
	for _, existing := range node.Spec.Taints {
		if existing.MatchTaint(&taint) {
			return true
		}
	}
	return false
}

// SetNodeTaint adds or removes the standby taint from a node in memory.
func SetNodeTaint(node *corev1.Node, standby bool) {
	if node == nil {
		return
	}
	taint := NodeTaint()
	filtered := make([]corev1.Taint, 0, len(node.Spec.Taints)+1)
	for _, existing := range node.Spec.Taints {
		if existing.Key != taint.Key {
			filtered = append(filtered, existing)
		}
	}
	if standby {
		filtered = append(filtered, taint)
	}
	node.Spec.Taints = filtered
}
