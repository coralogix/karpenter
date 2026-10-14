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
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

func StandbyEntryComplete(node *corev1.Node, nodeClaim *v1.NodeClaim) bool {
	if node == nil || nodeClaim == nil {
		return false
	}
	if !utilstandby.IsNodeClaimStandby(nodeClaim) {
		return false
	}
	return utilstandby.HasNodeTaint(node)
}

func StandbyEntrySucceeded(result TransitionResult) bool {
	switch result.Outcome {
	case OutcomeCompleted:
		return StandbyEntryComplete(result.Node, result.NodeClaim)
	case OutcomeUnchanged:
		return StandbyEntryComplete(result.Node, result.NodeClaim)
	default:
		return false
	}
}
