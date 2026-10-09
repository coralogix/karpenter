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

package disruption

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const defaultReclamationStandbyDelay = 15 * time.Second

// standbyLifecycleEnabled reports whether this pool uses standby marking and reclamation.
// consolidateAfter does not affect this mode.
func standbyLifecycleEnabled(nodePool *v1.NodePool) bool {
	return NodePoolUsesScoreBasedConsolidation(nodePool) &&
		nodePool.Spec.Replicas == nil &&
		nodePool.Spec.Disruption.ConsolidationPolicy == v1.ConsolidationPolicyWhenEmptyOrUnderutilized
}

func reclamationStandbyDelay(nodePool *v1.NodePool) time.Duration {
	if nodePool == nil || nodePool.Annotations == nil {
		return defaultReclamationStandbyDelay
	}
	value := nodePool.Annotations[v1.ReclamationStandbyDelayAnnotationKey]
	if value == "" {
		return defaultReclamationStandbyDelay
	}
	delay, err := time.ParseDuration(value)
	if err != nil {
		log.FromContext(context.Background()).V(1).Info("using default reclamation standby delay for invalid value", "NodePool", nodePool.Name, "annotation", v1.ReclamationStandbyDelayAnnotationKey, "value", value)
		return defaultReclamationStandbyDelay
	}
	if delay <= 0 {
		return 0
	}
	return delay
}

// standbyReclamationSoakElapsed reports whether a standby NodeClaim may be reclaimed.
// Legacy standby markers without a timestamp skip the soak gate.
func standbyReclamationSoakElapsed(nodePool *v1.NodePool, nodeClaim *v1.NodeClaim, now time.Time) bool {
	if !standby.IsNodeClaimStandby(nodeClaim) {
		return false
	}
	delay := reclamationStandbyDelay(nodePool)
	if delay <= 0 {
		return true
	}
	since, ok := standby.NodeClaimStandbySince(nodeClaim)
	if !ok {
		return true
	}
	return now.Sub(since) > delay
}

func nodeOccupyingPods(ctx context.Context, reader client.Reader, node *corev1.Node) ([]*corev1.Pod, error) {
	return standbypkg.NodeOccupyingPods(ctx, reader, node)
}

func nodeEmpty(ctx context.Context, reader client.Reader, node *corev1.Node) (bool, error) {
	return standbypkg.NodeEmpty(ctx, reader, node)
}
