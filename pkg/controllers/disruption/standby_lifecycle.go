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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
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

// nodeOccupyingPods returns bound workload pods that must leave before a node can enter standby,
// be reclaimed, or complete evacuation. DaemonSet pods are managed by their DaemonSet, terminal
// pods have completed, and Node-owned mirror pods are managed by the kubelet. A nonterminal
// terminating pod still occupies the node until it leaves or becomes terminal.
func nodeOccupyingPods(ctx context.Context, reader client.Reader, node *corev1.Node) ([]*corev1.Pod, error) {
	if node == nil {
		return nil, fmt.Errorf("checking node occupancy without a Node")
	}
	if reader == nil {
		return nil, fmt.Errorf("checking node occupancy without an API reader")
	}
	var podList corev1.PodList
	if err := reader.List(ctx, &podList, client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return nil, fmt.Errorf("listing pods, %w", err)
	}
	occupying := make([]*corev1.Pod, 0, len(podList.Items))
	for i := range podList.Items {
		pod := &podList.Items[i]
		if podutils.IsOwnedByDaemonSet(pod) || podutils.IsTerminal(pod) || podutils.IsOwnedByNode(pod) {
			continue
		}
		occupying = append(occupying, pod)
	}
	return occupying, nil
}

func nodeEmpty(ctx context.Context, reader client.Reader, node *corev1.Node) (bool, error) {
	pods, err := nodeOccupyingPods(ctx, reader, node)
	return len(pods) == 0, err
}
