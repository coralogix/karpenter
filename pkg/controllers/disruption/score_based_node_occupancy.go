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

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// scoreBasedNodeOccupyingPods returns bound workload pods that must leave before a score-based
// node can enter standby or be reclaimed. DaemonSet pods are managed by their DaemonSet,
// terminal pods have completed, and Node-owned mirror pods are managed by the kubelet. A
// nonterminal terminating pod still occupies the node until it leaves or becomes terminal.
func scoreBasedNodeOccupyingPods(ctx context.Context, reader client.Reader, node *corev1.Node) ([]*corev1.Pod, error) {
	if node == nil {
		return nil, fmt.Errorf("checking score-based node occupancy without a Node")
	}
	if reader == nil {
		return nil, fmt.Errorf("checking score-based node occupancy without an API reader")
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

func scoreBasedNodeEmpty(ctx context.Context, reader client.Reader, node *corev1.Node) (bool, error) {
	pods, err := scoreBasedNodeOccupyingPods(ctx, reader, node)
	return len(pods) == 0, err
}
