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

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	karpscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// SchedulerPrecompute holds scheduler construction inputs that are stable across
// scheduling simulations within a single disruption iteration.
type SchedulerPrecompute struct {
	DaemonSetPods         []*corev1.Pod
	DaemonOverhead        map[*NodeClaimTemplate]corev1.ResourceList
	DaemonHostPortUsage   map[*NodeClaimTemplate]*karpscheduling.HostPortUsage
	NodeLabelRequirements map[string]karpscheduling.Requirements
	NodeDaemonResources   map[string]corev1.ResourceList
}

type daemonPodPrecompute struct {
	pod          *corev1.Pod
	requirements []karpscheduling.Requirements
}

// NewSchedulerPrecompute builds reusable scheduler inputs.
func NewSchedulerPrecompute(ctx context.Context, inputs *NodePoolInputs, daemonSetPods []*corev1.Pod, stateNodes []*state.StateNode) *SchedulerPrecompute {
	if daemonSetPods == nil {
		daemonSetPods = []*corev1.Pod{}
	}
	templates := inputs.nodeClaimTemplates
	nodeLabelRequirements := buildNodeLabelRequirements(stateNodes)
	daemonPods := buildDaemonPodPrecomputes(daemonSetPods)
	precompute := &SchedulerPrecompute{
		DaemonSetPods:         daemonSetPods,
		DaemonOverhead:        getDaemonOverhead(ctx, templates, daemonSetPods),
		DaemonHostPortUsage:   getDaemonHostPortUsage(ctx, templates, daemonSetPods),
		NodeLabelRequirements: nodeLabelRequirements,
		NodeDaemonResources:   make(map[string]corev1.ResourceList, len(stateNodes)),
	}
	for _, node := range stateNodes {
		key := stateNodeCacheKey(node)
		precompute.NodeDaemonResources[key] = nodeDaemonResources(
			ctx, node.Taints(), daemonPods, nodeLabelRequirements[key],
		)
	}
	return precompute
}

func buildNodeLabelRequirements(nodes []*state.StateNode) map[string]karpscheduling.Requirements {
	reqs := make(map[string]karpscheduling.Requirements, len(nodes))
	for _, node := range nodes {
		reqs[stateNodeCacheKey(node)] = karpscheduling.NewLabelRequirements(node.Labels())
	}
	return reqs
}

// stateNodeCacheKey returns the stable identity used for scheduler caches. StateNode names
// are not unique across Nodes and NodeClaims, while provider IDs are cluster-unique.
func stateNodeCacheKey(node *state.StateNode) string {
	if providerID := node.ProviderID(); providerID != "" {
		return providerID
	}
	// Nodes without provider IDs are only possible outside of the cluster state, which
	// normalizes their provider ID to the node name. Keep this fallback for callers that
	// construct StateNodes directly (including tests).
	return node.Name()
}

func coreNodeCacheKey(node *corev1.Node) string {
	if node.Spec.ProviderID != "" {
		return node.Spec.ProviderID
	}
	return node.Name
}

func buildDaemonPodPrecomputes(pods []*corev1.Pod) []daemonPodPrecompute {
	precomputes := make([]daemonPodPrecompute, 0, len(pods))
	for _, p := range pods {
		precomputes = append(precomputes, daemonPodPrecompute{
			pod:          p,
			requirements: daemonPodRequirements(p),
		})
	}
	return precomputes
}

func nodeDaemonResources(
	ctx context.Context,
	taints []corev1.Taint,
	daemonPods []daemonPodPrecompute,
	nodeLabelRequirements karpscheduling.Requirements,
) corev1.ResourceList {
	var compatible []*corev1.Pod
	for _, daemon := range daemonPods {
		if shouldSkipDaemonPod(ctx, daemon.pod) {
			continue
		}
		if err := karpscheduling.Taints(taints).ToleratesPod(daemon.pod); err != nil {
			continue
		}
		if !requirementsCompatible(nodeLabelRequirements, daemon.requirements) {
			continue
		}
		compatible = append(compatible, daemon.pod)
	}
	return resources.RequestsForPods(compatible...)
}

func requirementsCompatible(nodeRequirements karpscheduling.Requirements, podRequirements []karpscheduling.Requirements) bool {
	for _, requirements := range podRequirements {
		if nodeRequirements.Compatible(requirements) == nil {
			return true
		}
	}
	return false
}

func cloneDaemonHostPortUsage(baseline map[*NodeClaimTemplate]*karpscheduling.HostPortUsage) map[*NodeClaimTemplate]*karpscheduling.HostPortUsage {
	if baseline == nil {
		return nil
	}
	cloned := make(map[*NodeClaimTemplate]*karpscheduling.HostPortUsage, len(baseline))
	for template, usage := range baseline {
		cloned[template] = usage.DeepCopy()
	}
	return cloned
}

func LabelRequirementsFor(cache map[string]karpscheduling.Requirements, nodeKey string, labels map[string]string) karpscheduling.Requirements {
	if reqs, ok := cache[nodeKey]; ok {
		return reqs
	}
	return karpscheduling.NewLabelRequirements(labels)
}
