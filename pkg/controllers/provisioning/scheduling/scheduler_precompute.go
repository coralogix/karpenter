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
	requirements karpscheduling.Requirements
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
		precompute.NodeDaemonResources[node.Name()] = nodeDaemonResources(
			ctx, node.Taints(), daemonPods, nodeLabelRequirements[node.Name()],
		)
	}
	return precompute
}

func buildNodeLabelRequirements(nodes []*state.StateNode) map[string]karpscheduling.Requirements {
	reqs := make(map[string]karpscheduling.Requirements, len(nodes))
	for _, node := range nodes {
		reqs[node.Name()] = karpscheduling.NewLabelRequirements(node.Labels())
	}
	return reqs
}

func buildDaemonPodPrecomputes(pods []*corev1.Pod) []daemonPodPrecompute {
	precomputes := make([]daemonPodPrecompute, 0, len(pods))
	for _, p := range pods {
		precomputes = append(precomputes, daemonPodPrecompute{
			pod:          p,
			requirements: karpscheduling.NewStrictPodRequirements(p),
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
		if err := nodeLabelRequirements.Compatible(daemon.requirements); err != nil {
			continue
		}
		compatible = append(compatible, daemon.pod)
	}
	return resources.RequestsForPods(compatible...)
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

func LabelRequirementsFor(cache map[string]karpscheduling.Requirements, nodeName string, labels map[string]string) karpscheduling.Requirements {
	if reqs, ok := cache[nodeName]; ok {
		return reqs
	}
	return karpscheduling.NewLabelRequirements(labels)
}
