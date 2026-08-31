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

	karpscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
)

// SchedulerPrecompute holds scheduler construction inputs that are stable across
// scheduling simulations within a single disruption iteration.
type SchedulerPrecompute struct {
	DaemonSetPods       []*corev1.Pod
	DaemonOverhead      map[*NodeClaimTemplate]corev1.ResourceList
	DaemonHostPortUsage map[*NodeClaimTemplate]*karpscheduling.HostPortUsage
}

// NewSchedulerPrecompute builds reusable scheduler inputs.
func NewSchedulerPrecompute(ctx context.Context, inputs *NodePoolInputs, daemonSetPods []*corev1.Pod) *SchedulerPrecompute {
	if daemonSetPods == nil {
		daemonSetPods = []*corev1.Pod{}
	}
	templates := inputs.nodeClaimTemplates
	return &SchedulerPrecompute{
		DaemonSetPods:       daemonSetPods,
		DaemonOverhead:      getDaemonOverhead(ctx, templates, daemonSetPods),
		DaemonHostPortUsage: getDaemonHostPortUsage(ctx, templates, daemonSetPods),
	}
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
