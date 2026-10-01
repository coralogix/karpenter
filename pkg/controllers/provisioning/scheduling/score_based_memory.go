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
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// ScoreBasedMemoryAllocatableOptions returns the allocatable CPU and memory
// shapes for every available offering compatible with this simulated
// NodeClaim, after subtracting the DaemonSet requests used by scheduling.
// Each returned resource list represents one compatible capacity shape; the
// caller can score the full set conservatively without picking an arbitrary
// InstanceTypeOptions[0].
func (n *NodeClaim) ScoreBasedMemoryAllocatableOptions() []corev1.ResourceList {
	if n == nil || len(n.InstanceTypeOptions) == 0 {
		return nil
	}
	var result []corev1.ResourceList
	for _, instanceType := range n.InstanceTypeOptions {
		if instanceType == nil {
			return nil
		}
		overhead, found := n.daemonOverheadFor(instanceType)
		if !found {
			return nil
		}
		for _, allocatable := range instanceType.AllocatableOfferingsList() {
			if len(allocatable.Offerings.Compatible(n.Requirements)) == 0 {
				continue
			}
			shape := resources.Subtract(allocatable.Allocatable, overhead)
			clampNegativeResource(shape, corev1.ResourceCPU)
			clampNegativeResource(shape, corev1.ResourceMemory)
			result = append(result, shape)
		}
	}
	return result
}

func (n *NodeClaim) daemonOverheadFor(instanceType *cloudprovider.InstanceType) (corev1.ResourceList, bool) {
	for _, group := range n.daemonOverheadGroups {
		for _, groupedInstanceType := range group.InstanceTypes {
			if groupedInstanceType == instanceType {
				return group.DaemonOverhead, true
			}
		}
	}
	return nil, false
}

func clampNegativeResource(resources corev1.ResourceList, name corev1.ResourceName) {
	quantity, ok := resources[name]
	if !ok || quantity.Sign() >= 0 {
		return
	}
	quantity.Set(0)
	resources[name] = quantity
}
