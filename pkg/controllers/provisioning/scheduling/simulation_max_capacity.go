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
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// SimulationMaxCapacityAnnotationKey is an opt-in NodePool annotation that
// limits nominal CPU and memory capacity considered by new-node fit checks.
const SimulationMaxCapacityAnnotationKey = "karpenter.coralogix.net/simulation-max-capacity"

func parseSimulationMaxCapacity(value string) (corev1.ResourceList, error) {
	var raw map[string]string
	if err := json.Unmarshal([]byte(value), &raw); err != nil {
		return nil, fmt.Errorf("expected a JSON object of resource quantities: %w", err)
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("expected at least one resource quantity")
	}
	result := corev1.ResourceList{}
	for name, value := range raw {
		resourceName := corev1.ResourceName(name)
		if resourceName != corev1.ResourceCPU && resourceName != corev1.ResourceMemory {
			return nil, fmt.Errorf("unsupported resource %q; only cpu and memory are supported", name)
		}
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return nil, fmt.Errorf("invalid %s quantity %q: %w", name, value, err)
		}
		if quantity.Sign() <= 0 {
			return nil, fmt.Errorf("%s quantity must be positive", name)
		}
		result[resourceName] = quantity
	}
	return result, nil
}

// capAllocatable applies a nominal capacity ceiling after provider overhead and
// offering overrides have been accounted for in the allocatable group. This
// preserves all actual overhead (including huge pages) while avoiding mutation
// of the shared InstanceType and its cached allocatable groups.
func capAllocatable(instanceType *cloudprovider.InstanceType, group cloudprovider.AllocatableOfferings, capacityCeiling corev1.ResourceList) corev1.ResourceList {
	capacity := instanceType.Capacity
	if len(group.Offerings) > 0 && len(group.Offerings[0].CapacityOverride) > 0 {
		capacity = resources.Merge(capacity, group.Offerings[0].CapacityOverride)
	}
	allocatable := group.Allocatable.DeepCopy()
	for resourceName, ceiling := range capacityCeiling {
		nominalCapacity, ok := capacity[resourceName]
		if !ok || nominalCapacity.Cmp(ceiling) <= 0 {
			continue
		}
		excess := nominalCapacity.DeepCopy()
		excess.Sub(ceiling)
		available := allocatable[resourceName]
		available.Sub(excess)
		allocatable[resourceName] = available
	}
	return allocatable
}
