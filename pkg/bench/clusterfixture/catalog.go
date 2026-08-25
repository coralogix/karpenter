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

package clusterfixture

import (
	"maps"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	fakecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// CatalogEntry describes one instance type offering for benchmarks.
type CatalogEntry struct {
	Name         string  `json:"name"`
	CPU          string  `json:"cpu"`
	Memory       string  `json:"memory"`
	Architecture string  `json:"architecture"`
	Zone         string  `json:"zone"`
	CapacityType string  `json:"capacityType"`
	Price        float64 `json:"price"`
}

// Catalog is a synthesized cloud provider instance catalog.
type Catalog struct {
	InstanceTypes         []CatalogEntry      `json:"instanceTypes"`
	NodePoolInstanceTypes map[string][]string `json:"nodePoolInstanceTypes"`
}

type catalogKey struct {
	name         string
	zone         string
	capacityType string
}

// BuildCatalog synthesizes instance types from fixture nodes and node pools.
//
//nolint:gocyclo
func BuildCatalog(fixture *Fixture) *Catalog {
	entries := map[catalogKey]CatalogEntry{}
	nodePoolTypes := map[string]map[string]struct{}{}

	addEntry := func(name, zone, capacityType, cpu, memory, arch string) {
		if name == "" || zone == "" || capacityType == "" {
			return
		}
		if arch == "" {
			arch = v1.ArchitectureAmd64
		}
		cpuQty := resource.MustParse(cpu)
		memQty := resource.MustParse(memory)
		price := fakecloudprovider.PriceFromResources(corev1.ResourceList{
			corev1.ResourceCPU:    cpuQty,
			corev1.ResourceMemory: memQty,
		})
		key := catalogKey{name: name, zone: zone, capacityType: capacityType}
		entries[key] = CatalogEntry{
			Name:         name,
			CPU:          cpu,
			Memory:       memory,
			Architecture: arch,
			Zone:         zone,
			CapacityType: capacityType,
			Price:        price,
		}
	}

	for _, node := range fixture.Nodes {
		labels := node.GetLabels()
		name := labels[corev1.LabelInstanceTypeStable]
		zone := labels[corev1.LabelTopologyZone]
		capacityType := labels[v1.CapacityTypeLabelKey]
		arch := labels[corev1.LabelArchStable]
		cpu := node.Status.Allocatable.Cpu().String()
		memory := node.Status.Allocatable.Memory().String()
		if cpu == "0" {
			cpu = "4"
		}
		if memory == "" || memory == "0" {
			memory = "8Gi"
		}
		addEntry(name, zone, capacityType, cpu, memory, arch)
		if pool := labels[v1.NodePoolLabelKey]; pool != "" && name != "" {
			if nodePoolTypes[pool] == nil {
				nodePoolTypes[pool] = map[string]struct{}{}
			}
			nodePoolTypes[pool][name] = struct{}{}
		}
	}

	catalog := &Catalog{
		InstanceTypes:         lo.Values(entries),
		NodePoolInstanceTypes: map[string][]string{},
	}
	for pool, names := range nodePoolTypes {
		catalog.NodePoolInstanceTypes[pool] = lo.Keys(names)
	}
	return catalog
}

// ApplyToCloudProvider configures a fake cloud provider from the catalog.
func (c *Catalog) ApplyToCloudProvider(cp *fakecloudprovider.CloudProvider) {
	if c == nil {
		return
	}
	byName := map[string]*cloudprovider.InstanceType{}
	for _, entry := range c.InstanceTypes {
		it, ok := byName[entry.Name]
		if !ok {
			it = fakecloudprovider.NewInstanceType(fakecloudprovider.InstanceTypeOptions{
				Name: entry.Name,
				Resources: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse(entry.CPU),
					corev1.ResourceMemory: resource.MustParse(entry.Memory),
				},
				Architecture: entry.Architecture,
			})
			byName[entry.Name] = it
		}
		reqs := map[string]string{
			v1.CapacityTypeLabelKey:  entry.CapacityType,
			corev1.LabelTopologyZone: entry.Zone,
			corev1.LabelArchStable:   entry.Architecture,
		}
		it.Offerings = append(it.Offerings, &cloudprovider.Offering{
			Available:    true,
			Price:        entry.Price,
			Requirements: scheduling.NewLabelRequirements(reqs),
		})
	}
	cp.InstanceTypes = lo.Values(byName)
	cp.InstanceTypesForNodePool = map[string][]*cloudprovider.InstanceType{}
	for pool, names := range c.NodePoolInstanceTypes {
		types := lo.FilterMap(names, func(name string, _ int) (*cloudprovider.InstanceType, bool) {
			it, ok := byName[name]
			return it, ok
		})
		if len(types) > 0 {
			cp.InstanceTypesForNodePool[pool] = types
		}
	}
	if len(cp.InstanceTypes) == 0 {
		cp.InstanceTypes = fakecloudprovider.InstanceTypesAssorted()
	}
}

// Clone returns a deep copy of the catalog.
func (c *Catalog) Clone() *Catalog {
	if c == nil {
		return nil
	}
	clone := &Catalog{
		InstanceTypes:         append([]CatalogEntry(nil), c.InstanceTypes...),
		NodePoolInstanceTypes: map[string][]string{},
	}
	maps.Copy(clone.NodePoolInstanceTypes, c.NodePoolInstanceTypes)
	return clone
}
