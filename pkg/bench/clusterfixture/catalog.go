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
	"strings"

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

// CatalogOffering describes a single offering on an instance type.
type CatalogOffering struct {
	Available    bool                                      `json:"available"`
	Price        float64                                   `json:"price"`
	Requirements []v1.NodeSelectorRequirementWithMinValues `json:"requirements"`
}

// CatalogInstanceTypeOverhead captures reserved resources on an instance type.
type CatalogInstanceTypeOverhead struct {
	KubeReserved      map[string]string `json:"kubeReserved,omitempty"`
	SystemReserved    map[string]string `json:"systemReserved,omitempty"`
	EvictionThreshold map[string]string `json:"evictionThreshold,omitempty"`
}

// CatalogInstanceTypeSpec captures a resolved cloud provider instance type.
type CatalogInstanceTypeSpec struct {
	Name         string                                    `json:"name"`
	CPU          string                                    `json:"cpu"`
	Memory       string                                    `json:"memory"`
	Requirements []v1.NodeSelectorRequirementWithMinValues `json:"requirements"`
	Capacity     map[string]string                         `json:"capacity,omitempty"`
	Overhead     *CatalogInstanceTypeOverhead              `json:"overhead,omitempty"`
	Offerings    []CatalogOffering                         `json:"offerings,omitempty"`
}

// Catalog is a synthesized cloud provider instance catalog.
type Catalog struct {
	InstanceTypes         []CatalogEntry            `json:"instanceTypes,omitempty"`
	InstanceTypeSpecs     []CatalogInstanceTypeSpec `json:"instanceTypeSpecs,omitempty"`
	NodePoolInstanceTypes map[string][]string       `json:"nodePoolInstanceTypes"`
}

// HasFullInstanceCatalog reports whether the catalog was exported from GetInstanceTypes
// (production-scale) rather than synthesized from running nodes only.
func (c *Catalog) HasFullInstanceCatalog() bool {
	return c != nil && len(c.InstanceTypeSpecs) > 0
}

// RegionFromFixture returns the AWS region recorded in or inferred from a fixture.
func RegionFromFixture(f *Fixture) string {
	if f == nil {
		return ""
	}
	if f.Metadata.Region != "" {
		return f.Metadata.Region
	}
	if r := regionFromContext(f.Metadata.Context); r != "" {
		return r
	}
	for _, node := range f.Nodes {
		if r := node.Labels[corev1.LabelTopologyRegion]; r != "" {
			return r
		}
	}
	return ""
}

func regionFromContext(context string) string {
	const marker = "-aws-"
	if i := strings.Index(context, marker); i >= 0 {
		return context[i+len(marker):]
	}
	return ""
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

// BuildCatalogFromInstanceTypes builds a catalog from GetInstanceTypes results.
func BuildCatalogFromInstanceTypes(perPool map[string][]*cloudprovider.InstanceType) *Catalog {
	specsByName := map[string]CatalogInstanceTypeSpec{}
	poolNames := map[string][]string{}

	for pool, its := range perPool {
		names := make([]string, 0, len(its))
		for _, it := range its {
			names = append(names, it.Name)
			if _, ok := specsByName[it.Name]; !ok {
				specsByName[it.Name] = specFromInstanceType(it)
			}
		}
		poolNames[pool] = names
	}

	return &Catalog{
		InstanceTypeSpecs:     lo.Values(specsByName),
		NodePoolInstanceTypes: poolNames,
	}
}

func resourceListToMap(list corev1.ResourceList) map[string]string {
	if len(list) == 0 {
		return nil
	}
	out := map[string]string{}
	for k, v := range list {
		out[string(k)] = v.String()
	}
	return out
}

func resourceMapToList(m map[string]string) corev1.ResourceList {
	if len(m) == 0 {
		return nil
	}
	out := corev1.ResourceList{}
	for k, v := range m {
		out[corev1.ResourceName(k)] = resource.MustParse(v)
	}
	return out
}

func overheadFromSpec(spec *CatalogInstanceTypeOverhead) *cloudprovider.InstanceTypeOverhead {
	if spec == nil {
		return &cloudprovider.InstanceTypeOverhead{
			KubeReserved: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("10Mi"),
			},
		}
	}
	return &cloudprovider.InstanceTypeOverhead{
		KubeReserved:      resourceMapToList(spec.KubeReserved),
		SystemReserved:    resourceMapToList(spec.SystemReserved),
		EvictionThreshold: resourceMapToList(spec.EvictionThreshold),
	}
}

func specFromInstanceType(it *cloudprovider.InstanceType) CatalogInstanceTypeSpec {
	capacity := map[string]string{}
	for k, v := range it.Capacity {
		capacity[string(k)] = v.String()
	}
	var overhead *CatalogInstanceTypeOverhead
	if it.Overhead != nil {
		overhead = &CatalogInstanceTypeOverhead{
			KubeReserved:      resourceListToMap(it.Overhead.KubeReserved),
			SystemReserved:    resourceListToMap(it.Overhead.SystemReserved),
			EvictionThreshold: resourceListToMap(it.Overhead.EvictionThreshold),
		}
	}
	return CatalogInstanceTypeSpec{
		Name:         it.Name,
		CPU:          it.Capacity.Cpu().String(),
		Memory:       it.Capacity.Memory().String(),
		Requirements: it.Requirements.NodeSelectorRequirements(),
		Capacity:     capacity,
		Overhead:     overhead,
		Offerings: lo.Map(it.Offerings, func(o *cloudprovider.Offering, _ int) CatalogOffering {
			return CatalogOffering{
				Available: o.Available,
				Price:     o.Price,
				Requirements: lo.Map(o.Requirements.Values(), func(req *scheduling.Requirement, _ int) v1.NodeSelectorRequirementWithMinValues {
					return req.NodeSelectorRequirement()
				}),
			}
		}),
	}
}

func instanceTypeFromSpec(spec CatalogInstanceTypeSpec) *cloudprovider.InstanceType {
	capacity := corev1.ResourceList{}
	for k, v := range spec.Capacity {
		capacity[corev1.ResourceName(k)] = resource.MustParse(v)
	}
	if len(capacity) == 0 {
		capacity = corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(spec.CPU),
			corev1.ResourceMemory: resource.MustParse(spec.Memory),
		}
	}
	offerings := lo.Map(spec.Offerings, func(o CatalogOffering, _ int) *cloudprovider.Offering {
		return &cloudprovider.Offering{
			Available:    o.Available,
			Price:        o.Price,
			Requirements: scheduling.NewNodeSelectorRequirementsWithMinValues(o.Requirements...),
		}
	})
	return &cloudprovider.InstanceType{
		Name:         spec.Name,
		Requirements: scheduling.NewNodeSelectorRequirementsWithMinValues(spec.Requirements...),
		Capacity:     capacity,
		Overhead:     overheadFromSpec(spec.Overhead),
		Offerings:    offerings,
	}
}

// ApplyToCloudProvider configures a fake cloud provider from the catalog.
func (c *Catalog) ApplyToCloudProvider(cp *fakecloudprovider.CloudProvider) {
	if c == nil {
		return
	}
	if len(c.InstanceTypeSpecs) > 0 {
		c.applySpecsToCloudProvider(cp)
		return
	}
	c.applyLegacyEntriesToCloudProvider(cp)
}

func (c *Catalog) applySpecsToCloudProvider(cp *fakecloudprovider.CloudProvider) {
	byName := map[string]*cloudprovider.InstanceType{}
	for _, spec := range c.InstanceTypeSpecs {
		byName[spec.Name] = instanceTypeFromSpec(spec)
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

func (c *Catalog) applyLegacyEntriesToCloudProvider(cp *fakecloudprovider.CloudProvider) {
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
		InstanceTypeSpecs:     append([]CatalogInstanceTypeSpec(nil), c.InstanceTypeSpecs...),
		NodePoolInstanceTypes: map[string][]string{},
	}
	maps.Copy(clone.NodePoolInstanceTypes, c.NodePoolInstanceTypes)
	return clone
}
