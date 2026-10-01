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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestScoreBasedMemoryAllocatableSubtractsTypeSpecificDaemonOverhead(t *testing.T) {
	first := scoreBasedMemoryInstanceType("first", "10", "20Gi", nil)
	second := scoreBasedMemoryInstanceType("second", "10", "20Gi", nil)

	nodeClaim := &NodeClaim{
		NodeClaimTemplate: NodeClaimTemplate{
			InstanceTypeOptions: cloudprovider.InstanceTypes{first, second},
			Requirements:        scheduling.NewRequirements(),
		},
		daemonOverheadGroups: []DaemonOverheadGroup{
			{InstanceTypes: []*cloudprovider.InstanceType{first}, DaemonOverhead: scoreBasedMemoryResources("1", "2Gi")},
			{InstanceTypes: []*cloudprovider.InstanceType{second}, DaemonOverhead: scoreBasedMemoryResources("250m", "4Gi")},
		},
	}

	got := nodeClaim.ScoreBasedMemoryAllocatableOptions()
	if len(got) != 2 {
		t.Fatalf("expected one shape per instance type, got %d", len(got))
	}
	assertScoreBasedMemoryResources(t, got[0], "9", "18Gi")
	assertScoreBasedMemoryResources(t, got[1], "9750m", "16Gi")
}

func TestScoreBasedMemoryAllocatableFiltersOfferingsAndIncludesCapacityOverrides(t *testing.T) {
	instanceType := scoreBasedMemoryInstanceType("override", "8", "0", []cloudprovider.Offering{
		scoreBasedMemoryOffering("zone-a", true, scoreBasedMemoryResources("0", "20Gi")),
		scoreBasedMemoryOffering("zone-b", true, scoreBasedMemoryResources("0", "40Gi")),
		scoreBasedMemoryOffering("zone-a", false, scoreBasedMemoryResources("0", "64Gi")),
	})
	nodeClaim := &NodeClaim{
		NodeClaimTemplate: NodeClaimTemplate{
			InstanceTypeOptions: cloudprovider.InstanceTypes{instanceType},
			Requirements:        scheduling.NewLabelRequirements(map[string]string{corev1.LabelTopologyZone: "zone-a"}),
		},
		daemonOverheadGroups: []DaemonOverheadGroup{{InstanceTypes: []*cloudprovider.InstanceType{instanceType}, DaemonOverhead: scoreBasedMemoryResources("1", "2Gi")}},
	}

	got := nodeClaim.ScoreBasedMemoryAllocatableOptions()
	if len(got) != 1 {
		t.Fatalf("expected only the available zone-a override shape, got %d shapes: %v", len(got), got)
	}
	assertScoreBasedMemoryResources(t, got[0], "7", "18Gi")
}

func TestScoreBasedMemoryAllocatableShapesDoNotMutateSharedResources(t *testing.T) {
	instanceType := scoreBasedMemoryInstanceType("shared", "10", "20Gi", nil)
	instanceType.Overhead = &cloudprovider.InstanceTypeOverhead{KubeReserved: scoreBasedMemoryResources("500m", "1Gi")}
	daemonOverhead := scoreBasedMemoryResources("250m", "2Gi")
	nodeClaim := &NodeClaim{
		NodeClaimTemplate: NodeClaimTemplate{
			InstanceTypeOptions: cloudprovider.InstanceTypes{instanceType},
			Requirements:        scheduling.NewRequirements(),
		},
		daemonOverheadGroups: []DaemonOverheadGroup{{InstanceTypes: []*cloudprovider.InstanceType{instanceType}, DaemonOverhead: daemonOverhead}},
	}

	first := nodeClaim.ScoreBasedMemoryAllocatableOptions()
	if len(first) != 1 {
		t.Fatalf("expected one allocatable shape, got %d", len(first))
	}
	assertScoreBasedMemoryResources(t, first[0], "9250m", "17Gi")
	mutateScoreBasedMemoryResources(first[0])

	assertScoreBasedMemoryResources(t, instanceType.Capacity, "10", "20Gi")
	assertScoreBasedMemoryResources(t, instanceType.Overhead.KubeReserved, "500m", "1Gi")
	assertScoreBasedMemoryResources(t, daemonOverhead, "250m", "2Gi")
	second := nodeClaim.ScoreBasedMemoryAllocatableOptions()
	assertScoreBasedMemoryResources(t, second[0], "9250m", "17Gi")
}

func TestScoreBasedMemoryAllocatableReturnsNilForUnknownOverheadGroup(t *testing.T) {
	instanceType := scoreBasedMemoryInstanceType("same-name", "8", "16Gi", nil)
	otherInstanceType := scoreBasedMemoryInstanceType("same-name", "8", "16Gi", nil)
	nodeClaim := &NodeClaim{
		NodeClaimTemplate: NodeClaimTemplate{
			InstanceTypeOptions: cloudprovider.InstanceTypes{instanceType},
			Requirements:        scheduling.NewRequirements(),
		},
		daemonOverheadGroups: []DaemonOverheadGroup{{InstanceTypes: []*cloudprovider.InstanceType{otherInstanceType}, DaemonOverhead: scoreBasedMemoryResources("1", "1Gi")}},
	}

	if got := nodeClaim.ScoreBasedMemoryAllocatableOptions(); got != nil {
		t.Fatalf("expected nil for an instance type without a matching overhead group, got %v", got)
	}
}

func scoreBasedMemoryInstanceType(name, cpu, memory string, offerings []cloudprovider.Offering) *cloudprovider.InstanceType {
	instanceType := &cloudprovider.InstanceType{
		Name:     name,
		Capacity: scoreBasedMemoryResources(cpu, memory),
		Overhead: &cloudprovider.InstanceTypeOverhead{},
	}
	if len(offerings) == 0 {
		offerings = []cloudprovider.Offering{scoreBasedMemoryOffering("zone-a", true, nil)}
	}
	instanceType.Offerings = make(cloudprovider.Offerings, len(offerings))
	for i := range offerings {
		instanceType.Offerings[i] = &offerings[i]
	}
	return instanceType
}

func scoreBasedMemoryOffering(zone string, available bool, capacityOverride corev1.ResourceList) cloudprovider.Offering {
	return cloudprovider.Offering{
		Available: available,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			corev1.LabelTopologyZone: zone,
			v1.CapacityTypeLabelKey:  v1.CapacityTypeSpot,
		}),
		CapacityOverride: capacityOverride,
	}
}

func scoreBasedMemoryResources(cpu, memory string) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse(cpu),
		corev1.ResourceMemory: resource.MustParse(memory),
	}
}

func assertScoreBasedMemoryResources(t *testing.T, got corev1.ResourceList, cpu, memory string) {
	t.Helper()
	for name, expected := range scoreBasedMemoryResources(cpu, memory) {
		quantity, ok := got[name]
		if !ok || quantity.Cmp(expected) != 0 {
			t.Errorf("expected %s=%s, got %s", name, expected.String(), quantity.String())
		}
	}
}

func mutateScoreBasedMemoryResources(resources corev1.ResourceList) {
	for _, name := range []corev1.ResourceName{corev1.ResourceCPU, corev1.ResourceMemory} {
		quantity := resources[name]
		quantity.Set(0)
		resources[name] = quantity
	}
}
