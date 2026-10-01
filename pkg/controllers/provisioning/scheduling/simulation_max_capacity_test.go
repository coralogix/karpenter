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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestParseSimulationMaxCapacity(t *testing.T) {
	parsed, err := parseSimulationMaxCapacity(`{"cpu":"64","memory":"256Gi"}`)
	if err != nil {
		t.Fatalf("parsing valid cap: %v", err)
	}
	cpuCap := parsed[corev1.ResourceCPU]
	memoryCap := parsed[corev1.ResourceMemory]
	if cpuCap.Cmp(resource.MustParse("64")) != 0 || memoryCap.Cmp(resource.MustParse("256Gi")) != 0 {
		t.Fatalf("unexpected parsed cap: %v", parsed)
	}

	for _, value := range []string{"", "null", "{}", `{"cpu":"0"}`, `{"memory":"-1Gi"}`, `{"cpu":"not-a-quantity"}`, `{"nvidia.com/gpu":"1"}`} {
		if _, err := parseSimulationMaxCapacity(value); err == nil {
			t.Errorf("expected %q to be invalid", value)
		}
	}
}

func TestSimulationMaxCapacitySplitsNewNodeFitWithoutPruningLaunchTypes(t *testing.T) {
	instanceTypes := []*cloudprovider.InstanceType{
		simulationCapacityInstanceType("cpu48", "48"),
		simulationCapacityInstanceType("cpu64", "64"),
		simulationCapacityInstanceType("cpu96", "96"),
	}
	cap := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("64")}
	requirements := scheduling.NewRequirements()

	firstPod := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("47")}
	remaining, _, err := filterInstanceTypesByRequirements(instanceTypes, requirements, &corev1.Pod{}, firstPod, simulationCapacityDaemonOverheadGroups(instanceTypes), firstPod, false, cap)
	if err != nil {
		t.Fatalf("filtering first pod with cap: %v", err)
	}
	if len(remaining) != 3 {
		t.Fatalf("expected every launch size to remain eligible for one pod, got %v", InstanceTypeList(remaining))
	}

	pair := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("94")}
	if remaining, _, err = filterInstanceTypesByRequirements(instanceTypes, requirements, &corev1.Pod{}, pair, simulationCapacityDaemonOverheadGroups(instanceTypes), pair, false, cap); err == nil || len(remaining) != 0 {
		t.Fatalf("expected a CPU 64 cap to reject two pods together, got %v, err=%v", InstanceTypeList(remaining), err)
	}

	// The same cap still permits a pod on the largest type when smaller types
	// are unavailable, while retaining that real type as a launch option.
	remaining, _, err = filterInstanceTypesByRequirements(instanceTypes[2:], requirements, &corev1.Pod{}, firstPod, simulationCapacityDaemonOverheadGroups(instanceTypes[2:]), firstPod, false, cap)
	if err != nil || len(remaining) != 1 || remaining[0].Name != "cpu96" {
		t.Fatalf("expected capped cpu96 to remain launchable for one pod, got %v, err=%v", InstanceTypeList(remaining), err)
	}
	if remaining, _, err = filterInstanceTypesByRequirements(instanceTypes[2:], requirements, &corev1.Pod{}, pair, simulationCapacityDaemonOverheadGroups(instanceTypes[2:]), pair, false, cap); err == nil || len(remaining) != 0 {
		t.Fatalf("expected two pods to split even when cpu96 is the only available type, got %v, err=%v", InstanceTypeList(remaining), err)
	}
}

func TestSimulationMaxCapacityPreservesEffectiveOverheadAndDoesNotMutateAllocatableCache(t *testing.T) {
	instanceType := &cloudprovider.InstanceType{
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU:                   resource.MustParse("96"),
			corev1.ResourceMemory:                resource.MustParse("512Gi"),
			corev1.ResourceName("hugepages-2Mi"): resource.MustParse("64Gi"),
		},
		Overhead: &cloudprovider.InstanceTypeOverhead{
			KubeReserved:      corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("3"), corev1.ResourceMemory: resource.MustParse("24Gi")},
			SystemReserved:    corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1"), corev1.ResourceMemory: resource.MustParse("8Gi")},
			EvictionThreshold: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("0Gi")},
		},
		Offerings: cloudprovider.Offerings{{
			Available: true,
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, "test-zone"),
			),
			CapacityOverride: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("8"),
				corev1.ResourceMemory: resource.MustParse("32Gi"),
			},
			OverheadOverride: &cloudprovider.InstanceTypeOverhead{
				SystemReserved: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("2"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
		}},
	}
	groups := instanceType.AllocatableOfferingsList()
	var overrideGroup *cloudprovider.AllocatableOfferings
	for i := range groups {
		if len(groups[i].Offerings) > 0 && len(groups[i].Offerings[0].CapacityOverride) > 0 {
			overrideGroup = &groups[i]
			break
		}
	}
	if overrideGroup == nil {
		t.Fatal("expected an allocatable group for the capacity override offering")
	}
	original := overrideGroup.Allocatable.DeepCopy()
	capped := capAllocatable(instanceType, *overrideGroup, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("64"),
		corev1.ResourceMemory: resource.MustParse("256Gi"),
	})
	if got := capped.Cpu(); got.Cmp(resource.MustParse("58")) != 0 {
		t.Errorf("expected CPU ceiling minus six cores of overhead to be 58, got %s", got)
	}
	if got := capped.Memory(); got.Cmp(resource.MustParse("152Gi")) != 0 {
		t.Errorf("expected memory ceiling minus overhead and huge pages to be 152Gi, got %s", got)
	}
	if got := overrideGroup.Allocatable; got.Cpu().Cmp(*original.Cpu()) != 0 || got.Memory().Cmp(*original.Memory()) != 0 {
		t.Fatal("applying the cap mutated the shared allocatable group")
	}
	if groupsAgain := instanceType.AllocatableOfferingsList(); groupsAgain[1].Allocatable.Cpu().Cmp(*original.Cpu()) != 0 {
		t.Fatal("applying the cap mutated the cached allocatable group")
	}
}

func TestSimulationMaxCapacityAppliesMemoryCeiling(t *testing.T) {
	instanceType := &cloudprovider.InstanceType{
		Name:         "memory512",
		Requirements: scheduling.NewRequirements(),
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("96"),
			corev1.ResourceMemory: resource.MustParse("512Gi"),
		},
		Overhead:  &cloudprovider.InstanceTypeOverhead{},
		Offerings: cloudprovider.Offerings{{Available: true, Requirements: scheduling.NewRequirements()}},
	}
	cap := corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("256Gi")}
	requests := corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("300Gi")}
	instanceTypes := []*cloudprovider.InstanceType{instanceType}
	remaining, _, err := filterInstanceTypesByRequirements(instanceTypes, scheduling.NewRequirements(), &corev1.Pod{}, requests, simulationCapacityDaemonOverheadGroups(instanceTypes), requests, false, cap)
	if err == nil || len(remaining) != 0 {
		t.Fatalf("expected memory request above the cap to be rejected, got %v, err=%v", InstanceTypeList(remaining), err)
	}
}

func TestNodePoolInputsRejectsInvalidSimulationMaxCapacityAndStaticTemplateIgnoresIt(t *testing.T) {
	nodePool := simulationCapacityNodePool(map[string]string{SimulationMaxCapacityAnnotationKey: `{"cpu":"0"}`}, false)
	fakeRecorder := record.NewFakeRecorder(1)
	inputs := NewNodePoolInputs(context.Background(), events.NewRecorder(fakeRecorder), []*v1.NodePool{nodePool}, map[string][]*cloudprovider.InstanceType{nodePool.Name: nil})
	if len(inputs.nodeClaimTemplates) != 0 {
		t.Fatal("expected invalid annotation to block new NodePool templates")
	}
	select {
	case event := <-fakeRecorder.Events:
		if !strings.Contains(event, "InvalidSimulationMaxCapacity") || !strings.Contains(event, SimulationMaxCapacityAnnotationKey) {
			t.Fatalf("expected a clear invalid-annotation warning event, got %q", event)
		}
	default:
		t.Fatal("expected an invalid-annotation warning event")
	}

	staticPool := simulationCapacityNodePool(map[string]string{SimulationMaxCapacityAnnotationKey: "malformed"}, true)
	if template := NewNodeClaimTemplate(staticPool); template.SimulationMaxCapacityErr != nil || len(template.SimulationMaxCapacity) != 0 {
		t.Fatalf("static NodePool should ignore the annotation, got cap=%v err=%v", template.SimulationMaxCapacity, template.SimulationMaxCapacityErr)
	}
}

func TestNodePoolInputsDeepCopySimulationMaxCapacity(t *testing.T) {
	inputs := &NodePoolInputs{nodeClaimTemplates: []*NodeClaimTemplate{{
		SimulationMaxCapacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("64.1")},
	}}}
	snapshot := inputs.DeepCopy()
	quantity := snapshot.nodeClaimTemplates[0].SimulationMaxCapacity[corev1.ResourceCPU]
	quantity.Add(resource.MustParse("0.1"))
	snapshot.nodeClaimTemplates[0].SimulationMaxCapacity[corev1.ResourceCPU] = quantity
	if got := inputs.nodeClaimTemplates[0].SimulationMaxCapacity[corev1.ResourceCPU]; got.Cmp(resource.MustParse("64.1")) != 0 {
		t.Fatalf("mutating a copied simulation ceiling changed shared inputs: %v", got)
	}
}

func simulationCapacityInstanceType(name, cpu string) *cloudprovider.InstanceType {
	return &cloudprovider.InstanceType{
		Name:         name,
		Requirements: scheduling.NewRequirements(),
		Capacity:     corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu)},
		Overhead:     &cloudprovider.InstanceTypeOverhead{},
		Offerings: cloudprovider.Offerings{{
			Available:    true,
			Requirements: scheduling.NewRequirements(),
		}},
	}
}

func simulationCapacityDaemonOverheadGroups(instanceTypes []*cloudprovider.InstanceType) []DaemonOverheadGroup {
	return []DaemonOverheadGroup{{InstanceTypes: instanceTypes, HostPortUsage: scheduling.NewHostPortUsage()}}
}

func simulationCapacityNodePool(annotations map[string]string, isStatic bool) *v1.NodePool {
	pool := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", UID: "pool-uid", Annotations: annotations},
		Spec: v1.NodePoolSpec{Template: v1.NodeClaimTemplate{Spec: v1.NodeClaimTemplateSpec{
			NodeClassRef: &v1.NodeClassReference{Group: "example.com", Kind: "TestNodeClass", Name: "test"},
		}}},
	}
	if isStatic {
		pool.Spec.Replicas = new(int64)
	}
	return pool
}
