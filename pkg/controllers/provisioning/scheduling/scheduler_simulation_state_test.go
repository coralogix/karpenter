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
	"fmt"
	"reflect"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

func TestPreparedSchedulerStateMaterializesIndependentAllocationState(t *testing.T) {
	ctx := schedulerBaselineTestContext()
	inputs := NewNodePoolInputs(ctx, events.NewRecorder(record.NewFakeRecorder(10)), []*v1.NodePool{test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool"},
		Spec:       v1.NodePoolSpec{Limits: v1.Limits{corev1.ResourceCPU: resource.MustParse("10")}},
	})}, map[string][]*cloudprovider.InstanceType{})
	baseline := NewSchedulerBaseline(ctx, inputs, nil)
	node := simulationStateTestNode()
	originalHostPortUsage := node.HostPortUsage().DeepCopy()
	originalVolumeUsage := node.VolumeUsage().DeepCopy()
	prepared, err := NewPreparedSchedulerState(ctx, baseline, []*state.StateNode{node})
	if err != nil {
		t.Fatalf("preparing scheduler state: %v", err)
	}

	first, err := prepared.NewScheduler(ctx, nil, nil, emptyTopology(), nil, clock.RealClock{}, NewCapturedVolumeSource(nil), sets.New[string]())
	if err != nil {
		t.Fatalf("creating first scheduler: %v", err)
	}
	second, err := prepared.NewScheduler(ctx, nil, nil, emptyTopology(), nil, clock.RealClock{}, NewCapturedVolumeSource(nil), sets.New[string]())
	if err != nil {
		t.Fatalf("creating second scheduler: %v", err)
	}
	if len(first.existingNodes) != 1 || len(second.existingNodes) != 1 {
		t.Fatalf("expected one existing node per scheduler, got %d and %d", len(first.existingNodes), len(second.existingNodes))
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default", UID: types.UID("pod")}}
	firstNode := first.existingNodes[0]
	secondNode := second.existingNodes[0]
	port := corev1.ContainerPort{ContainerPort: 8080, HostPort: 8080, Protocol: corev1.ProtocolTCP}
	pod.Spec.Containers = []corev1.Container{{Name: "container", Ports: []corev1.ContainerPort{port}}}
	firstNode.HostPortUsage().Add(pod, scheduling.GetHostPorts(pod))
	assertIndependentHostPortUsage(t, secondNode, pod)

	volumes := scheduling.Volumes{"driver": sets.New[string]("pvc")}
	additionalVolumes := scheduling.Volumes{"driver": sets.New[string]("pvc-2")}
	firstNode.VolumeUsage().AddLimit("driver", 1)
	secondNode.VolumeUsage().AddLimit("driver", 1)
	firstNode.VolumeUsage().Add(pod, volumes)
	assertIndependentVolumeUsage(t, firstNode, secondNode, additionalVolumes)

	resources.SubtractFrom(firstNode.remainingResources, corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")})
	if got := secondNode.remainingResources[corev1.ResourceCPU]; got.Cmp(resource.MustParse("4")) != 0 {
		t.Fatalf("remaining resources leaked between schedulers, got %s", got.String())
	}
	assertCapturedManagersUnchanged(t, node, pod, volumes, originalHostPortUsage, originalVolumeUsage)
}

func assertIndependentHostPortUsage(t *testing.T, node *ExistingNode, pod *corev1.Pod) {
	t.Helper()
	if err := node.HostPortUsage().Conflicts(pod, scheduling.GetHostPorts(pod)); err != nil {
		t.Fatalf("host-port allocation leaked between schedulers: %v", err)
	}
}

func assertIndependentVolumeUsage(t *testing.T, first, second *ExistingNode, additionalVolumes scheduling.Volumes) {
	t.Helper()
	if err := second.VolumeUsage().ExceedsLimits(additionalVolumes); err != nil {
		t.Fatalf("volume allocation leaked between schedulers: %v", err)
	}
	if err := first.VolumeUsage().ExceedsLimits(additionalVolumes); err == nil {
		t.Fatal("expected first scheduler's volume allocation to reach its limit")
	}
}

func assertCapturedManagersUnchanged(t *testing.T, node *state.StateNode, pod *corev1.Pod, volumes scheduling.Volumes, originalHostPortUsage *scheduling.HostPortUsage, originalVolumeUsage *scheduling.VolumeUsage) {
	t.Helper()
	if err := node.HostPortUsage().Conflicts(pod, scheduling.GetHostPorts(pod)); err != nil {
		t.Fatalf("host-port allocation leaked into captured StateNode: %v", err)
	}
	if got := node.VolumeUsage().ExceedsLimits(volumes); got != nil {
		t.Fatalf("volume allocation leaked into captured StateNode: %v", got)
	}
	if !reflect.DeepEqual(node.HostPortUsage(), originalHostPortUsage) || !reflect.DeepEqual(node.VolumeUsage(), originalVolumeUsage) {
		t.Fatal("captured allocation managers changed during scheduler construction")
	}
}

func TestPreparedSchedulerStateRestoresFiniteNodePoolLimits(t *testing.T) {
	ctx := schedulerBaselineTestContext()
	inputs := NewNodePoolInputs(ctx, events.NewRecorder(record.NewFakeRecorder(10)), []*v1.NodePool{test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool"},
		Spec:       v1.NodePoolSpec{Limits: v1.Limits{corev1.ResourceCPU: resource.MustParse("10")}},
	})}, map[string][]*cloudprovider.InstanceType{})
	baseline := NewSchedulerBaseline(ctx, inputs, nil)
	prepared, err := NewPreparedSchedulerState(ctx, baseline, []*state.StateNode{simulationStateTestNode()})
	if err != nil {
		t.Fatalf("preparing scheduler state: %v", err)
	}
	removed := sets.New[string]("node")
	scheduler, err := prepared.NewScheduler(ctx, nil, nil, emptyTopology(), nil, clock.RealClock{}, NewCapturedVolumeSource(nil), removed)
	if err != nil {
		t.Fatalf("creating scheduler: %v", err)
	}
	if len(scheduler.existingNodes) != 0 {
		t.Fatalf("expected removed node to be filtered, got %d existing nodes", len(scheduler.existingNodes))
	}
	if got := scheduler.remainingResources["pool"][corev1.ResourceCPU]; got.Cmp(resource.MustParse("10")) != 0 {
		t.Fatalf("expected removed node capacity to be restored, got %s", got.String())
	}
}

func TestPreparedSchedulerStateExcludesDeletingNodes(t *testing.T) {
	ctx := schedulerBaselineTestContext()
	inputs := NewNodePoolInputs(ctx, events.NewRecorder(record.NewFakeRecorder(10)), []*v1.NodePool{test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool"},
		Spec:       v1.NodePoolSpec{Limits: v1.Limits{corev1.ResourceCPU: resource.MustParse("10")}},
	})}, map[string][]*cloudprovider.InstanceType{})
	node := simulationStateTestNode()
	deletionTimestamp := metav1.Now()
	node.Node.DeletionTimestamp = &deletionTimestamp
	prepared, err := NewPreparedSchedulerState(ctx, NewSchedulerBaseline(ctx, inputs, nil), []*state.StateNode{node})
	if err != nil {
		t.Fatalf("preparing scheduler state: %v", err)
	}
	scheduler, err := prepared.NewScheduler(ctx, nil, nil, emptyTopology(), nil, clock.RealClock{}, NewCapturedVolumeSource(nil), sets.New[string]())
	if err != nil {
		t.Fatalf("creating scheduler: %v", err)
	}
	if len(scheduler.existingNodes) != 0 {
		t.Fatalf("expected deleting node to be absent from solver, got %d nodes", len(scheduler.existingNodes))
	}
	if got := scheduler.remainingResources["pool"][corev1.ResourceCPU]; got.Cmp(resource.MustParse("10")) != 0 {
		t.Fatalf("expected deleting node capacity to stay excluded, got %s", got.String())
	}
}

func TestPreparedSchedulerStatePreservesSortedNodeOrderWhenFiltering(t *testing.T) {
	ctx := schedulerBaselineTestContext()
	inputs := NewNodePoolInputs(ctx, events.NewRecorder(record.NewFakeRecorder(10)), []*v1.NodePool{test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool"},
		Spec:       v1.NodePoolSpec{Limits: v1.Limits{corev1.ResourceCPU: resource.MustParse("10")}},
	})}, map[string][]*cloudprovider.InstanceType{})
	prepared, err := NewPreparedSchedulerState(ctx, NewSchedulerBaseline(ctx, inputs, nil), []*state.StateNode{
		simulationStateTestNodeWithName("node-c", true),
		simulationStateTestNodeWithName("node-b", false),
		simulationStateTestNodeWithName("node-a", true),
	})
	if err != nil {
		t.Fatalf("preparing scheduler state: %v", err)
	}

	scheduler, err := prepared.NewScheduler(ctx, nil, nil, emptyTopology(), nil, clock.RealClock{}, NewCapturedVolumeSource(nil), sets.New[string]())
	if err != nil {
		t.Fatalf("creating scheduler: %v", err)
	}
	assertExistingNodeOrder(t, scheduler, []string{"node-a", "node-c", "node-b"})

	scheduler, err = prepared.NewScheduler(ctx, nil, nil, emptyTopology(), nil, clock.RealClock{}, NewCapturedVolumeSource(nil), sets.New("node-c"))
	if err != nil {
		t.Fatalf("creating filtered scheduler: %v", err)
	}
	assertExistingNodeOrder(t, scheduler, []string{"node-a", "node-b"})
}

func assertExistingNodeOrder(t *testing.T, scheduler *Scheduler, want []string) {
	t.Helper()
	got := make([]string, len(scheduler.existingNodes))
	for i, node := range scheduler.existingNodes {
		got[i] = node.Name()
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("existing node order = %v, want %v", got, want)
	}
}

func TestPreparedSchedulerStateMaterializesConcurrently(t *testing.T) {
	ctx := schedulerBaselineTestContext()
	inputs := NewNodePoolInputs(ctx, events.NewRecorder(record.NewFakeRecorder(10)), []*v1.NodePool{test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool"},
		Spec:       v1.NodePoolSpec{Limits: v1.Limits{corev1.ResourceCPU: resource.MustParse("10")}},
	})}, map[string][]*cloudprovider.InstanceType{})
	prepared, err := NewPreparedSchedulerState(ctx, NewSchedulerBaseline(ctx, inputs, nil), []*state.StateNode{simulationStateTestNode()})
	if err != nil {
		t.Fatalf("preparing scheduler state: %v", err)
	}

	const attempts = 8
	errs := make(chan error, attempts)
	var waitGroup sync.WaitGroup
	waitGroup.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer waitGroup.Done()
			scheduler, err := prepared.NewScheduler(ctx, nil, nil, emptyTopology(), nil, clock.RealClock{}, NewCapturedVolumeSource(nil), sets.New[string]())
			if err != nil {
				errs <- err
				return
			}
			node := scheduler.existingNodes[0]
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "default", UID: types.UID("pod")}}
			port := corev1.ContainerPort{ContainerPort: 8080, HostPort: 8080, Protocol: corev1.ProtocolTCP}
			pod.Spec.Containers = []corev1.Container{{Name: "container", Ports: []corev1.ContainerPort{port}}}
			node.HostPortUsage().Add(pod, scheduling.GetHostPorts(pod))
			node.VolumeUsage().AddLimit("driver", 1)
			node.VolumeUsage().Add(pod, scheduling.Volumes{"driver": sets.New[string]("pvc")})
			resources.SubtractFrom(node.remainingResources, corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")})
			if got := node.remainingResources[corev1.ResourceCPU]; got.Cmp(resource.MustParse("3")) != 0 {
				errs <- fmt.Errorf("remaining resources changed across attempts, got %s", got.String())
			}
		}()
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func simulationStateTestNode() *state.StateNode {
	return simulationStateTestNodeWithName("node", true)
}

func simulationStateTestNodeWithName(name string, initialized bool) *state.StateNode {
	nodePool := "pool"
	cpu := int64(4)
	node := state.NewNode()
	node.Node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
			v1.NodePoolLabelKey:  nodePool,
			corev1.LabelHostname: name,
		}},
		Status: corev1.NodeStatus{
			Capacity:    corev1.ResourceList{corev1.ResourceCPU: *resource.NewQuantity(cpu, resource.DecimalSI)},
			Allocatable: corev1.ResourceList{corev1.ResourceCPU: *resource.NewQuantity(cpu, resource.DecimalSI)},
		},
	}
	if !initialized {
		node.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	return node
}

func emptyTopology() *Topology {
	return &Topology{
		preferencePolicy:      PreferencePolicyRespect,
		topologyGroups:        map[uint64]*TopologyGroup{},
		inverseTopologyGroups: map[uint64]*TopologyGroup{},
		excludedPods:          sets.New[string](),
	}
}
