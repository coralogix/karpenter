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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	karpscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

func TestCloneDaemonHostPortUsageIsolatesMutations(t *testing.T) {
	template := &NodeClaimTemplate{}
	baseline := map[*NodeClaimTemplate]*karpscheduling.HostPortUsage{
		template: karpscheduling.NewHostPortUsage(),
	}
	cloned := cloneDaemonHostPortUsage(baseline)

	pod := test.Pod(test.PodOptions{HostPorts: []int32{8080}})
	hostPorts := karpscheduling.GetHostPorts(pod)
	cloned[template].Add(pod, hostPorts)

	pod2 := test.Pod(test.PodOptions{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "other"},
		HostPorts:  []int32{8080},
	})
	if cloned[template].Conflicts(pod2, karpscheduling.GetHostPorts(pod2)) == nil {
		t.Fatal("expected clone to track reserved host ports")
	}
	if baseline[template].Conflicts(pod2, karpscheduling.GetHostPorts(pod2)) != nil {
		t.Fatal("expected baseline host port usage to be unchanged after mutating clone")
	}
}

func TestNewSchedulerCreatesDistinctReservationManagers(t *testing.T) {
	ctx := context.Background()
	nodePool := test.NodePool()
	instanceTypes := []*cloudprovider.InstanceType{
		fake.NewInstanceType(fake.InstanceTypeOptions{
			Name: "default",
			Resources: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("4"),
				corev1.ResourceMemory: resource.MustParse("8Gi"),
				corev1.ResourcePods:   resource.MustParse("110"),
			},
		}),
	}
	inputs := NewNodePoolInputs(ctx, events.NewRecorder(&record.FakeRecorder{}), []*v1.NodePool{nodePool}, map[string][]*cloudprovider.InstanceType{
		nodePool.Name: instanceTypes,
	})
	precompute := NewSchedulerPrecompute(ctx, inputs, nil)

	s1 := newTestScheduler(t, ctx, inputs, precompute)
	s2 := newTestScheduler(t, ctx, inputs, precompute)
	if s1.reservationManager == s2.reservationManager {
		t.Fatal("expected distinct reservation managers per scheduler")
	}
}

func newTestScheduler(t *testing.T, ctx context.Context, inputs *NodePoolInputs, precompute *SchedulerPrecompute) *Scheduler {
	t.Helper()
	cloudProvider := fake.NewCloudProvider()
	cloudProvider.InstanceTypes = inputs.instanceTypes[inputs.nodePools[0].Name]
	client := fakecr.NewFakeClient()
	cluster := state.NewCluster(&clock.RealClock{}, client, cloudProvider)
	topology, err := NewTopology(ctx, client, cluster, nil, inputs, nil)
	if err != nil {
		t.Fatalf("creating topology: %v", err)
	}
	return NewScheduler(ctx, client, inputs, cluster, nil, topology, nil, precompute,
		events.NewRecorder(&record.FakeRecorder{}), &clock.RealClock{}, nil)
}
