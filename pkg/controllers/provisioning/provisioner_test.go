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

package provisioning

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	fakecr "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

func TestSchedulerFactoryDeepCopyNodesIsolated(t *testing.T) {
	baseline := state.NewNode()
	baseline.Node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-a",
			Labels: map[string]string{"snapshot": "original"},
		},
	}
	factory := &SchedulerFactory{stateNodes: state.StateNodes{baseline}}

	first := factory.DeepCopyNodes()
	first[0].Node.Labels["snapshot"] = "first-simulation"
	second := factory.DeepCopyNodes()

	if first[0] == second[0] {
		t.Fatal("expected simulations to receive distinct StateNodes")
	}
	if got := second[0].Node.Labels["snapshot"]; got != "original" {
		t.Fatalf("expected second simulation to receive an unmodified snapshot, got %q", got)
	}
}

func TestSchedulerFactoryDeepCopyNodesRetainsOriginalClusterSnapshot(t *testing.T) {
	ctx := context.Background()
	cluster := state.NewCluster(&clock.RealClock{}, fakecr.NewFakeClient(), fake.NewCloudProvider())
	initial := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-a",
			Labels: map[string]string{"snapshot": "original"},
		},
		Spec: corev1.NodeSpec{ProviderID: "provider-a"},
	}
	if err := cluster.UpdateNodeWithPods(ctx, initial, nil); err != nil {
		t.Fatalf("updating initial cluster node: %v", err)
	}
	factory := &SchedulerFactory{stateNodes: cluster.DeepCopyNodes()}

	updated := initial.DeepCopy()
	updated.Labels["snapshot"] = "updated-cluster"
	if err := cluster.UpdateNodeWithPods(ctx, updated, nil); err != nil {
		t.Fatalf("updating cluster node: %v", err)
	}

	snapshot := factory.DeepCopyNodes()
	if len(snapshot) != 1 {
		t.Fatalf("expected one node in factory snapshot, got %d", len(snapshot))
	}
	if got := snapshot[0].Node.Labels["snapshot"]; got != "original" {
		t.Fatalf("expected factory to retain original cluster snapshot, got %q", got)
	}
}
