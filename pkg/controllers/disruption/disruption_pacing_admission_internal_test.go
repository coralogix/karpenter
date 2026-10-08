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

package disruption

import (
	"context"
	"testing"
	"time"

	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	cloudproviderfake "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
)

func TestAdmissionRejectsEmptyCandidateMissingFromFinalManagedSnapshot(t *testing.T) {
	ctx := context.Background()
	nodePool := paceTestNodePool("1", "")
	nodePool.Spec.Template.Spec.NodeClassRef = &v1.NodeClassReference{
		Group: v1alpha1.Group,
		Kind:  "TestNodeClass",
		Name:  "test",
	}
	planningClient := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithObjects(nodePool).Build()
	finalReader := fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).Build()
	cloudProvider := cloudproviderfake.NewCloudProvider()

	plannedPools, err := listManagedNodePools(ctx, planningClient, cloudProvider)
	if err != nil || len(plannedPools) != 1 {
		t.Fatalf("planned managed NodePools = %d, error = %v; want one pool", len(plannedPools), err)
	}

	controller := &Controller{
		kubeClient:       planningClient,
		nodePoolReader:   finalReader,
		cloudProvider:    cloudProvider,
		disruptionPacing: NewDisruptionPacing(clocktesting.NewFakeClock(time.Now())),
	}
	accepted, err := controller.admitCommands(ctx, &Drift{}, []Command{*paceCommand(nodePool)}, true)
	if err != nil {
		t.Fatalf("admitCommands() error = %v, want nil", err)
	}
	if len(accepted) != 0 {
		t.Fatalf("admitCommands() accepted %v, want empty candidate rejected because its pool is absent from final snapshot", accepted)
	}
}
