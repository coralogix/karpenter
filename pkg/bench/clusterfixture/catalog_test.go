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
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	fakecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
)

func TestBuildCatalogFromInstanceTypes(t *testing.T) {
	its := fakecloudprovider.InstanceTypes(3)
	perPool := map[string][]*cloudprovider.InstanceType{
		"pool-a": its,
		"pool-b": its,
	}
	catalog := BuildCatalogFromInstanceTypes(perPool)
	if len(catalog.InstanceTypeSpecs) != 3 {
		t.Fatalf("instance type specs = %d, want 3", len(catalog.InstanceTypeSpecs))
	}
	if len(catalog.NodePoolInstanceTypes["pool-a"]) != 3 {
		t.Fatalf("pool-a assignments = %d, want 3", len(catalog.NodePoolInstanceTypes["pool-a"]))
	}

	cp := fakecloudprovider.NewCloudProvider()
	catalog.ApplyToCloudProvider(cp)
	poolITs, err := cp.GetInstanceTypes(context.Background(), &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool-a"}})
	if err != nil || len(poolITs) != 3 {
		t.Fatalf("GetInstanceTypes() = %d types, err=%v, want 3", len(poolITs), err)
	}
	if len(poolITs[0].Requirements) < 5 {
		t.Fatalf("requirements keys = %d, want at least 5", len(poolITs[0].Requirements))
	}
}
