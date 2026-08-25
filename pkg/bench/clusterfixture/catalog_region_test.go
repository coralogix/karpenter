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
	"testing"
)

func TestRegionFromFixture(t *testing.T) {
	fixture := &Fixture{
		Metadata: Metadata{
			Region:  "eu-west-1",
			Context: "gateway.coralogix.net-us2-cx498-aws-us-west-2",
		},
	}
	if got := RegionFromFixture(fixture); got != "eu-west-1" {
		t.Fatalf("RegionFromFixture() = %q, want eu-west-1", got)
	}

	fixture.Metadata.Region = ""
	if got := RegionFromFixture(fixture); got != "us-west-2" {
		t.Fatalf("RegionFromFixture() from context = %q, want us-west-2", got)
	}
}

func TestHasFullInstanceCatalog(t *testing.T) {
	if (&Catalog{}).HasFullInstanceCatalog() {
		t.Fatal("empty catalog should not be full")
	}
	if !(&Catalog{InstanceTypeSpecs: []CatalogInstanceTypeSpec{{Name: "m5.large"}}}).HasFullInstanceCatalog() {
		t.Fatal("expected catalog with specs to be full")
	}
}
