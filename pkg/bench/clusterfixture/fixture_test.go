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
	"path/filepath"
	"runtime"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

func miniFixtureDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve test file path")
	}
	return filepath.Join(filepath.Dir(file), "testdata", "mini")
}

func TestLoadMiniFixture(t *testing.T) {
	fixture, err := Load(miniFixtureDir(t))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(fixture.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(fixture.Nodes))
	}
	if len(fixture.Pods) != 2 {
		t.Fatalf("pods = %d, want 2", len(fixture.Pods))
	}
	if fixture.Catalog == nil || len(fixture.Catalog.InstanceTypes) == 0 {
		t.Fatal("expected synthesized instance type catalog")
	}
}

func TestBuildEnvAndReset(t *testing.T) {
	ctx := options.ToContext(context.Background(), test.Options())
	fixture, err := Load(miniFixtureDir(t))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	env, err := fixture.BuildEnv(ctx, Options{AssumeScoreBasedAllPools: true})
	if err != nil {
		t.Fatalf("BuildEnv() error = %v", err)
	}
	if env.NodeCount != 2 {
		t.Fatalf("NodeCount = %d, want 2", env.NodeCount)
	}
	if env.StateNodeForProviderID("aws:///us-west-2a/i-nodea") == nil {
		t.Fatal("expected state node for node-a provider ID")
	}

	env.Cluster.NominateNodeForPod(ctx, "aws:///us-west-2a/i-nodea")
	if err := env.Reset(ctx); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}
	if env.Cluster.IsNodeNominated("aws:///us-west-2a/i-nodea") {
		t.Fatal("expected nominations to be cleared after Reset()")
	}
}

func TestBuildCatalog(t *testing.T) {
	fixture, err := Load(miniFixtureDir(t))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	catalog := BuildCatalog(fixture)
	if len(catalog.InstanceTypes) < 2 {
		t.Fatalf("instance types = %d, want at least 2", len(catalog.InstanceTypes))
	}
	if len(catalog.NodePoolInstanceTypes["bench-pool"]) == 0 {
		t.Fatal("expected node pool instance type mapping")
	}
}

func TestBuildEnvSkipsTerminatingPods(t *testing.T) {
	ctx := options.ToContext(context.Background(), test.Options())
	fixture, err := Load(miniFixtureDir(t))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	now := metav1.Now()
	fixture.Pods[0].DeletionTimestamp = &now

	env, err := fixture.BuildEnv(ctx, Options{AssumeScoreBasedAllPools: true})
	if err != nil {
		t.Fatalf("BuildEnv() error = %v", err)
	}
	if env.PodCount != 1 {
		t.Fatalf("PodCount = %d, want 1 after filtering terminating pod", env.PodCount)
	}
}
