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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

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

func TestReadCacheReusesListResults(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-a", Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}
	kubeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(pod, node).
		WithInterceptorFuncs(newReadCache(scheme.Scheme).interceptorFuncs()).
		Build()

	first := &corev1.PodList{}
	second := &corev1.PodList{}
	if err := kubeClient.List(ctx, first); err != nil {
		t.Fatalf("first List() error = %v", err)
	}
	if err := kubeClient.List(ctx, second); err != nil {
		t.Fatalf("second List() error = %v", err)
	}
	if len(first.Items) != 1 || len(second.Items) != 1 {
		t.Fatalf("items = %d and %d, want 1 each", len(first.Items), len(second.Items))
	}

	gotNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: "node-a"}, gotNode); err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: "node-a"}, &corev1.Node{}); err != nil {
		t.Fatalf("second Get() error = %v", err)
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
