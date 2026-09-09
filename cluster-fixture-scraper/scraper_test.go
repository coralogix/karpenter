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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/karpenter/pkg/bench/clusterfixture"
	"sigs.k8s.io/yaml"
)

func TestResolveRegionPrecedence(t *testing.T) {
	t.Setenv("AWS_REGION", "env-region")
	if got := resolveRegion("flag-region", "node-region"); got != "flag-region" {
		t.Fatalf("resolveRegion() = %q, want flag-region", got)
	}
	if got := resolveRegion("", "node-region"); got != "env-region" {
		t.Fatalf("resolveRegion() = %q, want env-region", got)
	}
	t.Setenv("AWS_REGION", "")
	if got := resolveRegion("", "node-region"); got != "node-region" {
		t.Fatalf("resolveRegion() = %q, want node-region", got)
	}
}

func TestFirstNodeRegion(t *testing.T) {
	if got := firstNodeRegion(map[string]any{"items": []any{
		map[string]any{"metadata": map[string]any{"labels": map[string]any{}}},
		map[string]any{"metadata": map[string]any{"labels": map[string]any{"topology.kubernetes.io/region": "eu-west-1"}}},
	}}); got != "eu-west-1" {
		t.Fatalf("firstNodeRegion() = %q, want eu-west-1", got)
	}
	if got := firstNodeRegion(map[string]any{"items": []any{"not-an-object"}}); got != "" {
		t.Fatalf("firstNodeRegion() = %q, want empty result", got)
	}
}

func TestResolveOutputPathDefault(t *testing.T) {
	root, err := repositoryRoot()
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolveOutputPath("", "example-cluster")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "testdata", "clusterfixtures", "example-cluster")
	if got != want {
		t.Fatalf("resolveOutputPath() = %q, want %q", got, want)
	}
}

func TestResolveOutputPathMakesAbsolute(t *testing.T) {
	got, err := resolveOutputPath(filepath.Join("relative", "fixture"), "ignored")
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(got) || !strings.HasSuffix(got, filepath.Join("relative", "fixture")) {
		t.Fatalf("resolveOutputPath() = %q, want absolute relative/fixture path", got)
	}
}

func TestCaptureUsesDynamicClientAndFindsRegion(t *testing.T) {
	objects := make([]runtime.Object, 0, len(resourceSpecs))
	for _, spec := range resourceSpecs {
		object := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": spec.gvr.GroupVersion().String(),
			"kind":       strings.TrimSuffix(spec.kind, "List"),
			"metadata": map[string]any{
				"name":      spec.file,
				"namespace": "default",
			},
		}}
		if spec.file == "nodes.yaml" {
			object.Object["metadata"].(map[string]any)["labels"] = map[string]any{
				"topology.kubernetes.io/region": "us-west-2",
			}
		}
		objects = append(objects, object)
	}
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objects...)
	captured, err := capture(context.Background(), client)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(captured.lists), len(resourceSpecs); got != want {
		t.Fatalf("capture() returned %d lists, want %d", got, want)
	}
	if captured.region != "us-west-2" {
		t.Fatalf("capture() region = %q, want us-west-2", captured.region)
	}
	if got, want := captured.resourceCount(), len(resourceSpecs); got != want {
		t.Fatalf("capture() resource count = %d, want %d", got, want)
	}
}

func TestWriteCaptureSerializesResourcesAndMetadata(t *testing.T) {
	dir := t.TempDir()
	captured := captureResult{lists: make(map[string]map[string]any)}
	for _, spec := range resourceSpecs {
		captured.lists[spec.file] = map[string]any{
			"apiVersion": "v1",
			"kind":       "List",
			"items": []any{
				map[string]any{"metadata": map[string]any{"name": spec.file}},
			},
		}
	}
	if err := writeCapture(dir, "cluster-a", "context-a", "eu-central-1", captured); err != nil {
		t.Fatal(err)
	}

	var metadata map[string]any
	data, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{
		"cluster":        "cluster-a",
		"context":        "context-a",
		"region":         "eu-central-1",
		"nodeCount":      float64(1),
		"namespaceCount": float64(1),
		"podCount":       float64(1),
		"pdbCount":       float64(1),
		"nodePoolCount":  float64(1),
	} {
		if metadata[key] != want {
			t.Errorf("metadata[%q] = %#v, want %#v", key, metadata[key], want)
		}
	}
	for _, spec := range resourceSpecs {
		data, err := os.ReadFile(filepath.Join(dir, spec.file))
		if err != nil {
			t.Errorf("reading %s: %v", spec.file, err)
			continue
		}
		var list map[string]any
		if err := yaml.Unmarshal(data, &list); err != nil {
			t.Errorf("decoding %s: %v", spec.file, err)
		}
		if got := list["kind"]; got != "List" {
			t.Errorf("%s kind = %#v, want List", spec.file, got)
		}
		if got := list["apiVersion"]; got != "v1" {
			t.Errorf("%s apiVersion = %#v, want v1", spec.file, got)
		}
	}
}

func TestWriteCaptureRoundTripsThroughFixtureLoader(t *testing.T) {
	dir := t.TempDir()
	captured := captureResult{lists: make(map[string]map[string]any)}
	for _, spec := range resourceSpecs {
		captured.lists[spec.file] = map[string]any{
			"apiVersion": "v1",
			"kind":       "List",
			"items":      []any{},
		}
	}
	captured.lists["nodes.yaml"]["items"] = []any{map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]any{
			"name": "node-a",
			"labels": map[string]any{
				"topology.kubernetes.io/region": "us-west-2",
			},
		},
	}}
	captured.lists["pods.yaml"]["items"] = []any{map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":      "pod-a",
			"namespace": "default",
		},
	}}
	captured.lists["nodepools.yaml"]["items"] = []any{map[string]any{
		"apiVersion": "karpenter.sh/v1",
		"kind":       "NodePool",
		"metadata": map[string]any{
			"name": "pool-a",
		},
	}}
	if err := writeCapture(dir, "cluster-a", "context-a", "us-west-2", captured); err != nil {
		t.Fatal(err)
	}
	fixture, err := clusterfixture.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(fixture.Nodes) != 1 || fixture.Nodes[0].Name != "node-a" {
		t.Fatalf("loaded nodes = %#v, want node-a", fixture.Nodes)
	}
	if len(fixture.Pods) != 1 || fixture.Pods[0].Name != "pod-a" {
		t.Fatalf("loaded pods = %#v, want pod-a", fixture.Pods)
	}
	if len(fixture.NodePools) != 1 || fixture.NodePools[0].Name != "pool-a" {
		t.Fatalf("loaded node pools = %#v, want pool-a", fixture.NodePools)
	}
}

func TestWriteCaptureRejectsMissingResource(t *testing.T) {
	err := writeCapture(t.TempDir(), "cluster", "context", "region", captureResult{lists: map[string]map[string]any{}})
	if err == nil || !strings.Contains(err.Error(), "missing captured resource") {
		t.Fatalf("writeCapture() error = %v, want missing-resource error", err)
	}
}

func TestCapturePropagatesListErrors(t *testing.T) {
	listKinds := make(map[schema.GroupVersionResource]string, len(resourceSpecs))
	for _, spec := range resourceSpecs {
		listKinds[spec.gvr] = spec.kind
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), listKinds)
	client.PrependReactor("list", "*", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden")
	})
	_, err := capture(context.Background(), client)
	if err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("capture() error = %v, want forbidden error", err)
	}
}

func TestValidateOutputPath(t *testing.T) {
	root := t.TempDir()
	missing := filepath.Join(root, "missing")
	if err := validateOutputPath(missing, false); err != nil {
		t.Fatalf("validateOutputPath(missing) = %v", err)
	}
	dir := filepath.Join(root, "existing")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validateOutputPath(dir, false); err == nil || !strings.Contains(err.Error(), "pass --overwrite") {
		t.Fatalf("validateOutputPath(existing, false) = %v, want overwrite error", err)
	}
	if err := validateOutputPath(dir, true); err == nil || !strings.Contains(err.Error(), "not a cluster fixture") {
		t.Fatalf("validateOutputPath(existing, true) = %v, want non-fixture error", err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, []byte("fixture"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := validateOutputPath(file, true); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("validateOutputPath(file) = %v, want not-directory error", err)
	}
}

func TestReplaceOutputInstallsAndReplacesAtomically(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "fixture")
	staging := filepath.Join(root, "staging")
	if err := os.Mkdir(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "marker"), []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"nodes.yaml", "metadata.json"} {
		if err := os.WriteFile(filepath.Join(staging, marker), []byte("fixture"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := replaceOutput(staging, output, false); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(output, "marker")); err != nil || string(data) != "new" {
		t.Fatalf("installed marker = %q, err = %v", data, err)
	}

	newStaging := filepath.Join(root, "new-staging")
	if err := os.Mkdir(newStaging, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(newStaging, "marker"), []byte("replacement"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := replaceOutput(newStaging, output, true); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(output, "marker")); err != nil || string(data) != "replacement" {
		t.Fatalf("replaced marker = %q, err = %v", data, err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".cluster-fixture-backup-") {
			t.Fatalf("backup directory %q was not cleaned up", entry.Name())
		}
	}
}

func TestReplaceOutputRefusesExistingOutputWithoutOverwrite(t *testing.T) {
	root := t.TempDir()
	output := filepath.Join(root, "fixture")
	staging := filepath.Join(root, "staging")
	if err := os.Mkdir(output, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	err := replaceOutput(staging, output, false)
	if err == nil || !strings.Contains(err.Error(), "pass --overwrite") {
		t.Fatalf("replaceOutput() error = %v, want overwrite error", err)
	}
	if _, err := os.Stat(staging); err != nil {
		t.Fatalf("staging directory should remain after refused replacement: %v", err)
	}
}

func TestRunCleansStagingAfterCatalogFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "v1",
			"kind":       "List",
			"items":      []any{},
		}); err != nil {
			t.Errorf("encoding fake Kubernetes response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	kubeconfig := filepath.Join(t.TempDir(), "config")
	if err := os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
clusters:
- name: fake
  cluster:
    server: `+server.URL+`
users:
- name: fake
  user: {}
contexts:
- name: fake
  context:
    cluster: fake
    user: fake
current-context: fake
`), 0o600); err != nil {
		t.Fatal(err)
	}

	oldExporter := catalogExporter
	catalogExporter = func(context.Context, *clusterfixture.Fixture, string) (*clusterfixture.Catalog, error) {
		return nil, errors.New("catalog unavailable")
	}
	t.Cleanup(func() { catalogExporter = oldExporter })

	parent := t.TempDir()
	output := filepath.Join(parent, "fixture")
	err := run(context.Background(), options{
		cluster:    "fake-cluster",
		output:     output,
		kubeconfig: kubeconfig,
		context:    "fake",
		region:     "us-west-2",
	})
	if err == nil || !strings.Contains(err.Error(), "catalog unavailable") {
		t.Fatalf("run() error = %v, want catalog failure", err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("output path exists after failed run, stat error = %v", err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".cluster-fixture-") {
			t.Fatalf("staging path %q was not cleaned up", entry.Name())
		}
	}
}

func TestListCountHandlesMissingAndMalformedItems(t *testing.T) {
	if got := listCount(nil); got != 0 {
		t.Fatalf("listCount(nil) = %d, want 0", got)
	}
	if got := listCount(map[string]any{"items": "not-a-list"}); got != 0 {
		t.Fatalf("listCount(malformed) = %d, want 0", got)
	}
}

func TestLoadKubeConfigRejectsMissingConfig(t *testing.T) {
	_, _, err := loadKubeConfig(filepath.Join(t.TempDir(), "missing-kubeconfig"), "")
	if err == nil {
		t.Fatal("loadKubeConfig() unexpectedly succeeded for missing explicit config")
	}
	if !strings.Contains(err.Error(), "loading kubeconfig") {
		t.Fatalf("loadKubeConfig() error = %v, want loading-kubeconfig context", err)
	}
}

func TestLoadKubeConfigHonorsContextOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config")
	data := []byte(`apiVersion: v1
kind: Config
clusters:
- name: one
  cluster:
    server: https://one.example
- name: two
  cluster:
    server: https://two.example
users:
- name: user
  user: {}
contexts:
- name: one-context
  context:
    cluster: one
    user: user
- name: two-context
  context:
    cluster: two
    user: user
current-context: one-context
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, contextName, err := loadKubeConfig(path, "two-context")
	if err != nil {
		t.Fatal(err)
	}
	if contextName != "two-context" {
		t.Fatalf("selected context = %q, want two-context", contextName)
	}
}
