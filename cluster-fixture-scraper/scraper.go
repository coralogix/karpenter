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
	"fmt"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/karpenter/pkg/bench/clusterfixture"
	"sigs.k8s.io/yaml"
)

type resourceSpec struct {
	file string
	gvr  schema.GroupVersionResource
	kind string
}

var resourceSpecs = []resourceSpec{
	{file: "nodes.yaml", gvr: schema.GroupVersionResource{Version: "v1", Resource: "nodes"}, kind: "NodeList"},
	{file: "namespaces.yaml", gvr: schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}, kind: "NamespaceList"},
	{file: "pods.yaml", gvr: schema.GroupVersionResource{Version: "v1", Resource: "pods"}, kind: "PodList"},
	{file: "daemonsets.yaml", gvr: schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}, kind: "DaemonSetList"},
	{file: "pdbs.yaml", gvr: schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"}, kind: "PodDisruptionBudgetList"},
	{file: "nodepools.yaml", gvr: schema.GroupVersionResource{Group: "karpenter.sh", Version: "v1", Resource: "nodepools"}, kind: "NodePoolList"},
	{file: "nodeclaims.yaml", gvr: schema.GroupVersionResource{Group: "karpenter.sh", Version: "v1", Resource: "nodeclaims"}, kind: "NodeClaimList"},
	{file: "nodeclasses.yaml", gvr: schema.GroupVersionResource{Group: "karpenter.k8s.aws", Version: "v1", Resource: "ec2nodeclasses"}, kind: "EC2NodeClassList"},
	{file: "persistentvolumeclaims.yaml", gvr: schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}, kind: "PersistentVolumeClaimList"},
	{file: "persistentvolumes.yaml", gvr: schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}, kind: "PersistentVolumeList"},
	{file: "storageclasses.yaml", gvr: schema.GroupVersionResource{Group: "storage.k8s.io", Version: "v1", Resource: "storageclasses"}, kind: "StorageClassList"},
	{file: "csinodes.yaml", gvr: schema.GroupVersionResource{Group: "storage.k8s.io", Version: "v1", Resource: "csinodes"}, kind: "CSINodeList"},
}

type captureResult struct {
	lists  map[string]map[string]any
	region string
}

func (c captureResult) resourceCount() int {
	count := 0
	for _, list := range c.lists {
		items, _ := list["items"].([]any)
		count += len(items)
	}
	return count
}

func loadKubeConfig(kubeconfig, contextName string) (*rest.Config, string, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	if contextName != "" {
		overrides.CurrentContext = contextName
	}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)
	config, err := loader.ClientConfig()
	if err != nil {
		return nil, "", fmt.Errorf("loading kubeconfig: %w", err)
	}
	raw, err := loader.RawConfig()
	if err != nil {
		return nil, "", fmt.Errorf("reading kubeconfig context: %w", err)
	}
	selectedContext := raw.CurrentContext
	if contextName != "" {
		selectedContext = contextName
	}
	if selectedContext == "" {
		return nil, "", errors.New("kubeconfig has no current context")
	}
	return config, selectedContext, nil
}

func newDynamicClient(config *rest.Config) (dynamic.Interface, error) {
	return dynamic.NewForConfig(config)
}

func capture(ctx context.Context, client dynamic.Interface) (captureResult, error) {
	result := captureResult{lists: make(map[string]map[string]any, len(resourceSpecs))}
	for _, spec := range resourceSpecs {
		resource := client.Resource(spec.gvr)
		var list *unstructuredList
		var err error
		if isNamespaced(spec.gvr) {
			list, err = listResource(ctx, resource.Namespace(metav1.NamespaceAll))
		} else {
			list, err = listResource(ctx, resource)
		}
		if err != nil {
			return captureResult{}, fmt.Errorf("listing %s: %w", spec.gvr.Resource, err)
		}
		content := list.object
		// clusterfixture's decoder intentionally accepts the generic List shape
		// emitted by kubectl, including lists of custom resources.
		content["apiVersion"] = "v1"
		content["kind"] = "List"
		if items, ok := content["items"].([]any); !ok || items == nil {
			content["items"] = []any{}
		}
		result.lists[spec.file] = content
		if spec.file == "nodes.yaml" {
			result.region = firstNodeRegion(content)
		}
	}
	return result, nil
}

// unstructuredList is kept small so collection can be tested without coupling
// the scraper to generated Kubernetes list types.
type unstructuredList struct {
	object map[string]any
}

func listResource(ctx context.Context, resource interface {
	List(context.Context, metav1.ListOptions) (*unstructured.UnstructuredList, error)
}) (*unstructuredList, error) {
	list, err := resource.List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return &unstructuredList{object: list.UnstructuredContent()}, nil
}

func isNamespaced(gvr schema.GroupVersionResource) bool {
	return (gvr.Group == "" && (gvr.Resource == "pods" || gvr.Resource == "persistentvolumeclaims")) ||
		gvr.Group == "apps" || gvr.Group == "policy"
}

func firstNodeRegion(list map[string]any) string {
	items, _ := list["items"].([]any)
	for _, raw := range items {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		metadata, _ := item["metadata"].(map[string]any)
		labels, _ := metadata["labels"].(map[string]any)
		if region, _ := labels[corev1.LabelTopologyRegion].(string); region != "" {
			return region
		}
	}
	return ""
}

func resolveRegion(flagRegion, nodeRegion string) string {
	if flagRegion != "" {
		return flagRegion
	}
	if envRegion := os.Getenv("AWS_REGION"); envRegion != "" {
		return envRegion
	}
	return nodeRegion
}

func writeCapture(dir, cluster, contextName, region string, captured captureResult) error {
	for _, spec := range resourceSpecs {
		content, ok := captured.lists[spec.file]
		if !ok {
			return fmt.Errorf("missing captured resource %s", spec.file)
		}
		data, err := yaml.Marshal(content)
		if err != nil {
			return fmt.Errorf("encoding %s: %w", spec.file, err)
		}
		if err := os.WriteFile(filepath.Join(dir, spec.file), data, 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", spec.file, err)
		}
	}
	metadata := map[string]any{
		"cluster":        cluster,
		"context":        contextName,
		"region":         region,
		"dumpedAt":       time.Now().UTC().Format(time.RFC3339),
		"nodeCount":      listCount(captured.lists["nodes.yaml"]),
		"namespaceCount": listCount(captured.lists["namespaces.yaml"]),
		"podCount":       listCount(captured.lists["pods.yaml"]),
		"pdbCount":       listCount(captured.lists["pdbs.yaml"]),
		"nodePoolCount":  listCount(captured.lists["nodepools.yaml"]),
	}
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding metadata: %w", err)
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(dir, "metadata.json"), data, 0o644)
}

func listCount(list map[string]any) int {
	if list == nil {
		return 0
	}
	items, _ := list["items"].([]any)
	return len(items)
}

func loadFixture(dir string) (*clusterfixture.Fixture, error) {
	return clusterfixture.Load(dir)
}

func writeCatalog(dir string, catalog *clusterfixture.Catalog) error {
	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(dir, "instance-types.json"), data, 0o644)
}

func validateOutputPath(output string, overwrite bool) error {
	info, err := os.Lstat(output)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("checking output path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("refusing to overwrite symlink output path %q", output)
	}
	if !info.IsDir() {
		return fmt.Errorf("output path %q is not a directory", output)
	}
	if !overwrite {
		return fmt.Errorf("output path %q already exists (pass --overwrite to replace it)", output)
	}
	for _, marker := range []string{"nodes.yaml", "metadata.json"} {
		markerInfo, markerErr := os.Lstat(filepath.Join(output, marker))
		if markerErr != nil {
			if os.IsNotExist(markerErr) {
				return fmt.Errorf("refusing to overwrite %q: it is not a cluster fixture (missing %s)", output, marker)
			}
			return fmt.Errorf("checking fixture marker %q: %w", marker, markerErr)
		}
		if !markerInfo.Mode().IsRegular() {
			return fmt.Errorf("refusing to overwrite %q: fixture marker %s is not a regular file", output, marker)
		}
	}
	return nil
}

func replaceOutput(staging, output string, overwrite bool) error {
	if err := validateOutputPath(output, overwrite); err != nil {
		return err
	}
	if _, err := os.Lstat(output); err == nil {
		if !overwrite {
			return fmt.Errorf("output path %q already exists (pass --overwrite to replace it)", output)
		}
		backup, err := os.MkdirTemp(filepath.Dir(output), ".cluster-fixture-backup-*")
		if err != nil {
			return fmt.Errorf("creating backup path: %w", err)
		}
		if err := os.Remove(backup); err != nil {
			return fmt.Errorf("preparing backup path: %w", err)
		}
		if err := os.Rename(output, backup); err != nil {
			return fmt.Errorf("moving existing output aside: %w", err)
		}
		if err := os.Rename(staging, output); err != nil {
			_ = os.Rename(backup, output)
			return fmt.Errorf("moving staged output into place: %w", err)
		}
		if err := os.RemoveAll(backup); err != nil {
			return fmt.Errorf("removing old output backup: %w", err)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("checking output path: %w", err)
	}
	if err := os.Rename(staging, output); err != nil {
		return err
	}
	return nil
}
