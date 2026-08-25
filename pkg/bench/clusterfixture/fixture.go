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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Metadata describes a dumped cluster fixture.
type Metadata struct {
	Cluster       string `json:"cluster"`
	Context       string `json:"context"`
	DumpedAt      string `json:"dumpedAt"`
	NodeCount     int    `json:"nodeCount"`
	PodCount      int    `json:"podCount"`
	PDBCount      int    `json:"pdbCount"`
	NodePoolCount int    `json:"nodePoolCount"`
}

// Fixture holds parsed cluster objects from a dump directory.
type Fixture struct {
	Dir string

	Metadata Metadata

	Nodes      []*corev1.Node
	Pods       []*corev1.Pod
	DaemonSets []*appsv1.DaemonSet
	PDBs       []*policyv1.PodDisruptionBudget
	NodePools  []*v1.NodePool
	NodeClaims []*v1.NodeClaim

	Catalog *Catalog

	podsByNode map[string][]*corev1.Pod
}

// Exists reports whether a fixture directory is present on disk.
func Exists(dir string) bool {
	_, err := os.Stat(filepath.Join(ResolveDir(dir), "nodes.yaml"))
	return err == nil
}

// ResolveDir returns the first existing path for dir, checking the module root.
func ResolveDir(dir string) string {
	if dir == "" {
		return dir
	}
	if filepath.IsAbs(dir) {
		return dir
	}
	candidates := []string{dir}
	if root, err := moduleRoot(); err == nil {
		candidates = append(candidates, filepath.Join(root, dir))
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(filepath.Join(candidate, "nodes.yaml")); err == nil {
			return candidate
		}
	}
	return dir
}

func moduleRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from %s", wd)
		}
		dir = parent
	}
}

// Load reads a cluster fixture from dir.
func Load(dir string) (*Fixture, error) {
	dir = ResolveDir(dir)
	fixture := &Fixture{Dir: dir}

	if data, err := os.ReadFile(filepath.Join(dir, "metadata.json")); err == nil {
		if err := json.Unmarshal(data, &fixture.Metadata); err != nil {
			return nil, fmt.Errorf("parsing metadata.json: %w", err)
		}
	}

	var err error
	if fixture.Nodes, err = decodeYAMLFile[*corev1.Node](filepath.Join(dir, "nodes.yaml")); err != nil {
		return nil, fmt.Errorf("decoding nodes: %w", err)
	}
	if fixture.Pods, err = decodeYAMLFile[*corev1.Pod](filepath.Join(dir, "pods.yaml")); err != nil {
		return nil, fmt.Errorf("decoding pods: %w", err)
	}
	if fixture.DaemonSets, err = decodeYAMLFile[*appsv1.DaemonSet](filepath.Join(dir, "daemonsets.yaml")); err != nil {
		return nil, fmt.Errorf("decoding daemonsets: %w", err)
	}
	if fixture.PDBs, err = decodeYAMLFile[*policyv1.PodDisruptionBudget](filepath.Join(dir, "pdbs.yaml")); err != nil {
		return nil, fmt.Errorf("decoding pdbs: %w", err)
	}
	if fixture.NodePools, err = decodeYAMLFile[*v1.NodePool](filepath.Join(dir, "nodepools.yaml")); err != nil {
		return nil, fmt.Errorf("decoding nodepools: %w", err)
	}
	if fixture.NodeClaims, err = decodeYAMLFile[*v1.NodeClaim](filepath.Join(dir, "nodeclaims.yaml")); err != nil {
		return nil, fmt.Errorf("decoding nodeclaims: %w", err)
	}

	catalogPath := filepath.Join(dir, "instance-types.json")
	if data, err := os.ReadFile(catalogPath); err != nil {
		fixture.Catalog = BuildCatalog(fixture)
	} else if err := json.Unmarshal(data, &fixture.Catalog); err != nil {
		return nil, fmt.Errorf("parsing instance-types.json: %w", err)
	}

	return fixture, nil
}
