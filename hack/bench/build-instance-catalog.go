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
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"sigs.k8s.io/karpenter/pkg/bench/clusterfixture"
)

func main() {
	fixtureDir := flag.String("fixture", "", "path to cluster fixture directory")
	output := flag.String("output", "", "path to write instance-types.json")
	fromNodes := flag.Bool("from-nodes", false, "build catalog from fixture nodes only (legacy, no AWS)")
	region := flag.String("region", "", "AWS region for cloud provider export (dump-time only)")
	skipIfPresent := flag.Bool("skip-if-present", false, "skip AWS export when output already has a full catalog")
	flag.Parse()

	if *fixtureDir == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "usage: build-instance-catalog --fixture <dir> --output <file>")
		os.Exit(2)
	}

	if *skipIfPresent && catalogAlreadyPresent(*output) {
		return
	}

	if *fromNodes {
		writeNodesCatalog(*fixtureDir, *output)
		return
	}

	repoRoot, err := moduleRoot()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolving repo root: %v\n", err)
		os.Exit(1)
	}
	fixtureAbs, err := filepath.Abs(*fixtureDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolving fixture path: %v\n", err)
		os.Exit(1)
	}
	outputAbs, err := filepath.Abs(*output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolving output path: %v\n", err)
		os.Exit(1)
	}

	args := []string{"run", ".", "--fixture", fixtureAbs, "--output", outputAbs}
	if *region != "" {
		args = append(args, "--region", *region)
	}
	cmd := exec.Command("go", args...)
	cmd.Dir = filepath.Join(repoRoot, "hack/bench/catalogtool")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}

func catalogAlreadyPresent(output string) bool {
	data, err := os.ReadFile(output)
	if err != nil {
		return false
	}
	var catalog clusterfixture.Catalog
	if err := json.Unmarshal(data, &catalog); err != nil {
		return false
	}
	if !catalog.HasFullInstanceCatalog() {
		return false
	}
	total := 0
	for _, names := range catalog.NodePoolInstanceTypes {
		total += len(names)
	}
	fmt.Printf("catalog already present at %s (%d specs, %d pool assignments), skipping AWS export\n",
		output, len(catalog.InstanceTypeSpecs), total)
	return true
}

func writeNodesCatalog(fixtureDir, output string) {
	fixture, err := clusterfixture.Load(fixtureDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading fixture: %v\n", err)
		os.Exit(1)
	}
	catalog := clusterfixture.BuildCatalog(fixture)
	data, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshaling catalog: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(output, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "writing catalog: %v\n", err)
		os.Exit(1)
	}
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
