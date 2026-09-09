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

// Command cluster-fixture-scraper captures a Kubernetes cluster fixture and
// exports the production instance catalog used by cluster-fixture benchmarks.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

type options struct {
	cluster    string
	output     string
	kubeconfig string
	context    string
	region     string
	overwrite  bool
}

func main() {
	var opts options
	flags := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	flags.StringVar(&opts.cluster, "cluster", "", "name to record in the fixture metadata (required)")
	flags.StringVar(&opts.output, "output", "", "fixture output directory (default: <repository>/testdata/clusterfixtures/<cluster>)")
	flags.StringVar(&opts.kubeconfig, "kubeconfig", "", "kubeconfig path (default: standard client-go loading rules)")
	flags.StringVar(&opts.context, "context", "", "kubeconfig context to use")
	flags.StringVar(&opts.region, "region", "", "AWS region (default: AWS_REGION or the first node's region label)")
	flags.BoolVar(&opts.overwrite, "overwrite", false, "replace an existing fixture directory")
	if err := flags.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		os.Exit(2)
	}

	if err := run(context.Background(), opts); err != nil {
		fmt.Fprintf(os.Stderr, "cluster fixture scrape failed: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, opts options) error {
	if opts.cluster == "" {
		return errors.New("--cluster is required")
	}

	output, err := resolveOutputPath(opts.output, opts.cluster)
	if err != nil {
		return err
	}
	if err := validateOutputPath(output, opts.overwrite); err != nil {
		return err
	}

	config, contextName, err := loadKubeConfig(opts.kubeconfig, opts.context)
	if err != nil {
		return err
	}
	kubeClient, err := newDynamicClient(config)
	if err != nil {
		return fmt.Errorf("creating Kubernetes client: %w", err)
	}

	captured, err := capture(ctx, kubeClient)
	if err != nil {
		return fmt.Errorf("capturing cluster resources: %w", err)
	}
	region := resolveRegion(opts.region, captured.region)
	if region == "" {
		return errors.New("could not determine AWS region (set --region, AWS_REGION, or label a node with topology.kubernetes.io/region)")
	}

	parent := filepath.Dir(output)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("creating output parent %q: %w", parent, err)
	}
	staging, err := os.MkdirTemp(parent, ".cluster-fixture-*")
	if err != nil {
		return fmt.Errorf("creating staging directory: %w", err)
	}
	if err := os.Chmod(staging, 0o755); err != nil {
		_ = os.RemoveAll(staging)
		return fmt.Errorf("setting staging directory permissions: %w", err)
	}
	completed := false
	defer func() {
		if !completed {
			_ = os.RemoveAll(staging)
		}
	}()

	if err := writeCapture(staging, opts.cluster, contextName, region, captured); err != nil {
		return fmt.Errorf("writing Kubernetes fixture: %w", err)
	}
	fixture, err := loadFixture(staging)
	if err != nil {
		return fmt.Errorf("loading staged fixture: %w", err)
	}
	catalog, err := catalogExporter(ctx, fixture, region)
	if err != nil {
		return fmt.Errorf("exporting instance catalog: %w", err)
	}
	if catalog == nil {
		return errors.New("exporting instance catalog: exporter returned a nil catalog")
	}
	if err := writeCatalog(staging, catalog); err != nil {
		return fmt.Errorf("writing instance catalog: %w", err)
	}
	if err := replaceOutput(staging, output, opts.overwrite); err != nil {
		return fmt.Errorf("installing fixture: %w", err)
	}
	completed = true

	totalAssignments := 0
	for _, names := range catalog.NodePoolInstanceTypes {
		totalAssignments += len(names)
	}
	fmt.Printf("wrote %s (%d resources, %d instance type specs, %d node pool assignments)\n", output, captured.resourceCount(), len(catalog.InstanceTypeSpecs), totalAssignments)
	return nil
}

func resolveOutputPath(output, cluster string) (string, error) {
	if output == "" {
		root, err := repositoryRoot()
		if err != nil {
			return "", fmt.Errorf("resolving repository root: %w", err)
		}
		output = filepath.Join(root, "testdata", "clusterfixtures", cluster)
	}
	path, err := filepath.Abs(output)
	if err != nil {
		return "", fmt.Errorf("resolving output path: %w", err)
	}
	return filepath.Clean(path), nil
}

func repositoryRoot() (string, error) {
	workingDir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir := workingDir; ; dir = filepath.Dir(dir) {
		data, readErr := os.ReadFile(filepath.Join(dir, "go.mod"))
		if readErr == nil && len(data) > len("module sigs.k8s.io/karpenter") {
			firstLine := string(data)
			if end := len(firstLine); end > 0 {
				for i, c := range firstLine {
					if c == '\n' {
						firstLine = firstLine[:i]
						break
					}
				}
			}
			if firstLine == "module sigs.k8s.io/karpenter" {
				return dir, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("repository root not found")
		}
	}
}
