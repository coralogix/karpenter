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

	"sigs.k8s.io/karpenter/pkg/bench/clusterfixture"
)

func main() {
	fixtureDir := flag.String("fixture", "", "path to cluster fixture directory")
	output := flag.String("output", "", "path to write instance-types.json")
	flag.Parse()

	if *fixtureDir == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "usage: build-instance-catalog --fixture <dir> --output <file>")
		os.Exit(2)
	}

	fixture, err := clusterfixture.Load(*fixtureDir)
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
	if err := os.WriteFile(*output, data, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "writing catalog: %v\n", err)
		os.Exit(1)
	}
}
