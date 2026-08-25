//go:build test_performance

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

package disruption

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/bench/clusterfixture"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/operator/logging"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

func init() {
	log.SetLogger(logging.NopLogger)
}

// Run against a dumped cluster fixture:
//
//	CLUSTER_FIXTURE_DIR=testdata/clusterfixtures/cx498 \
//	go test -tags=test_performance -run='^$' \
//	  -bench=BenchmarkEvaluateMoveSet_ClusterFixture -benchtime=10s -count=1 \
//	  -cpuprofile=/tmp/evaluate_move_set_cx498.cpu.pprof ./pkg/controllers/disruption
//
//	go tool pprof -http=:0 /tmp/evaluate_move_set_cx498.cpu.pprof

const clusterFixtureDirEnvVar = "CLUSTER_FIXTURE_DIR"

type clusterFixtureBench struct {
	ctx        context.Context
	env        *clusterfixture.Env
	compute    consolidationComputer
	candidates []*Candidate
	rng        *rand.Rand
}

var (
	clusterFixtureBenchOnce sync.Once
	clusterFixtureBenchData *clusterFixtureBench
	clusterFixtureBenchErr  error
)

func clusterFixtureDir() string {
	if dir := os.Getenv(clusterFixtureDirEnvVar); dir != "" {
		return dir
	}
	return "testdata/clusterfixtures/cx498"
}

func getClusterFixtureBench(tb testing.TB) *clusterFixtureBench {
	tb.Helper()
	clusterFixtureBenchOnce.Do(func() {
		clusterFixtureBenchData, clusterFixtureBenchErr = newClusterFixtureBench(clusterFixtureDir())
	})
	if clusterFixtureBenchErr != nil {
		tb.Fatalf("setting up cluster fixture benchmark: %v", clusterFixtureBenchErr)
	}
	return clusterFixtureBenchData
}

func newClusterFixtureBench(dir string) (*clusterFixtureBench, error) {
	dir = clusterfixture.ResolveDir(dir)
	if !clusterfixture.Exists(dir) {
		return nil, fmt.Errorf("cluster fixture not found at %q (set %s or run coralogix-fork/bench/dump-cluster-fixture.sh)", dir, clusterFixtureDirEnvVar)
	}

	fixture, err := clusterfixture.Load(dir)
	if err != nil {
		return nil, err
	}

	ctx := options.ToContext(context.Background(), test.Options())
	env, err := fixture.BuildEnv(ctx, clusterfixture.Options{AssumeScoreBasedAllPools: true})
	if err != nil {
		return nil, err
	}

	env.Provisioner = provisioning.NewProvisioner(env.Client, env.Recorder, env.CloudProvider, env.Cluster, env.Clock)
	queue := NewQueue(env.Client, env.Recorder, env.Cluster, env.Clock, env.Provisioner)
	consolidation := MakeConsolidation(env.Clock, env.Cluster, env.Client, env.Provisioner, env.CloudProvider, env.Recorder, queue, nil)
	scoreBased := &ScoreBasedConsolidation{consolidation: consolidation}

	candidates, err := GetCandidates(ctx, env.Cluster, env.Client, env.Recorder, env.Clock, env.CloudProvider, scoreBased.ShouldDisrupt, GracefulDisruptionClass, queue)
	if err != nil {
		return nil, fmt.Errorf("listing candidates: %w", err)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no eligible score-based consolidation candidates in fixture %q", dir)
	}

	return &clusterFixtureBench{
		ctx:        ctx,
		env:        env,
		compute:    consolidation.computeConsolidation,
		candidates: candidates,
		rng:        rand.New(rand.NewSource(42)), //nolint:gosec
	}, nil
}

func BenchmarkEvaluateMoveSet_ClusterFixture(b *testing.B) {
	dir := clusterfixture.ResolveDir(clusterFixtureDir())
	if !clusterfixture.Exists(dir) {
		b.Skipf("cluster fixture not found at %q", dir)
	}

	bench := getClusterFixtureBench(b)
	b.ReportMetric(float64(bench.env.NodeCount), "nodes")
	b.ReportMetric(float64(bench.env.PodCount), "pods")
	b.ReportMetric(float64(len(bench.candidates)), "candidates")

	stats := &moveSetSearchStats{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		candidate := bench.candidates[bench.rng.Intn(len(bench.candidates))]
		_ = evaluateMoveSet(bench.ctx, moveSet{Nodes: []*Candidate{candidate}}, bench.compute, stats)
	}
}
