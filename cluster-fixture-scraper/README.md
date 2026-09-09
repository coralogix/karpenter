# Cluster fixture scraper

The cluster fixture scraper captures a Kubernetes cluster snapshot and resolves
the AWS instance catalog used by the
`BenchmarkSimulateScheduling_ClusterFixture` benchmark. It is a single command
so the Kubernetes snapshot and production-like catalog are generated together.

Run it from the repository root:

```bash
go -C cluster-fixture-scraper run . --cluster example-cluster
```

The command reads Kubernetes credentials using the standard kubeconfig loading
rules. It does not invoke `kubectl`. AWS credentials must be available through
the standard AWS SDK credential chain because the instance catalog is resolved
from EC2 data.

By default, the fixture is written to
`testdata/clusterfixtures/<cluster>`. Existing fixtures are protected; pass
`--overwrite` to replace one. The other options are:

```text
--output <dir>       Fixture output directory
--kubeconfig <path>  Kubeconfig path
--context <name>     Kubeconfig context
--region <region>    AWS region
--overwrite          Replace an existing fixture
```

The region is selected from `--region`, then `AWS_REGION`, then the standard
`topology.kubernetes.io/region` label on a node. Kubernetes API or RBAC errors
are reported instead of being converted into empty resource lists. A temporary
sibling directory is used while the snapshot and catalog are generated, so a
failed run does not leave a partial fixture behind.

## Run the benchmark

The benchmark is fully offline after the fixture has been captured:

```bash
CLUSTER_FIXTURE_DIR=testdata/clusterfixtures/example-cluster \
go test -tags=test_performance -run='^$' \
  -bench=BenchmarkSimulateScheduling_ClusterFixture -benchtime=30s -count=1 \
  ./pkg/controllers/disruption
```

The committed mini fixture can be used for a local smoke test without a cluster:

```bash
CLUSTER_FIXTURE_DIR=pkg/bench/clusterfixture/testdata/mini \
go test -tags=test_performance -run='^$' \
  -bench=BenchmarkSimulateScheduling_ClusterFixture -benchtime=1x -count=1 \
  ./pkg/controllers/disruption
```

For a CPU profile:

```bash
PROFILE=profiles/simulate_scheduling_example-cluster-$(date +%Y%m%d-%H%M%S).cpu.pprof
mkdir -p profiles
CLUSTER_FIXTURE_DIR=testdata/clusterfixtures/example-cluster \
go test -tags=test_performance -run='^$' \
  -bench=BenchmarkSimulateScheduling_ClusterFixture -benchtime=30s -count=1 \
  -cpuprofile="$PROFILE" \
  ./pkg/controllers/disruption
go tool pprof -http=:8080 "$PROFILE"
```

The benchmark builds the candidate list once and randomly selects an eligible
candidate (seed `42`) for each `SimulateScheduling` iteration.
