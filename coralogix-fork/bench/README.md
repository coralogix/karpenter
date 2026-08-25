# Benchmarks

Run from the **repo root**.

## evaluateMoveSet cluster-fixture benchmark

Benchmark score-based consolidation against a real cluster snapshot.

### 1. Dump a cluster fixture

```bash
./coralogix-fork/bench/dump-cluster-fixture.sh cx498
```

Writes a gitignored fixture to `testdata/clusterfixtures/cx498/`. Terminating resources are dropped at load time.

**Dump time (needs kubectl + AWS EC2 read once):** `instance-types.json` is generated from the AWS provider’s instance-type resolution (same as production `GetInstanceTypes`, without node overlays). Region is stored in `metadata.json`.

**Benchmark time (fully offline):** `go test` only reads YAML/JSON from the fixture directory and uses the fake cloud provider. It does **not** call AWS or the internet. Do not re-run `build-instance-catalog.go` unless you intend to refresh the catalog from AWS.

If `instance-types.json` is missing, the loader falls back to a tiny node-derived catalog (~30 instance types on cx498) which does not match production.

**Local smoke test** (committed mini fixture, no dump):

```bash
export CLUSTER_FIXTURE_DIR=pkg/bench/clusterfixture/testdata/mini
```

### 2. Run the benchmark

```bash
CLUSTER_FIXTURE_DIR=testdata/clusterfixtures/cx498 \
go test -tags=test_performance -run='^$' \
  -bench=BenchmarkEvaluateMoveSet_ClusterFixture -benchtime=30s -count=1 \
  ./pkg/controllers/disruption
```

Skipped when `CLUSTER_FIXTURE_DIR` is missing (CI-safe).

### 3. Flamegraph

```bash
PROFILE=profiles/evaluate_move_set_cx498-$(date +%Y%m%d-%H%M%S).cpu.pprof
mkdir -p profiles

CLUSTER_FIXTURE_DIR=testdata/clusterfixtures/cx498 \
go test -tags=test_performance -run='^$' \
  -bench=BenchmarkEvaluateMoveSet_ClusterFixture -benchtime=30s -count=1 \
  -cpuprofile="$PROFILE" \
  ./pkg/controllers/disruption

PPROF_PORT=${PPROF_PORT:-8080}
go tool pprof -http=:"$PPROF_PORT" "$PROFILE"
```

Open **Flame Graph** at http://localhost:8080 (or the port you set via `PPROF_PORT`).

### What it measures

Each iteration picks a **random eligible node** (seed `42`) from the candidate list built once via `GetCandidates`, then calls `evaluateMoveSet` — matching `searchForMoveSets` in production.

## NewLabelRequirements microbenchmark

Microbenchmark of `scheduling.NewLabelRequirements` using embedded node labels (no cluster fixture).

### Run the benchmark

```bash
go test -tags=test_performance -run='^$' \
  -bench=BenchmarkNewLabelRequirements -benchtime=30s -count=1 \
  ./pkg/controllers/provisioning/scheduling
```

### Flamegraph

```bash
PROFILE=profiles/new_label_requirements-$(date +%Y%m%d-%H%M%S).cpu.pprof
mkdir -p profiles

go test -tags=test_performance -run='^$' \
  -bench=BenchmarkNewLabelRequirements -benchtime=30s -count=1 \
  -cpuprofile="$PROFILE" \
  ./pkg/controllers/provisioning/scheduling

PPROF_PORT=${PPROF_PORT:-8080}
go tool pprof -http=:"$PPROF_PORT" "$PROFILE"
```

Open **Flame Graph** at http://localhost:8080 (or the port you set via `PPROF_PORT`).

### What it measures

Each iteration calls `scheduling.NewLabelRequirements` with a fixed representative node label map — matching the per-daemon-pod call in `isDaemonPodCompatibleWithNode`.
