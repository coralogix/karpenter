# evaluateMoveSet cluster-fixture benchmark

Benchmark score-based consolidation against a real cluster snapshot.

Run from the **repo root**.

## 1. Dump a cluster fixture

```bash
./coralogix-fork/bench/dump-cluster-fixture.sh cx498
```

Writes a gitignored fixture to `testdata/clusterfixtures/cx498/`. Terminating resources are dropped at load time.

**Local smoke test** (committed mini fixture, no dump):

```bash
export CLUSTER_FIXTURE_DIR=pkg/bench/clusterfixture/testdata/mini
```

## 2. Run the benchmark

```bash
CLUSTER_FIXTURE_DIR=testdata/clusterfixtures/cx498 \
go test -tags=test_performance -run='^$' \
  -bench=BenchmarkEvaluateMoveSet_ClusterFixture -benchtime=30s -count=1 \
  ./pkg/controllers/disruption
```

Skipped when `CLUSTER_FIXTURE_DIR` is missing (CI-safe).

## 3. Flamegraph

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

## What it measures

Each iteration picks a **random eligible node** (seed `42`) from the candidate list built once via `GetCandidates`, then calls `evaluateMoveSet` — matching `searchForMoveSets` in production.
