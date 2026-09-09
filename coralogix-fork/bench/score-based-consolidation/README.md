# evaluateMoveSet cluster-fixture benchmark

Benchmark score-based consolidation against a real cluster snapshot.

Run from the **repo root**.

## 1. Dump a cluster fixture

```bash
./coralogix-fork/bench/score-based-consolidation/dump-fixture.sh example-cluster
```

Writes a gitignored fixture to `testdata/clusterfixtures/example-cluster/`. Terminating resources are dropped at load time. The fixture includes PVCs, PVs, StorageClasses, and CSINodes so volume topology and attachment limits remain part of scheduling simulation. Re-dump fixtures created before these storage resources were added.

**Dump time (needs kubectl + AWS EC2 read once):** `instance-types.json` is generated from the AWS provider’s instance-type resolution (same as production `GetInstanceTypes`, without node overlays). Region is stored in `metadata.json`; the dump script uses `AWS_REGION` when set, otherwise the first node’s standard `topology.kubernetes.io/region` label. The catalog exporter also accepts an explicit `--region`.

**Benchmark time (fully offline):** `go test` only reads YAML/JSON from the fixture directory and uses the fake cloud provider. It does **not** call AWS or the internet. Do not re-run the instance catalog exporter unless you intend to refresh the catalog from AWS.

If `instance-types.json` is missing, the loader falls back to a small node-derived catalog which does not match production.

**Local smoke test** (committed mini fixture, no dump):

```bash
export CLUSTER_FIXTURE_DIR=pkg/bench/clusterfixture/testdata/mini
```

## 2. Run the benchmark

The benchmark runs with `PreferencePolicy=Ignore`. Required scheduling constraints remain enforced.

```bash
CLUSTER_FIXTURE_DIR=testdata/clusterfixtures/example-cluster \
go test -tags=test_performance -run='^$' \
  -bench=BenchmarkEvaluateMoveSet_ClusterFixture -benchtime=30s -count=1 \
  ./pkg/controllers/disruption
```

Skipped when `CLUSTER_FIXTURE_DIR` is missing (CI-safe).

## 3. Flamegraph

```bash
PROFILE=profiles/evaluate_move_set_example-cluster-$(date +%Y%m%d-%H%M%S).cpu.pprof
mkdir -p profiles

CLUSTER_FIXTURE_DIR=testdata/clusterfixtures/example-cluster \
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
