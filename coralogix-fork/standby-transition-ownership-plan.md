# Give the coordinator complete standby transition ownership

Status: implemented on branch. Second of three follow-up plans
from the architecture review of `c7074ecd` against `coralogix-fork-v1.14.1`.

## Problem and outcome

Persistent standby transitions are spread across the queue,
`pkg/standby.Coordinator`, and `pkg/utils/standby.Lifecycle`. Callers can invoke
low-level repair operations without a single completion contract.

There is also a confirmed partial-write bug: post-evacuation entry writes the claim
marker before the node taint. If the taint write fails, the retry sees the marker
and returns `OutcomeUnchanged` without completing the taint. The queue accepts that
result and can remove the temporary disruption barrier. The review reproduced
`claimStandby=true`, `nodeStandbyTaint=false`, and no returned error after one
injected transient Node patch failure.

One component should own each complete persistent standby transition. Retrying or
recovering a transition must establish its entire invariant, including after a
process restart. Merely finding one marker is not success.

## State and ownership contract

| State/result | Required properties |
|---|---|
| Standby complete | Matching live Node/NodeClaim identity; claim standby marker and Node standby taint present; no activation in progress. |
| Active complete | Standby marker, standby taint, activation markers, and activation-source metadata cleared. |
| Interrupted | Persisted state permits a later retry or repair to complete without overriding newer activation or deleting/replaced objects. |
| Skipped or superseded | No claim that standby entry completed; callers must handle concurrent activation, deletion, identity change, or loss of eligibility explicitly. |

The coordinator owns live reads and persistent standby marker/taint writes. The
queue owns command progress, source eviction, deletion, and the lifetime of the
command's temporary disruption barrier. Provisioning owns placement decisions.
Pure representation helpers may remain in `pkg/utils/standby`; API-mutating
lifecycle operations become private implementation of `pkg/standby`.

Temporary disruption taints are shared with other disruption paths. An operation
must only release a barrier when its caller owns it or recovery has established
that no in-flight command owns it. Activation must not unconditionally clear an
unrelated disruption barrier.

## Implementation steps

1. Add the failing partial-write regression before moving code. Verify a retry
   after claim marking but before Node tainting repairs the taint, preserves the
   original standby timestamp, and cannot report completion prematurely.
2. Define completion predicates and result semantics for standby entry, activation,
   and interrupted-transition repair. Keep a small API; do not introduce a generic
   state-machine framework or new persisted phase format.
3. Move live mutation/retry helpers from `pkg/utils/standby/lifecycle.go` into private
   coordinator implementation. Retain pure annotation/taint helpers separately.
   Preserve ordered reads, optimistic resource-version checks, and UID/provider
   identity checks at boundaries where stale objects could be mutated.
4. Keep distinct entry preconditions for naturally empty nodes and evacuated nodes.
   Share invariant establishment and retry/repair behavior, rather than forcing
   both flows into a large boolean-driven protocol. A preexisting standby marker
   must trigger reconciliation of the complete state, not an early success return.
5. Refactor queue/provisioner callers to request complete operations and consume
   explicit outcomes. Remove forwarding methods that expose individual mutation
   steps once their callers migrate. In particular, the queue must not treat an
   entry skipped due to changed state as equivalent to completed standby entry.
6. Let the coordinator reconcile partial persistent state; keep discovery of nodes
   needing repair in the existing controller flows. Consolidate duplicate repair
   logic without changing command ownership or adding a controller.
7. Return fresh objects or an explicit refresh requirement so callers update local
   state consistently. Remove ad hoc local marker clearing only when replaced by
   an equally clear authoritative completion contract.
8. Preserve activation source labels and metric meaning. Do not promise exactly-once
   metrics across process crashes; prevent normal retries from double reporting.

Primary code: `pkg/standby/`, `pkg/utils/standby/lifecycle.go`,
`pkg/controllers/disruption/queue_evacuation.go`, `queue_standby.go`,
`queue_standby_recovery.go`, and `pkg/controllers/provisioning/standby.go`.

## Regression coverage

Use a small table of meaningful interrupted boundaries, reusing existing clients
and conflict-injection helpers. Proposed unit-test prefix: `TestStandbyTransition`.
Proposed integration group: `Standby transition recovery`.

- Marker write succeeds, Node taint write fails once: retry establishes complete
  standby state before the queue releases its barrier.
- A fresh coordinator recovers the same interrupted state after restart; a retry
  does not reset the reclamation-delay timestamp.
- Activation starts between entry writes: entry never re-taints activated capacity
  or clears another operation's reservation.
- Occupancy appears during empty marking or recovery: the node returns to active
  capacity, preserving workload safety and the existing occupancy rules.
- Node or NodeClaim is replaced/deleted between reads: no mutation is applied to a
  replacement object, and no incomplete transition is reported as complete.
- Activation resumes from claim-only and Node-only residual activation markers.
  Reclamation remains blocked until the required reservation is safely resolved.
- Separate stale cached and authoritative views verify that transition completion
  uses live reads. Include a successful unchanged eligible control case.

Keep existing race tests and reuse the approach in the
[read-consistency plan](live-read-test-coverage-plan.md). Prefer a few tests of full
outcomes over a large matrix of trivial helper tests or brittle API-call counts.

## Scope and sequencing

Follow [scheduling and activation](standby-scheduling-activation-plan.md); the
partial-write correctness fix may be taken first if needed. Keep that plan's reuse
regressions passing during caller migration. This refactor defines the stable
boundary used by [reclamation simplification](reclamation-execution-simplification-plan.md).

Preserve annotations, opt-in behavior, reclamation delay, occupancy semantics,
budgets, pacing, and metrics. Do not add a CRD, controller, durable command store,
warm-capacity policy, or wholesale queue redesign. Preserve all existing local
work, including reclamation-delay terminology changes.

## Validation and acceptance

Use proposed selectors only after adding their tests. During iteration:

```sh
KUBEBUILDER_ASSETS="/Users/rsum/Library/Application Support/io.kubebuilder.envtest/k8s/1.36.2-darwin-arm64" \
K8S_VERSION=1.36.2 \
hack/check-fast.sh --run '^Test(StandbyTransition|EnterStandby|Activate|Lifecycle|EnsureStandby)' \
  ./pkg/standby ./pkg/utils/standby

KUBEBUILDER_ASSETS="/Users/rsum/Library/Application Support/io.kubebuilder.envtest/k8s/1.36.2-darwin-arm64" \
K8S_VERSION=1.36.2 \
hack/check-fast.sh --run '^Test(APIs|StandbyMarking|CleanupRecovers)' \
  --focus 'Standby transition recovery|Standby reuse|should wait for non-daemon pods to disappear before retaining an evacuated node' \
  ./pkg/controllers/disruption ./pkg/controllers/provisioning
```

Check each package's test/spec counts. Discover installed Kubernetes 1.36.2 with
`go tool -modfile=go.tools.mod setup-envtest use -i -p path 1.36.2`; verify executable
`etcd`, `kube-apiserver`, and `kubectl` and conflicting `TEST_ASSET_*` or
`USE_EXISTING_CLUSTER` overrides. Follow repository setup instructions if missing.

After all edits and any requested review, the lead runs:

```sh
KUBEBUILDER_ASSETS="/Users/rsum/Library/Application Support/io.kubebuilder.envtest/k8s/1.36.2-darwin-arm64" \
K8S_VERSION=1.36.2 GOTOOLCHAIN=auto \
go test -mod=readonly -count=1 -timeout=20m \
  ./pkg/standby ./pkg/utils/standby ./pkg/controllers/disruption \
  ./pkg/controllers/provisioning

GOTOOLCHAIN=auto \
GOLANGCI_LINT_CACHE="$(git rev-parse --absolute-git-dir)/golangci-lint-cache" \
make verify
```

Extend final tests to any other changed package. Include verification-generated
changes and rerun verification when necessary. Acceptance requires one owner of
persistent transition ordering, no successful incomplete transition, and passing
reuse, conflict, and recovery regressions. Report commands, counts, and results.
