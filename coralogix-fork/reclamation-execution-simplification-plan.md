# Simplify reclamation planning and queue execution

Status: implemented. Third of three follow-up plans
from the architecture review of `c7074ecd` against `coralogix-fork-v1.14.1`.

## Problem and outcome

Reclamation's policy is simple: delete eligible empty standby nodes after their
reclamation delay. Its implementation carries repeated candidate refreshes,
reporting callbacks, method-specific command branches, and deletion concurrency
equal to the entire selected batch size.

The validator refreshes and validates selected candidates twice consecutively,
rebuilding PDB and instance-type context. Final execution already performs live
safety checks and conditional deletion. Repeated cached observations are not an
atomic safety boundary.

Reduce duplicated work and execution plumbing while preserving safety and the
existing policy. Bound API concurrency without changing the number of candidates
the policy selects or coupling reclamation to disruption budgets.

## Ownership and invariants

| Component | Owns |
|---|---|
| Disruption methods | Cached policy, candidate selection, and one coherent planning validation pass. |
| Queue | Execution progress, eviction, live pre-delete validation, conditional deletion, and execution reporting. |
| Standby coordinator | Complete persistent standby transitions and interrupted-transition repair. |
| Provisioner | Placement decisions and activation requests. |

Cached planning may lag the API. Every candidate still requires live identity,
deletion, activation, taint, and occupancy checks immediately before reclamation
deletion. Retain NodeClaim UID/resource-version delete preconditions. Retry a delete
conflict only after renewed live checks; never convert it into an unconditional
delete. Keep NodePool policy reads cached, as documented today.

This protocol does not atomically exclude a late Kubernetes pod binding. Preserve
normal drain/termination behavior for that documented race rather than claiming
that conditional NodeClaim deletion locks pod scheduling.

## Implementation steps

1. Map each existing planning and execution check to the invariant it protects.
   Remove only duplication, not identity, nomination, do-not-disrupt, PDB, or
   occupancy protections. In particular, empty-node occupancy rules do not by
   themselves replace all daemon-pod disruption checks.
2. Collapse the back-to-back calls in `reclamationValidator.Validate` into one
   selected-candidate refresh/validation pass. Retain refreshed identities and
   NodePool policy in the resulting command. Keep selected-pool caching within that
   pass and preserve current invalid-candidate handling.
3. Stop reconstructing planning dependencies merely to execute commands. Preserve
   existing queue ownership of final safety checks. Do not remove cloud-provider
   instance-type dependencies from generic candidate construction unless a separate
   change demonstrates that all eligibility semantics are preserved.
4. Bound concurrent candidate deletion with a small internal constant, initially
   `maxConcurrentCandidateDeletes = 16`, and use the lesser of that cap and batch
   size. Keep per-candidate live reads inside each worker immediately before its
   delete, rather than validating the whole batch before workers wait their turn.
   Respect context cancellation and existing API-client rate limiting.
5. Audit `OnSuccess` and `OnDeleteSuccess` users. Move callbacks used only for
   reclamation metrics/logging into queue-owned execution reporting. Preserve
   per-candidate progress so a retry neither deletes completed candidates again nor
   duplicates their normal success reporting. Only remove callback-pending state
   after proving there is no remaining fallible callback contract to preserve.
6. Keep planned intent distinct from mutable execution progress. Use the existing
   deletion/evacuation state structures where sufficient; avoid introducing a new
   executor interface or a generic action registry. Centralize unavoidable concrete
   action dispatch rather than spreading more method-type checks across callers.
7. Preserve partial-batch handling and unaffected upstream deletion/replacement
   paths. A candidate failing validation must not erase successful progress or
   cause successful candidates to be reprocessed on retry.
8. Update tests and documentation around observable behavior and the simplified
   ownership. Record before/after production line counts and dependencies as review
   evidence; do not target an arbitrary line-count reduction.

Primary code: `pkg/controllers/disruption/reclamation.go`,
`reclamation_validation.go`, `queue_deletion.go`, `queue_reclamation.go`,
`queue.go`, `types.go`, and reclamation reporting helpers.

## Regression coverage

Reuse existing conditional-delete, replacement-identity, nomination, activation,
and do-not-disrupt cases. Add unit tests under `TestReclamationExecution` and use a
Ginkgo group named `Reclamation execution` for workflow regressions.

- One selected-pool validation pass retains all safety decisions. Replace the
  existing assertion that mandates two cloud-provider refreshes; assert selected
  scope and outcomes, with a narrow call-count check only for the duplication being
  removed.
- Stale planning says empty standby while live state has a workload or activation:
  execution refuses deletion. An unchanged eligible control is still deleted.
- Activation between the final read and delete produces a conditional-delete
  conflict; renewed checks reject deletion. Same-name replacement is protected.
- A batch larger than the concurrency cap never exceeds the cap. Use a gated fake
  delete/read operation and counters rather than sleeps, and prove all eligible
  candidates eventually execute.
- A candidate changes while waiting for a worker: its fresh checks run after it
  acquires the worker and prevent unsafe deletion. Cancellation stops further work.
- A partial delete failure followed by retry preserves successful progress and
  reporting, while allowing remaining eligible candidates to complete.
- Upstream ordinary deletion/replacement and evacuation completion still work.
  If callback removal affects those paths, retain equivalent behavior coverage.

Coordinate stale-view cases with the [read-consistency plan](live-read-test-coverage-plan.md).
Use authoritative writes and separately advanced cached snapshots; avoid tests in
which one fake store unintentionally makes all reads fresh.

## Scope and sequencing

Build on [standby reuse](standby-scheduling-activation-plan.md) and
[transition ownership](standby-transition-ownership-plan.md). The older
[queue execution ownership plan](queue-execution-ownership-plan.md) describes the
earlier helper-ownership cleanup, much of which is already in `c7074ecd`; reconcile
against actual code and do not repeat it or overwrite that plan.

Keep the current reclamation delay, selection of all eligible candidates, stable
ordering, policy eligibility, budget exemption, and pacing exemption. The worker
cap limits concurrent API operations; it is not a per-pass removal cap or a new
retention policy. Do not add minimum warm capacity, demand forecasting, a CRD,
controller, public tuning knob, or general command framework.

Preserve existing uncommitted work, including reclamation-delay terminology. Prefer
editing fork-owned helpers and keep necessary upstream changes narrow.

## Validation and acceptance

During iteration, after adding the proposed regression groups:

```sh
KUBEBUILDER_ASSETS="/Users/rsum/Library/Application Support/io.kubebuilder.envtest/k8s/1.36.2-darwin-arm64" \
K8S_VERSION=1.36.2 \
hack/check-fast.sh --run '^Test(APIs|Reclamation)' \
  --focus 'Reclamation execution|Queue' ./pkg/controllers/disruption
```

Check the reported tests/specs, and choose narrower queue cases if the existing
`Queue` group grows beyond the affected execution behavior. Discover installed
Kubernetes 1.36.2 using
`go tool -modfile=go.tools.mod setup-envtest use -i -p path 1.36.2`, verify executable
`etcd`, `kube-apiserver`, and `kubectl`, and check conflicting `TEST_ASSET_*` and
`USE_EXISTING_CLUSTER` overrides. Follow repository setup instructions if missing.

After all edits and any requested review, the lead runs:

```sh
KUBEBUILDER_ASSETS="/Users/rsum/Library/Application Support/io.kubebuilder.envtest/k8s/1.36.2-darwin-arm64" \
K8S_VERSION=1.36.2 GOTOOLCHAIN=auto \
go test -mod=readonly -count=1 -timeout=20m ./pkg/controllers/disruption

GOTOOLCHAIN=auto \
GOLANGCI_LINT_CACHE="$(git rev-parse --absolute-git-dir)/golangci-lint-cache" \
make verify
```

Add full tests for other affected packages if scope expands. Include changes made
by verification and rerun it. Acceptance requires one coherent planning validation
pass, retained live conditional-delete safety, bounded worker concurrency, and
unchanged policy and retry outcomes. Report commands, counts, and results.
