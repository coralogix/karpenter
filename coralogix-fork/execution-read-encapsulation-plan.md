# Encapsulate disruption execution reads in Queue

Status: implemented.

## Summary

Make `Queue` the owner of disruption execution and its live reads. Planning methods
produce commands using cached state; they no longer obtain readers or invoke
transition logic through `Queue`.

Preserve current behavior and the existing standby coordinator. Add no executor
object, dependency container, or reconciliation state machine. Leave the
test-expansion plan deferred.

## Implementation changes

### Move transition operations out of planning methods

- Move standby marking's execution code into `queue_standby.go`: transition
  eligibility, coordinator invocation, cluster-state refresh, and success reporting.
- Use `q.apiReader`, `q.kubeClient`, `q.cluster`, and `q.clock` directly inside these
  operations. Keep NodePool policy reads cached and occupancy checks live.
- Retain the existing command-method check for standby actions, but stop passing a
  `StandbyMarking` instance into execution helpers.
- Leave `StandbyMarking` responsible for cached candidate filtering and command
  construction. Remove its `executionReader()` accessor.

### Make reclamation safety an execution responsibility

- Move final reclamation checks into `queue_reclamation.go`, retaining their
  ordering, candidate refreshes, error classification, and NotFound behavior.
- Remove `Command.BeforeDelete` and `Command.DeleteWithPreconditions`, including
  their assignments in reclamation planning.
- Identify reclamation using the existing `cmd.Method` concrete type. Use that same
  classification to select both final validation and UID/resource-version
  conditional deletion; add no new command action or flag.
- Validate immediately before each pending candidate's deletion attempt. After a
  conflict, the next reconciliation must repeat validation before retrying deletion.
- Preserve ordinary deletion behavior for other methods and the existing
  per-candidate completion phases.
- Leave reporting callbacks (`OnSuccess`, `OnDeleteSuccess`) and their retry
  semantics intact. They do not expose live readers.
- Remove `Reclamation.executionReader()`. Reclamation planning and validation
  retain only cached reads.

### Put recovery behind Queue operations

- Move stale-disruption cleanup, occupied-standby recovery, standby-taint
  restoration, and post-transition refresh into `queue_standby_recovery.go`.
- Have the controller invoke `q.cleanupStaleDisruptionState(ctx)` at the same point
  in reconciliation. Keep controller-specific inventory refresh and budget
  selection on the controller.
- Remove `Controller.apiReader`, its option field, `WithAPIReader`, the fallback
  initialization, and production option wiring.
- Keep `NewQueue` and the shared `standby.Coordinator` constructor contracts
  unchanged. Production still supplies the API reader directly to those execution
  owners.
- Preserve queue membership checks, recovery ordering, conflict handling, and
  existing lock boundaries.

## Acceptance criteria and tests

- Neither planning methods nor the controller access `queue.apiReader` or
  `queue.standbyCoordinator`; they invoke execution operations.
- No `executionReader()`, `WithAPIReader`, `BeforeDelete`, or
  `DeleteWithPreconditions` remains.
- Planning retains its existing queue dependency for candidate/in-flight
  validation; removing that unrelated dependency is outside scope.
- Live/cached read choices, transition protocols, delete preconditions, metrics,
  events, and retry behavior remain unchanged.

Adapt existing tests rather than creating another fixture framework:

- Update standby and recovery tests to use Queue-owned operations and the reduced
  controller construction.
- Update reclamation tests to exercise Queue-owned validation.
- Remove callback instrumentation from deletion-race fixtures. Retain assertions
  on delete attempts, requeue/completion, activation state, and same-name
  replacement survival.
- Existing reclamation commands must receive final checks and delete preconditions
  solely from their method identity.
- Retain ordinary deletion, callback-retry, late pod-binding, and
  evacuation-completion coverage. Do not implement the deferred separate-cache
  test matrix.

## Validation and constraints

- Preserve uncommitted work and place moved implementation in fork-owned files.
- During edits, run `hack/check-fast.sh` with explicit disruption-package unit
  filters and Ginkgo focus for `Queue` and relevant score-based specs. Check
  reported counts.
- After all edits, run full tests for `./pkg/controllers/disruption` and
  `./pkg/controllers`.
- Use Kubernetes **1.36.2**, setting both `KUBEBUILDER_ASSETS` and `K8S_VERSION` for
  every test invocation; verify installed executables and exclude conflicting
  overrides.
- Finish with:

  ```sh
  GOTOOLCHAIN=auto \
  GOLANGCI_LINT_CACHE="$(git rev-parse --absolute-git-dir)/golangci-lint-cache" \
  make verify
  ```

- Follow repository rerun requirements and report commands/results. No CRD,
  configuration, dependency, or persisted-state migration is required.
