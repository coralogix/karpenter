# Standby coordinator — design note

Status: Implemented. Relates to [Compaction and reclamation design](compaction-and-reclamation-design.md) and [Score-based consolidation](score-based-consolidation.md).

## Goal

Give standby transitions one owner and make their callers smaller. Provisioning chooses scheduling destinations; disruption chooses compaction, marking, and reclamation candidates. Neither should assemble the shared standby mutation protocol.

The refactor should remove duplicated transition sequences and client wiring. An extra façade over every existing `Lifecycle` method would improve dependency direction but do little to shorten caller code paths.

**Done when:** disruption no longer calls provisioning for standby transitions; production has no reader setters or silent cached-client fallbacks for standby; `markEmptyNodeStandby`, evacuation `persistStandby`, and shared activation logic are replaced by coordinator calls; regression tests for races and partial writes still pass.

## Current problems

- `pkg/utils/standby.Lifecycle` implements activation and optimistic patch primitives, but callers assemble standby entry and recovery themselves.
- Provisioning owns the shared activation entrypoint and activation metrics. Disruption calls `provisioner.ActivateStandbyNodes` for compaction and recovery.
- Provisioner and queue default their API readers to the cached client, then replace them during registration. Disruption registration also sets the provisioner's reader. Marking, reclamation, and compaction have repeated reader fallback helpers.
- `markEmptyNodeStandby` mixes command orchestration with temporary tainting, ordered live reads, standby persistence, rollback, occupied-node recovery, and state refresh.
- Evacuation's `persistStandby` uses cached-client reads while other standby transitions use the API reader.
- Recovery and marking independently reread transitioned objects and refresh internal state. Provisioning instead edits its scheduling snapshot after resuming activation.

The manager is already available in `pkg/controllers/controllers.go` when these dependencies are constructed. Production wiring can supply its API reader there, rather than repairing dependencies during controller registration.

### Today → target (call sites)

| Today | After migration |
|-------|-----------------|
| `Provisioner.ActivateStandbyNodes` / `activateStandbyNode` | `Coordinator.Activate` |
| `Provisioner.resumeStandbyActivations` (startup) | `Coordinator.Repair` or `Activate` with recovery source, then provisioning snapshot cleanup |
| `Queue` compaction activation via provisioner | `Coordinator.Activate` (compaction source) |
| `markEmptyNodeStandby` | `Coordinator.EnterStandby` (empty marking mode) |
| `persistStandby` / `ensureStandbyTaint` after evacuation | `Coordinator.EnterStandby` (post-evacuation mode) |
| `recoverOccupiedStandbyNode`, `controller_standby_recovery` | `Coordinator.Repair` and/or `Activate` with recovery source |
| `updateStandbyState` | Stays in disruption; fed from coordinator results |

Reclamation validation and conditional delete stay in disruption; they only share occupancy helpers with the coordinator.

## Proposed ownership

Add a fork-owned `pkg/standby.Coordinator`, constructed once with the uncached reader, existing write client, and clock.

| Component | Owns |
|-----------|------|
| `pkg/utils/standby` | Annotations, taints, activation sources, and low-level mutation protocol |
| `pkg/standby.Coordinator` | Complete standby transitions, transition live reads, retry/rollback sequencing, and activation reporting |
| Provisioning | Destination selection, activation requests, and its scheduling snapshot |
| Disruption | Pool policy, candidate selection, queue reservations, eviction, reclamation, and cluster-state refresh |

Use `pkg/standby`, rather than adding the coordinator to `pkg/utils/standby`. The current `standbymetrics` package imports `utils/standby`; a coordinator importing those metrics from inside `utils/standby` would create a cycle. Initially the coordinator can use the existing metrics package. Moving metrics to a neutral location is optional follow-up work, not a prerequisite.

Keep `Lifecycle` as the low-level implementation during migration. Its public surface can shrink once controllers stop calling its patch primitives. Do not require both layers to remain permanently if they become redundant.

### Construction and wiring

Create the coordinator in `NewControllers` (or a small fork-owned helper next to it) with `mgr.GetAPIReader()` and the same write `client.Client` used by provisioning and disruption.

- Pass the coordinator into `provisioning.NewProvisioner` and `disruption.NewController` / `disruption.NewQueue` at construction time.
- Keep `disruption.WithNodePoolReader(mgr.GetAPIReader())` (or equivalent) for NodePool policy reads; do not add a fourth wiring path in `Register` / `SetupWithManager`.
- Remove `Provisioner.SetAPIReader`, `Queue.SetAPIReader`, and disruption registration blocks that patch the provisioner's reader after the coordinator exists.
- Provisioning may depend on a narrow `Activator` interface (`Activate` only) if that avoids importing the full coordinator surface.

## Operation boundary

Expose operations whose completion has meaning to the caller:

- **Activate** — reserve activation, persist its source, remove the standby taint, and complete marker cleanup. Callers pass `standby.ActivationSource` (provisioning, compaction, recovery). This is the only path that records activation metrics (at successful taint removal, using the persisted source).
- **EnterStandby** — perform the empty-to-standby handoff: live rechecks, marker and taint writes, rollback sequencing, and occupied-node recovery when a pod binds during the transition. Naturally empty marking and post-evacuation persistence share one internal sequence; callers choose an **entry mode** (see below).
- **RepairStandby** — reconcile interrupted or inconsistent standby state without a full policy pass: complete a stuck activation (reservation + source already present), restore a missing standby taint, remove an orphan standby taint, or fix marker/taint skew. When the correct end state is active capacity (e.g. occupied standby), **Repair** delegates to **Activate** with `ActivationSourceRecovery` rather than duplicating activation steps.

**Activate vs Repair:** Use **Activate** when the caller intentionally selected standby capacity for scheduling or compaction. Use **Repair** when the controller discovers drift or partial work (startup `resumeStandbyActivations`, standby recovery loop, rollback side effects). Repair may return “already consistent” without writes.

These describe the intended boundary, not final Go signatures. Prefer a concrete `Coordinator` type. Avoid interfaces for every operation or a generic transition framework.

Do not expose a second set of `PatchNodeClaimStandby`, `PatchNodeStandbyTaint`, `Get`, and `List` methods on the coordinator. Those would preserve the existing call-site choreography under different names. Live object reads remain implementation details or shared low-level helpers where validation needs them.

### EnterStandby entry modes

Share one protocol implementation; differ only what the contract requires at the edges:

| Mode | Caller context | Coordinator behavior |
|------|----------------|----------------------|
| **Empty marking** | `StandbyMarking`; node already empty in policy | Apply temporary disruption taint as scheduling barrier; run post-taint eligibility; full rollback on failure |
| **Post-evacuation** | Evacuation completed; temporary disruption taint already held by queue | Skip applying barrier taint; use uncached reads for marker/taint writes (fixes cached `persistStandby`); preserve evacuation retry semantics at the queue layer |

Express modes as explicit options on `EnterStandby` (e.g. barrier taint already present), not as separate duplicated functions.

### Caller obligations

The coordinator performs Kubernetes transitions only. It does **not** acquire or release disruption queue reservations, emit standby-marking or evacuation command events, or update `state.Cluster` / the provisioning scheduling snapshot.

- Disruption must hold queue ownership for the candidate for the duration of `EnterStandby` / related repair, as today.
- Callers run `updateStandbyState` (or equivalent) after a **completed** enter outcome before another disruption pass treats the node as standby or active.
- Provisioning applies in-memory snapshot updates only for **completed** activation outcomes in the current scheduling pass.

### Policy and transition preconditions

Disruption keeps score-based opt-in, dynamic-pool eligibility, soak duration, do-not-disrupt rules, nomination checks, and candidate selection. The coordinator owns protocol invariants such as object identity, deletion/activation state, and the occupancy checks needed to complete a transition safely.

Distinguish the definition of occupancy from the decision made from it. Keep one shared definition: bound non-DaemonSet, nonterminal, non-Node-owned workload pods occupy a node. Marking, evacuation, repair, and reclamation must use that same definition (shared helper in `pkg/utils/standby` or `pkg/standby`).

Moving the protocol must preserve policy rechecks at their current mutation boundaries. For example, checking marking eligibility only before `EnterStandby` would lose the live recheck after temporary tainting. A narrow caller-supplied **eligibility** function over freshly read Node, NodeClaim, and pods preserves disruption policy at those boundaries. Keep it specific to standby entry; avoid generic mutation hooks. Failed eligibility means roll back (marking) or skip without persisting standby (evacuation), never silent continuation.

## Live reads remain necessary in disruption

The coordinator encapsulates uncached reads for transitions. This removes the provisioner's reader dependency, but does not eliminate every API reader from disruption.

Disruption still needs live reads for:

- NodePool policy and managed-pool validation;
- compaction candidate validation;
- reclamation occupancy, activation, identity, and final deletion checks.

Supply one explicit reader through disruption's shared dependencies, including the queue where required. Remove repeated per-method fallback helpers and registration-time setters. Do not route unrelated policy reads through a coordinator `Get`/`List` proxy merely to hide the dependency.

Production construction must supply the API reader explicitly. Tests may supply a fake or deliberately stale reader to exercise the relevant consistency boundary. Avoid a silent fallback to the cached client in production.

Optimistic locking remains required. Cached reads can miss concurrent markers or repeatedly supply obsolete resource versions; live reads improve the protocol but do not make multiple Kubernetes objects atomic.

## Results, errors, and in-memory state

The coordinator owns persisted transitions, not `state.Cluster` or the scheduling snapshot. Each operation returns a **transition result** for one node pair (batch APIs return a slice, preserving per-node outcomes when another node fails).

### Outcomes (caller-facing)

| Outcome | Meaning | Typical caller action |
|---------|---------|------------------------|
| **Completed** | Target standby or active state reached on API objects | Refresh snapshot/cluster state; emit success events/metrics owned by caller |
| **Skipped** | Eligibility or policy recheck failed with no durable standby write | No snapshot update; marking may try another candidate |
| **Recovered** | Transition aborted but capacity restored to active (e.g. occupied during enter) | Refresh state; do not emit standby-marked; activation metrics via **Activate** if recovery activated |
| **Unchanged** | Already in desired state (repair no-op) | Optional light refresh only |
| **Failed** | Error after partial writes possible | No “success” snapshot update; **Repair** on a later pass |

Map today's `markEmptyNodeStandby` `(marked bool, err error)` to **Completed** vs **Skipped** / **Recovered** / **Failed**, not to “err == nil” alone.

### Errors

- **Retry at coordinator:** transient API errors and optimistic-lock conflicts on protocol patches (same as today's `retry.OnError` on individual steps where appropriate).
- **Return to caller without wrapping policy:** eligibility failures are outcomes, not necessarily errors.
- **Return error:** unrecoverable API failure, rollback failure, or inconsistent objects after rollback. Callers requeue or surface command failure as today.

A partial write is possible even when an operation returns **Failed**. Do not update a snapshot as though the transition completed; **Repair** must recover from persisted intermediate state. A failed final refresh in the caller remains an error even if the coordinator already completed the API transition.

Record activation metrics only inside **Activate** at the existing successful taint-removal boundary. Retries must not double-count. Keep command-level standby-marking and evacuation reporting with disruption orchestration.

## Safety constraints

This is a refactor of ownership, not a relaxation of concurrency safeguards:

- Preserve live identity checks and the ordering of Node/NodeClaim reads where optimistic patches protect against concurrent activation.
- Preserve patches that act as concurrency barriers even when the desired annotation or taint already matches.
- Preserve activation reservation and source persistence across retries and restarts.
- Preserve queue reservations while marking or evacuation owns a candidate.
- Preserve late-binding recovery and checks for occupied standby capacity.
- Keep reclamation's final live validation adjacent to its UID/resource-version-conditional delete. Do not replace it with an earlier coordinator snapshot.

Kubernetes offers no atomic barrier between pod binding and NodeClaim deletion. The coordinator does not change that limitation or require a durable command resource, additional controller, or new CRD.

## Migration

Each step should merge only when its **exit criteria** are met. Do not expand coordinator API surface if call sites still duplicate the old sequence.

### 1. Extract activation and startup wiring

Move shared activation and activation metrics to the coordinator. Inject at construction; switch compaction, provisioning scheduling activation, and recovery paths that today call `ActivateStandbyNodes`.

**Exit criteria:** no `provisioner.ActivateStandbyNodes` from disruption; no provisioner `apiReader` / `SetAPIReader` for activation; per-node activation errors and provisioning snapshot behavior unchanged; test that activation metric fires once per successful activation across retries.

### 2. Make disruption's live reader explicit

Wire the API reader for reclamation, compaction validation, and standby marking at construction. Delete `apiReader()` fallbacks and registration-time reader setters on queue and provisioner.

**Exit criteria:** single injected reader in disruption tests and production wiring; no cached-client fallback in reclamation or marking validation paths.

### 3. Consolidate entry and repair

Replace `markEmptyNodeStandby`, evacuation `persistStandby` / `ensureStandbyTaint`, and overlapping recovery sequences with `EnterStandby` / `RepairStandby` and shared occupancy helpers. Remove redundant reread/refresh blocks superseded by coordinator results.

**Exit criteria:** measurable deletion of the gocyclo marking path and cached reads in evacuation persistence; policy rechecks still at post-taint boundary; evacuation vs marking retry behavior preserved.

### 4. Trim redundant primitives and tests

Shrink public `Lifecycle` surface where unused. Collapse duplicate unit tests onto coordinator protocol tests; keep controller integration tests for policy, queue ownership, and state refresh.

Step 1 is independently reviewable and improves dependency direction only—it should not be described as a large line-count win. Most shortening happens in step 3.

## Scope and validation

Keep scheduling, score calculation, reclamation policy, and eviction semantics unchanged. Reclamation and its validator are a separate simplification opportunity; the coordinator alone will not remove their candidate reconstruction, policy checks, or deletion guards.

**Regression themes (keep or move, do not drop):**

- Interrupted activation and activation source attribution
- Marking races, rollback after post-taint occupancy, activation winning the race
- Occupied-standby recovery during marking and dedicated recovery loop
- Evacuation persistence and standby taint consistency
- Activation vs reclamation conditional delete (UID/resourceVersion)

Test the shared protocol once in `pkg/standby` where possible; retain disruption integration tests for destination selection, eligibility at boundaries, queue reservations, and `updateStandbyState`. Follow the repository's focused iteration checks and final affected-package validation workflow.

**Stop rule:** If a step adds coordinator methods but cross-controller calls, reader setters, or duplicated transition sequencing remain, revise the boundary before merging.
