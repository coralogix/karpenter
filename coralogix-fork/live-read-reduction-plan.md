# Reduce live reads in disruption planning

Status: implemented.

## Summary

Use cached state for planning, metrics, and policy evaluation, following upstream's
consistency model. Retain live reads for synchronous standby operations, evacuation
completion, and final reclamation safety checks.

The selected scope is an incremental reduction in API calls: cache planning and
policy, including pacing, while keeping the remaining execution wiring and
synchronous transition design. The later recommendation to encapsulate execution
reads is recorded separately below; it has not replaced this scope.

## Implementation changes

### Use cached reads for planning and policy

- Switch standby candidate filtering, reclamation inventory/metrics, reclamation
  candidate selection, and reclamation command validation to `kubeClient`.
- Switch score-based compaction's additional candidate validation to `kubeClient`.
  Preserve its identity, activation, deletion, and occupancy checks; rename helpers
  and error messages that currently describe these reads as "live."
- Use `kubeClient` for NodePool policy reads throughout these paths, including
  standby eligibility and final reclamation policy checks.
- Preserve existing validation passes, delays, nomination checks, occupancy
  definitions, and error handling. Change the source of observations without
  removing checks.
- Do not fall back to live reads when cached reads fail or return NotFound.

### Simplify pacing admission

- Remove `nodePoolReader`, `WithNodePoolReader`, and their production wiring.
- Keep the final pacing refresh immediately before admission, using the same
  cached-client refresh implementation as planning.
- Preserve group consistency checks, cooldown accounting, batch caps, and static
  reservation release.
- Configuration changes and deleted NodePools take effect when observed by the
  cache. Do not promise immediate enforcement of API updates.

### Keep live reads at execution boundaries

- Preserve live Node/NodeClaim/pod reads immediately before reclamation deletion,
  including the ordering of checks and UID/resource-version delete preconditions.
- Preserve coordinator transition reads, optimistic patches, recovery, and
  post-transition cluster-state refreshes.
- Preserve live evacuation pod polling: its empty result currently directly
  authorizes standby completion, and the post-evacuation transition does not
  independently recheck occupancy.
- Remove redundant Node/NodeClaim rereads from standby marking's eligibility
  callback. Evaluate the live pair already supplied by the coordinator; retain
  live occupancy checks and use cached NodePool policy.
- Remove the score-based method's now-unused `apiReader()` accessor. Retain the
  reader fields/accessors still needed for execution; do not introduce a shared
  dependency container or new service abstraction in this incremental scope.

Document the cached-policy behavior and the remaining execution consistency
requirements in the existing fork documentation.

## Tests and acceptance criteria

Use separate cached and authoritative test views so cache lag is represented
explicitly.

- Planning, inventory, and command validation perform no live reads, while
  retaining rejection behavior for changes visible in the cache.
- Pacing uses the cached policy until that view advances, then enforces the updated
  configuration. Adapt the existing missing-NodePool admission regression to
  change the cached view between planning and admission.
- A stale empty-node observation cannot bypass final live checks: a newly bound
  pod or activation blocks reclamation or prevents an incorrect standby
  transition.
- Existing same-name replacement, concurrent activation, optimistic conflict, and
  late pod-binding regressions continue to pass.
- Standby eligibility uses the coordinator-provided pair without another pair
  read, while preserving checks before and after the scheduling barrier.
- Evacuation continues waiting for occupying pods to disappear before completing.

Keep tests focused on these consistency boundaries; avoid blanket call-count
assertions over the entire transition protocol.

## Validation and constraints

- During implementation, use `hack/check-fast.sh` with explicit affected packages
  and narrow unit filters for the affected regressions. Use Ginkgo focus covering
  `Shared disruption pacing`, `Non-empty node disruption pacing`,
  `ScoreBasedConsolidation`, and the relevant evacuation specs. Check reported
  test/spec counts.
- Final validation: full tests for `./pkg/controllers/disruption` and
  `./pkg/controllers`, plus any additional packages actually changed, followed by:

  ```sh
  GOTOOLCHAIN=auto \
  GOLANGCI_LINT_CACHE="$(git rev-parse --absolute-git-dir)/golangci-lint-cache" \
  make verify
  ```

- Every test invocation uses both `KUBEBUILDER_ASSETS` and `K8S_VERSION=1.36.2`,
  with conflicting `TEST_ASSET_*` overrides unset. Do not use an existing cluster.
  Discover installed assets with
  `go tool -modfile=go.tools.mod setup-envtest use -i -p path 1.36.2` and verify that
  `etcd`, `kube-apiserver`, and `kubectl` are executable. At planning time, the
  verified directory was
  `/Users/rsum/Library/Application Support/io.kubebuilder.envtest/k8s/1.36.2-darwin-arm64`.
- Preserve existing uncommitted work. Prefer fork-owned files. No CRD changes,
  feature flags, transition state-machine redesign, or independent review
  delegation are included.
- Report validation commands, results, and the deliberate change to cached policy
  propagation. Follow the repository's final-validation rerun requirements if
  checks modify files or subsequent edits affect validated behavior.

## Complexity assessment and latest recommendation

The incremental plan simplifies consistency reasoning and reduces API traffic,
but only modestly simplifies structure. `Controller` and `Queue` still hold
readers, and standby marking and reclamation still obtain execution readers
through `Queue`.

The latest recommendation is to extend the scope with narrowly scoped execution
encapsulation: execution operations should own their live reads, so planning
methods no longer retrieve readers through `Queue`. Keep synchronous transitions
and their safety checks; avoid a general dependency container or a broad state
machine rewrite.

That extension would address the original concern about dependencies passing
through multiple layers more directly. Its concrete interfaces and ownership
changes still need to be planned before implementation; it is recorded here as
the recommended next refinement, not as an already agreed implementation step.
