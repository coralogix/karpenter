# Score-based consolidation

This fork adds an alternate compaction method for NodePools that opt in via annotation. It sorts non-empty active nodes by `nodePriorityScore`, a search-guidance heuristic that ranks expensive, underused nodes higher (price divided by non-daemon pod CPU/memory requests).

The annotation opts a NodePool into this behavior. Score-based consolidation applies only to dynamic NodePools whose `consolidationPolicy` is `WhenEmptyOrUnderutilized`. In score-based mode, `consolidateAfter` is ignored entirely, including `Never`. Removing the score-based annotation returns the pool to upstream consolidation behavior. An annotated pool with another policy does not run score-based consolidation; remove the annotation to use upstream consolidation for that pool.

## Configuration

Add the annotation to a `NodePool`:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: example
  annotations:
    karpenter.coralogix.net/score-based-consolidation: ""
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 0s # Required by the NodePool schema; ignored by score-based mode
```

| Annotation | Meaning |
|------------|---------|
| `karpenter.coralogix.net/score-based-consolidation` | Opt the NodePool into score-based consolidation (presence of the key is sufficient; value is ignored) |
| `karpenter.coralogix.net/reclamation-interval` | Minimum Go duration between successful empty-node reclamations; defaults to `1m`. Invalid or non-positive values use the default. |
| `karpenter.coralogix.net/utilisation-weight` | Weight for the CPU slack term in move-set priority scoring, in m$/vCPU/h; defaults to `1`. Invalid or non-positive values use the default. |

## Behavior

- Eligible annotated dynamic NodePools use two methods: `ScoreBasedReclamation` immediately after upstream `Emptiness`, and `ScoreBasedConsolidation` after multi-node consolidation and before single-node consolidation. Upstream emptiness, multi-node, and single-node methods continue to skip annotated pools.
- The `WhenEmptyOrUnderutilized` policy and dynamic NodePool requirement still apply, but `consolidateAfter` does not gate score-based compaction or reclamation. Its value, including `Never`, has no effect while the score-based annotation is present.
- For an initialized NodeClaim in an annotated dynamic `WhenEmptyOrUnderutilized` pool, the `Consolidatable` condition is set without waiting for `consolidateAfter`. This condition indicates timing eligibility; it does not guarantee that a feasible, profitable move exists or that budgets and pacing allow one now.
- Reclamation runs before drift and multi-node consolidation, so it can act without waiting for their calculations. When it starts a command, the normal method boundary ends that controller pass and schedules a retry from the top. Score-based compaction is reconsidered on that retry, subject to methods earlier in the order; when reclamation finds no command, the controller continues through the remaining methods in the same pass.
- Drift and static drift are unchanged; annotated NodePools continue to use the standard drift methods.
- `ScoreBasedConsolidation` handles only non-empty active compaction candidates. It reports disruption reason `Underutilized` and honors the `Underutilized` NodePool disruption budget and optional underutilized pacing. Empty-node reclamation is handled by its own method and reports `Empty`; it is exempt from NodePool disruption budgets and underutilized pacing.
- Non-empty active compaction candidates are sorted by `nodePriorityScore` descending (`price / workloadSize`) to guide parallel search. `workloadSize` is `cpu_cores + 0.125 × memory_gib` from non-daemon pod requests. The search evaluates move sets in parallel with up to `runtime.GOMAXPROCS` workers. Valid simulated commands are ranked by `moveSetPriorityScore`: simulated hourly savings in m$/h minus replacement cost, divided by allocatable CPU on removed nodes (m$/vCPU/h), plus `utilisation-weight` (m$/vCPU/h, default `1`) times unallocated CPU over allocatable CPU per removed node. After search completes or reaches its deadline, the method keeps the top 10 by that score, then the controller waits for any remaining consolidation TTL time since the pass started. The first command that still passes validation is executed. Each pass has a 15-second timeout (single-node consolidation keeps the upstream 3-minute timeout).
- **Temporary (experimental):** `ExperimentalReclamationRemoveAllEmptyImmediately` in `score_based_reclamation.go` is currently `true`. While enabled, every pass reclaims all eligible empty nodes in each configured pool (no half-batch or interval gate). Revert to interval + half-batch pacing when reclamation policy is finalized.
- Empty-node reclamation is checked per annotated dynamic NodePool. When experimental immediate reclamation is off, it is due on the first pass and again after the configured interval following a successful reclamation. The default interval is one minute; the NodePool annotation can override it. A due pass counts empty, non-terminating nodes, then reclaims up to half rounded up (or all empties while the experimental flag is on), preferring the highest instance price per vCPU. NodePool disruption budgets do not limit reclamation, while eligibility checks and final live emptiness and activation checks still apply. Nodes with any bound non-DaemonSet pods are not empty, including terminating and terminal pods. When the standalone method returns a reclamation command, the controller ends the pass at the normal method boundary and requeues.
- The controller stores the last successful reclamation time in the NodePool annotation `karpenter.coralogix.net/last-reclamation` using RFC3339Nano. This metadata is updated only after an individual NodeClaim deletion succeeds, so a partially completed batch still records its progress.
- Reclamation exposes the counter `karpenter_voluntary_disruption_score_based_reclamation_node_removals_total` labeled by `nodepool`, incremented after each successful deletion callback and timestamp update. Batch selection details are logged at verbosity 1; a fully completed batch produces one structured Info log with reclamation counts grouped by NodePool. These logs omit node and pod names. Score-based search details are available at verbosity 1, with evaluation failures counted by `karpenter_voluntary_disruption_score_based_move_set_evaluation_errors_total` and summarized once per search with the first error.
- Score-based compaction emits one Info summary whenever an error-free pass returns no command. Its reason distinguishes a consolidated cluster, no eligible or filtered candidates, budget or pace filtering, a timed-out search, no positive or feasible moves, and top moves rejected during validation. The summary includes candidate and evaluation counts, no-op and non-positive-savings counts, validation attempts, and sorted NodePool names blocked by budgets or pace limits, including when other candidates were evaluated. When candidate discovery returns none, the controller logs the no-eligible-candidates summary before skipping command computation.
- Score-based compaction emits `ConsolidationCandidate` events on a Node and NodeClaim only for the move selected after validation. A move rejected during final validation emits `ConsolidationRejected`, including when a lower-ranked move is selected instead. A feasible simulated move with no positive estimated savings emits a deduplicated `Unconsolidatable` event. Existing disruption and scheduling blockers continue to emit their usual events. An effective `Underutilized` budget exhausted by in-flight disruptions, or a configured pace limit blocking compaction, emits a deduplicated `DisruptionBlocked` event on the NodePool. Lower-ranked feasible moves do not receive blocker events because they remain eligible for a later pass.
- Evacuation records one terminal log per command and exposes `karpenter_voluntary_disruption_evacuation_commands_total{outcome,reason}` plus `karpenter_voluntary_disruption_evacuation_duration_seconds{reason}`. Successful evacuation also increments `karpenter_nodeclaims_evacuated_total{reason,nodepool,capacity_type}` once per distinct source NodeClaim handed off to standby; failed evacuations do not increment it. This complements `karpenter_nodeclaims_disrupted_total`, which counts NodeClaim deletions, while the command counter remains one count per evacuation command. Standby activation records one log when the taint is removed and increments `karpenter_nodes_standby_activated_total{nodepool,instance_type,source}` once per successful activation. `source` identifies the demand path (`provisioning` or `compaction`); `recovery` identifies an interrupted activation whose original source was not recorded. A missing instance-type label is reported as `unknown`.
- `karpenter_nodepools_empty_nodes{nodepool,state}` gauges the current number of empty nodes in each NodePool using score-based reclamation, including when reclamation is not due. `state` is `standby` for nodes with both the persistent standby marker and standby taint, and `other` for all remaining empty nodes. Both states are reported at zero when applicable. The gauge uses reclamation's live emptiness definition and includes empty nodes that cannot currently be reclaimed. Its series disappear when a NodePool leaves score-based reclamation or is deleted.
- Standby NodeClaims carry the `karpenter.coralogix.net/standby` annotation and their Nodes carry a matching `NoSchedule` taint. Reclamation can reclaim an empty standby node when its pool is due. Normal compaction considers only non-empty active nodes and excludes standby and activating nodes. Nodes reserved for activation with the `standby-activating` annotation are excluded from reclamation, and the controller rechecks activation state and emptiness just before deletion.
- Optional underutilized pace annotations (`max-underutilized-node-disruptions-per-minute`, `max-underutilized-nodes-per-consolidation`) apply to non-empty score-based compaction as well as single- and multi-node consolidation. Empty-node reclamation reports `Empty` and is not paced, matching upstream `Emptiness` behavior.

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not run score-based consolidation.
- Annotated NodePools resume standard emptiness and underutilized consolidation behavior.
- No manifest or CRD changes are required to roll back.

Removing the annotation from an individual NodePool also returns that pool to the upstream consolidation methods.
