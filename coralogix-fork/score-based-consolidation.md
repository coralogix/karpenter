# Score-based consolidation

This fork adds an alternate consolidation disruption method for NodePools that opt in via annotation. It works like single-node consolidation except instead of sorting nodes by DisruptionCost it sorts them by `nodePriorityScore`, a search-guidance heuristic that ranks expensive, underused nodes higher (price divided by non-daemon pod CPU/memory requests).

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

## Behavior

- Eligible annotated dynamic NodePools are handled by the score-based consolidation method, which runs after multi-node consolidation and before single-node consolidation.
- The `WhenEmptyOrUnderutilized` policy and dynamic NodePool requirement still apply, but `consolidateAfter` does not gate score-based compaction or reclamation. Its value, including `Never`, has no effect while the score-based annotation is present.
- Annotated NodePools are excluded from emptiness, single-node consolidation, and multi-node consolidation.
- Drift and static drift are unchanged; annotated NodePools continue to use the standard drift methods.
- The method reports `Underutilized` as its disruption reason. Non-empty compaction honors the `Underutilized` disruption budget; empty-node reclamation is exempt from NodePool disruption budgets.
- Non-empty active compaction candidates are sorted by `nodePriorityScore` descending (`price / workloadSize`). `workloadSize` is `cpu_cores + 0.125 × memory_gib` from non-daemon pod requests. The search evaluates up to `runtime.GOMAXPROCS` move sets in parallel. Up to 10 valid commands with positive estimated savings are collected (or fewer on timeout), sorted by score, then the controller waits for any remaining consolidation TTL time since the pass started. The first command that still passes validation is executed. Each pass has a 20-second timeout (single-node consolidation keeps the upstream 3-minute timeout).
- Empty-node reclamation is checked per annotated dynamic NodePool. It is due on the first pass and again after the configured interval following a successful reclamation. The default interval is one minute; the NodePool annotation can override it. A due pass counts all empty, non-terminating nodes, then reclaims up to half rounded up, preferring the highest instance price per vCPU. NodePool disruption budgets do not limit reclamation, while eligibility checks and final live emptiness and activation checks still apply. Nodes with any bound non-DaemonSet pods are not empty, including terminating and terminal pods. A due reclamation batch takes precedence over normal score-based compaction in that NodePool; other NodePools can still compact normally.
- The controller stores the last successful reclamation time in the NodePool annotation `karpenter.coralogix.net/last-reclamation` using RFC3339Nano. This metadata is updated only after an individual NodeClaim deletion succeeds, so a partially completed batch still records its progress.
- Reclamation exposes the unlabeled counter `karpenter_voluntary_disruption_score_based_reclamation_node_removals_total`, incremented after each successful deletion callback and timestamp update. Batch selection details are logged at verbosity 1; a fully completed batch produces one structured Info log with reclamation counts grouped by NodePool. These logs omit node and pod names. Score-based search details are available at verbosity 1, with evaluation failures counted by `karpenter_voluntary_disruption_score_based_move_set_evaluation_errors_total` and summarized once per search with the first error.
- Evacuation records one terminal log per command and exposes `karpenter_voluntary_disruption_evacuation_commands_total{outcome,reason}` plus `karpenter_voluntary_disruption_evacuation_duration_seconds{reason}`. Standby activation records one log when the taint is removed and increments `karpenter_nodes_standby_activated_total{nodepool}` once per successful activation. These metrics use bounded labels; logs omit pod lists.
- Standby NodeClaims carry the `karpenter.coralogix.net/standby` annotation and their Nodes carry a matching `NoSchedule` taint. Reclamation can reclaim an empty standby node when its pool is due. Normal compaction considers only non-empty active nodes and excludes standby and activating nodes. Nodes reserved for activation with the `standby-activating` annotation are excluded from reclamation, and the controller rechecks activation state and emptiness just before deletion.
- Optional underutilized pace annotations (`max-underutilized-node-disruptions-per-minute`, `max-underutilized-nodes-per-consolidation`) apply to non-empty score-based consolidations as well as single- and multi-node consolidation. Empty-node reclamation is not paced, matching upstream `Emptiness` behavior.

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not run score-based consolidation.
- Annotated NodePools resume standard emptiness and underutilized consolidation behavior.
- No manifest or CRD changes are required to roll back.

Removing the annotation from an individual NodePool also returns that pool to the upstream consolidation methods.
