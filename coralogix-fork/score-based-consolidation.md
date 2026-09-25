# Score-based consolidation

This fork adds an alternate consolidation disruption method for NodePools that opt in via annotation. It works like single-node consolidation except instead of sorting nodes by DisruptionCost it sorts them by `nodePriorityScore`, a search-guidance heuristic that ranks expensive, underused nodes higher (price divided by non-daemon pod CPU/memory requests).

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
    consolidateAfter: 0s
```

| Annotation | Meaning |
|------------|---------|
| `karpenter.coralogix.net/score-based-consolidation` | Opt the NodePool into score-based consolidation (presence of the key is sufficient; value is ignored) |
| `karpenter.coralogix.net/reclamation-interval` | Minimum Go duration between successful empty-node reclamation batches; defaults to `2m`. Invalid or non-positive values use the default. |

## Behavior

- NodePools with the annotation are handled by the score-based consolidation method, which runs after multi-node consolidation and before single-node consolidation.
- Annotated NodePools are excluded from emptiness, single-node consolidation, and multi-node consolidation.
- Drift and static drift are unchanged; annotated NodePools continue to use the standard drift methods.
- The method reports `Underutilized` as its disruption reason for budget accounting. Empty-node removals on annotated pools also consume the `Underutilized` budget, not the `Empty` budget.
- Candidates are sorted by `nodePriorityScore` descending (`price / workloadSize`, or `price` when the node is empty), then evaluated in priority order with up to `runtime.GOMAXPROCS` move sets in parallel. `workloadSize` is `cpu_cores + 0.125 × memory_gib` from non-daemon pod requests. Up to 10 valid commands with positive estimated savings are collected (or fewer on timeout), sorted by score, then the controller waits for any remaining consolidation TTL time since the pass started. The first command that still passes validation is executed. Each pass has a 20-second timeout (single-node consolidation keeps the upstream 3-minute timeout).
- Empty-node reclamation is checked per annotated dynamic NodePool. It is due on the first pass and again after the configured interval following a successful removal. A due pass counts all empty, non-terminating nodes, then removes up to half rounded up, preferring the highest instance price per vCPU and respecting the NodePool's `Underutilized` budget. Nodes with bound non-DaemonSet pods are not empty, including terminating pods. A due reclamation batch takes precedence over normal score-based compaction in that NodePool; other NodePools can still compact normally.
- The controller stores the last successful removal time in the NodePool annotation `karpenter.coralogix.net/last-reclamation` using RFC3339Nano. This metadata is updated only after an individual NodeClaim deletion succeeds, so a partially completed batch still records its progress.
- Reclamation exposes the unlabeled counter `karpenter_voluntary_disruption_score_based_reclamation_node_removals_total`, incremented after each successful deletion callback and timestamp update. Batch selection details are logged at verbosity 1; a fully completed batch produces one structured Info log with removal counts grouped by NodePool. These logs omit node and pod names. Score-based search details are available at verbosity 1, with evaluation failures counted by `karpenter_voluntary_disruption_score_based_move_set_evaluation_errors_total` and summarized once per search with the first error.
- Evacuation records one terminal log per command and exposes `karpenter_voluntary_disruption_evacuation_commands_total{outcome,reason}` plus `karpenter_voluntary_disruption_evacuation_duration_seconds{reason}`. Standby activation records one log when the taint is removed and increments `karpenter_nodes_standby_activated_total{nodepool}` once per successful activation. These metrics use bounded labels; logs omit pod lists.
- Standby NodeClaims carry the `karpenter.coralogix.net/standby` annotation and their Nodes carry a matching `NoSchedule` taint. Reclamation can remove an empty standby node when its pool is due. Nodes reserved for activation with the `standby-activating` annotation are excluded, and the controller rechecks activation state and emptiness just before deletion.
- Optional underutilized pace annotations (`max-underutilized-node-disruptions-per-minute`, `max-underutilized-nodes-per-consolidation`) apply to non-empty score-based consolidations as well as single- and multi-node consolidation. Empty-node removals are not paced (matching upstream `Emptiness` behavior).

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not run score-based consolidation.
- Annotated NodePools resume standard emptiness and underutilized consolidation behavior.
- No manifest or CRD changes are required to roll back.
