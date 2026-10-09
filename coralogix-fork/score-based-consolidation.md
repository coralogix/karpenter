# Score-based consolidation

This fork adds score-based disruption for NodePools that opt in via annotation. **Compaction** evicts workloads from underused nodes but leaves the emptied node in place (evacuation). **Standby** marks naturally empty active nodes as retained capacity. **Reclamation** deletes all validated standby empty nodes that have completed the standby soak on each disruption pass. Compaction still uses `nodePriorityScore` as a search-guidance heuristic (price divided by non-daemon pod CPU/memory requests). See [Compaction and reclamation design](compaction-and-reclamation-design.md) for the full flow.

| Annotation | Meaning |
|------------|---------|
| `karpenter.coralogix.net/reclamation-standby-delay` | Minimum time empty standby nodes must remain in standby before reclamation (default 15s; `0s` disables) |

NodeClaims in standby store `karpenter.coralogix.net/standby` as the RFC3339Nano UTC time standby began. Legacy `true` values remain supported and skip soak gating.

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

## Behavior

- Disruption order for annotated pools: emptiness (non-score-based pools only) → standby marking → reclamation → drift → multi-node → score-based compaction → single-node.
- Standby marking and reclamation report consolidation type `standby-marking` and `reclamation` in disruption metrics; compaction reports `score-based`.
- Annotated NodePools are excluded from standard emptiness, single-node consolidation, and multi-node consolidation. Empty nodes are handled by standby and reclamation instead.
- Compaction reports `Underutilized` for budget and pacing; standby and reclamation use the `Empty` reason and are budget-exempt. Reclamation is not disruption-paced; compaction respects optional disruption pacing annotations.
- Dynamic and static drift continue to use the standard drift methods and `Drifted` disruption budgets. Optional disruption pacing also limits drift candidates and shares the per-NodePool or group cooldown with compaction.
- Score-based pools ignore `consolidateAfter` for consolidatability; the nodeclaim controller marks initialized nodes consolidatable without waiting on that timer.
- Candidates are sorted by `nodePriorityScore` descending (`price / workloadSize`, or `price` when the node is empty), then evaluated in priority order with up to `runtime.GOMAXPROCS` move sets in parallel. `workloadSize` is `cpu_cores + 0.125 × memory_gib` from non-daemon pod requests. Up to 10 valid commands with positive estimated savings are collected (or fewer on timeout), sorted by score, then the controller waits for any remaining consolidation TTL time since the pass started. The first command that still passes validation is executed. Each pass has a 20-second timeout (single-node consolidation keeps the upstream 3-minute timeout).
- Optional disruption pacing annotations (`karpenter.coralogix.net/disruption-pacing-per-minute` and `karpenter.coralogix.net/disruption-pacing-per-batch`) apply to non-empty score-based, single-node, and multi-node consolidation, plus dynamic and static drift. NodePools can share a limit with `karpenter.coralogix.net/disruption-pacing-group`; empty-node removals are not paced. See [Disruption pacing](disruption-pacing.md) for configuration and behavior.

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not run score-based consolidation.
- Annotated NodePools resume standard emptiness and underutilized consolidation behavior.
- No manifest or CRD changes are required to roll back.
