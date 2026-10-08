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

## Behavior

- NodePools with the annotation are handled by the score-based consolidation method, which runs after multi-node consolidation and before single-node consolidation.
- Annotated NodePools are excluded from emptiness, single-node consolidation, and multi-node consolidation.
- Dynamic and static drift continue to use the standard drift methods and `Drifted` disruption budgets. Optional disruption pacing also limits drift candidates and shares the per-NodePool or group cooldown with consolidation.
- The method reports `Underutilized` as its disruption reason for budget accounting. Known limitation: empty nodes in annotated pools currently fail score-based candidate revalidation, while the standard `Empty` method excludes those pools, so they are not consolidated.
- Candidates are sorted by `nodePriorityScore` descending (`price / workloadSize`, or `price` when the node is empty), then evaluated in priority order with up to `runtime.GOMAXPROCS` move sets in parallel. `workloadSize` is `cpu_cores + 0.125 × memory_gib` from non-daemon pod requests. Up to 10 valid commands with positive estimated savings are collected (or fewer on timeout), sorted by score, then the controller waits for any remaining consolidation TTL time since the pass started. The first command that still passes validation is executed. Each pass has a 20-second timeout (single-node consolidation keeps the upstream 3-minute timeout).
- Optional disruption pacing annotations (`karpenter.coralogix.net/disruption-pacing-per-minute` and `karpenter.coralogix.net/disruption-pacing-per-batch`) apply to non-empty score-based, single-node, and multi-node consolidation, plus dynamic and static drift. NodePools can share a limit with `karpenter.coralogix.net/disruption-pacing-group`; empty-node removals are not paced. See [Disruption pacing](disruption-pacing.md) for configuration and behavior.

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not run score-based consolidation.
- Annotated NodePools resume standard emptiness and underutilized consolidation behavior.
- No manifest or CRD changes are required to roll back.
