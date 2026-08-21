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
- Drift and static drift are unchanged; annotated NodePools continue to use the standard drift methods.
- The method reports `Underutilized` as its disruption reason for budget accounting. Empty-node removals on annotated pools also consume the `Underutilized` budget, not the `Empty` budget.
- Candidates are sorted by `nodePriorityScore` descending (`price / workloadSize`, or `price` when the node is empty), then evaluated in priority order with up to `runtime.GOMAXPROCS` move sets in parallel. `workloadSize` is `cpu_cores + 0.125 × memory_gib` from non-daemon pod requests. The first valid command with positive estimated savings wins. Each pass has a 20-second timeout (single-node consolidation keeps the upstream 3-minute timeout).
- Optional underutilized pace annotations (`max-underutilized-node-disruptions-per-minute`, `max-underutilized-nodes-per-consolidation`) apply to non-empty score-based consolidations as well as single- and multi-node consolidation. Empty-node removals are not paced (matching upstream `Emptiness` behavior).

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not run score-based consolidation.
- Annotated NodePools resume standard emptiness and underutilized consolidation behavior.
- No manifest or CRD changes are required to roll back.
