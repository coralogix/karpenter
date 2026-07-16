# Coralogix Fork

This document describes Coralogix-specific extensions in this Karpenter fork. These features are implemented without changes to NodePool CRDs or specs, so they can be rolled back to upstream Karpenter without modifying manifests.

Warning: At the moment this doc is mostly LLM generated (quality may vary)

## Underutilized consolidation pace

By default, Karpenter limits underutilized consolidation concurrency through disruption budgets (`spec.disruption.budgets`), not through a per-minute disruption rate. This fork adds an optional per-NodePool pace limit for underutilized consolidation only.

### Configuration

Set both annotations on a `NodePool`:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: example
  annotations:
    karpenter.coralogix.net/max-underutilized-node-disruptions-per-minute: "1"
    karpenter.coralogix.net/max-underutilized-nodes-per-consolidation: "2"
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 0s
```

| Annotation | Meaning |
|------------|---------|
| `max-underutilized-node-disruptions-per-minute` | Maximum disrupted candidate nodes per minute for the NodePool |
| `max-underutilized-nodes-per-consolidation` | Maximum candidate nodes in one underutilized consolidation command |

| Configuration | Meaning |
|---------------|---------|
| Both omitted | No rate limit beyond disruption budgets (upstream behavior) |
| Both present and valid | Node-weighted pacing and per-command batch cap apply |
| Only one present, zero, negative, or invalid | Fail closed for underutilized consolidation on that NodePool |

Fractional rates are supported for `max-underutilized-node-disruptions-per-minute`.

Example values:

| Rate | Meaning |
|------|---------|
| `1` | On average, one disrupted node per minute |
| `0.5` | On average, one disrupted node every two minutes |
| `2` | On average, two disrupted nodes per minute |

### Behavior

- Applies only to underutilized consolidation (single-node and multi-node).
- Does not affect emptiness, drift, expiration, or disruption budgets.
- Each successfully started consolidation command charges one unit per disrupted candidate node per participating NodePool. Delete and replace commands both charge existing candidate nodes, not net node reduction.
- A command with `N` candidate nodes at rate `R` advances that NodePool's next eligible time by `N / R` minutes.
- `max-underutilized-nodes-per-consolidation` caps how many candidate nodes from a NodePool may appear in one multi-node command. Excess candidates are excluded and may be handled by later consolidation passes, including single-node fallback.
- Pacing is enforced in memory and resets on controller restart or leader failover. After restart, a NodePool may immediately disrupt up to `max-underutilized-nodes-per-consolidation` candidate nodes in the first command.
- When paced, consolidation is deferred without marking the cluster consolidated, similar to budget exhaustion.
- If queue startup fails after admission, the pace charge is rolled back.

### Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotations remain valid Kubernetes metadata.
- Upstream ignores the annotations and does not enforce pacing.
- No manifest or CRD changes are required to roll back.

The only effect of rollback is loss of rate limiting; consolidation resumes at upstream speed subject to disruption budgets.
