# Coralogix Fork

This document describes Coralogix-specific extensions in this Karpenter fork. These features are implemented without changes to NodePool CRDs or specs, so they can be rolled back to upstream Karpenter without modifying manifests.

Warning: At the moment this doc is mostly LLM generated (quality may vary)

## Underutilized consolidation pace

By default, Karpenter limits underutilized consolidation concurrency through disruption budgets (`spec.disruption.budgets`), not through a per-minute start rate. This fork adds an optional per-NodePool pace limit for underutilized consolidation only.

### Configuration

Set the following annotation on a `NodePool`:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: example
  annotations:
    karpenter.coralogix.net/max-underutilized-consolidations-per-minute: "0.5"
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 0s
```

| Value | Meaning |
|-------|---------|
| Omitted | No rate limit beyond disruption budgets (upstream behavior) |
| `1` | At most one underutilized consolidation command per minute |
| `0.5` | On average, one command every two minutes |
| `0`, negative, or invalid | Fail closed for underutilized consolidation on that NodePool |

Fractional values are supported.

### Behavior

- Applies only to underutilized consolidation (single-node and multi-node).
- Does not affect emptiness, drift, expiration, or disruption budgets.
- Each successfully started consolidation command counts once per participating NodePool, regardless of how many nodes a multi-node command removes.
- Pacing is enforced in memory and resets on controller restart or leader failover; one immediate consolidation may be allowed after restart.
- When paced, consolidation is deferred without marking the cluster consolidated, similar to budget exhaustion.

### Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not enforce pacing.
- No manifest or CRD changes are required to roll back.

The only effect of rollback is loss of rate limiting; consolidation resumes at upstream speed subject to disruption budgets.
