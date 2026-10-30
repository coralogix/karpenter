# Score-based consolidation

This fork adds an alternate consolidation disruption method for NodePools that opt in via annotation. It works like single-node consolidation except instead of sorting nodes by DisruptionCost it sorts them by `nodePriorityScore`, a search-guidance heuristic that ranks expensive, underused nodes higher (price divided by non-daemon pod CPU/memory requests).

## NodePool configuration

You configure standby and score-based consolidation with **NodePool metadata annotations**. Set only the keys in this table; do not copy controller-written marks from a live node onto a NodePool.

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
| `karpenter.coralogix.net/reclamation-delay` | Go duration a node must stay in standby before reclamation may delete it. Absent or invalid values default to **15s**. `0s` or negative values mean no minimum age and no mandatory extra pass. |

## Standby and reclamation

On opted-in dynamic NodePools, empty nodes are marked **standby** instead of being deleted immediately. Standby marking and reclamation require `spec.disruption.consolidateAfter` to be set; if it is unset, they are disabled entirely even when the score-based annotation is present. **Reclamation** deletes a standby node after the reclamation delay if it is still standby and still empty. Reclamation uses the `Empty` disruption reason and `Empty` budgets; it is not disruption-paced. Standby marking evicts nothing and does not consume disruption budget.

### Standby marks (set by Karpenter)

When a node enters standby, Karpenter writes the marks below. **Do not set or edit these in manifests**; they are observability and control state on live Nodes. Reactivation and disruption repair clear them when the node leaves standby.

| Mark | Object | Set by | Meaning |
|------|--------|--------|---------|
| `karpenter.coralogix.net/standby:NoSchedule` | Node (taint) | Disruption | Ordinary pods cannot bind while the node is standby. |
| `karpenter.coralogix.net/standby` | Node (annotation) | Disruption | Standby start time in **RFC3339Nano UTC** (for example `2026-10-27T11:00:00.123456789Z`). Reclamation compares this timestamp to the pool’s `reclamation-delay`. The value survives a Karpenter restart so time already spent in standby still counts. If the taint is present but the annotation is missing or not an RFC3339Nano timestamp, disruption backfills the timestamp with now. A positive delay starts from that timestamp. With zero or negative delay, same-pass reclamation is permitted if the timestamp is visible in the cache, but is not guaranteed. |

Entering standby writes the taint (with no value) and timestamp together in one Node update; reactivation and repair clear both together. The taint determines whether a node is standby. Repair clears an annotation without a taint; it cannot make the node reclaimable. Later standby entry writes a fresh timestamp. Repair also clears standby when a reschedulable pod appears or the NodePool stops qualifying.

To inspect standby state: `kubectl describe node <name>` for the taint, and `kubectl get node <name> -o jsonpath='{.metadata.annotations.karpenter\.coralogix\.net/standby}'` for the timestamp.

### Reactivation

Provisioning and disruption treat a standby node as existing capacity. When choosing where pods fit, the scheduling simulation ignores the standby taint and keeps every other taint, resource, affinity, and topology constraint. When Karpenter acts on a result that places pods on a standby node, it clears the taint and the timestamp so the pods can bind there, and does not launch a new node for them. This applies to provisioning, capacity-buffer pods, and every disruption method that moves pods (consolidation and drift included). A simulation that is not acted on leaves standby unchanged.

- Disruption clears standby on its destinations before it taints the source nodes. If clearing fails, the command does not start and a later pass retries.
- Nothing is reserved: once standby is cleared, any pod may bind. A node reactivated for a real pod is nominated for the usual nomination window, so standby marking does not hide it again before the pods arrive. Capacity-buffer pods do not nominate the node; buffer accounting keeps it from being marked standby again.
- A restart during the nomination window may mark the node standby again; a later scheduling pass reactivates it.

Non-empty score-based consolidation still deletes its source node. `WhenEmpty` score-based pools only mark/reclaim empty nodes (no evacuation of nodes that still have pods).

## Behavior

- NodePools with the annotation are handled by the score-based consolidation method, which runs after multi-node consolidation and before single-node consolidation.
- Annotated NodePools are excluded from emptiness, single-node consolidation, and multi-node consolidation.
- Dynamic and static drift continue to use the standard drift methods and `Drifted` disruption budgets. Optional disruption pacing also limits drift candidates and shares the per-NodePool or group cooldown with consolidation.
- The method reports `Underutilized` as its disruption reason for budget accounting. Empty nodes are handled by standby marking and reclamation instead of score-based consolidation or standard emptiness.
- Candidates are sorted by `nodePriorityScore` descending (`price / workloadSize`, or `price` when the node is empty), then evaluated in priority order with up to `runtime.GOMAXPROCS` move sets in parallel. `workloadSize` is `cpu_cores + 0.125 × memory_gib` from non-daemon pod requests. Up to 10 valid commands with positive estimated savings are collected (or fewer on timeout), sorted by score, then the controller waits for any remaining consolidation TTL time since the pass started. The first command that still passes validation is executed. Each pass has a 20-second timeout (single-node consolidation keeps the upstream 3-minute timeout).
- Optional disruption pacing annotations (`karpenter.coralogix.net/disruption-pacing-per-minute` and `karpenter.coralogix.net/disruption-pacing-per-batch`) apply to non-empty score-based, single-node, and multi-node consolidation, plus dynamic and static drift. NodePools can share a limit with `karpenter.coralogix.net/disruption-pacing-group`; empty-node removals are not paced. See [Disruption pacing](disruption-pacing.md) for configuration and behavior.

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not run score-based consolidation.
- Annotated NodePools resume standard emptiness and underutilized consolidation behavior.
- No manifest or CRD changes are required to roll back.

After rollback, nodes may still carry the standby **taint** until you remove it manually (`kubectl taint nodes <node> karpenter.coralogix.net/standby:NoSchedule-`), so scheduling is not blocked with no controller managing standby. Leftover **Node** `karpenter.coralogix.net/standby` annotations are inert for upstream; they disappear when the Node is deleted.
