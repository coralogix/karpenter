# Score-based consolidation

This fork adds an alternate consolidation disruption method for NodePools that opt in via annotation. It shuffles eligible source nodes, simulates single-node moves, and ranks them by estimated hourly dollar savings.

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
| `karpenter.coralogix.net/reclamation-delay` | Go duration a node must stay in standby before reclamation may delete it. Absent or invalid values default to **15s**. `0s` means no minimum age and no mandatory extra pass. Empty, malformed, or negative values raise an event on the NodePool. |
| `karpenter.coralogix.net/evacuation-timeout` | Go duration an evacuation may run, counted from when the command starts (including any wait for a replacement), before it is abandoned. Absent or invalid values default to **5m**. `0s` means an immediate timeout. Empty, malformed, or negative values raise an event on the NodePool. |

Both duration annotations accept zero literally and use their defaults for empty, malformed, or negative values. Choose a positive evacuation timeout to allow draining; `0s` causes every evacuation to be abandoned immediately.

## Standby and reclamation

On opted-in dynamic NodePools, empty nodes are marked **standby** instead of being deleted immediately. Standby marking and reclamation require `spec.disruption.consolidateAfter` to be set; if it is unset, they are disabled entirely even when the score-based annotation is present. **Reclamation** deletes a standby node after the reclamation delay if it is still standby and still empty. Reclamation uses the `Empty` disruption reason and `Empty` budgets; it is not disruption-paced. Standby marking evicts nothing and does not consume disruption budget.

### Standby marks (set by Karpenter)

When a node enters standby, Karpenter writes the marks below. **Do not set or edit these in manifests**; they are observability and control state on live Nodes. Reactivation and disruption repair clear them when the node leaves standby.

| Mark | Object | Set by | Meaning |
|------|--------|--------|---------|
| `karpenter.coralogix.net/standby:NoSchedule` | Node (taint) | Disruption | Ordinary pods cannot bind while the node is standby. |
| `karpenter.coralogix.net/standby` | Node (annotation) | Disruption | Standby start time in **RFC3339Nano UTC** (for example `2026-10-27T11:00:00.123456789Z`). Reclamation compares this timestamp to the pool’s `reclamation-delay`. The value survives a Karpenter restart so time already spent in standby still counts. If the taint is present but the annotation is missing or not an RFC3339Nano timestamp, disruption backfills the timestamp with now. A positive delay starts from that timestamp. With zero delay, same-pass reclamation is permitted if the timestamp is visible in the cache, but is not guaranteed. |

Entering standby writes the taint (with no value) and timestamp together in one Node update; reactivation and repair clear both together. The taint determines whether a node is standby. Repair clears an annotation without a taint; it cannot make the node reclaimable. Later standby entry writes a fresh timestamp. Repair also clears standby when a reschedulable pod appears or the NodePool stops qualifying.

To inspect standby state: `kubectl describe node <name>` for the taint, and `kubectl get node <name> -o jsonpath='{.metadata.annotations.karpenter\.coralogix\.net/standby}'` for the timestamp.

### Reactivation

Provisioning and disruption treat a standby node as existing capacity. When choosing where pods fit, the scheduling simulation ignores the standby taint and keeps every other taint, resource, affinity, and topology constraint. When Karpenter acts on a result that places pods on a standby node, it clears the taint and the timestamp so the pods can bind there, and does not launch a new node for them. This applies to provisioning, capacity-buffer pods, and every disruption method that moves pods (consolidation and drift included). A simulation that is not acted on leaves standby unchanged.

- Disruption clears standby on its destinations before it taints the source nodes. If clearing fails, the command does not start and a later pass retries.
- Nothing is reserved: once standby is cleared, any pod may bind. A node reactivated for a real pod is nominated for the usual nomination window, so standby marking does not hide it again before the pods arrive. Capacity-buffer pods do not nominate the node; buffer accounting keeps it from being marked standby again.
- A restart during the nomination window may mark the node standby again; a later scheduling pass reactivates it.

### Evacuation

On `WhenEmptyOrUnderutilized` and `Balanced` score-based pools, each evacuation moves workloads off one source node and leaves that node standby with its NodeClaim intact. The source receives the temporary disruption `NoSchedule` taint as soon as the command starts. If replacement capacity is needed, eviction waits until the replacement is initialized. Karpenter requests workload pod evictions about once a second until the node is empty, respecting PodDisruptionBudgets; daemon and mirror pods remain on the running node. Blocked evictions emit `FailedDraining` events on the source node.

Once draining finishes, Karpenter writes the standby taint and timestamp before removing the temporary disruption taint. Both the source and any replacement remain until the source is reactivated or reclaimed. Evacuation continues to use `Underutilized` budgets and disruption pacing. `WhenEmpty` score-based pools only mark/reclaim empty nodes (no evacuation of nodes that still have pods).

If the source is not empty within the NodePool's `evacuation-timeout`, the evacuation is abandoned: Karpenter stops evicting, removes the temporary disruption taint so the source takes pods again, and keeps any replacement. A later pass may select the node again.

Nodes running a workload pod that tolerates `karpenter.sh/disrupted:NoSchedule` are not evacuated, because such a pod can be scheduled back onto the source during the drain.

After a controller restart, the existing repair pass clears the temporary disruption taint for commands lost from memory. An already-empty source becomes standby on a later pass; a source still running workloads can be evacuated again.

## Behavior

- NodePools with the annotation are handled by the score-based consolidation method, which runs after multi-node consolidation and before single-node consolidation.
- Annotated NodePools are excluded from emptiness, single-node consolidation, and multi-node consolidation.
- Dynamic and static drift continue to use the standard drift methods and `Drifted` disruption budgets. Optional disruption pacing also limits drift candidates and shares the per-NodePool or group cooldown with consolidation.
- The method reports `Underutilized` as its disruption reason for budget accounting. Empty nodes are handled by standby marking and reclamation instead of score-based consolidation or standard emptiness.
- Candidates are shuffled and all single-node moves are attempted with up to `runtime.GOMAXPROCS` simulations in parallel. After 15 seconds no new simulations are started; simulations already running finish and their results are used. Partial searches retain successful results. The ten moves with highest positive savings are kept; moves requiring more than one new node are rejected. Savings subtract the estimated new-node price and the prices of standby destinations that would reactivate.
- After the remaining 15-second validation delay since the pass started, moves are validated in savings order. Validation rechecks all source budgets, standby destinations, and whole-command pacing. The first valid move is evacuated; validation does not re-rank moves. Simulation errors preserve successful evaluations and prevent marking the cluster consolidated. The cluster is marked consolidated only when every eligible move was evaluated and none were blocked by budget or pacing.
- Each searched pass writes one Info decision log: `score-based consolidation move selected` includes source identity and prices, new capacity, reactivated standby destinations, savings, search counts, deadline, search duration (excluding the validation wait), rank, and up to three alternatives with validation results (`passed`, `not_attempted`, or `failed: <reason>` where `<reason>` is the same category as upstream validation failures, with `pacing` for whole-command pacing). `score-based consolidation found no move` includes counts, sorted blocked NodePools, and one reason: `no_candidates`, `blocked_by_budget_or_pacing`, `timed_out`, `no_saving_move`, `all_rejected_by_validation`, or `simulation_error`. Simulation errors take precedence when nothing is selected. Passes skipped because the cluster is consolidated log nothing. These logs contain no pod names.

- Optional disruption pacing annotations (`karpenter.coralogix.net/disruption-pacing-per-minute` and `karpenter.coralogix.net/disruption-pacing-per-batch`) apply to non-empty score-based, single-node, and multi-node consolidation, plus dynamic and static drift. NodePools can share a limit with `karpenter.coralogix.net/disruption-pacing-group`; empty-node removals are not paced. See [Disruption pacing](disruption-pacing.md) for configuration and behavior.

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not run score-based consolidation.
- Annotated NodePools resume standard emptiness and underutilized consolidation behavior.
- No manifest or CRD changes are required to roll back.

After rollback, nodes may still carry the standby **taint** until you remove it manually (`kubectl taint nodes <node> karpenter.coralogix.net/standby:NoSchedule-`), so scheduling is not blocked with no controller managing standby. Leftover **Node** `karpenter.coralogix.net/standby` annotations are inert for upstream; they disappear when the Node is deleted.
