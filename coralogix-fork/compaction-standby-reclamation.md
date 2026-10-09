# Compaction, standby, and reclamation

For opted-in dynamic NodePools, Karpenter separates **packing workloads** from **managing empty capacity**. Score-based **compaction** evicts pods from underused nodes but keeps the instance running as retained capacity. **Standby marking** moves naturally empty active nodes into that retained state. **Reclamation** deletes empty standby nodes when the pool no longer needs them. Compaction scoring, timeouts, and pacing are documented in [Score-based consolidation](score-based-consolidation.md).

## Opt-in and NodePool requirements

Standby marking and reclamation run only when all of the following hold:

- The NodePool has the score-based consolidation annotation (key presence is enough; the value is ignored).
- `spec.disruption.consolidationPolicy` is `WhenEmptyOrUnderutilized`.
- The NodePool is **dynamic** (`spec.replicas` unset). Static NodePools are not eligible.

Removing the score-based annotation returns the pool to upstream emptiness and consolidation behavior. Annotated pools **ignore** `consolidateAfter` for consolidatability (including `Never`).

### Annotations

| Annotation | Applies to | Meaning |
|------------|------------|---------|
| `karpenter.coralogix.net/score-based-consolidation` | NodePool | Enables score-based compaction plus standby marking and reclamation. |
| `karpenter.coralogix.net/reclamation-standby-delay` | NodePool | Minimum time a node must stay in standby before reclamation may delete it. Go duration syntax; missing or invalid values default to **15s**; **`0s` disables** the soak. |

Example:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: workers
  annotations:
    karpenter.coralogix.net/score-based-consolidation: ""
    karpenter.coralogix.net/reclamation-standby-delay: 30s
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 0s
```

### Persisted node state

Standby capacity is visible on the cluster API:

| Mechanism | Key / taint | Meaning |
|-----------|-------------|---------|
| NodeClaim annotation | `karpenter.coralogix.net/standby` | Standby entry time as **RFC3339Nano UTC**, or legacy `true` (no timestamp). |
| NodeClaim annotation | `karpenter.coralogix.net/standby-activating` | Activation in progress (survives restarts). |
| NodeClaim annotation | `karpenter.coralogix.net/standby-activation-source` | `provisioning`, `compaction`, or `recovery`. |
| Node taint | `karpenter.coralogix.net/standby=true:NoSchedule` | Blocks ordinary scheduling while standby. |

Activation clears the standby marker and taint when provisioning or compaction selects the node, or when the controller recovers occupied standby capacity.

## What each disruption method does

| Method | When it runs | Effect on nodes | Disruption reason | NodePool budget |
|--------|----------------|-----------------|-------------------|-----------------|
| **Standby marking** | Empty active nodes in opted-in pools | Marker + standby taint; no pod eviction | `Empty` | Exempt |
| **Reclamation** | Empty standby nodes past soak | Deletes NodeClaim (instance terminated) | `Empty` | Exempt |
| **Score-based compaction** | Non-empty underused nodes | Evacuates pods; node becomes standby | `Underutilized` | Honored |
| **Emptiness** (upstream) | Other pools only | Deletes empty nodes directly | `Empty` | Per upstream rules |

Standby marking and reclamation do **not** use the consolidation TTL. Reclamation does **not** use disruption pacing. Compaction honors budgets and optional pacing annotations (see [Disruption pacing](disruption-pacing.md)).

On each disruption controller pass, methods run in this order: **emptiness** (non-score-based pools) → **standby marking** → **reclamation** → drift → multi-node consolidation → **score-based compaction** → single-node consolidation. Annotated pools skip standard emptiness, single-node, and multi-node consolidation for empty-capacity handling; empty nodes are covered by standby marking and reclamation instead.

If standby marking or reclamation starts a command, the controller finishes that method and requeues; score-based compaction is evaluated on a later pass.

## End-to-end lifecycle

```text
Active (empty) ──standby marking──► Standby (empty, tainted)
Active (workloads) ──compaction evacuate──► Standby (empty after eviction)
Standby ──reclamation──► Deleting / terminated
Standby ──provisioning or compaction activate──► Active (schedulable)
```

- **Compaction** simulates placement, may activate existing standby nodes as destinations, taints sources, evicts non-daemon workloads, waits until occupying pods are gone, then persists standby on the evacuated node. It does **not** delete the NodeClaim.
- **Standby marking** applies the same standby persistence to nodes that are **already** empty, without eviction.
- **Reclamation** selects **all** soak-eligible standby candidates in each pool on a pass (stable sort by node name within the pool), validates live state, then deletes with a UID/resourceVersion precondition so activation and deletion cannot both succeed.

Soak eligibility: standby time must be **strictly greater than** the configured delay. Legacy marker value `true` skips soak gating. Nodes with an in-progress activation marker are not reclaimable.

## “Empty” and occupancy

The same occupancy rules apply to standby marking, evacuation completion, reclamation, and occupied-standby recovery:

- **Occupied:** any bound pod that is not a DaemonSet, not terminal (Succeeded/Failed with no blocking finalizers), and not a Node-owned mirror pod. Zero-request workload pods still count as occupied.
- **Not occupying:** DaemonSet pods, terminal pods, mirror pods, and pods that are terminating but no longer block completion once finalizers are cleared.

Evacuation waits until occupying pods disappear from the API (terminating pods with finalizers still block). Accepting an eviction request alone is not enough.

## Activation and recovery

**Provisioning** and **score-based compaction** decide *which* standby nodes to use; `pkg/standby.Coordinator` performs the shared activation protocol (reservation markers, taint removal, metrics).

- **Provisioning** activates standby nodes chosen during scheduling (`provisioning` source).
- **Compaction** activates standby destinations before evacuating sources (`compaction` source).
- **Recovery** runs on startup and during disruption cleanup: if a standby node has gained a workload (including after a race with the scheduler), the controller activates it back to active capacity (`recovery` source) instead of leaving it tainted standby.

The disruption controller also clears stale `karpenter.sh/disruption` taints from nodes not in an in-flight queue command, restores missing standby taints on marked claims, and reconciles cluster state after coordinator transitions.

## Observability

Disruption evaluation metrics label consolidation type `standby-marking`, `reclamation`, or `score-based` as appropriate.

| Prometheus metric | When it increments / updates |
|-------------------|------------------------------|
| `karpenter_nodes_standby_marked_total` | Empty node successfully marked standby (label: `nodepool`). |
| `karpenter_nodes_standby_activated_total` | Standby taint removed after successful activation (labels: `nodepool`, `instance_type`, `source`). |
| `karpenter_nodepools_reclamation_removals_total` | Reclamation delete succeeds (label: `nodepool`). |
| `karpenter_nodepools_empty_nodes` | Per-pool gauge of empty nodes (`state=standby` or `state=other`); updated after each reclamation inventory scan. |
| `karpenter_voluntary_disruption_evacuation_commands_total` | Evacuation command completed or failed (score-based compaction). |
| `karpenter_voluntary_disruption_evacuation_duration_seconds` | Time to finish an evacuation command. |
| `karpenter_voluntary_disruption_nodeclaims_evacuated_total` | Source NodeClaims evacuated per completed command. |

Standby marking does not emit evacuation or consolidation deletion metrics. Reclamation uses reason `Empty` in disruption events and budgets.

## Why these are separate steps

Historically, consolidation both moved pods and deleted source nodes in one flow. Splitting the steps allows:

- Reusing **running** empty instances as cheap warm capacity instead of terminating immediately.
- Letting compaction optimize placement while reclamation throttles removal independently (soak delay instead of paced batch removal).
- Activating standby nodes for new or relocated workloads without a full launch when requirements match.

The tradeoff is ongoing cost for standby instances until reclamation removes them or they are activated.

## Rollback to upstream

- Fork annotations remain valid metadata; upstream ignores them.
- Annotated NodePools resume standard emptiness and underutilized consolidation.
- Standby markers and taints are not removed automatically on rollback; operators may clear them or let nodes drain through normal termination paths.
- No CRD or manifest changes are required to roll back the controller binary.

## Limitations

- Whole-node evacuation only; selective pod eviction for placement is out of scope.
- Standby nodes stay **running**; there is no stop/start instance integration in this fork.
- A pod may bind after reclamation’s final empty check; normal drain/termination paths apply if the NodeClaim is already deleting.
- Reclamation removes **all** eligible standby nodes in a pool per command; there is no per-pass cap tied to the NodePool disruption budget.
