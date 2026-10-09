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
| `karpenter.coralogix.net/reclamation-standby-delay` | NodePool | Minimum time a node must stay in standby before reclamation may delete it. Go duration syntax; missing or invalid values default to **15s**; **`0s` disables** reclamation delay. |

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
| **Reclamation** | Empty standby nodes past reclamation delay | Deletes NodeClaim (instance terminated) | `Empty` | Exempt |
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
- **Reclamation** selects **all** standby candidates past reclamation delay in each pool on a pass (stable sort by node name within the pool), using the same cached candidate admission as other disruption methods. At execution, each candidate is checked independently (identity, standby/activation, nomination, occupancy) and deleted with a UID/resourceVersion precondition so activation and deletion cannot both succeed. Candidates that become ineligible after selection are skipped without blocking unrelated deletions in the same batch.

A non-DaemonSet workload that tolerates `karpenter.sh/disruption:NoSchedule` can still be admitted as a compaction workload and its node can be selected as a source. During evacuation, Karpenter's `IsEvictable` check exempts that pod, while the evacuation wait still counts it as occupying. This is an accepted limitation: evacuation waits for the pod to leave on its own, and may reach the queue timeout and roll back with the source instance retained. Other workloads may already have been evicted, and replacements or activated standby destinations may remain. Upstream consolidation can ignore these pods during drain because it ultimately terminates the source instance.

The eviction exemption and the strict physical-emptiness check are intentional. Compaction does not force-evict a workload that tolerates the disruption taint, and a node is not marked standby or reclaimed until its occupying workloads have actually left. This limitation does not reject the source from compaction admission.

Reclamation delay: standby time must be **strictly greater than** the configured delay. Legacy marker value `true` skips reclamation delay. Nodes with an in-progress activation marker are not reclaimable.

## Consistency and API reads

Disruption planning (candidate filtering, standby counts, score-based compaction revalidation, and NodePool policy including pacing) uses the controller **cache** (`kubeClient`), matching upstream’s consistency model. Reclamation does not rebuild consolidation candidates between selection and deletion. Configuration and deleted NodePools take effect when the cache observes them; there is no fallback to uncached reads when the cache is stale or returns `NotFound`.

At final reclamation validation, one cached NodePool read checks both whether the standby lifecycle is still enabled and whether the current delay has elapsed. A delay increase already visible to the cache therefore skips a candidate selected under the older delay; it is reconsidered by later candidate discovery. This uses the existing cache read and does not rebuild candidates or make NodePool policy transactional with deletion. The transient tradeoff is cache convergence plus the short interval between validation and delete: the queue honors changes it has observed, while a later policy change can still race the conditional NodeClaim delete.

**Live reads** remain at execution boundaries: standby coordinator transitions (including occupancy before completing empty marking), evacuation pod polling, and the final reclamation delete safety checks (Node, NodeClaim, and pod occupancy immediately before conditional delete). UID/resourceVersion preconditions still protect NodeClaim identity and concurrent claim updates; NodePool policy is checked through the cache.

## “Empty” and occupancy

The same occupancy rules apply to standby marking, evacuation completion, reclamation, and occupied-standby recovery:

- **Occupied:** any bound pod that is not a DaemonSet, not terminal (Succeeded/Failed with no blocking finalizers), and not a Node-owned mirror pod. Zero-request workload pods still count as occupied.
- **Not occupying:** DaemonSet pods, terminal pods, mirror pods, and pods that are terminating but no longer block completion once finalizers are cleared.

Evacuation waits until occupying pods disappear from the API (terminating pods with finalizers still block). Accepting an eviction request alone is not enough.

## Activation and recovery

**Provisioning** and **score-based compaction** decide *which* standby nodes to use; `pkg/standby.Coordinator` performs the shared activation protocol (reservation markers, taint removal, metrics).

- **Provisioning** and **score-based compaction** treat eligible standby nodes as schedulable during planning by
  applying a planning-only view that omits the controller-owned standby `NoSchedule` taint. Cluster objects are not
  mutated for planning; activation clears persisted markers and taints through the coordinator after a standby node
  is selected.
- **Provisioning** activates standby nodes chosen during scheduling (`provisioning` source).
- **Compaction** activates standby destinations before evacuating sources (`compaction` source).
- **Recovery** runs on startup and during disruption cleanup: if a standby node has gained a workload (including after a race with the scheduler), the controller activates it back to active capacity (`recovery` source) instead of leaving it tainted standby.

The disruption controller also clears stale `karpenter.sh/disruption` taints from nodes not in an in-flight queue command. During cleanup it runs one coordinator repair pass per selected node to resume interrupted activation, remove orphan standby taints, restore missing standby taints, recover occupied standby capacity, and refresh cluster state after successful repairs.

## Observability

Disruption evaluation metrics label consolidation type `standby-marking`, `reclamation`, or `score-based` as appropriate.

| Prometheus metric | When it increments / updates |
|-------------------|------------------------------|
| `karpenter_nodes_standby_marked_total` | Empty node successfully marked standby (label: `nodepool`). |
| `karpenter_nodes_standby_activated_total` | Standby taint removed after successful activation (labels: `nodepool`, `instance_type`, `source`). |
| `karpenter_nodepools_reclamation_removals_total` | Reclamation delete succeeds (label: `nodepool`). |
| `karpenter_nodepools_standby_nodes` | Per-pool gauge refreshed during reclamation candidate discovery. |
| `karpenter_voluntary_disruption_evacuation_commands_total` | Evacuation command completed or failed (score-based compaction). |
| `karpenter_voluntary_disruption_evacuation_duration_seconds` | Time to finish an evacuation command. |
| `karpenter_voluntary_disruption_nodeclaims_evacuated_total` | Source NodeClaims evacuated per completed command. |

The standby gauge counts non-deleting cluster-state nodes that have both a Node and NodeClaim and whose NodeClaim has the standby marker. It includes retained markers even when a node is protected, nominated, queued, buffer-reserved, still within its reclamation delay, or its NodePool has left standby lifecycle scope. Enabled dynamic pools receive explicit zero-valued series; counting reads no pod occupancy data.

Standby marking does not emit evacuation or consolidation deletion metrics. Reclamation uses reason `Empty` in disruption events and budgets.

## Why these are separate steps

Historically, consolidation both moved pods and deleted source nodes in one flow. Splitting the steps allows:

- Reusing **running** empty instances as cheap warm capacity instead of terminating immediately.
- Letting compaction optimize placement while reclamation throttles removal independently (reclamation delay instead of paced batch removal).
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
