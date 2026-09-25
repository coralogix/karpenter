# Separating compaction from capacity reclamation

Status: Implemented for score-based consolidation on annotated dynamic NodePools. This document summarizes the initial design; see [Score-based consolidation](score-based-consolidation.md) for the current behavior and configuration.

This mechanism applies only to NodePools that use score-based consolidation. Other NodePools retain their existing behavior.

## Motivation

Consolidation currently couples disrupting pods to improve placement with removing their source nodes. Separate these responsibilities so workload placement and spare-capacity management can evolve independently.

The intended benefits are clearer compaction objectives, reuse of already running nodes, keeping valuable capacity such as cheap Spot instances, and potentially shorter time to running for new pods. Keeping spare capacity trades ongoing instance cost for availability and launch latency.

## Responsibilities

**Compaction** decides whether evacuating nodes is worth the workload disruption, aiming to pack workloads onto cheaper or otherwise preferable capacity. Karpenter controls disruption and simulates placement; the Kubernetes scheduler determines actual pod placement. Compaction can activate suitable standby nodes as destinations instead of creating new nodes, including when cheap standby capacity can replace expensive active capacity. It may arrange replacement capacity before eviction when needed.

Compaction completes by handing over an empty, tainted node. It does not decide whether that node should subsequently be removed, reactivated, or retained.

**Reclamation** decides only whether to keep empty nodes or remove them. It runs as part of score-based consolidation, taking precedence over normal compaction when a reclamation removal is due. Reclamation never activates nodes.

**Provisioning and compaction** own activation. When they need capacity, they can untaint suitable standby nodes instead of launching new instances. Provisioning's scheduling simulation should consider standby capacity so activation and new capacity creation form one coordinated decision. Compatibility with pod requirements still matters.

## Initial reclamation policy

For each score-based NodePool:

1. Check whether more than the configured interval has elapsed since its last reclamation removal. The default interval is two minutes. Configure it with the NodePool annotation `karpenter.coralogix.net/reclamation-interval`, using Go duration syntax such as `90s` or `5m`; missing, invalid, and non-positive values use the two-minute default.
2. If reclamation is due and there are currently empty nodes, perform reclamation instead of normal score-based compaction for that pool in this pass.
3. Count all currently empty nodes, including tainted standby nodes and naturally empty active nodes. Select `ceil(emptyNodeCount / 2)` for removal.
4. Prioritize nodes with the highest cost per vCPU. The removal quantity is based on node count, not summed CPU or memory capacity.
5. Persist the last successful reclamation-removal timestamp per NodePool in `karpenter.coralogix.net/last-reclamation` as RFC3339Nano so restarts do not accelerate removal.

If reclamation is not due, proceed with normal score-based compaction. If there are no empty nodes, proceed with normal compaction without resetting the reclamation timestamp.

Rounding is upward: three empty nodes means removing two; a solitary empty node is removed on the next eligible pass. This is a periodic pool-wide decision, not a minimum standby lifetime for each node, so recently emptied nodes are eligible too. More sophisticated capacity accounting is deferred.

## Lifecycle and ownership

```text
Active --compaction--> Evacuating --compaction completes--> Standby
  ^                                                          |
  +---------- provisioning / compaction activates ------------+
                                                             |
                                                   reclamation removes
                                                             v
                                                          Deleting
```

Evacuating and standby nodes remain tainted against ordinary scheduling. Standby nodes are running capacity, not stopped instances. Provisioning and compaction own reactivation; reclamation owns removal. A standby node must be activated before serving as a scheduling destination.

Naturally empty active nodes are included in reclamation's removal candidates. Whether and when to taint those nodes into standby remains to be specified.

“Empty” means no non-daemon pods remain, including terminating non-daemon pods. Evacuation completes only once those pods are gone; accepting eviction requests is insufficient. Daemon pods may remain, so evacuation should not blindly reuse full termination drain behavior.

## Restart and cancellation behavior

Evacuation can remain a best-effort, in-memory operation, like current disruption orchestration. A restart may abandon an interrupted evacuation through stale-state cleanup rather than resume the original plan:

- Clear temporary evacuation state and restore scheduling eligibility.
- Accept pod evictions that already happened; they cannot be rolled back.
- Reconsider the resulting placement on subsequent reconciliations.
- Allow reclamation to handle nodes that became empty.

A durable command resource is not required by this design. Standby, however, is intentional persistent state and must be distinguishable from an abandoned evacuation. A small NodeClaim marker or condition could provide this distinction; the representation is undecided.

The handoff persists standby state while the node remains tainted, before forgetting the evacuation command. A restart before that persistence may abandon the evacuation; a restart afterward preserves standby and lets reclamation reconcile it. Stale-state cleanup must respect this distinction.

Cancellation within a running process must also stop queued eviction work before restoring scheduling. Successful evacuation that retains a node must clear any internal deletion bookkeeping. The per-NodePool reclamation-removal timestamp survives restarts.

## Integration with the current implementation

The existing consolidation method framework appears suitable for both compaction and removal decisions. Reclamation is part of the score-based consolidation path, with a due reclamation removal taking precedence over normal compaction for the affected pool. Execution needs an explicit evacuation action that does not delete the source NodeClaim afterward. Other consolidation paths must preserve their existing behavior.

Relevant areas are:

- `pkg/controllers/disruption/types.go`: command actions currently infer deletion or replacement from candidates and replacements.
- `pkg/controllers/disruption/queue.go`: orchestration currently waits for replacement initialization, then deletes source NodeClaims; source deletion bookkeeping also needs adjustment.
- `pkg/controllers/disruption/controller.go`: stale disruption cleanup needs to preserve intentional standby state.
- `pkg/controllers/disruption/score_based_consolidation.go`: reclamation timing, empty-node selection, and precedence over normal compaction for score-based pools.
- `pkg/controllers/disruption/emptiness.go`: existing empty-node removal behavior for other pools remains unchanged.
- `pkg/controllers/node/termination/terminator/`: reusable eviction machinery, with evacuation semantics separated from full termination.
- Provisioning and scheduling state: recognize standby capacity and coordinate its activation with new capacity creation.

## Policy and coordination questions

- Prevent provisioning from launching duplicate capacity while standby activation is in progress.
- Protect newly activated nodes from immediate re-evacuation, using demand nominations, cooldowns, or another mechanism.
- Preserve workload disruption safeguards and define budget accounting for evacuation separately from subsequent empty-node removal.
- Recheck emptiness before committing removal; taints do not replace this check.
- Define initialization of the reclamation timestamp and how partial or budget-blocked removal batches update it.
- Define the vCPU denominator used for cost ranking (instance vCPU count versus allocatable CPU), missing-price handling, and tie-breaking.
- Keep compaction scores independent of individual reclamation decisions while evaluating the combined economic behavior. Evacuating a node creates an opportunity to save money; savings are realized when capacity is actually removed.
- Define precedence with drift, expiration, interruption, and other termination paths. The separation discussed here concerns consolidation, not a requirement that all node removal wait for compaction.

## Scope and AWS provider implications

Start with whole-node evacuation. Selective pod eviction to improve placement is a separate extension.

The design is expected to fit in Karpenter core using the existing provider launch and termination interface. It should not inherently require changes to AWS-specific logic. Deployment would require a custom AWS controller build incorporating the modified core, through a module replacement or a pinned core fork. A small provider build patch or fork may be convenient for that dependency change.

New AWS-specific capabilities, such as stopping instances rather than retaining running nodes, would expand this scope. Provider compatibility has not yet been validated through implementation.
