# Separating compaction from capacity reclamation

Status: Implemented for score-based consolidation on annotated dynamic NodePools. See [Score-based consolidation](score-based-consolidation.md) for current behavior and configuration.

This mechanism applies only to NodePools that use score-based consolidation. Other NodePools retain their existing behavior.

The score-based annotation is an explicit opt-in. It applies only to dynamic NodePools with `consolidationPolicy: WhenEmptyOrUnderutilized`; `consolidateAfter` is ignored in this mode, including `Never`. Removing the annotation returns the pool to upstream consolidation behavior.

## Motivation

Consolidation currently couples disrupting pods to improve placement with removing their source nodes. Separate these responsibilities so workload placement and spare-capacity management can evolve independently.

The intended benefits are clearer compaction objectives, reuse of already running nodes, keeping valuable capacity such as cheap Spot instances, and potentially shorter time to running for new pods. Keeping spare capacity trades ongoing instance cost for availability and launch latency.

## Responsibilities

**Compaction** considers only non-empty active nodes and decides whether evacuating them is worth the workload disruption, aiming to pack workloads onto cheaper or otherwise preferable capacity. A node is empty when it has no bound non-DaemonSet pods; terminating and terminal pods still count while bound. Karpenter controls disruption and simulates placement; the Kubernetes scheduler determines actual pod placement. Compaction can activate suitable standby nodes as destinations instead of creating new nodes, including when cheap standby capacity can replace expensive active capacity. It may arrange replacement capacity before eviction when needed. Compaction honors NodePool disruption budgets and pacing.

Compaction completes by handing over an empty, tainted node. It does not decide whether that node should subsequently be removed, reactivated, or retained.

**Reclamation** decides only whether to keep empty nodes or remove them. For opted-in NodePools, a standalone `ScoreBasedReclamation` method runs immediately after upstream `Emptiness`, before drift and multi-node consolidation. It reports reason `Empty` and is exempt from NodePool disruption budgets, while preserving candidate eligibility checks and rechecking live emptiness and activation before deletion. Reclamation never activates nodes.

**Provisioning and compaction** own activation. When they need capacity, they can untaint suitable standby nodes instead of launching new instances. Provisioning's scheduling simulation should consider standby capacity so activation and new capacity creation form one coordinated decision. Compatibility with pod requirements still matters.

## Reclamation policy

For each score-based NodePool:

1. Check whether more than the configured interval has elapsed since its last successful reclamation. The default interval is one minute. Configure it with the NodePool annotation `karpenter.coralogix.net/reclamation-interval`, using Go duration syntax such as `90s` or `5m`; missing, invalid, and non-positive values use the one-minute default. The elapsed-time comparison is strict, so exactly one minute is not yet due.
2. If reclamation is due and there are currently empty nodes, the reclamation method returns a command. The controller starts it and requeues at the normal method boundary, then retries from the top; score-based compaction can run on a later pass. The reclamation decision does not wait for drift or multi-node consolidation calculations.
3. Count all currently empty nodes, including tainted standby nodes and naturally empty active nodes. Reclaim `ceil(emptyNodeCount / 2)` eligible candidates without clipping the batch to the NodePool disruption budget.
4. Prioritize nodes with the highest cost per vCPU. The reclamation quantity is based on node count, not summed CPU or memory capacity.
5. Persist the last successful reclamation timestamp per NodePool in `karpenter.coralogix.net/last-reclamation` as RFC3339Nano so restarts do not accelerate reclamation.

If reclamation returns no command, the controller proceeds through its remaining methods in the same pass. Score-based compaction remains after multi-node consolidation. If there are no empty nodes, reclamation does not reset its timestamp.

Rounding is upward: three empty nodes means reclaiming two; a solitary empty node is reclaimed on the next eligible pass. This is a periodic pool-wide decision, not a minimum standby lifetime for each node, so recently emptied nodes are eligible too. More sophisticated capacity accounting is deferred.

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

Evacuating and standby nodes remain tainted against ordinary scheduling. Standby nodes are running capacity, not stopped instances. Provisioning and compaction own reactivation; reclamation owns empty-node reclamation. A standby node must be activated before serving as a scheduling destination.

Naturally empty active nodes and empty standby nodes are eligible for reclamation. Standby nodes reserved for activation are excluded.

“Empty” means no non-daemon pods remain, including terminating non-daemon pods. Evacuation completes only once those pods are gone; accepting eviction requests is insufficient. Daemon pods may remain, so evacuation should not blindly reuse full termination drain behavior.

## Restart and cancellation behavior

Evacuation can remain a best-effort, in-memory operation, like current disruption orchestration. A restart may abandon an interrupted evacuation through stale-state cleanup rather than resume the original plan:

- Clear temporary evacuation state and restore scheduling eligibility.
- Accept pod evictions that already happened; they cannot be rolled back.
- Reconsider the resulting placement on subsequent reconciliations.
- Allow reclamation to handle nodes that became empty.

A durable command resource is not required by this design. Standby, however, is intentional persistent state and must be distinguishable from an abandoned evacuation. A small NodeClaim marker or condition could provide this distinction; the representation is undecided.

The handoff persists standby state while the node remains tainted, before forgetting the evacuation command. A restart before that persistence may abandon the evacuation; a restart afterward preserves standby and lets reclamation reconcile it. Stale-state cleanup must respect this distinction.

Cancellation within a running process must also stop queued eviction work before restoring scheduling. Successful evacuation that retains a node must clear any internal deletion bookkeeping. The per-NodePool reclamation timestamp survives restarts.

## Integration with the current implementation

The consolidation method framework runs `ScoreBasedReclamation` immediately after upstream `Emptiness`; `ScoreBasedConsolidation` later handles only non-empty compaction. A reclamation command uses disruption reason `Empty` and a validation path exempt from NodePool disruption budgets. Compaction continues to use reason `Underutilized`, with its existing budget and pacing checks. Execution uses an explicit evacuation action that does not delete the source NodeClaim afterward. Other consolidation paths preserve their existing behavior.

Relevant areas are:

- `pkg/controllers/disruption/types.go`: command actions currently infer deletion or replacement from candidates and replacements.
- `pkg/controllers/disruption/queue.go`: orchestration currently waits for replacement initialization, then deletes source NodeClaims; source deletion bookkeeping also needs adjustment.
- `pkg/controllers/disruption/controller.go`: stale disruption cleanup needs to preserve intentional standby state.
- `pkg/controllers/disruption/score_based_reclamation.go`: reclamation timing, empty-node selection, and budget-exempt validation for score-based pools.
- `pkg/controllers/disruption/score_based_consolidation.go`: score-guided compaction of non-empty nodes.
- `pkg/controllers/disruption/emptiness.go`: existing empty-node reclamation behavior for other pools remains unchanged.
- `pkg/controllers/node/termination/terminator/`: reusable eviction machinery, with evacuation semantics separated from full termination.
- Provisioning and scheduling state: recognize standby capacity and coordinate its activation with new capacity creation.

## Policy and coordination

- Prevent provisioning from launching duplicate capacity while standby activation is in progress.
- Protect newly activated nodes from immediate re-evacuation, using demand nominations, cooldowns, or another mechanism.
- Preserve workload disruption safeguards for compaction. Reclamation does not consume NodePool disruption budgets, but retains candidate eligibility checks and rechecks emptiness and activation before deleting a NodeClaim.
- Persist the reclamation timestamp after each successful individual deletion. A partial batch therefore records progress; empty passes and failed deletions do not reset the timestamp.
- Rank reclamation candidates by instance price per vCPU, with a stable node-name tie-breaker and ineligible nodes excluded from the batch.
- Keep compaction scores independent of individual reclamation decisions while evaluating the combined economic behavior. Evacuating a node creates an opportunity to save money; savings are realized when capacity is reclaimed.
- Run score-based reclamation before drift and multi-node consolidation calculations. Once a reclamation command starts, the controller requeues at the normal method boundary; score-based compaction is reconsidered on a later pass. Drift, expiration, interruption, and other termination paths retain their own method semantics.

## Scope and AWS provider implications

Start with whole-node evacuation. Selective pod eviction to improve placement is a separate extension.

The design is expected to fit in Karpenter core using the existing provider launch and termination interface. It should not inherently require changes to AWS-specific logic. Deployment would require a custom AWS controller build incorporating the modified core, through a module replacement or a pinned core fork. A small provider build patch or fork may be convenient for that dependency change.

New AWS-specific capabilities, such as stopping instances rather than retaining running nodes, would expand this scope. Provider compatibility has not yet been validated through implementation.
