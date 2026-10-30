# The move score

Status: accepted

Score-based consolidation ranks moves by a score with four terms: normalized cost, CPU slack, memory balance, and instance-type concentration. The formulas are those of the earlier implementation (branch `CX-54658-compaction-reclamation-1.14`) and are kept unchanged. That implementation is rewritten with simpler edge-case handling. On score-based pools the score replaces upstream's `Balanced` approval rule, so `Balanced` and `WhenEmptyOrUnderutilized` behave the same there.

A move can be chosen only if it saves real dollars and its total score is positive. **Savings** are the sum of source prices minus the estimated price of the new node, if any, minus the price of every standby node the move reactivates. A reactivated standby node is not free: left alone, reclamation would delete it, so reactivating it means paying for it.

## Terms

All baselines use one set of **baseline nodes** per pass, taken from the simulation's node snapshot: managed, initialized, not deleting, and not standby. Standby nodes are excluded everywhere because they are about to be reclaimed.

- **Cost**: `f × Σ aᵢ` over sources. `aᵢ = priceᵢ / (allocatableCPUᵢ × baseline)`, where `baseline` is `Σ price / Σ allocatable CPU` of the baseline nodes in the source's NodePool and capacity type, and `f = savings / Σ source price`. Comparing a node with the average price per CPU of its own pool and capacity type stops cheap Spot nodes from always losing to on-demand nodes on raw dollars. `f` makes a move that keeps most of the cost score low.
- **Utilisation**: `Σ w × (allocatableCPUᵢ − requestedCPUᵢ) / allocatableCPUᵢ` over sources, with requests clamped to the allocatable range.
- **Memory balance**: shapes are allocatable CPU and memory minus DaemonSet requests. Per pool, `q` is the baseline nodes' memory per CPU, and `t = (1 + surplus) × requested memory / requested CPU` of their non-DaemonSet pods. `pressure` is 0 when `0.95 ≤ q/t ≤ 1.05`, otherwise `clamp(log2(q/t), −1, 1)`. A shape's delta is `memory / q − CPU`. A pool's delta is the removed source deltas minus the added deltas of the new node and the reactivated standby nodes in that pool. Its contribution is `weight × pressure × clamp(delta / total CPU removed, −1, 1) × number of sources`, summed over the affected pools.
- **Instance-type concentration**: each source whose instance type makes up more than `threshold` of its pool's baseline nodes adds `weight`.

A term that cannot be computed contributes 0. That covers a missing price (treated as 0), a zero baseline, an unknown pool ratio, and a missing instance-type label.

## Tuning

| NodePool annotation (`karpenter.coralogix.net/…`) | Default | Valid |
|---|---|---|
| `utilisation-weight` | 1 | finite, > 0 |
| `memory-surplus` | 0.2 | finite, ≥ 0 |
| `memory-balance-weight` | 0 (term off) | finite, ≥ 0 |
| `instance-type-concentration-threshold` | 0.5 | finite, in `[0, 1)` |
| `instance-type-concentration-weight` | 0 (term off) | finite, ≥ 0 |

An invalid value uses the default and raises an event on the NodePool, as duration annotations do (ADR-0006). An annotation edit does not reset the "already consolidated" state, because the NodePool watcher ignores metadata-only changes. The edit takes effect on the next cluster change, or within five minutes.

## Considered Options

- **Assess the new node's memory shape against every instance type it could launch as, and use the least favourable.** This was the earlier implementation. Rejected: it needed a scheduler addition and per-type DaemonSet overhead for a term that is off by default. The new node uses the same instance type as its price estimate, so price and memory describe the same node.
- **Count reactivated standby nodes as free.** Rejected: it makes moving onto an expensive standby node look like a full saving.
- **Reject moves with unknown prices, and track a status for each term.** Rejected: the positive-savings and positive-score rules already stop bad moves, and the extra states multiplied corner cases.
- **Reset the consolidated state when scoring annotations change.** Rejected: it needs a fingerprint of the annotations for a delay of at most five minutes.
