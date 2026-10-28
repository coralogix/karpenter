# Reclamation policy: delay, budgets, and deleting from the selection snapshot

Status: accepted

**Delay.** The reclamation delay is how long a node must already have been in standby before a disruption pass may select it. It is the NodePool annotation `karpenter.coralogix.net/reclamation-delay`, a Go duration. Absent means 15 seconds. An invalid value means 15 seconds and an event on the NodePool. `0s` or a negative value means no minimum age on a later pass. It is separate from `consolidateAfter`, so the reclamation delay is the only clock for removing an empty score-based node. An earlier draft called the annotation `reclamation-standby-delay`; the shorter name has the same meaning. When there is no minimum age (`0s` or a negative delay), a node whose timestamp was backfilled by repair in that pass may be reclaimed immediately; that is accepted, not a bug.

**Budgets.** Reclamation reports `Empty`, is limited by the NodePool's `Empty` budgets, and is not paced. Standby marking takes no budget, because it evicts nothing and the node stays in the cluster. Evacuation keeps `Underutilized` budgets and pacing. This departs from the score-driven RFC, which counts every score-driven move, empty-node removals included, against `Underutilized`. Operators who already cap how fast empty nodes disappear keep that cap on the same reason, and a pool with a blocking `Empty` budget still cannot lose standby nodes. Charging `Underutilized` would let a budget written for non-empty moves control deletions it was never meant for. The cost is that two budgets now shape one pool's node removals.

**Delete from the snapshot.**

**Rejected attempts.** Emptiness and consolidation sleep 15 seconds after deciding, then rebuild candidates from the cache before deleting. Reclamation does not. A delete-time recheck was tried. It read the Node, the NodeClaim, and the pods from the same cache that selection used, so it could see a new pod or a cleared taint only if the informer had already delivered it, and it guaranteed nothing more. A direct read of the API server was rejected because the rest of disruption does not do that, and the requirement is to avoid a stuck node, not to make deletion linearizable.

**Accepted design.** Once a pass accepts a standby node, the queue deletes that NodeClaim on the next reconcile. Deleting a node that has gained a pod since selection is acceptable, because termination evicts the pods that are there. The command keeps every candidate the snapshot accepted, one command for the whole pass. A failed delete retries that command, and a timeout takes the existing failure path and clears the disruption taint.

**Drift.** Drift is unchanged. A drifted empty score-based node is not special-cased, so it may be marked standby and reclaimed after the delay, and drift may still take a standby node without waiting. This gap against the original spec is accepted. Do not add a drift bypass unless the product direction changes.
