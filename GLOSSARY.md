# Karpenter disruption

Decisions about taking pods off a node, holding that node, and either deleting it or putting it back into service.

## Language

**Evacuation**:
A decision to take the pods off one node so that node can become standby.
_Avoid_: compaction, consolidation

**Evacuation timeout**:
The longest an evacuation may run before it is abandoned and its node takes pods again.
_Avoid_: drain timeout

**Standby**:
A node that stays in the cluster and is held back from ordinary scheduling until it is reclaimed or reactivated.
_Avoid_: empty node, cordoned node

**Reclamation**:
The decision to delete a standby node.
_Avoid_: emptiness, empty consolidation

**Reclamation delay**:
The time a node must stay in standby before reclamation may delete it.
_Avoid_: consolidateAfter

**Kept capacity**:
The CPU a NodePool holds, its active and standby nodes together.

**Active capacity**:
The CPU of nodes that are not standby, whether or not pods use all of it. For capacity decay it includes the NodePool's own nodes and those of the NodePools whose work it should be ready to take over.
_Avoid_: in-use capacity, requested CPU

**Capacity decay**:
How a NodePool's kept capacity falls from a recent peak of active capacity back toward the current active capacity, faster or slower depending on whether new capacity was recently hard to get. Standby nodes beyond the kept capacity are reclaimed.
_Avoid_: standby decay, exponential reclamation

**Standby loop**:
The loop that marks nodes as standby and reclaims them, apart from the disruption loop that runs consolidation and the other disruption methods. Each pass also repairs standby state and samples active capacity for capacity decay.

**Reactivation**:
The decision to let a standby node take pods again. Provisioning and evacuation make this decision in the same way.
_Avoid_: activation

**Consolidation**:
A disruption method that moves pods onto cheaper or better-packed capacity. Upstream consolidation deletes its source nodes in the same action; score-based consolidation evacuates them.
_Avoid_: compaction

**Score-based consolidation**:
The consolidation method for opted-in NodePools that ranks moves by score and carries out the chosen move as evacuations.
_Avoid_: compaction, score-based evacuation

**Move**:
One or more source nodes chosen to be evacuated together, with the simulated placement of their pods.
_Avoid_: move set, compaction

**Batch move**:
A move of several source nodes whose pods all fit on existing or standby nodes, with no new node.
_Avoid_: batch

**Pair move**:
A move of exactly two source nodes whose pods fit on at most one new node.
_Avoid_: pair, 2→1 move

**Score**:
The number that ranks moves within one pass. A move whose score is not positive is never chosen.
_Avoid_: priority, nodePriorityScore

**Budget-blocked pool**:
A NodePool that cannot start further scored consolidation moves in this pass because its configurable disruption budget for the relevant reason allows none.
_Avoid_: blocked pool

**Pacing-blocked pool**:
A NodePool whose non-empty disruption is held back by pool-scoped pacing: invalid pacing configuration or an active rate cooldown for its pacing scope.
_Avoid_: blocked pool, throttled pool
