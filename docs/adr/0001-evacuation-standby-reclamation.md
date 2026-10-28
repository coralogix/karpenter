# Evacuation, reclamation, and reactivation are separate decisions

Status: accepted

**Before.** Score-based consolidation drains a node and deletes it in one disruption, so a later decision cannot use that node, and an empty node is removed only by an in-memory wait.

**After.** On score-based NodePools, evacuation takes the pods off one node and marks it standby. It is one move off one node, and the node stops taking pods when it starts. A chain of moves happens across later decisions, not inside one. Reclamation is the only decision that deletes a standby node. Drift is unchanged.

Reactivation is how a standby node comes back. Provisioning and disruption place pods on standby capacity through the shared scheduling simulation, which ignores the standby taint and keeps every other scheduling constraint. Simulations have no side effects. Only applying a scheduling result clears standby, and that holds for every disruption method that uses the scheduling simulation, drift included. Virtual buffer pods reactivate standby too, since a buffer counted on capacity ordinary pods cannot use would be a false promise. Buffer accounting protects a buffer-only placement without nominating it, and real pod placements use the existing nomination window. Once cleared, the node is open to any pod. If clearing standby fails, disruption does not evict on the strength of that destination.

Nothing records which decision woke a node, and nothing reserves it. Reactivation and reclamation stay independent, with no coordination or race checks between them. A check cannot guarantee the node is still available between the check and the deletion, and stronger coordination adds state that has to survive restarts. That was what made the previous attempt large. A destination may therefore disappear after selection, and pending pods are handled by later provisioning. Partial updates are retried by the repair pass in ADR-0002, with no persistent activating state.
