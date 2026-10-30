# Standby state lives on the node and is repaired from it

Status: accepted

Standby has its own taint and its own NodeClaim timestamp. It does not reuse `karpenter.sh/disrupted`, because disruption removes that taint from any node that is not in its in-memory queue. After a restart, that would silently end every standby. Both marks are written to the API server, so standby and the time already spent in it survive a crash.

The taint is `karpenter.coralogix.net/standby` with effect `NoSchedule` and no value, like the other taints in this codebase. The timestamp is the NodeClaim annotation `karpenter.coralogix.net/standby`, holding the RFC3339Nano UTC start time. Karpenter sets both. Operators never do. Their user-facing documentation is in `coralogix-fork/score-based-consolidation.md`.

Nothing records a phase such as "marking" or "waking". Every disruption pass reads the node and repairs it. Standby on a node that has gained a reschedulable pod is cleared. Standby on a pool that no longer qualifies is cleared. A taint with no usable timestamp gets one starting now. The timestamp is written before the taint, so a crash between the two leaves an unmarked node and never a taint with a stale time. A restart can leave a node marked again or not yet marked, and the next pass fixes it. The alternative, a stored phase with an owner, is what made the previous attempt large.

On rollback, upstream ignores the standby taint and the controller no longer removes it. The taint has to be removed by hand.
