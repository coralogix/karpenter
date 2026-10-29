# Standby state lives on the Node

Status: accepted

Standby uses its own Node taint and a timestamp annotation on that same Node. Entering standby sets both in one Node update; leaving standby clears both in one update. Both persist across controller restarts. The taint is `karpenter.coralogix.net/standby` with effect `NoSchedule` and no value; the Node annotation uses the same key and holds the RFC3339Nano UTC start time. Karpenter sets these marks; operators do not.

The taint is authoritative. Repair clears standby when the node has gained a reschedulable pod or its pool no longer qualifies. A taint with no usable timestamp gets one starting now. An annotation without a taint does not establish standby or permit reclamation and is cleared. Subsequent entry into standby writes a fresh timestamp with the taint. Zero or negative reclamation delay requires no extra pass after backfill, but cache visibility may defer reclamation; no synchronization is added to force it.

This revises the previous Node taint plus NodeClaim timestamp design. That split required two writes and allowed partial states. Timestamp-first can count time before scheduling is blocked; reversing the order can pair a newly added taint with a stale valid timestamp if the timestamp write fails. Co-locating both marks removes that cross-object partial-write problem without stored marking or waking phases. It does not coordinate reclamation with reactivation or eliminate scheduling races. The previous solution has not been deployed, so no migration, fallback reads, or legacy annotation cleanup is required. Implementation is tracked in issue 04 under `.scratch/evacuation-standby-reclamation/`.

Standby does not reuse `karpenter.sh/disrupted`: disruption removes that taint from nodes outside its in-memory queue, which would silently end standby after a restart. On rollback to upstream, the standby taint must be removed manually. Update the public mark documentation in `coralogix-fork/score-based-consolidation.md` when implementing issue 04.
