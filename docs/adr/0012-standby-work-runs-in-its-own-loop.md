# Standby marking and reclamation run in their own loop

Status: accepted

The disruption loop runs its methods one at a time. A pass that reaches score-based consolidation takes 8–25 seconds, most of it the 15-second validation wait and the single-node search. On cx972 that meant an emptied node waited about 11 seconds (p50) to be marked standby, and about 27 seconds more to be reclaimed after its reclamation delay. Standby marking and reclamation now run in a separate singleton loop, together with the per-pass standby work: the standby-node metric, capacity decay sampling ([ADR-0011](0011-reclamation-by-capacity-decay.md)) and standby repair ([ADR-0002](0002-standby-state-lives-on-the-node.md)). The loop polls every second and starts its next pass at once after it starts a command. The main disruption loop no longer runs any of this. Both loops start commands through the same queue.

**No start lock.** Running both loops at once lets a standby-loop command land between score-based consolidation's validation snapshot and the start of its command, about 3–4 seconds on cx972. Instead of serializing command starts and rechecking under a lock, each operation tolerates the state it finds when it acts, in the spirit of [ADR-0001](0001-evacuation-standby-reclamation.md). A lock would not make Kubernetes state stop changing between a check and the action that follows it.

- Starting a score-based command clears standby on every destination that receives pods, judged from the stored Node rather than the snapshot, so a destination marked standby after the snapshot is reactivated.
- Clearing standby on a Node that no longer exists, or that is being deleted, is not an error. The command starts, and pods whose destination is gone stay pending until provisioning places them.
- Marking keeps an existing standby timestamp, so evacuating a source that already went standby does not push back its reclamation.
- A node that gains a pod right after it is marked is repaired on the next standby-loop pass.
- A node being reclaimed keeps its standby taint, so the main loop's removal of stray `karpenter.sh/disrupted` taints cannot make it schedulable. Only the main loop runs that cleanup.
- Reclamation still deletes from its snapshot ([ADR-0003](0003-reclamation-policy.md)), so it may delete a node that a move reactivated moments earlier. Those pods are evicted again and handled by provisioning.

The one exception is the queue's own map. Checking that no command holds a node and recording the new command happen in one step under the queue's mutex, so drift and reclamation cannot both take the same drifted standby node. A command that finds a node already held fails and is retried on the next pass.

## Consequences

- Each loop computes disruption budgets from its own snapshot, so a pool can briefly exceed a budget by the nodes the other loop started in the meantime.
- Capacity decay samples active capacity on every standby-loop pass, about once a second, so it catches shorter peaks than it did once per disruption pass.
- Building full candidates for every node costs about 0.55 seconds per method on a 1,400-node cluster. The standby loop first keeps only nodes in pools that can hold standby nodes and that look empty or carry the standby taint.
- Single-node consolidation keeps its one-minute search limit, because it still delays score-based consolidation.

## Considered Options

- **One loop that runs marking and reclamation more often**, at the start of every pass and during the validation wait, with a shorter idle poll. It adds no concurrency, but it ties standby work into score-based consolidation's internals and only partly removes the wait.
- **One lock held across every command start, with rechecks under it.** Rejected: it adds code to every start without making the operations safe on their own.
- **Triggering on pod and Node events instead of polling.** Not worth the extra watches over a one-second poll.
