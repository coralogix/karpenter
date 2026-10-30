# Moves may evacuate several nodes

Status: accepted

A move of score-based consolidation may start several evacuations in one command: a batch move, whose sources' pods all fit on existing or standby nodes, or a pair move, whose two sources' pods fit on exactly one new node. No move needs more than one new node. Each evacuation still concerns one node and ends with that node in standby (ADR-0001).

Pair moves exist because a chain of single-node moves cannot merge two half-full nodes onto one new node while every move must save money: the first step, one node onto a bigger new node, costs more on its own. Batch moves only add throughput, but disruption runs one command per pass after a 15-second validation delay, so emptying a pool one node at a time is slow.

All sources of a move drain at the same time. The evacuation timeout covers the whole command (ADR-0005), so draining sources one after another would make a batch move time out in proportion to its size.

## Considered Options

- **Single-node moves only, with chains across passes.** Rejected: it cannot reach pair-move outcomes, and it is slower for the cases batch moves cover.
- **Moves needing several new nodes.** Rejected, as upstream does: trading one node for several rarely saves money and widens what a failed launch can leave behind.
