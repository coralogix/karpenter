# Karpenter disruption

Decisions about taking pods off a node, holding that node, and either deleting it or putting it back into service.

## Language

**Evacuation**:
A decision to take the pods off one node so that node can become standby.
_Avoid_: compaction, consolidation

**Standby**:
A node that stays in the cluster and is held back from ordinary scheduling until it is reclaimed or reactivated.
_Avoid_: empty node, cordoned node

**Reclamation**:
The decision to delete a standby node.
_Avoid_: emptiness, empty consolidation

**Reclamation delay**:
The time a node must stay in standby before reclamation may delete it.
_Avoid_: consolidateAfter

**Reactivation**:
The decision to let a standby node take pods again. Provisioning and evacuation make this decision in the same way.
_Avoid_: activation

**Consolidation**:
A disruption decision that removes a node's pods and deletes the node as one action.
_Avoid_: evacuation
