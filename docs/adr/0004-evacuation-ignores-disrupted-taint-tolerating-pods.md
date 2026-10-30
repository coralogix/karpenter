# Evacuation does not select nodes running pods that tolerate the disruption taint

Status: accepted

Evacuation must wait for every reschedulable workload pod to leave the source, or standby repair immediately undoes standby. A pod that tolerates `karpenter.sh/disrupted:NoSchedule` can be scheduled back onto the source while it carries that taint, so evacuating such a node cannot succeed. Score-based consolidation therefore does not select a node running such a pod, the same way it leaves alone a node running a `karpenter.sh/do-not-disrupt` pod. Tolerating only the standby taint does not exclude a node: that taint is not on the source during the drain.

## Considered Options

- **Wait for these pods until the evacuation timeout.** Rejected: every attempt times out, spends `Underutilized` budget and pacing, and may leave a replacement behind.
- **Evict them anyway.** Rejected: they can return to the source during the drain, and many of them tolerate every taint, so they can also return after it becomes standby.
