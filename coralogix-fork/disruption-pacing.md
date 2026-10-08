# Disruption pacing

This fork adds optional disruption pacing for non-empty consolidation and drift. A NodePool can have its own pacing state, or several managed NodePools can share one pacing group. Pacing applies to single-node, multi-node, and score-based consolidation, plus dynamic and static drift. Empty-node deletion is exempt.

## Configuration

Set a finite positive rate on a NodePool. The batch cap is optional; when omitted, there is no pacing batch cap.

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: example
  annotations:
    karpenter.coralogix.net/disruption-pacing-per-minute: "1"
    karpenter.coralogix.net/disruption-pacing-per-batch: "2"
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 0s
```

| Annotation | Meaning |
|------------|---------|
| `karpenter.coralogix.net/disruption-pacing-per-minute` | Target average number of non-empty candidate nodes started per minute. Fractional rates are supported. |
| `karpenter.coralogix.net/disruption-pacing-per-batch` | Optional positive integer cap on non-empty candidates started in one disruption-method pass. |
| `karpenter.coralogix.net/disruption-pacing-group` | Optional non-empty DNS label. Managed NodePools with the same value share pacing state and must have matching rate and batch settings. |

Without a group annotation, pacing is per NodePool. A missing or invalid rate leaves an ungrouped NodePool unpaced; a missing or invalid batch cap is ignored. A rate must be finite and greater than zero. The batch cap must be a positive integer when set; omission means unlimited.

### Sharing a limit across NodePools

Add the same group, rate, and batch cap to every managed NodePool that should share pacing. Every managed member is checked, including pools with no current disruption candidates. Members must have the same finite positive parsed rate and batch cap; numeric spellings such as `0.2` and `0.20` are equivalent. Omit the cap on every member for an unlimited cap. A grouped member with a missing, invalid, or conflicting rate or cap blocks non-empty consolidation and dynamic or static drift for the whole group and records an event. A present but empty or invalid group annotation also blocks its affected member(s); it is not treated as an absent group. Pacing resumes when the group configuration is consistent; fixing the configuration does not reset its outstanding cooldown. During a multi-manifest rollout, temporarily inconsistent settings pause the group.

This example gives on-demand and reserved pools one shared allowance of one non-empty node every five minutes while candidates remain available:

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: on-demand-node-pool
  annotations:
    karpenter.coralogix.net/disruption-pacing-group: stable-workloads
    karpenter.coralogix.net/disruption-pacing-per-minute: "0.2"
    karpenter.coralogix.net/disruption-pacing-per-batch: "1"
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 0s
---
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: reserved-node-pool
  annotations:
    karpenter.coralogix.net/disruption-pacing-group: stable-workloads
    karpenter.coralogix.net/disruption-pacing-per-minute: "0.2"
    karpenter.coralogix.net/disruption-pacing-per-batch: "1"
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 0s
```

The rate and cap are group-wide. The cap of `1` applies across both pools and all commands started in one disruption-method pass, including static drift; it is not reapplied for each pool or command.

## Behavior

- A candidate counts as non-empty for pacing when it has one or more reschedulable pods. Candidates with no reschedulable pods and empty-node deletion through the `Empty` disruption method are exempt.
- A rate of `1` targets one candidate per minute; `0.5` targets one every two minutes; `2` targets two per minute on average. This is an average rate, not a strict rolling-window limit. When a batch cap is greater than one, work can start in a batch up to the cap. Idle time does not accumulate extra credit.
- For each pacing scope, the next-eligible time is the latest successful `StartCommand` time plus the pass's aggregate count of successfully started non-empty candidates divided by the rate, in minutes. A command that fails to start does not consume pacing allowance. A later failure in queued work does not refund an already successful start.
- Pacing limits successful admission and command-start spacing; it does not guarantee spacing between later pod evictions. Delete and replace commands charge the existing non-empty candidates, not net node reduction.
- Pacing is an additional gate. Per-NodePool disruption budgets and their existing reasons remain unchanged: drift uses `Drifted`, and consolidation uses its existing reason. Pacing does not change expiration behavior.
- Outstanding cooldown carries across rate or group-membership changes. Neither a slower nor a faster rate changes an existing deadline; the next successful start uses the rate from final admission. State is in memory and resets on controller restart or leader failover.
- When a grouped configuration becomes invalid, the whole group's non-empty consolidation and dynamic or static drift are blocked until all members agree again. Once corrected, processing resumes with the existing cooldown state.

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotations remain valid Kubernetes metadata.
- Upstream ignores the annotations and does not enforce pacing.
- No manifest or CRD changes are required to roll back.

After rollback, non-empty consolidation and drift resume at upstream speed, subject to disruption budgets.
