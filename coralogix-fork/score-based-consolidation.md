# Score-based consolidation

This fork adds an alternate consolidation disruption method for NodePools that opt in via annotation. It simulates single-node moves and ranks them by estimated hourly dollar savings. Empty nodes are not deleted right away; they go on **standby** and are reclaimed later.

## NodePool configuration

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: example
  annotations:
    karpenter.coralogix.net/score-based-consolidation: ""
spec:
  disruption:
    consolidationPolicy: WhenEmptyOrUnderutilized
    consolidateAfter: 0s
```

| Annotation | Meaning |
|------------|---------|
| `karpenter.coralogix.net/score-based-consolidation` | Opts the NodePool in (presence is enough; value is ignored). |
| `karpenter.coralogix.net/reclamation-delay` | How long a node stays on standby before it may be deleted. Default **15s**. |
| `karpenter.coralogix.net/evacuation-timeout` | How long an evacuation may run before it is abandoned. Default **5m**. |

Both durations are Go durations. `0s` is taken literally; empty, malformed, or negative values fall back to the default and raise an event on the NodePool.

Optional [disruption pacing](disruption-pacing.md) annotations apply as well.

## How it works

Annotated NodePools are handled by the score-based method instead of standard emptiness, single-node and multi-node consolidation. Drift is unchanged and still uses the standard drift methods and `Drifted` budgets.

### Standby and reclamation

Empty nodes are tainted `karpenter.coralogix.net/standby:NoSchedule` and annotated with the time they entered standby, rather than deleted. Standby requires `spec.disruption.consolidateAfter` to be set. Karpenter manages these marks; do not set them in manifests. Marking evicts nothing and uses no disruption budget.

**Reclamation** deletes a node that is still empty and has been on standby for `reclamation-delay`. It uses the `Empty` reason and budgets and is not paced.

### Reactivation

A standby node counts as existing capacity for provisioning and disruption. If a scheduling result places pods on it, Karpenter clears the standby marks so the pods can bind, instead of launching a new node.

### Evacuation

On `WhenEmptyOrUnderutilized` pools, the method picks the single-node move with the highest savings and evacuates that node: it is tainted, its workloads are evicted (respecting PodDisruptionBudgets, after any replacement is ready), and it is left on standby with its NodeClaim intact until it is reactivated or reclaimed. Evacuation uses `Underutilized` budgets and pacing. If the node is not empty within `evacuation-timeout`, the evacuation is abandoned and the node takes pods again.

Savings account for the price of any new node and of standby nodes that would be reactivated. Moves needing more than one new node are rejected. Each searched pass logs its decision, including why no move was chosen.

## Rollback to upstream

Rolling back to upstream Karpenter is safe and needs no manifest or CRD changes. Upstream ignores the annotations, and the NodePools resume standard emptiness and consolidation. Nodes may keep the standby taint; remove it manually so scheduling is not blocked:

```sh
kubectl taint nodes <node> karpenter.coralogix.net/standby:NoSchedule-
```

Leftover standby annotations are inert and disappear with the Node.
