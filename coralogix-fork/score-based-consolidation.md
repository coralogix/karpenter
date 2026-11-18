# Score-based consolidation

This fork adds an alternate consolidation disruption method for NodePools that opt in via annotation. It simulates single-node moves and ranks them by a score combining normalized cost savings and unused CPU. Empty nodes are not deleted right away; they go on **standby** and are reclaimed later.

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
| `karpenter.coralogix.net/utilization-weight` | How much unused CPU counts in the score. Default **1**; anything other than a positive number falls back to the default and raises an event on the NodePool. |
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

On `WhenEmptyOrUnderutilized` pools, the method picks the single-node move with the highest score and evacuates that node: it is tainted, its workloads are evicted (respecting PodDisruptionBudgets, after any replacement is ready), and it is left on standby with its NodeClaim intact until it is reactivated or reclaimed. Evacuation uses `Underutilized` budgets and pacing. If the node is not empty within `evacuation-timeout`, the evacuation is abandoned and the node takes pods again.

Moves needing more than one new node are rejected. Each searched pass logs its decision, including why no move was chosen and how the chosen move's score was computed.

After the 15-second validation delay, the method re-simulates its best moves on a fresh view of the cluster and runs the highest-scoring one.

### Score

Each move's **savings** are the source nodes' hourly prices minus the estimated price of any new node and of standby nodes that would be reactivated. A move is chosen only if both its savings and its score are positive. The score is the sum of two terms:

- **Cost**: each source's price per CPU, relative to the average of its NodePool and capacity type, scaled by the fraction of the sources' price the move saves. Comparing within a capacity type stops cheap Spot nodes from always losing to on-demand ones.
- **Utilization**: each source's unused CPU fraction, multiplied by `utilization-weight`.

## Events

| Event | On | When |
|-------|----|------|
| `ConsolidationCandidate` | Nodes and NodeClaims | The node is a source of the chosen move, after validation. Moves simulated during the search raise no candidate events. |
| `ConsolidationRejected` | Nodes and NodeClaims | The node is a source of a kept move that failed validation and ranked above the chosen move (or of every failed move when none was chosen). The message gives the reason, such as `scheduling`, `budget`, `pacing` or `no_savings`. |
| `Unconsolidatable` | Nodes and NodeClaims | Unchanged from upstream: simulation found the node cannot be consolidated. |
| `DisruptionBlocked` | NodePool | The pool's `Underutilized` budget is used up by disruptions already in progress, such as running evacuations. A budget that allows no disruptions at all raises upstream's budget event instead. |
| `DisruptionBlocked` | NodePool | [Disruption pacing](disruption-pacing.md) holds back all of the pool's candidates. |

`DisruptionBlocked` events are deduplicated for one minute.

## Metrics

Upstream disruption metrics are filled with `consolidation_type="score-based"`:

- `karpenter_voluntary_disruption_decision_evaluation_duration_seconds`: duration of each pass.
- `karpenter_voluntary_disruption_decisions_total`: commands started.
- `karpenter_voluntary_disruption_failed_validations_total`: once per kept move that fails validation.
- `karpenter_voluntary_disruption_consolidation_timeouts_total`: every search the 15-second deadline cut short, including searches that still found moves.

The fork adds:

- `karpenter_voluntary_disruption_score_based_move_evaluations_total{result}`: moves simulated by the search. `result` is `feasible` (saves money and scores above 0), `no_op` (nothing to do, or needs more than one new node), `no_savings`, `non_positive_score`, or `error`.
- `karpenter_voluntary_disruption_score_based_moves_selected_total{kind}`: chosen moves. `kind` is `single`.

## Rollback to upstream

Rolling back to upstream Karpenter is safe and needs no manifest or CRD changes. Upstream ignores the annotations, and the NodePools resume standard emptiness and consolidation. Nodes may keep the standby taint; remove it manually so scheduling is not blocked:

```sh
kubectl taint nodes <node> karpenter.coralogix.net/standby:NoSchedule-
```

Leftover standby annotations are inert and disappear with the Node.
