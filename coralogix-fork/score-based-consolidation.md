# Score-based consolidation

This fork adds an alternate consolidation disruption method for NodePools that opt in via annotation. It simulates single-node moves, merges pairs of nodes onto one new node when no single-node move helps, grows batch moves that empty several nodes at once, and ranks moves by a score combining normalized cost savings and unused CPU. Empty nodes are not deleted right away; they go on **standby** and are reclaimed later.

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
| `karpenter.coralogix.net/evacuation-timeout` | How long an evacuation may run before it is abandoned. Default **5m**. A move whose sources span NodePools uses the shortest. |

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

On `WhenEmptyOrUnderutilized` pools, the method picks the move with the highest score and evacuates its source nodes: they are tainted, their workloads are evicted (respecting PodDisruptionBudgets, after any replacement is ready), and each is left on standby with its NodeClaim intact until it is reactivated or reclaimed. All sources of a move drain at the same time, and each goes on standby as soon as it is empty. Evacuation uses `Underutilized` budgets and pacing, which counts one node per source. If the sources are not all empty within `evacuation-timeout`, the evacuation is abandoned: sources already on standby stay there, and the rest take pods again.

Moves needing more than one new node are rejected. Each searched pass logs its decision, including why no move was chosen and how the chosen move's score was computed.

When no single-node move saves money and scores above zero, and the search's 15-second deadline has not passed, the method tries **pair moves**: two source nodes whose pods fit together on at most one new node, such as two half-full nodes that cannot move anywhere cheaper alone. A pair is tried only if it fits each source pool's `Underutilized` budget and [disruption pacing](disruption-pacing.md). When more than 256 pairs qualify, 256 are sampled, favouring nodes with low CPU use, so each pass tries a different sample. Half of the 256 go to pairs from one NodePool, because pods already running on a pool are likelier to fit on one replacement together than pods split across pools for a reason; when either kind has fewer pairs than its half, the other kind gets the rest. A pair moves like any other move: any new node launches first, then both sources drain and go on standby.

After the 15-second validation delay, the method re-simulates its ten best moves on a fresh view of the cluster. It then grows **batch moves** from the three best of those that need no new node: other single-node moves that need no new node are added one at a time, and an addition is kept only if the whole batch still fits on existing or standby nodes, saves money, and scores higher than before. A batch never exceeds a source pool's `Underutilized` budget or the [disruption pacing](disruption-pacing.md) per-batch cap, and each starting move gets at most 32 joint simulations. The method runs the highest-scoring of the re-simulated moves and the grown batches.

### Score

Each move's **savings** are the source nodes' hourly prices minus the estimated price of any new node and of standby nodes that would be reactivated. A move is chosen only if both its savings and its score are positive. The score is the sum of two terms:

- **Cost**: each source's price per CPU, relative to the average of its NodePool and capacity type, scaled by the fraction of the sources' price the move saves. Comparing within a capacity type stops cheap Spot nodes from always losing to on-demand ones.
- **Utilization**: each source's unused CPU fraction, multiplied by `utilization-weight`.

## Events

| Event | On | When |
|-------|----|------|
| `ConsolidationCandidate` | Nodes and NodeClaims | The node is a source of the chosen move, after validation; for a batch or pair move, every source. Moves simulated during the search or batch growth raise no candidate events. |
| `ConsolidationRejected` | Nodes and NodeClaims | The node is a source of a kept move that failed validation. The message gives the reason, such as `scheduling`, `budget`, `pacing` or `no_savings`. |
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

- `karpenter_voluntary_disruption_score_based_move_evaluations_total{result}`: moves simulated by the search and by batch growth. `result` is `feasible` (saves money and scores above 0), `no_op` (nothing to do, or needs more than one new node), `no_savings`, `non_positive_score`, or `error`. A batch growth addition that validation rejects is not counted.
- `karpenter_voluntary_disruption_score_based_moves_selected_total{kind}`: chosen moves. `kind` is `single`, `batch`, or `pair`.
- `karpenter_nodeclaims_evacuated_total{nodepool, capacity_type}`: sources put on standby by an evacuation, once each. Evacuations delete nothing, so they never appear in `karpenter_nodeclaims_disrupted_total`.
- `karpenter_voluntary_disruption_evacuation_duration_seconds{outcome}`: time from the start of an evacuation until it ends. `outcome` is `succeeded` when every source is on standby, or `failed` on timeout or another failure, even if some sources already reached standby.
- `karpenter_nodes_standby_reactivated_total{nodepool, source}`: standby nodes reactivated. `source` is `provisioning` or `disruption`, whichever decision placed pods on the node.
- `karpenter_nodepools_standby_nodes{nodepool}`: standby nodes per NodePool, set on every disruption pass. A NodePool's series disappears when the pool is deleted, stops using score-based consolidation, or can no longer hold standby nodes (`consolidateAfter: Never`).

## Spans

Each pass has one `karpenter.disruption.score_based_consolidation` span. It carries the chosen move's `rank` (its position by search score; a batch move ranks as the move it grew from) and `kind`. Its children, prefixed with the pass span's name:

- `.search`: building the simulator and the search. It has no children when no candidate is eligible. Otherwise its children `.search.single` and `.search.pair` (when the single phase found nothing before the deadline) carry `candidates`, `evaluated_moves` and `deadline_hit`.
- `.validation_wait`: the rest of the validation delay after the search.
- `.validate`: the fresh snapshot, candidate preparation and rescoring of the kept moves.
- `.batch`: batch growth, with `seeds` and `joint_simulations`.

`karpenter.disruption.simulate_scheduling` spans nest under the phase that ran them.

## Rollback to upstream

Rolling back to upstream Karpenter is safe and needs no manifest or CRD changes. Upstream ignores the annotations, and the NodePools resume standard emptiness and consolidation. Nodes may keep the standby taint; remove it manually so scheduling is not blocked:

```sh
kubectl taint nodes <node> karpenter.coralogix.net/standby:NoSchedule-
```

Leftover standby annotations are inert and disappear with the Node.
