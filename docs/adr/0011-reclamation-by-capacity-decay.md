# Reclamation by capacity decay

Status: accepted

A NodePool can opt into capacity decay through `karpenter.coralogix.net/reclamation-capacity-decay-half-life`. When it does, decay replaces the fixed reclamation delay ([ADR-0003](0003-reclamation-policy.md)). The pool tracks the decaying peak E of its active capacity, measured in allocatable CPU cores. E rises at once to any higher active capacity and falls back toward the current active capacity with the configured half-life. The pool keeps standby nodes up to `E − its own active capacity`, and reclamation deletes the rest, oldest first, within the `Empty` budget.

The goal is to keep spot capacity through recurring demand, so pods don't spill to on-demand fallback pools, and to shed it steadily once demand stops recurring. A fixed delay releases a whole burst at once. It keeps all of a recurring peak or none of it.

The decisions a later reader would question:

- **Active capacity, not requests.** Active capacity is the allocatable CPU of initialized, non-deleting, non-standby nodes. Pods often cannot use all of a node, for example a 16xlarge that fills like a 12xlarge, so requests would undercount the capacity the pool needs.
- **Other pools count.** `reclamation-capacity-decay-pools` adds the active capacity of other NodePools, such as an on-demand fallback pool, to this pool's signal. Decay still applies only to the annotated pool. This lets spot hold enough standby to take back the fallback pool's work. The configuration is per pool and creates no pacing-style group.
- **Two half-lives.** `reclamation-capacity-decay-insufficient-capacity-half-life` applies while the pool's own NodeClaims have failed to launch with insufficient capacity in the last hour, a window `reclamation-capacity-decay-insufficient-capacity-window` can change. Core sees a launch failure only when the whole launch fails; partial fleet failures are invisible to it. During scarcity, the provider's unavailable offerings route pods to the fallback pool without any launch attempt. A count or rate of failures would therefore go silent while capacity is scarce. "Any failure in the last hour" does not, because the first failure of each episode is enough. The record lives in memory, and a restart forgets it.
- **Evacuated capacity counts.** Evacuated capacity earns headroom like any other freed capacity, so consolidation savings arrive about one half-life later. Telling the two apart would need a cause mark on the node, which [ADR-0001](0001-evacuation-standby-reclamation.md) avoids.
- **E is in memory.** It is updated once per disruption pass, whether or not any method runs. After a restart it starts at the larger of the active capacity and the pool's current kept capacity, so a restart only delays reclamation.

## Considered Options

- **Decaying the pool size toward zero.** Rejected because a small spike on a large pool is forgotten almost at once.
- **Decaying a count of standby nodes.** Rejected because it has no memory of the peak, and nodes of 16 to 96 cores are not interchangeable.
- **Per-node random lifetimes.** Rejected because they encode no pool-level logic.
- **One annotation holding JSON, or a custom grammar such as `2m,insufficient-capacity=10m`.** Rejected in favour of plain durations that share the parser with the other reclamation annotations ([ADR-0006](0006-duration-annotations-share-validation.md)).
- **Adapting to insufficient-capacity failure rates, unavailable-offering fractions, or fallback-pool landings.** Deferred.
