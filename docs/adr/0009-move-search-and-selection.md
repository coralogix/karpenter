# Move search and selection

Status: accepted

Score-based consolidation searches in up to three phases sharing one 15-second submission deadline. The producer checks the deadline before submitting each move. Submission can wait for a worker, so moves already submitted or waiting to be submitted may start and finish after the deadline; their results are still used. The deadline does not cancel simulations or impose a hard limit on search duration. A search is reported as timed out only when the producer stops submitting moves after observing expiry; pending work can finish after expiry without setting that flag. The phases and limits come from the earlier implementation and are fixed constants, not settings.

1. **Single-node moves.** The candidates are shuffled, then every one is simulated alone. Shuffling replaces the earlier price-per-workload sort: order only matters when the deadline cuts the search short, and a fixed order would leave the same nodes unevaluated every time.
2. **Batch moves**, only if a single-node move was found. Growth starts from the three best single-node moves that need no new node. Other such moves are added one at a time, and each addition is simulated jointly and kept only if the batch still needs no new node. Each starting move gets at most 256 joint simulations. A batch move may not exceed any source pool's `Underutilized` budget or disruption-pacing per-batch cap.
3. **Pair moves**, only if no single-node move saves money and the deadline was not hit. All pairs are tried when there are at most 256. Otherwise 256 distinct pairs are sampled, weighting each node by `max(0.05, 1 − CPU utilization)`. A pair is kept only if it lands on exactly one new node.

The ten best moves by score are kept, plus the best single-node move if it is not among them. Batch moves usually outscore single-node moves, because their terms add up. Without that one reserved place, all ten kept moves could share a node that fails validation, and the pass would be wasted. The method waits out the rest of the validation delay, then validates every kept move in search-score order. Validation re-simulates the whole move and rechecks budgets and pacing for all of its sources. Changed standby destinations are allowed: each move that passes validation is rescored using the re-simulated destinations and their prices from the search snapshot, while keeping the replacement launch options used for the original price estimate. The move with the highest positive rescored score is run, using its validated pod placements. Equal rescored scores retain search order. This accounts for changes in standby cost during the validation delay that can change which move saves the most. Pacing charges one unit per source node. The cluster is marked consolidated only when every eligible move was evaluated, no move was kept, no simulation failed, and no candidate was lost to budgets or pacing.

Candidates still pass upstream's `consolidateAfter` check. The earlier implementation ignored it, which required changing the upstream NodeClaim controller for a setting operators can already set to `0s`.

## Consequences

The search is one unit that takes the candidates, a simulate function, a random source, and the per-pass baselines, and returns ranked moves. That makes it the test boundary: search rules and the score are tested in plain Go with a fake simulator and hand-computed numbers, which is much faster than envtest. Ginkgo specs only show the controller carrying out a single-node move, a batch move, and a pair move.

## Considered Options

- **Also reserve places for the single-node move that overlaps least with the others, and for the smallest batch move**, as the earlier implementation did. Rejected: one reserved single-node move covers the failure that matters.
- **Run the first move that passes validation and reject changed standby destinations.** Rejected: a changed destination can still save money, and rescoring all kept moves can find a better move after the validation delay.
- **Enforce the deadline again when a worker starts a simulation.** Rejected: the submission deadline is sufficient; allowing pending work to finish keeps deadline handling simple and preserves its results.
- **Make the search limits configurable.** Rejected: nothing showed a need, and each setting adds parsing, validation, and documentation.
