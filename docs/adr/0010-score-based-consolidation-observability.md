# Observability of score-based consolidation

Status: accepted

The per-decision log is the record of why score-based consolidation acted or did not. Each pass in which a search ran writes exactly one Info log. If a move was chosen, the log carries everything needed to recompute its score by hand:
- the move's kind and sources;
- the new node and any reactivated standby nodes;
- the savings;
- each term's contribution with its inputs and baselines, plus a short reason when a term that is on contributed 0;
- search counts and duration;
- the chosen move's rank and how many higher-ranked moves failed validation;
- up to three alternatives.

A pass that fails with an error writes no decision log, because the controller reports the error. If nothing was chosen, the log gives one reason with the search counts. The reason `blocked_by_budget_or_pacing` is used only when budgets or pacing left no candidate to search. When some NodePools are blocked and the remaining candidates find nothing, the reason describes the search (for example `no_saving_move`) and the log's `blockedNodePools` lists the blocked pools. A pass skipped because nothing changed since the last empty result logs nothing, so an idle cluster stays quiet. Logs carry no pod names.

Events describe only decisions that were made. Upstream simulation emits `ConsolidationCandidate` for every move it simulates, so score-based consolidation suppresses those during the search. It emits `ConsolidationCandidate` for the chosen move's sources only, and `ConsolidationRejected` for each kept move that failed validation. Upstream `Unconsolidatable` events are unchanged. A deduplicated `DisruptionBlocked` event on the NodePool covers the two cases upstream leaves silent: an `Underutilized` budget used up by disruptions already in progress, and pacing holding back all of a pool's candidates.

Metrics reuse upstream disruption metrics with `consolidation_type="score-based"`. `consolidation_timeouts_total` counts every search the deadline cut short, not only searches that found nothing. The fork adds:
- `karpenter_voluntary_disruption_score_based_move_evaluations_total{result}`, for what the search rejects;
- `karpenter_voluntary_disruption_score_based_moves_selected_total{kind}`, for whether batch and pair moves are used;
- `karpenter_nodeclaims_evacuated_total{nodepool, capacity_type}`, because evacuations delete nothing and so never appear in `nodeclaims_disrupted_total`;
- `karpenter_voluntary_disruption_evacuation_duration_seconds{outcome}`;
- `karpenter_nodes_standby_reactivated_total{nodepool, source}`, where the caller that clears standby supplies `source`, so nothing is stored on the node (ADR-0001);
- `karpenter_nodepools_standby_nodes{nodepool}`.

Tracing has one span per disruption controller pass, with a child span for each method and its outcome. The score-based method's span contains the search phases, the validation wait, and validation.

## Considered Options

The earlier implementation emitted much more. The following were dropped deliberately:
- score values as metrics, which belong in the log;
- separate counters for evacuation commands, standby marking, and reclamation, which the duration histogram, the gauge, and `nodeclaims_disrupted_total{reason="empty"}` already cover;
- a gauge of empty nodes with a state label;
- a disruption loop iteration counter;
- a separate search-complete log;
- spans for short phases, whose counts are in the log;
- an extra "no positive savings" event, which upstream's "can't replace with a cheaper node" already covers.
