# Historical implementation plan: standalone reclamation and non-empty compaction

Status: superseded by the current implementation and design. See [Score-based consolidation](score-based-consolidation.md) for current configuration and runtime behavior, and [Separating compaction from capacity reclamation](compaction-and-reclamation-design.md) for lifecycle details.

This plan records an earlier design in which reclamation selected naturally empty active nodes and waited for the consolidation TTL. Current behavior has a separate `ScoreBasedStandby` method that moves eligible empty active nodes into standby with its own action, logs, and metrics. `ScoreBasedReclamation` selects only nodes carrying both the standby marker and taint, checks live occupancy and activation immediately before deletion, and uses a conditional NodeClaim delete without a consolidation TTL. Score-based compaction still handles only non-empty active nodes.

The remaining sections are historical implementation notes; their old active-empty reclamation and TTL-validation instructions no longer apply.

## Intended behavior

These changes apply only to the score-based consolidation path.

- A standalone `ScoreBasedReclamation` method runs immediately after upstream `Emptiness` for annotated pools. It reports reason `Empty` and removes eligible empty nodes without being limited by NodePool disruption budgets, including a zero budget. Compaction continues to honor budgets and pacing.
- When interval-based reclamation is enabled, its default interval is one minute. The existing per-NodePool interval annotation remains configurable. `ExperimentalReclamationRemoveAllEmptyImmediately` is currently enabled and bypasses interval and half-batch pacing.
- `ScoreBasedConsolidation` handles only non-empty active nodes. Empty active and standby nodes are handled by reclamation for removal.

Reclamation runs before drift and multi-node consolidation calculations. If it starts a command, the normal method boundary ends that controller pass and schedules a retry from the top; compaction is reconsidered on a later pass. If reclamation finds no command, the controller continues through the remaining methods in the same pass. Score-based compaction remains after multi-node consolidation.

Keep the existing reclamation selection rule: target half of the pool's empty nodes, rounded up, choosing eligible nodes with the highest cost per vCPU first. Eligibility may reduce the actual batch; disruption budgets must not.

## 1. Keep reclamation independent of disruption budgets

Primary files: `pkg/controllers/disruption/score_based_reclamation.go`, `pkg/controllers/disruption/score_based_consolidation.go`, and the validation integration in `pkg/controllers/disruption/validation.go`.

- Reclamation planning and selection do not use the disruption-budget map. Select up to `ceil(emptyNodeCount / 2)` available candidates without clipping the selection to the pool's budget. Keep cost ordering and existing pool-wide empty-node counting when interval-based batches are enabled.
- Reclamation has a dedicated validation path. The shared `ConsolidationValidator` rebuilds and enforces budgets, so reclamation bypasses its budget admission while retaining the required validation checks.
- Preserve candidate refresh, the existing validation delay, nominations, activation exclusion, pool eligibility, and fresh emptiness checks. Preserve the final `BeforeDelete` checks so a node that gained a non-daemon pod or became needed for activation is not reclaimed.
- Keep the shared compaction validator and other consolidation methods budget-aware. Do not exempt every delete command: a normal compaction delete can disrupt pods.
- The controller skips budget-map construction for reclamation. Later budget-aware methods build their own maps if the pass reaches them, avoiding a misleading budget-blocked event while reclamation proceeds.
- Reclamation reports reason `Empty`; score-based compaction reports `Underutilized`. Reason labeling is independent of the dedicated budget-exempt validation path.

This exemption concerns NodePool disruption-budget admission for reclamation. It does not remove other candidate eligibility checks or change cluster-wide health/deleting-node accounting. Empty-node reclamation remains outside non-empty disruption pacing.

## 2. Interval-based reclamation behavior

Primary file: `pkg/controllers/disruption/score_based_reclamation.go`.

- `defaultScoreBasedReclamationInterval` is `time.Minute`.
- Preserve `karpenter.coralogix.net/reclamation-interval` overrides. An explicitly configured `2m` continues to mean two minutes.
- Missing, invalid, zero, or negative interval values use the new one-minute default.
- Preserve the strict elapsed-time comparison: reclamation is due when elapsed time is greater than the interval.
- Preserve immediate eligibility when no last-removal timestamp exists and the current invalid-timestamp behavior.
- Continue persisting `karpenter.coralogix.net/last-reclamation` after successful individual deletions, including partial batches. Do not reset the timestamp for empty passes or failed removals.
- While `ExperimentalReclamationRemoveAllEmptyImmediately` is true, every pass reclaims all eligible empty nodes and does not apply the interval or half-batch limit. The interval policy takes effect when that experiment is disabled.

## 3. Keep score-based compaction non-empty

Primary file: `pkg/controllers/disruption/score_based_consolidation.go` and its validator setup.

- Score-based compaction excludes empty nodes before scoring or scheduling simulation. It also excludes standby and activating nodes as compaction sources.
- Use the existing strict emptiness definition consistently: no bound non-DaemonSet pods means empty. Daemon-only nodes are empty; terminating and terminal non-daemon pods count while still bound. Do not equate emptiness with zero resource requests.
- Apply the compaction-only predicate during candidate revalidation too. A candidate that becomes empty during the validation delay is dropped from compaction; the reclamation method can consider it on a later pass.
- Leave completion of already-started evacuation unchanged: becoming empty is its intended outcome and still leads to standby.
- Preserve existing scoring, search limits, feasibility checks, budget enforcement, and pacing for eligible non-empty candidates.

## 4. Focused regression coverage

Extend existing score-based consolidation and reclamation tests, using real validation for budget-exemption coverage:

- With experimental immediate reclamation disabled, three eligible empty nodes and a zero `Underutilized` budget result in two selected nodes in cost-per-vCPU order. A budget of one must not cap that batch either.
- With reclamation not due, a zero budget still blocks non-empty compaction; upstream consolidation remains budget-aware.
- Reclamation rejects a node that acquires a non-daemon pod or becomes nominated/activating during validation or before deletion.
- Default timing is not due at exactly one minute and is due just after it; explicit overrides and invalid-value fallback retain their defined behavior.
- Empty active and daemon-only nodes never produce compaction commands when reclamation is not due. A feasible non-empty candidate can still be selected in the same search.
- A zero-request non-daemon pod is not mistaken for an empty node. Existing terminating/terminal-pod emptiness semantics remain covered.
- A candidate that becomes empty during compaction validation is rejected. Controller ordering verifies that reclamation runs before drift and multi-node consolidation, and that starting a reclamation command triggers the normal requeue before score-based compaction is reconsidered.

Prefer extending existing cases over duplicating tests for unchanged ranking, rounding, timestamp persistence, and activation behavior.

## 5. Documentation and verification

- Update `coralogix-fork/score-based-consolidation.md` and `coralogix-fork/compaction-and-reclamation-design.md` to consistently use “reclamation,” document the one-minute default, explain the budget exemption, and state that compaction requires non-empty sources.
- Run focused disruption tests covering reclamation, compaction filtering, and validation. Run relevant existing upstream budget tests if shared validation code changes.
- Run `make verify` before handing off implementation changes. Include any generated changes and rerun verification if it modifies files.

Deliver this as a focused change to the existing implementation; no new policy settings, provider changes, or sandbox-cluster deployment are needed.
