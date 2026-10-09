# Coralogix Fork

This directory documents Coralogix-specific extensions in this Karpenter fork. These features are implemented without changes to NodePool CRDs or specs, so they can be rolled back to upstream Karpenter without modifying manifests.

Warning: At the moment this documentation is mostly LLM generated (quality may vary).

## Features

- [Disruption pacing](disruption-pacing.md) — optional per-NodePool or per-NodePool-group rate limit for non-empty node disruptions
- [Simulation max capacity](simulation-max-capacity.md) — optional per-NodePool nominal CPU/memory ceiling for new-node packing simulations

## Experimental Features

- [Score-based consolidation](score-based-consolidation.md) — compaction, standby, and reclamation for opted-in NodePools
- [Compaction and reclamation design](compaction-and-reclamation-design.md) — separation of workload evacuation, standby retention, and empty-node deletion
- OpenTelemetry tracing (`pkg/cxtracing`) — spans aligned with fork scheduling/disruption phase metrics; export via standard `OTEL_*` env vars (configured in [eng-karpenter](https://github.com/coralogix/eng-karpenter))

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- Fork annotations remain valid Kubernetes metadata.
- Upstream ignores the annotations and does not enforce fork behavior.
- No manifest or CRD changes are required to roll back.

See each feature document for feature-specific rollback notes.
