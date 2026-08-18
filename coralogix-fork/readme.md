# Coralogix Fork

This directory documents Coralogix-specific extensions in this Karpenter fork. These features are implemented without changes to NodePool CRDs or specs, so they can be rolled back to upstream Karpenter without modifying manifests.

Warning: At the moment this documentation is mostly LLM generated (quality may vary).

## Features

- [Underutilized consolidation pace](underutilized-consolidation-pace.md) — optional per-NodePool rate limit for underutilized consolidation
- [Score-based consolidation](score-based-consolidation.md) — alternate consolidation path for opted-in NodePools

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- Fork annotations remain valid Kubernetes metadata.
- Upstream ignores the annotations and does not enforce fork behavior.
- No manifest or CRD changes are required to roll back.

See each feature document for feature-specific rollback notes.
