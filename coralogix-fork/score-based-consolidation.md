# Score-based consolidation

This fork adds an alternate consolidation disruption method for NodePools that opt in via annotation. The method is currently a placeholder: it identifies eligible candidates and logs them, but does not disrupt nodes.

## Configuration

Add the annotation to a `NodePool`:

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
| `karpenter.coralogix.net/score-based-consolidation` | Opt the NodePool into score-based consolidation (presence of the key is sufficient; value is ignored) |

## Behavior

- NodePools with the annotation are handled by the score-based consolidation method, which runs last in the disruption controller's method list.
- Annotated NodePools are excluded from emptiness, single-node consolidation, and multi-node consolidation.
- Drift and static drift are unchanged; annotated NodePools continue to use the standard drift methods.
- The method reports `Underutilized` as its disruption reason for budget accounting, but the current placeholder implementation always returns no commands, so no nodes are disrupted.
- At verbosity level 1, the controller logs up to 10 matching candidate node names per evaluation cycle, plus a count of any additional candidates.

## Rollback to upstream

Rolling back the controller to upstream Karpenter is safe:

- The annotation remains valid Kubernetes metadata.
- Upstream ignores the annotation and does not run score-based consolidation.
- Annotated NodePools resume standard emptiness and underutilized consolidation behavior.
- No manifest or CRD changes are required to roll back.
