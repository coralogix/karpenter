# Simulation max capacity

The `karpenter.coralogix.net/simulation-max-capacity` annotation lets a dynamic NodePool include large instance types as launch options without letting their full CPU or memory capacity control how many new nodes Karpenter simulates.

```yaml
apiVersion: karpenter.sh/v1
kind: NodePool
metadata:
  name: workloads
  annotations:
    karpenter.coralogix.net/simulation-max-capacity: '{"cpu":"64","memory":"256Gi"}'
```

The value is a JSON object with one or both of `cpu` and `memory`, expressed as Kubernetes resource quantities. For example, `{"cpu":"64"}` caps nominal CPU capacity at 64 cores. The cap is applied independently to each resource.

The cap is used only when checking whether pods fit on newly simulated NodeClaims, including replacement capacity considered by consolidation. Karpenter first accounts for the actual instance type and offering capacity, then retains the real kubelet/system/eviction overhead, huge-page reservations, and type-specific DaemonSet requests. Instance types above the cap remain eligible launch options when the pending pod fits within the capped capacity. NodeClaim resource requests, instance-type requirements, pricing, and provider data are not reduced or rewritten.

Existing nodes keep their actual allocatable capacity. The annotation does not enforce replica separation after launch: Kubernetes may later place multiple replicas on one larger node. Since the cap intentionally avoids relying on capacity above the ceiling, a burst can request more nodes than are ultimately needed if oversized instances launch and workloads pack onto them afterward. A pod that individually exceeds the cap cannot use that NodePool for new capacity; configure a higher cap or a separate NodePool for such pods.

An absent annotation disables the behavior. On dynamic NodePools, an empty, malformed, empty-object, non-positive, or unsupported-resource value is invalid and blocks new provisioning from that pool with a Warning event. Existing nodes remain available to scheduling. Static NodePools ignore this annotation.

The supported resources are deliberately limited to CPU and memory so the ceiling expresses instance size without unexpectedly changing extended-resource or device scheduling.
