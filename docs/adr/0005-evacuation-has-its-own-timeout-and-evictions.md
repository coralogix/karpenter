# Evacuation has its own timeout and requests its own evictions

Status: accepted

Evacuation drains its source inside the disruption command, unlike upstream deletion, which hands draining to termination. It is bounded by the NodePool annotation `karpenter.coralogix.net/evacuation-timeout` (default 5 minutes, counted from command creation), because an operation without a deadline can hold a node cordoned and a budget slot indefinitely. The deadline is checked before evictions are requested, so none happen after it. Evacuation requests evictions itself on each pass instead of using the eviction queue shared with termination: that queue keeps retrying after an abandoned command, so using it requires cancellation state and changes to upstream code.

## Considered Options

- **Keep the queue's retry window as the deadline.** Rejected: it was inherited by accident, scales with queue length, and has a 10-minute minimum.
- **Start the clock when draining starts.** Rejected: it needs extra command state and a bypass of the upstream deadline. Counting from command creation means a replacement's boot time counts against the timeout.
- **Use the shared eviction queue and cancel on timeout.** Rejected: more code in upstream files, and evictions can still outlive the command.
