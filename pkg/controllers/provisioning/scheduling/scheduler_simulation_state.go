/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduling

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// PreparedSchedulerState binds one scheduler baseline to the captured node
// snapshot used by a group of simulations. Its node facts are immutable after
// construction. NewScheduler materializes only attempt-owned allocation state
// and applies the scenario's removed nodes to capacity.
type PreparedSchedulerState struct {
	baseline           *SchedulerBaseline
	existingNodes      []*preparedExistingNode
	remainingResources map[string]corev1.ResourceList
}

// NewPreparedSchedulerState prepares the node facts that are independent of
// a scheduling attempt. stateNodes must be the owned snapshot for the same
// simulation input that owns baseline.
func NewPreparedSchedulerState(ctx context.Context, baseline *SchedulerBaseline, stateNodes []*state.StateNode) (*PreparedSchedulerState, error) {
	if baseline == nil {
		return nil, errors.New("scheduler baseline is required")
	}
	if baseline.ignoreDRARequests != karpopts.FromContext(ctx).IgnoreDRARequests {
		return nil, fmt.Errorf("scheduler baseline was prepared with IgnoreDRARequests=%t but simulation context has IgnoreDRARequests=%t", baseline.ignoreDRARequests, karpopts.FromContext(ctx).IgnoreDRARequests)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	daemonSetPods := baseline.daemonSetPods
	preparedNodes := make([]*preparedExistingNode, 0, len(stateNodes))
	remainingResources := nodePoolRemainingResources(baseline.inputs)
	for _, node := range stateNodes {
		if node == nil {
			continue
		}
		// Deleting nodes remain in the topology snapshot so their bound pods
		// continue to contribute counts, but they are never solver capacity.
		if node.MarkedForDeletion() {
			continue
		}
		taints := node.Taints()
		daemons := getCompatibleDaemonPods(ctx, node, taints, daemonSetPods)
		daemonResources := resources.RequestsForPods(daemons...)
		prepared := prepareExistingNode(node, taints, daemonResources)
		preparedNodes = append(preparedNodes, prepared)
		if nodePoolName := node.Labels()[v1.NodePoolLabelKey]; nodePoolName != "" {
			if remaining, ok := remainingResources[nodePoolName]; ok && remaining != nil {
				remainingResources[nodePoolName] = resources.Subtract(remaining, node.Capacity())
			}
		}
	}
	sortPreparedExistingNodes(preparedNodes)

	return &PreparedSchedulerState{
		baseline:           baseline,
		existingNodes:      preparedNodes,
		remainingResources: remainingResources,
	}, nil
}

// NewScheduler creates one isolated attempt from the prepared node state.
// removedNodeNames must be a subset of the captured node snapshot; callers
// that expose scenario selection should validate that boundary before calling
// this method.
func (p *PreparedSchedulerState) NewScheduler(
	ctx context.Context,
	cluster *state.Cluster,
	topology *Topology,
	recorder events.Recorder,
	clock clock.Clock,
	volumeSource VolumeSource,
	removedNodeNames sets.Set[string],
) (*Scheduler, error) {
	if p == nil || p.baseline == nil {
		return nil, errors.New("scheduler simulation state is required")
	}
	if volumeSource == nil {
		return nil, errors.New("volume source must be provided")
	}
	if p.baseline.ignoreDRARequests != karpopts.FromContext(ctx).IgnoreDRARequests {
		return nil, fmt.Errorf("scheduler baseline was prepared with IgnoreDRARequests=%t but attempt context has IgnoreDRARequests=%t", p.baseline.ignoreDRARequests, karpopts.FromContext(ctx).IgnoreDRARequests)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s := newSchedulerWithBaseline(p.baseline, cluster, topology, recorder, clock, volumeSource, p.remainingResourcesFor(removedNodeNames))
	s.existingNodes = make([]*ExistingNode, 0, len(p.existingNodes))
	for _, prepared := range p.existingNodes {
		if removedNodeNames.Has(prepared.stateNode.Name()) {
			continue
		}
		s.existingNodes = append(s.existingNodes, prepared.materialize(topology))
	}
	return s, nil
}

func sortPreparedExistingNodes(nodes []*preparedExistingNode) {
	sort.SliceStable(nodes, func(i, j int) bool {
		return existingNodeLess(nodes[i].stateNode, nodes[j].stateNode)
	})
}

func (p *PreparedSchedulerState) remainingResourcesFor(removed sets.Set[string]) map[string]corev1.ResourceList {
	remaining := cloneResourceLists(p.remainingResources)
	for _, node := range p.existingNodes {
		if !removed.Has(node.stateNode.Name()) {
			continue
		}
		nodePoolName := node.stateNode.Labels()[v1.NodePoolLabelKey]
		if limits, ok := remaining[nodePoolName]; ok && limits != nil {
			// Restore only resources present in a finite NodePool limit. A
			// missing resource means that the NodePool is unlimited for it.
			for resourceName, capacity := range node.stateNode.Capacity() {
				if _, limited := limits[resourceName]; !limited {
					continue
				}
				quantity := limits[resourceName]
				quantity.Add(capacity)
				limits[resourceName] = quantity
			}
		}
	}
	return remaining
}

func nodePoolRemainingResources(inputs *NodePoolInputs) map[string]corev1.ResourceList {
	remaining := make(map[string]corev1.ResourceList, len(inputs.nodePools))
	for _, nodePool := range inputs.nodePools {
		remaining[nodePool.Name] = corev1.ResourceList(nodePool.Spec.Limits).DeepCopy()
	}
	return remaining
}

func cloneResourceLists(in map[string]corev1.ResourceList) map[string]corev1.ResourceList {
	result := make(map[string]corev1.ResourceList, len(in))
	for key, value := range in {
		result[key] = value.DeepCopy()
	}
	return result
}

// getCompatibleDaemonPods retains the ordinary scheduler's daemon filtering
// rules while allowing the simulation state to evaluate them once per node.
func getCompatibleDaemonPods(ctx context.Context, node *state.StateNode, taints []corev1.Taint, daemonSetPods []*corev1.Pod) []*corev1.Pod {
	var daemons []*corev1.Pod
	for _, p := range daemonSetPods {
		if shouldSkipDaemonPod(ctx, p) {
			continue
		}
		if isDaemonPodCompatibleWithNode(p, taints, node.Labels()) {
			daemons = append(daemons, p)
		}
	}
	return daemons
}

// shouldSkipDaemonPod checks if a daemon pod should be skipped due to DRA requirements.
func shouldSkipDaemonPod(ctx context.Context, p *corev1.Pod) bool {
	return podHasDRARequirements(p) && karpopts.FromContext(ctx).IgnoreDRARequests
}

// isDaemonPodCompatibleWithNode checks if a daemon pod is compatible with the node.
func isDaemonPodCompatibleWithNode(p *corev1.Pod, taints []corev1.Taint, nodeLabels map[string]string) bool {
	if err := scheduling.Taints(taints).ToleratesPod(p); err != nil {
		return false
	}
	if err := scheduling.NewLabelRequirements(nodeLabels).Compatible(scheduling.NewStrictPodRequirements(p)); err != nil {
		return false
	}
	return true
}

func podHasDRARequirements(p *corev1.Pod) bool {
	return pod.HasDRARequirements(p)
}
