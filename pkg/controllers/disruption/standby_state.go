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

package disruption

import (
	"context"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var (
	NodePoolStandbyNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "standby_nodes",
			Help:      "The number of standby nodes in a score-based NodePool. Labeled by NodePool.",
		},
		[]string{metrics.NodePoolLabel},
	)
	standbyNodesMetricStore = metrics.NewStore()
)

// recordStandbyNodes sets the standby node count of every NodePool that keeps standby nodes, and removes the series of
// NodePools that no longer do or were deleted.
func recordStandbyNodes(cluster *state.Cluster, nodePoolMap map[string]*v1.NodePool) {
	standbyNodesByPool := map[string]int{}
	for name, np := range nodePoolMap {
		if scoreBasedStandbyPool(np) {
			standbyNodesByPool[name] = 0
		}
	}
	// Not Active(): the sources of a running evacuation stay marked for deletion after they reach standby.
	for _, node := range cluster.DeepCopyNodes() {
		pool := node.Labels()[v1.NodePoolLabelKey]
		if _, ok := standbyNodesByPool[pool]; ok && !node.Deleted() && standby.HasStandbyTaint(node.Node) {
			standbyNodesByPool[pool]++
		}
	}
	standbyNodesMetricStore.ReplaceAll(lo.MapValues(standbyNodesByPool, func(count int, pool string) []*metrics.StoreMetric {
		return []*metrics.StoreMetric{{GaugeMetric: NodePoolStandbyNodes, Value: float64(count), Labels: map[string]string{metrics.NodePoolLabel: pool}}}
	}))
}

// repairStandbyState reconciles standby marks on active nodes: orphan annotations, disqualified pools, non-empty nodes, and missing timestamps.
func repairStandbyState(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, recorder events.Recorder, clk clock.Clock, nodePoolMap map[string]*v1.NodePool) error {
	var errs []error
	for _, node := range cluster.DeepCopyNodes().Active() {
		if err := repairStandbyNode(ctx, kubeClient, recorder, clk, nodePoolMap, node); err != nil {
			errs = append(errs, err)
		}
	}
	return multierr.Combine(errs...)
}

// orphan annotation without the standby taint is cleared
// tainted node whose pool no longer qualifies is cleared
// tainted node with a reschedulable pod is cleared
// tainted node with a missing or invalid timestamp gets the timestamp set to now
func repairStandbyNode(ctx context.Context, kubeClient client.Client, recorder events.Recorder, clk clock.Clock, nodePoolMap map[string]*v1.NodePool, node *state.StateNode) error {
	if node.Node == nil || !node.Managed() || !node.Initialized() {
		return nil
	}
	clearStandby := func() error {
		_, err := standby.Clear(ctx, kubeClient, node.Node)
		return err
	}
	if !standby.HasStandbyTaint(node.Node) {
		if standby.HasAnnotation(node.Node) {
			return clearStandby()
		}
		return nil
	}
	if !scoreBasedStandbyPool(nodePoolMap[node.Labels()[v1.NodePoolLabelKey]]) {
		return clearStandby()
	}
	reschedulable, err := node.CurrentlyReschedulablePods(ctx, kubeClient, clk, recorder)
	if err != nil {
		return err
	}
	if computeRescheduleDisruptionCost(ctx, reschedulable) > PerNodeBaseDisruptionCost {
		return clearStandby()
	}
	if _, hasTS := standby.Timestamp(node.Node); !hasTS {
		return standby.Mark(ctx, kubeClient, node.Node, clk.Now())
	}
	return nil
}

// candidateEmptyForStandbyWork also requires no buffer pods: they are virtual, so IsEmpty cannot see them on the node.
func candidateEmptyForStandbyWork(cluster *state.Cluster, c *Candidate) bool {
	return scoreBasedStandbyPool(c.NodePool) && c.IsEmpty() && !cluster.HasBufferPods(c.ProviderID())
}

func scoreBasedStandbyPool(np *v1.NodePool) bool {
	if np == nil || !NodePoolUsesScoreBasedConsolidation(np) {
		return false
	}
	if np.Spec.Replicas != nil {
		return false
	}
	return np.Spec.Disruption.ConsolidateAfter.Duration != nil
}
