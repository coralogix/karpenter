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
	"sync"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	reclamationEmptyNodeStateLabel   = "state"
	reclamationEmptyNodeStateStandby = "standby"
	reclamationEmptyNodeStateOther   = "other"
)

var (
	ReclamationNodeRemovalsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "reclamation_removals_total",
			Help:      "Number of empty nodes reclaimed by reclamation, labeled by NodePool.",
		},
		[]string{metrics.NodePoolLabel},
	)
	ReclamationEmptyNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "empty_nodes",
			Help:      "Number of empty nodes in NodePools with standby lifecycle enabled. Labeled by NodePool and whether the node is standby or other empty capacity.",
		},
		[]string{metrics.NodePoolLabel, reclamationEmptyNodeStateLabel},
	)
	reclamationEmptyNodeMetricState = struct {
		sync.Mutex
		pools map[string]struct{}
	}{pools: map[string]struct{}{}}
)

type emptyNodeCounts struct {
	standby int
	other   int
}

func (c emptyNodeCounts) add(isStandby bool) emptyNodeCounts {
	if isStandby {
		c.standby++
	} else {
		c.other++
	}
	return c
}

func updateReclamationEmptyNodeMetrics(counts map[string]emptyNodeCounts) {
	reclamationEmptyNodeMetricState.Lock()
	defer reclamationEmptyNodeMetricState.Unlock()

	for pool := range reclamationEmptyNodeMetricState.pools {
		if _, ok := counts[pool]; !ok {
			ReclamationEmptyNodes.DeletePartialMatch(map[string]string{metrics.NodePoolLabel: pool})
		}
	}
	for pool, count := range counts {
		ReclamationEmptyNodes.Set(float64(count.standby), map[string]string{
			metrics.NodePoolLabel:          pool,
			reclamationEmptyNodeStateLabel: reclamationEmptyNodeStateStandby,
		})
		ReclamationEmptyNodes.Set(float64(count.other), map[string]string{
			metrics.NodePoolLabel:          pool,
			reclamationEmptyNodeStateLabel: reclamationEmptyNodeStateOther,
		})
	}
	reclamationEmptyNodeMetricState.pools = make(map[string]struct{}, len(counts))
	for pool := range counts {
		reclamationEmptyNodeMetricState.pools[pool] = struct{}{}
	}
}
