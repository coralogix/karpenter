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
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
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
	ReclamationStandbyNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "standby_nodes",
			Help:      "Number of non-deleting nodes with a standby-marked NodeClaim, labeled by NodePool.",
		},
		[]string{metrics.NodePoolLabel},
	)
	reclamationStandbyNodeMetricStore = metrics.NewStore()
)

func updateReclamationStandbyNodeMetrics(counts map[string]int) {
	store := make(map[string][]*metrics.StoreMetric, len(counts))
	for pool, count := range counts {
		store[pool] = []*metrics.StoreMetric{{
			GaugeMetric: ReclamationStandbyNodes,
			Value:       float64(count),
			Labels: prometheus.Labels{
				metrics.NodePoolLabel: pool,
			},
		}}
	}
	reclamationStandbyNodeMetricStore.ReplaceAll(store)
}
