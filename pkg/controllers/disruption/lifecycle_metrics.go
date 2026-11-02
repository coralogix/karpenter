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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var (
	standbyMetricsMu          sync.Mutex
	standbyMetricPools        = map[string]struct{}{}
	EvacuationDurationSeconds = opmetrics.NewPrometheusHistogram(crmetrics.Registry, prometheus.HistogramOpts{
		Namespace: metrics.Namespace, Subsystem: voluntaryDisruptionSubsystem, Name: "evacuation_duration_seconds", Help: "Duration of evacuation commands by outcome.", Buckets: metrics.DurationBuckets(),
	}, []string{"outcome"})
	StandbyNodes = opmetrics.NewPrometheusGauge(crmetrics.Registry, prometheus.GaugeOpts{
		Namespace: metrics.Namespace, Subsystem: metrics.NodePoolSubsystem, Name: "standby_nodes", Help: "Number of standby nodes by NodePool.",
	}, []string{metrics.NodePoolLabel})
)

func (c *Controller) recordStandbyNodes(pools map[string]*v1.NodePool) {
	counts := map[string]float64{}
	for name, pool := range pools {
		if NodePoolUsesScoreBasedConsolidation(pool) {
			counts[name] = 0
		}
	}
	for _, node := range c.cluster.DeepCopyNodes() {
		name := node.Labels()[v1.NodePoolLabelKey]
		if _, ok := counts[name]; ok && standby.HasStandbyTaint(node.Node) {
			counts[name]++
		}
	}
	standbyMetricsMu.Lock()
	defer standbyMetricsMu.Unlock()
	for name := range standbyMetricPools {
		if _, exists := counts[name]; !exists {
			StandbyNodes.Delete(map[string]string{metrics.NodePoolLabel: name})
			delete(standbyMetricPools, name)
		}
	}
	for name, count := range counts {
		StandbyNodes.Set(count, map[string]string{metrics.NodePoolLabel: name})
		standbyMetricPools[name] = struct{}{}
	}
}
