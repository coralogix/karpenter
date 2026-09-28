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
	scoreBasedEmptyNodeStateLabel   = "state"
	scoreBasedEmptyNodeStateStandby = "standby"
	scoreBasedEmptyNodeStateOther   = "other"
)

var (
	ScoreBasedReclamationEmptyNodes = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "empty_nodes",
			Help:      "Number of empty nodes in score-based reclamation NodePools. Labeled by NodePool and whether the node is standby or other empty capacity.",
		},
		[]string{metrics.NodePoolLabel, scoreBasedEmptyNodeStateLabel},
	)
	scoreBasedEmptyNodeMetricState = struct {
		sync.Mutex
		pools map[string]struct{}
	}{pools: map[string]struct{}{}}
)

type scoreBasedEmptyNodeCounts struct {
	standby int
	other   int
}

func (c scoreBasedEmptyNodeCounts) add(isStandby bool) scoreBasedEmptyNodeCounts {
	if isStandby {
		c.standby++
	} else {
		c.other++
	}
	return c
}

// updateScoreBasedEmptyNodeMetrics replaces the previously reported inventory after a complete
// scan. Keeping the last successful pool set here lets us remove series when pools leave scope.
func updateScoreBasedEmptyNodeMetrics(counts map[string]scoreBasedEmptyNodeCounts) {
	scoreBasedEmptyNodeMetricState.Lock()
	defer scoreBasedEmptyNodeMetricState.Unlock()

	for pool := range scoreBasedEmptyNodeMetricState.pools {
		if _, ok := counts[pool]; !ok {
			ScoreBasedReclamationEmptyNodes.DeletePartialMatch(map[string]string{metrics.NodePoolLabel: pool})
		}
	}
	for pool, count := range counts {
		ScoreBasedReclamationEmptyNodes.Set(float64(count.standby), map[string]string{
			metrics.NodePoolLabel:         pool,
			scoreBasedEmptyNodeStateLabel: scoreBasedEmptyNodeStateStandby,
		})
		ScoreBasedReclamationEmptyNodes.Set(float64(count.other), map[string]string{
			metrics.NodePoolLabel:         pool,
			scoreBasedEmptyNodeStateLabel: scoreBasedEmptyNodeStateOther,
		})
	}
	scoreBasedEmptyNodeMetricState.pools = make(map[string]struct{}, len(counts))
	for pool := range counts {
		scoreBasedEmptyNodeMetricState.pools[pool] = struct{}{}
	}
}
