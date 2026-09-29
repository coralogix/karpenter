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

var ScoreBasedStandbyNodesMarkedTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.NodeSubsystem,
		Name:      "standby_marked_total",
		Help:      "Number of naturally empty nodes marked as standby capacity by score-based consolidation, labeled by NodePool.",
	},
	[]string{metrics.NodePoolLabel},
)
