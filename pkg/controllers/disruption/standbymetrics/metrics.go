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

package standbymetrics

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/standby"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	instanceTypeLabel = "instance_type"
	sourceLabel       = "source"
)

var StandbyNodesMarkedTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.NodeSubsystem,
		Name:      "standby_marked_total",
		Help:      "Number of naturally empty nodes marked as standby capacity by standby marking, labeled by NodePool.",
	},
	[]string{metrics.NodePoolLabel},
)

var StandbyNodesActivatedTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.NodeSubsystem,
		Name:      "standby_activated_total",
		Help:      "Number of standby nodes activated by Karpenter. Labeled by the owning NodePool, instance type, and activation source (provisioning, compaction, or recovery).",
	},
	[]string{metrics.NodePoolLabel, instanceTypeLabel, sourceLabel},
)

func RecordActivation(ctx context.Context, stateNode *state.StateNode, source standby.ActivationSource) {
	nodePool := stateNode.NodeClaim.Labels[v1.NodePoolLabelKey]
	if nodePool == "" {
		nodePool = "unknown"
	}
	instanceType := stateNode.NodeClaim.Labels[corev1.LabelInstanceTypeStable]
	if instanceType == "" {
		instanceType = "unknown"
	}
	StandbyNodesActivatedTotal.Inc(map[string]string{
		metrics.NodePoolLabel: nodePool,
		instanceTypeLabel:     instanceType,
		sourceLabel:           string(source),
	})
	log.FromContext(ctx).WithValues(
		"Node", klog.KObj(stateNode.Node),
		"NodeClaim", klog.KObj(stateNode.NodeClaim),
		"NodePool", klog.KRef("", nodePool),
	).Info("activated standby node")
}
