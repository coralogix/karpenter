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

package provisioning

import (
	"context"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	scheduler "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

// Reactivation sources say which decision cleared standby. Nothing is stored on the node (ADR-0001).
const (
	StandbyReactivationSourceProvisioning = "provisioning"
	StandbyReactivationSourceDisruption   = "disruption"
)

const standbyReactivationSourceLabel = "source"

var StandbyReactivatedTotal = opmetrics.NewPrometheusCounter(
	crmetrics.Registry,
	prometheus.CounterOpts{
		Namespace: metrics.Namespace,
		Subsystem: metrics.NodeSubsystem,
		Name:      "standby_reactivated_total",
		Help:      "Number of standby nodes reactivated. Labeled by NodePool and the decision that reactivated them.",
	},
	[]string{metrics.NodePoolLabel, standbyReactivationSourceLabel},
)

// ReactivateStandby clears standby on every existing node the results place pods on, so those pods can bind.
// Only results that are acted on may be passed; simulations never change standby.
func (p *Provisioner) ReactivateStandby(ctx context.Context, results scheduler.Results, source string) error {
	var errs []error
	for _, n := range results.ExistingNodes {
		if len(n.Pods) == 0 || !standby.HasStandbyTaint(n.Node) {
			continue
		}
		cleared, err := standby.Clear(ctx, p.kubeClient, n.Node)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if cleared {
			StandbyReactivatedTotal.Inc(map[string]string{metrics.NodePoolLabel: n.Labels()[v1.NodePoolLabelKey], standbyReactivationSourceLabel: source})
		}
	}
	return multierr.Combine(errs...)
}
