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

package metrics

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	NodeClaimsEvacuatedTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: NodeClaimSubsystem, Name: "evacuated_total", Help: "Number of NodeClaims evacuated into standby.",
	}, []string{NodePoolLabel, CapacityTypeLabel})
	StandbyReactivatedTotal = opmetrics.NewPrometheusCounter(crmetrics.Registry, prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: NodeSubsystem, Name: "standby_reactivated_total", Help: "Number of standby nodes reactivated by source.",
	}, []string{NodePoolLabel, "source"})
)
