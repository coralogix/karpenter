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

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/cxtracing"
	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	ControllerLabel                  = "controller"
	schedulingIDLabel                = "scheduling_id"
	schedulerSubsystem               = "scheduler"
	newSchedulerPhaseLabel           = "phase"
	PhaseListNodePools               = "list_node_pools"
	PhaseGetInstanceTypes            = "get_instance_types"
	PhaseVolumeTopology              = "volume_topology"
	PhaseNewTopology                 = "new_topology"
	PhaseListDaemonSets              = "list_daemonsets"
	PhasePrecomputeScheduler         = "precompute_scheduler"
	PhaseFilterInstanceTypes         = "filter_instance_types"
	PhaseDaemonOverhead              = "daemon_overhead"
	PhaseDaemonHostPorts             = "daemon_host_ports"
	PhaseReservationManager          = "reservation_manager"
	PhaseCalculateExistingNodeClaims = "calculate_existing_node_claims"
	PhaseBuildDomainGroups           = "build_domain_groups"
	PhaseUpdateInverseAffinities     = "update_inverse_affinities"
	PhaseTopologyUpdate              = "topology_update"
	PhaseCountDomains                = "count_domains"
)

// MeasureNewSchedulerPhase records a phase metric and span. Use the returned context only for
// work inside the phase; pass the original ctx when starting sibling phases.
func MeasureNewSchedulerPhase(ctx context.Context, phase string) (context.Context, func()) {
	metricStop := metrics.Measure(NewSchedulerPhaseDurationSeconds, map[string]string{newSchedulerPhaseLabel: phase})
	return cxtracing.Measure(ctx, metricStop, "karpenter.scheduler.new."+phase, attribute.String("phase", phase))
}

var (
	NewSchedulerPhaseDurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "new_scheduler_phase_duration_seconds",
			Help:      "Duration of phases within NewScheduler in seconds. Labeled by phase.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]string{newSchedulerPhaseLabel},
	)
	DurationSeconds = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "scheduling_duration_seconds",
			Help:      "Duration of scheduling simulations used for deprovisioning and provisioning in seconds.",
			Buckets:   metrics.DurationBuckets(),
		},
		[]string{
			ControllerLabel,
		},
	)
	QueueDepth = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "queue_depth",
			Help:      "The number of pods currently waiting to be scheduled.",
		},
		[]string{
			ControllerLabel,
			schedulingIDLabel,
		},
	)
	UnfinishedWorkSeconds = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "unfinished_work_seconds",
			Help:      "How many seconds of work has been done that is in progress and hasn't been observed by scheduling_duration_seconds.",
		},
		[]string{
			ControllerLabel,
			schedulingIDLabel,
		},
	)
	IgnoredPodCount = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "ignored_pods_count",
			Help:      "Number of pods ignored during scheduling by Karpenter",
		},
		[]string{},
	)
	UnschedulablePodsCount = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: schedulerSubsystem,
			Name:      "unschedulable_pods_count",
			Help:      "The number of unschedulable Pods.",
		},
		[]string{
			ControllerLabel,
		},
	)
)
