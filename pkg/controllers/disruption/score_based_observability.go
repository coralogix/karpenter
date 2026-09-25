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
	"sync"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/log"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

var (
	ScoreBasedMoveSetEvaluationErrorsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "score_based_move_set_evaluation_errors_total",
			Help:      "Number of score-based consolidation move set evaluations that failed.",
		},
		[]string{},
	)
	ScoreBasedReclamationNodeRemovalsTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: voluntaryDisruptionSubsystem,
			Name:      "score_based_reclamation_node_removals_total",
			Help:      "Number of NodeClaim deletion requests successfully processed by score-based reclamation.",
		},
		[]string{},
	)
)

type moveSetSearchStats struct {
	mu         sync.Mutex
	count      int
	sum        time.Duration
	max        time.Duration
	errors     int
	firstError string
}

func (s *moveSetSearchStats) record(d time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.count++
	s.sum += d
	if d > s.max {
		s.max = d
	}
	if err != nil {
		s.errors++
		if s.firstError == "" {
			s.firstError = err.Error()
		}
		ScoreBasedMoveSetEvaluationErrorsTotal.Inc(nil)
	}
}

func (s *moveSetSearchStats) avg() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count == 0 {
		return 0
	}
	return s.sum / time.Duration(s.count)
}

func (s *moveSetSearchStats) maxDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.max
}

func (s *moveSetSearchStats) errorCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.errors
}

func (s *moveSetSearchStats) firstErrorMessage() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.firstError
}

func logMoveSetSearchComplete(
	ctx context.Context,
	moveSets int,
	evaluated int,
	valid int,
	timedOut bool,
	searchDuration time.Duration,
	stats *moveSetSearchStats,
) {
	log.FromContext(ctx).V(1).Info("score-based consolidation move set search complete",
		"moveSets", moveSets,
		"evaluated", evaluated,
		"valid", valid,
		"timedOut", timedOut,
		"searchDuration", searchDuration,
		"avgMoveSetEvalDuration", stats.avg(),
		"maxMoveSetEvalDuration", stats.maxDuration(),
		"computeErrors", stats.errorCount(),
		"firstComputeError", stats.firstErrorMessage(),
	)
}
