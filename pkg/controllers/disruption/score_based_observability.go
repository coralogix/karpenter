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

	"sigs.k8s.io/controller-runtime/pkg/log"
)

type moveSetSearchStats struct {
	mu     sync.Mutex
	count  int
	sum    time.Duration
	max    time.Duration
	errors int
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

func logMoveSetSearchComplete(
	ctx context.Context,
	moveSets int,
	evaluated int,
	valid int,
	timedOut bool,
	searchDuration time.Duration,
	stats *moveSetSearchStats,
) {
	log.FromContext(ctx).Info("score-based consolidation move set search complete",
		"moveSets", moveSets,
		"evaluated", evaluated,
		"valid", valid,
		"timedOut", timedOut,
		"searchDuration", searchDuration,
		"avgMoveSetEvalDuration", stats.avg(),
		"maxMoveSetEvalDuration", stats.maxDuration(),
		"computeErrors", stats.errorCount(),
	)
}
