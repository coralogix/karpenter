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
	"time"

	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

const ReclamationConsolidationType = "reclamation"

const defaultReclamationDelay = 15 * time.Second

// Reclamation deletes standby score-based nodes after the pool reclamation delay, using Empty disruption budgets.
type Reclamation struct {
	cluster  *state.Cluster
	recorder events.Recorder
	clock    clock.Clock
}

func NewReclamation(cluster *state.Cluster, recorder events.Recorder, clk clock.Clock) *Reclamation {
	return &Reclamation{cluster: cluster, recorder: recorder, clock: clk}
}

func (r *Reclamation) ShouldDisrupt(_ context.Context, c *Candidate) bool {
	return candidateReadyForReclamation(r.cluster, r.recorder, r.clock, c)
}

func (r *Reclamation) ComputeCommands(_ context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	reclaimable := make([]*Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		reclaimable = append(reclaimable, candidate)
		disruptionBudgetMapping[candidate.NodePool.Name]--
	}
	if len(reclaimable) == 0 {
		return []Command{}, nil
	}
	return []Command{{
		Candidates:          reclaimable,
		PoolDisruptionCosts: computePoolDisruptionCosts(reclaimable),
	}}, nil
}

func (r *Reclamation) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonEmpty
}

func (r *Reclamation) Class() string {
	return GracefulDisruptionClass
}

func (r *Reclamation) ConsolidationType() string {
	return ReclamationConsolidationType
}

func candidateReadyForReclamation(cluster *state.Cluster, recorder events.Recorder, clk clock.Clock, c *Candidate) bool {
	if c == nil || c.Node == nil || c.NodeClaim == nil {
		return false
	}
	if !candidateEmptyForStandbyWork(cluster, c) || !nodeHasStandbyTaint(c.Node) {
		return false
	}
	ts, ok := standbyTimestamp(c.NodeClaim)
	return ok && !clk.Now().Before(ts.Add(reclamationDelay(c.NodePool, recorder)))
}

// reclamationDelay returns how long a node must stay in standby before reclamation may delete it.
func reclamationDelay(np *v1.NodePool, recorder events.Recorder) time.Duration {
	raw, ok := np.Annotations[v1.ReclamationDelayAnnotationKey]
	if !ok || raw == "" {
		return defaultReclamationDelay
	}
	delay, err := time.ParseDuration(raw)
	if err != nil {
		recorder.Publish(disruptionevents.InvalidReclamationDelay(np, raw, defaultReclamationDelay))
		return defaultReclamationDelay
	}
	return max(delay, 0)
}
