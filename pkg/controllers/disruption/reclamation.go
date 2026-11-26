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

	"github.com/samber/lo"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const ReclamationConsolidationType = "reclamation"

const defaultReclamationDelay = 15 * time.Second

// Reclamation deletes standby score-based nodes after the pool reclamation delay, keeping those within the pool's kept
// capacity when capacity decay is on, using Empty disruption budgets.
type Reclamation struct {
	cluster            *state.Cluster
	recorder           events.Recorder
	clock              clock.Clock
	keptCapacityByPool map[string]keptCapacity
}

func NewReclamation(cluster *state.Cluster, recorder events.Recorder, clk clock.Clock) *Reclamation {
	return &Reclamation{cluster: cluster, recorder: recorder, clock: clk}
}

func (r *Reclamation) ShouldDisrupt(_ context.Context, c *Candidate) bool {
	if c == nil || c.Node == nil || c.NodeClaim == nil {
		return false
	}
	if !candidateEmptyForStandbyWork(r.cluster, c) || !standby.HasStandbyTaint(c.Node) {
		return false
	}
	ts, ok := standby.Timestamp(c.Node)
	return ok && !r.clock.Now().Before(ts.Add(reclamationDelay(c.NodePool, r.recorder)))
}

func (r *Reclamation) ComputeCommands(_ context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	reclaimable := make([]*Candidate, 0, len(candidates))
	for _, candidate := range r.dropKeptStandby(candidates) {
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

// reclamationDelay returns how long a node must stay in standby before reclamation may delete it.
func reclamationDelay(np *v1.NodePool, recorder events.Recorder) time.Duration {
	return lo.FromPtrOr(readDurationAnnotation(np, recorder, v1.ReclamationDelayAnnotationKey, events.InvalidReclamationDelay, defaultReclamationDelay.String()), defaultReclamationDelay)
}

// observeReclaimedStandby records, once a reclamation command has succeeded, the standby duration of every node it
// deleted, up to when the command started. A failed command leaves its nodes on standby, so they are recorded when they
// later leave it.
func observeReclaimedStandby(cmd *Command) {
	if _, ok := cmd.Method.(*Reclamation); !ok || !cmd.Succeeded {
		return
	}
	for _, c := range cmd.Candidates {
		provisioning.ObserveStandbyDuration(c.Node, provisioning.StandbyOutcomeReclaimed, cmd.CreationTimestamp)
	}
}
