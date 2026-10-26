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

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

const StandbyMarkingConsolidationType = "standby-marking"

// disruptionReasonStandbyMarking satisfies Method.Reason() for metrics and logging only.
// Standby marking does not consume NodePool disruption budgets; this reason is not user-configurable.
const disruptionReasonStandbyMarking v1.DisruptionReason = "StandbyMarking"

// StandbyMarking moves empty score-based nodes into standby without evicting workloads or consuming disruption budget.
type StandbyMarking struct {
	cluster *state.Cluster
}

func NewStandbyMarking(cluster *state.Cluster) *StandbyMarking {
	return &StandbyMarking{cluster: cluster}
}

func usesDisruptionBudget(m Method) bool {
	return m.ConsolidationType() != StandbyMarkingConsolidationType
}

func (s *StandbyMarking) ShouldDisrupt(_ context.Context, c *Candidate) bool {
	if !candidateEmptyForStandbyWork(s.cluster, c) {
		return false
	}
	if nodeHasStandbyTaint(c.Node) {
		return false
	}
	return true
}

func (s *StandbyMarking) ComputeCommands(_ context.Context, _ map[string]int, candidates ...*Candidate) ([]Command, error) {
	if len(candidates) == 0 {
		return []Command{}, nil
	}
	return []Command{{
		Candidates: candidates,
	}}, nil
}

func (s *StandbyMarking) Reason() v1.DisruptionReason {
	return disruptionReasonStandbyMarking
}

func (s *StandbyMarking) Class() string {
	return GracefulDisruptionClass
}

func (s *StandbyMarking) ConsolidationType() string {
	return StandbyMarkingConsolidationType
}
