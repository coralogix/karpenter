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
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const StandbyMarkingType = "standby-marking"

// StandbyMarking marks empty active nodes in score-based NodePools as standby capacity.
type StandbyMarking struct {
	consolidation
}

func NewStandbyMarking(c consolidation) *StandbyMarking {
	return &StandbyMarking{consolidation: c}
}

//nolint:gocyclo // This predicate groups the complete empty-active-node eligibility gate.
func (s *StandbyMarking) ShouldDisrupt(ctx context.Context, candidate *Candidate) bool {
	if !standbyLifecycleEnabled(candidate.NodePool) {
		return false
	}
	if !candidate.NodeClaim.DeletionTimestamp.IsZero() {
		return false
	}
	if standby.IsNodeClaimStandby(candidate.NodeClaim) || standby.IsNodeClaimActivating(candidate.NodeClaim) || standby.HasNodeTaint(candidate.Node) {
		return false
	}
	if len(candidate.reschedulablePods) > 0 {
		return false
	}
	if s.cluster != nil && s.cluster.IsNodeNominated(candidate.ProviderID()) {
		return false
	}
	if s.cluster != nil && s.cluster.HasBufferPods(candidate.ProviderID()) {
		return false
	}
	empty, err := nodeEmpty(ctx, s.kubeClient, candidate.Node)
	return err == nil && empty
}

func (s *StandbyMarking) ComputeCommands(_ context.Context, _ map[string]int, candidates ...*Candidate) ([]Command, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	return []Command{{Method: s, Action: StandbyAction, Candidates: candidates}}, nil
}

func (s *StandbyMarking) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonEmpty
}

func (s *StandbyMarking) Class() string {
	return GracefulDisruptionClass
}

func (s *StandbyMarking) ConsolidationType() string {
	return StandbyMarkingType
}
