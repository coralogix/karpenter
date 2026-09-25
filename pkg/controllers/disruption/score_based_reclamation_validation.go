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
	"errors"
	"fmt"
	"time"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// scoreBasedReclamationValidator refreshes and rechecks empty-node candidates without applying
// NodePool disruption budgets. Reclamation candidates carry no workload to reschedule, so the
// consolidation scheduling simulation is not needed here.
type scoreBasedReclamationValidator struct {
	validation
	filter         CandidateFilter
	validationType string
}

func (s *ScoreBasedConsolidation) reclamationValidator() *scoreBasedReclamationValidator {
	return &scoreBasedReclamationValidator{
		validation: validation{
			clock:         s.clock,
			cluster:       s.cluster,
			kubeClient:    s.kubeClient,
			provisioner:   s.provisioner,
			cloudProvider: s.cloudProvider,
			recorder:      s.recorder,
			queue:         s.queue,
			reason:        v1.DisruptionReasonUnderutilized,
		},
		filter:         s.ShouldReclaim,
		validationType: s.ConsolidationType(),
	}
}

func (s *scoreBasedReclamationValidator) Validate(ctx context.Context, cmd Command, validationPeriod time.Duration) (Command, error) {
	if validationPeriod > 0 {
		select {
		case <-ctx.Done():
			return Command{}, errors.New("context canceled")
		case <-s.clock.After(validationPeriod):
		}
	}
	validatedCandidates, err := s.validateCandidates(ctx, cmd.Candidates...)
	if err != nil {
		return Command{}, err
	}
	// Refresh a second time to catch candidate changes while the first validation was running.
	validatedCandidates, err = s.validateCandidates(ctx, validatedCandidates...)
	if err != nil {
		return Command{}, err
	}
	cmd.Candidates = validatedCandidates
	return cmd, nil
}

func (s *scoreBasedReclamationValidator) validateCandidates(ctx context.Context, candidates ...*Candidate) ([]*Candidate, error) {
	validatedCandidates, err := GetCandidates(ctx, s.cluster, s.kubeClient, s.recorder, s.clock, s.cloudProvider, s.filter, GracefulDisruptionClass, s.queue)
	if err != nil {
		return nil, fmt.Errorf("constructing reclamation validation candidates, %w", err)
	}
	validatedCandidates = mapCandidates(candidates, validatedCandidates)
	if len(validatedCandidates) != len(candidates) {
		FailedValidationsTotal.Add(float64(len(candidates)-len(validatedCandidates)), map[string]string{ConsolidationTypeLabel: s.validationType})
		return nil, NewChurnValidationError(fmt.Errorf("%d reclamation candidates are no longer valid", len(candidates)-len(validatedCandidates)))
	}
	for _, candidate := range validatedCandidates {
		if s.cluster.IsNodeNominated(candidate.ProviderID()) {
			FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: s.validationType})
			return nil, NewBudgetValidationError(fmt.Errorf("a reclamation candidate was nominated during validation"))
		}
	}
	return validatedCandidates, nil
}

// ShouldReclaim is deliberately separate from the combined score-based candidate predicate.
// It admits only currently due, empty, eligible candidates and never consults disruption budgets.
func (s *ScoreBasedConsolidation) ShouldReclaim(ctx context.Context, candidate *Candidate) bool {
	return s.isReclamationCandidateAvailable(ctx, candidate)
}
