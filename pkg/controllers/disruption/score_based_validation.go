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
	"slices"
	"sort"
	"time"

	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var errWholeCommandPacing = errors.New("whole-command pacing")

// resultsValidator compares a command's original scheduling results with the re-simulated ones.
type resultsValidator func(expected, actual pscheduling.Results) error

// scoreMoveValidator rechecks pacing for the whole move after the consolidation validator has passed it.
type scoreMoveValidator struct {
	Validator
	pacing *DisruptionPacing
}

func scoreBasedValidationFailureReason(err error) string {
	var ve *ValidationError
	if errors.As(err, &ve) && errors.Is(ve.error, errWholeCommandPacing) {
		return "pacing"
	}
	return getValidationFailureReason(err)
}

func (v scoreMoveValidator) Validate(ctx context.Context, cmd Command, delay time.Duration) (Command, error) {
	validated, err := v.Validator.Validate(ctx, cmd, delay)
	if err != nil {
		return Command{}, err
	}
	if !v.pacing.CommandAllowed(cmd) {
		return Command{}, NewValidationError(errWholeCommandPacing)
	}
	return validated, nil
}

// validateStandbyDestinations rejects a move whose re-simulation reactivates different standby nodes than the search did.
func validateStandbyDestinations(expected, actual pscheduling.Results) error {
	if !slices.Equal(standbyDestinations(expected), standbyDestinations(actual)) {
		return NewSchedulingValidationError(fmt.Errorf("standby destinations changed"))
	}
	return nil
}

// standbyDestinations returns the sorted names of the standby nodes that results place pods on.
func standbyDestinations(results pscheduling.Results) []string {
	names := []string{}
	for _, n := range results.ExistingNodes {
		if len(n.Pods) > 0 && standby.HasStandbyTaint(n.Node) {
			names = append(names, n.Name())
		}
	}
	sort.Strings(names)
	return names
}
