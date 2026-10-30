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
	"sort"
	"time"

	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

var errWholeCommandPacing = errors.New("whole-command pacing")

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
	if !v.pacing.CommandAllowed(validated) {
		return Command{}, NewValidationError(errWholeCommandPacing)
	}
	return validated, nil
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
