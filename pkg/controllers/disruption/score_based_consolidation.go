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
	"fmt"
	"strings"

	"github.com/samber/lo"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

const (
	ScoreBasedConsolidationType           = "score-based"
	maxLoggedScoreBasedConsolidationNodes = 10
)

// NodePoolUsesScoreBasedConsolidation reports whether the NodePool opts into score-based consolidation.
func NodePoolUsesScoreBasedConsolidation(np *v1.NodePool) bool {
	if np == nil || np.Annotations == nil {
		return false
	}
	_, ok := np.Annotations[v1.ScoreBasedConsolidationAnnotationKey]
	return ok
}

// ScoreBasedConsolidation is a disruption method for NodePools that opt in via annotation.
type ScoreBasedConsolidation struct {
	consolidation
}

func NewScoreBasedConsolidation(c consolidation) *ScoreBasedConsolidation {
	return &ScoreBasedConsolidation{consolidation: c}
}

func (s *ScoreBasedConsolidation) ShouldDisrupt(ctx context.Context, cn *Candidate) bool {
	if !NodePoolUsesScoreBasedConsolidation(cn.NodePool) {
		return false
	}
	return s.consolidation.ShouldDisrupt(ctx, cn)
}

func (s *ScoreBasedConsolidation) ComputeCommands(ctx context.Context, _ map[string]int, candidates ...*Candidate) ([]Command, error) {
	if len(candidates) == 0 {
		return []Command{}, nil
	}

	nodeNames := lo.Map(candidates, func(c *Candidate, _ int) string { return c.Name() })
	logged := nodeNames
	remaining := 0
	if len(logged) > maxLoggedScoreBasedConsolidationNodes {
		remaining = len(logged) - maxLoggedScoreBasedConsolidationNodes
		logged = logged[:maxLoggedScoreBasedConsolidationNodes]
	}

	msg := fmt.Sprintf("score-based consolidation candidates: %s", strings.Join(logged, ", "))
	if remaining > 0 {
		msg = fmt.Sprintf("%s and %d more", msg, remaining)
	}
	log.FromContext(ctx).V(1).Info(msg, "candidate-count", len(candidates))

	return []Command{}, nil
}

func (s *ScoreBasedConsolidation) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonUnderutilized
}

func (s *ScoreBasedConsolidation) Class() string {
	return GracefulDisruptionClass
}

func (s *ScoreBasedConsolidation) ConsolidationType() string {
	return ScoreBasedConsolidationType
}
