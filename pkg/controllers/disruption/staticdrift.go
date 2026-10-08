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
	"math"
	"sort"

	"github.com/samber/lo"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"

	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

// StaticDrift is a subreconciler that deletes drifted static candidates.
type StaticDrift struct {
	cluster          *state.Cluster
	provisioner      *provisioning.Provisioner
	cloudprovider    cloudprovider.CloudProvider
	disruptionPacing *DisruptionPacing
}

func NewStaticDrift(cluster *state.Cluster, provisioner *provisioning.Provisioner, cloudprovider cloudprovider.CloudProvider, paces ...*DisruptionPacing) *StaticDrift {
	var pace *DisruptionPacing
	if len(paces) > 0 {
		pace = paces[0]
	}
	return &StaticDrift{
		cluster:          cluster,
		provisioner:      provisioner,
		cloudprovider:    cloudprovider,
		disruptionPacing: pace,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *StaticDrift) ShouldDisrupt(_ context.Context, c *Candidate) bool {
	return c.OwnedByStaticNodePool() && c.NodeClaim.StatusConditions().Get(v1.ConditionTypeDrifted).IsTrue()
}

func (d *StaticDrift) ComputeCommands(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) ([]Command, error) {
	// Group candidates by nodepool name
	candidatesByNodePool := lo.GroupBy(candidates, func(candidate *Candidate) string {
		return candidate.NodePool.Name
	})

	var cmds []Command
	nodePoolNames := make([]string, 0, len(candidatesByNodePool))
	for npName := range candidatesByNodePool {
		nodePoolNames = append(nodePoolNames, npName)
	}
	sort.Strings(nodePoolNames)
	scopeBatchCounts := map[string]int{}
	for _, npName := range nodePoolNames {
		cmds = append(cmds, d.computeCommandsForNodePool(npName, candidatesByNodePool[npName], disruptionBudgetMapping[npName], scopeBatchCounts)...)
	}
	return cmds, nil
}

func (d *StaticDrift) computeCommandsForNodePool(npName string, npCandidates []*Candidate, disruptionBudget int, scopeBatchCounts map[string]int) []Command {
	if disruptionBudget == 0 {
		return nil
	}
	np := npCandidates[0].NodePool
	limit, ok := np.Spec.Limits[resources.Node]
	nodeLimit := lo.Ternary(ok, limit.Value(), int64(math.MaxInt64))
	// Current nodes (includes in-flight per your cluster state)
	runningNodes, _, nodesPendingDisruptionCount := d.cluster.NodePoolState.GetNodeCount(npName)
	if int64(runningNodes+nodesPendingDisruptionCount) > lo.FromPtr(np.Spec.Replicas) {
		return nil
	}

	scope := d.disruptionPacing.Scope(np)
	pacedCandidates := make([]*Candidate, 0, len(npCandidates))
	selectedNonEmpty := scopeBatchCounts[scope]
	for _, candidate := range npCandidates {
		if len(candidate.reschedulablePods) > 0 && !d.disruptionPacing.CandidateAllowed(np, selectedNonEmpty) {
			continue
		}
		pacedCandidates = append(pacedCandidates, candidate)
		if len(candidate.reschedulablePods) > 0 {
			selectedNonEmpty++
		}
	}
	if len(pacedCandidates) == 0 {
		return nil
	}

	maxDrifts := lo.Min([]int64{
		int64(disruptionBudget),
		int64(len(pacedCandidates)),
	})
	maxAllowedDrifts := d.cluster.NodePoolState.ReserveNodeCount(npName, nodeLimit, maxDrifts)
	if maxAllowedDrifts == 0 {
		return nil
	}

	commands := make([]Command, 0, maxAllowedDrifts)
	for _, candidate := range pacedCandidates[:maxAllowedDrifts] {
		nct := scheduling.NewNodeClaimTemplate(np)
		result := scheduling.Results{
			NewNodeClaims: []*scheduling.NodeClaim{{NodeClaimTemplate: *nct}},
		}
		commands = append(commands, Command{
			Candidates:          []*Candidate{candidate},
			Replacements:        replacementsFromNodeClaims(result.NewNodeClaims...),
			Results:             result,
			PoolDisruptionCosts: computePoolDisruptionCosts([]*Candidate{candidate}),
		})
		if len(candidate.reschedulablePods) > 0 {
			scopeBatchCounts[scope]++
		}
	}
	return commands
}

func (d *StaticDrift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *StaticDrift) Class() string {
	return EventualDisruptionClass
}

func (d *StaticDrift) ConsolidationType() string {
	return ""
}
