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

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

type clusterSnapshot struct {
	candidates                 []*Candidate
	disruptionBudgetByNodePool map[string]int
	// budgetUsedUpByDisruptionsInProgress holds, for each NodePool whose budget allows disruptions but has none left,
	// the number of its nodes already disrupting.
	budgetUsedUpByDisruptionsInProgress map[string]int
	scoreInputs                         scoreInputs
}

// A nil onlyNodes builds candidates for every node.
func (s *ScoreBasedConsolidation) prepare(ctx context.Context, onlyNodes sets.Set[string]) (clusterSnapshot, error) {
	nodes := s.cluster.DeepCopyNodes()
	nodePools, instanceTypes, err := BuildNodePoolMap(ctx, s.kubeClient, s.cloudProvider)
	if err != nil {
		return clusterSnapshot{}, err
	}
	pdbs, err := pdb.NewLimits(ctx, s.kubeClient)
	if err != nil {
		return clusterSnapshot{}, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
	}
	budgets, err := BuildDisruptionBudgetMapping(ctx, s.cluster, s.clock, s.kubeClient, s.cloudProvider, s.recorder, s.Reason())
	if err != nil {
		return clusterSnapshot{}, fmt.Errorf("building disruption budgets, %w", err)
	}
	snapshot := clusterSnapshot{
		disruptionBudgetByNodePool:          budgets,
		budgetUsedUpByDisruptionsInProgress: budgetUsedUpByDisruptionsInProgress(nodes, nodePools, budgets, s.clock, s.Reason()),
	}
	for _, node := range nodes {
		if onlyNodes != nil && !onlyNodes.Has(node.Name()) {
			continue
		}
		if c, err := NewCandidate(ctx, s.kubeClient, s.recorder, s.clock, node, pdbs, nodePools, instanceTypes, s.queue, s.Class()); err == nil && s.ShouldDisrupt(ctx, c) {
			snapshot.candidates = append(snapshot.candidates, c)
		}
	}
	snapshot.scoreInputs = newScoreInputs(nodes, instanceTypes, snapshot.candidates, s.recorder)
	return snapshot, nil
}

// A budget that allows no disruptions at all is left out, because BuildDisruptionBudgetMapping already reports it.
func budgetUsedUpByDisruptionsInProgress(nodes state.StateNodes, nodePools map[string]*v1.NodePool, remainingBudgets map[string]int, clk clock.Clock, reason v1.DisruptionReason) map[string]int {
	numNodes, disrupting := countBudgetNodes(nodes)
	usedUp := map[string]int{}
	for name, nodePool := range nodePools {
		if remainingBudgets[name] == 0 && disrupting[name] > 0 && nodePool.MustGetAllowedDisruptions(clk, numNodes[name], reason) > 0 {
			usedUp[name] = disrupting[name]
		}
	}
	return usedUp
}
