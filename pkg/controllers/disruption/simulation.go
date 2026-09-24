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

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/cxtracing"
	"sigs.k8s.io/karpenter/pkg/metrics"
	operatorlogging "sigs.k8s.io/karpenter/pkg/operator/logging"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

// SchedulingSimulator owns the immutable inputs for every scheduling attempt
// in one disruption search. Candidate policy stays in the disruption methods;
// this value only captures the pod and node data needed to evaluate scenarios.
type SchedulingSimulator struct {
	inputs              *provisioning.PreparedSimulationInputs
	pendingPodIDs       []provisioning.SimulationPodID
	deletingNodePodIDs  []provisioning.SimulationPodID
	candidatePodIDs     map[string][]provisioning.SimulationPodID
	candidateUniverse   sets.Set[string]
	deletingCandidates  sets.Set[string]
	removedNodeNames    []string
	deletingNodePodKeys map[client.ObjectKey]struct{}
}

type schedulingSimulationSnapshot struct {
	nodes              state.StateNodes
	pendingPods        []*corev1.Pod
	deletingPods       []*corev1.Pod
	candidatePods      map[string][]*corev1.Pod
	candidateUniverse  sets.Set[string]
	deletingCandidates sets.Set[string]
	removedNodeNames   []string
}

func captureSimulationNodesAndPods(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner) (state.StateNodes, []*corev1.Pod, []*corev1.Pod, pdb.Limits, error) {
	nodes := cluster.DeepCopyNodes()
	phaseCtx, stop := measureSimulateSchedulingPhase(ctx, phaseGetPendingPods)
	pendingPods, err := provisioner.GetPendingPods(phaseCtx)
	stop()
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("determining pending pods, %w", err)
	}
	pdbs, err := pdb.NewLimits(ctx, kubeClient)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
	}
	deletingPods, err := nodes.Deleting().Pods(ctx, kubeClient)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to get pods from deleting nodes, %w", err)
	}
	deletingPods = lo.Filter(deletingPods, func(pod *corev1.Pod, _ int) bool {
		return pdbs.IsCurrentlyReschedulable(pod)
	})
	return nodes, pendingPods, deletingPods, pdbs, nil
}

func captureSimulationCandidatePods(candidates []*Candidate, deletingNodes state.StateNodes, pdbs pdb.Limits) (map[string][]*corev1.Pod, sets.Set[string], sets.Set[string], error) {
	candidatePods := map[string][]*corev1.Pod{}
	candidateUniverse := sets.New[string]()
	deletingCandidates := sets.New[string]()
	for _, candidate := range candidates {
		name := candidate.Name()
		if candidateUniverse.Has(name) {
			return nil, nil, nil, fmt.Errorf("duplicate candidate %q in scheduling simulation", name)
		}
		candidateUniverse.Insert(name)
		if _, ok := lo.Find(deletingNodes, func(node *state.StateNode) bool { return node.Name() == name }); ok {
			deletingCandidates.Insert(name)
		}
		candidatePods[name] = lo.Filter(candidate.reschedulablePods, func(pod *corev1.Pod, _ int) bool {
			return pdbs.IsCurrentlyReschedulable(pod)
		})
	}
	return candidatePods, candidateUniverse, deletingCandidates, nil
}

func buildSchedulingSimulatorSnapshot(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, candidates ...*Candidate) (*schedulingSimulationSnapshot, error) {
	nodes, pendingPods, deletingPods, pdbs, err := captureSimulationNodesAndPods(ctx, kubeClient, cluster, provisioner)
	if err != nil {
		return nil, err
	}
	candidatePods, candidateUniverse, deletingCandidates, err := captureSimulationCandidatePods(candidates, nodes.Deleting(), pdbs)
	if err != nil {
		return nil, err
	}
	return &schedulingSimulationSnapshot{
		nodes:              nodes,
		pendingPods:        pendingPods,
		deletingPods:       deletingPods,
		candidatePods:      candidatePods,
		candidateUniverse:  candidateUniverse,
		deletingCandidates: deletingCandidates,
		removedNodeNames:   lo.Map(nodes.Deleting(), func(node *state.StateNode, _ int) string { return node.Name() }),
	}, nil
}

func simulationCatalogIDs(inputs *provisioning.PreparedSimulationInputs, snapshot *schedulingSimulationSnapshot) ([]provisioning.SimulationPodID, []provisioning.SimulationPodID, map[string][]provisioning.SimulationPodID, error) {
	pendingPodIDs, err := inputs.PodIDsFor(snapshot.pendingPods)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cataloging pending pods, %w", err)
	}
	deletingNodePodIDs, err := inputs.PodIDsFor(snapshot.deletingPods)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("cataloging deleting-node pods, %w", err)
	}
	candidatePodIDs := make(map[string][]provisioning.SimulationPodID, len(snapshot.candidatePods))
	for name, pods := range snapshot.candidatePods {
		ids, err := inputs.PodIDsFor(pods)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("cataloging candidate %q pods, %w", name, err)
		}
		candidatePodIDs[name] = ids
	}
	return pendingPodIDs, deletingNodePodIDs, candidatePodIDs, nil
}

// NewSchedulingSimulator captures pending pods, deleting-node pods, PDB
// eligibility, and candidate pods once. The provisioning package owns the
// resulting catalog, baseline, node snapshot, and volume data.
func NewSchedulingSimulator(ctx context.Context, kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, candidates ...*Candidate) (*SchedulingSimulator, error) {
	snapshot, err := buildSchedulingSimulatorSnapshot(ctx, kubeClient, cluster, provisioner, candidates...)
	if err != nil {
		return nil, err
	}

	allPods := append([]*corev1.Pod{}, snapshot.pendingPods...)
	allPods = append(allPods, snapshot.deletingPods...)
	for _, pods := range snapshot.candidatePods {
		allPods = append(allPods, pods...)
	}
	inputs, err := provisioner.NewPreparedSimulationInputs(
		log.IntoContext(ctx, operatorlogging.NopLogger),
		allPods,
		snapshot.nodes,
		simulationSchedulerOptions(ctx)...,
	)
	if err != nil {
		return nil, fmt.Errorf("preparing scheduling simulation, %w", err)
	}

	pendingPodIDs, deletingNodePodIDs, candidatePodIDs, err := simulationCatalogIDs(inputs, snapshot)
	if err != nil {
		return nil, err
	}
	deletingNodePodKeys := lo.SliceToMap(snapshot.deletingPods, func(pod *corev1.Pod) (client.ObjectKey, struct{}) {
		return client.ObjectKeyFromObject(pod), struct{}{}
	})
	return &SchedulingSimulator{
		inputs:              inputs,
		pendingPodIDs:       pendingPodIDs,
		deletingNodePodIDs:  deletingNodePodIDs,
		candidatePodIDs:     candidatePodIDs,
		candidateUniverse:   snapshot.candidateUniverse,
		deletingCandidates:  snapshot.deletingCandidates,
		removedNodeNames:    snapshot.removedNodeNames,
		deletingNodePodKeys: deletingNodePodKeys,
	}, nil
}

func simulationSchedulerOptions(ctx context.Context) []scheduling.Options {
	var opts []scheduling.Options
	if options.FromContext(ctx).PreferencePolicy == options.PreferencePolicyIgnore {
		opts = append(opts, scheduling.IgnorePreferences)
	}
	return append(opts, scheduling.MinValuesPolicy(options.FromContext(ctx).MinValuesPolicy))
}

// SimulateScheduling runs one scenario from a captured scheduling search.
//
//nolint:gocyclo
func (simulator *SchedulingSimulator) Simulate(ctx context.Context, candidates ...*Candidate) (scheduling.Results, error) {
	ctx, stopRoot := cxtracing.Measure(ctx, metrics.Measure(SimulateSchedulingDurationSeconds, map[string]string{}), "karpenter.disruption.simulate_scheduling")
	defer stopRoot()

	candidateNames := sets.New[string]()
	for _, candidate := range candidates {
		name := candidate.Name()
		if !simulator.candidateUniverse.Has(name) {
			return scheduling.Results{}, fmt.Errorf("candidate %q is outside scheduling simulation", name)
		}
		if simulator.deletingCandidates.Has(name) {
			return scheduling.Results{}, errCandidateDeleting
		}
		candidateNames.Insert(name)
	}

	ids := make([]provisioning.SimulationPodID, 0, len(simulator.pendingPodIDs)+len(simulator.deletingNodePodIDs))
	seen := sets.New[provisioning.SimulationPodID]()
	appendIDs := func(values ...provisioning.SimulationPodID) {
		for _, id := range values {
			if seen.Has(id) {
				continue
			}
			seen.Insert(id)
			ids = append(ids, id)
		}
	}
	appendIDs(simulator.pendingPodIDs...)
	for _, candidate := range candidates {
		appendIDs(simulator.candidatePodIDs[candidate.Name()]...)
	}
	appendIDs(simulator.deletingNodePodIDs...)

	removedNodeNames := append([]string{}, simulator.removedNodeNames...)
	removedNodeNames = append(removedNodeNames, candidateNames.UnsortedList()...)
	attempt, err := simulator.inputs.NewRun(ctx, provisioning.Scenario{
		PodIDs:           ids,
		RemovedNodeNames: removedNodeNames,
	})
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("creating scheduler, %w", err)
	}

	phaseCtx, stop := measureSimulateSchedulingPhase(ctx, phaseSolve)
	results, err := attempt.Solve(log.IntoContext(phaseCtx, operatorlogging.NopLogger))
	stop()
	if err != nil {
		return scheduling.Results{}, fmt.Errorf("scheduling pods, %w", err)
	}
	results = results.TruncateInstanceTypes(ctx, scheduling.MaxInstanceTypes)
	for _, node := range results.ExistingNodes {
		if !node.Initialized() {
			for _, pod := range node.Pods {
				if _, ok := simulator.deletingNodePodKeys[client.ObjectKeyFromObject(pod)]; !ok {
					results.PodErrors[pod] = NewUninitializedNodeError(node)
				}
			}
		}
	}
	return results, nil
}
