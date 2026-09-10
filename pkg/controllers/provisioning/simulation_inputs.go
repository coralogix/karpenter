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

package provisioning

import (
	"context"
	"errors"
	"fmt"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	karpscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
)

// SimulationPodID identifies a pod in one PreparedSimulationInputs catalog. The catalog
// pointer makes IDs from another simulation inputs value invalid by construction
// at NewRun, even when the underlying pod names happen to match.
type SimulationPodID struct {
	catalog *simulationPodCatalog
	index   int
}

type simulationPodCatalog struct {
	pods       []*corev1.Pod
	byIdentity map[string]int
}

// Scenario selects the frozen pods to solve and the nodes to remove from the
// captured node snapshot. PodIDs must come from the same PreparedSimulationInputs value.
type Scenario struct {
	PodIDs           []SimulationPodID
	RemovedNodeNames []string
}

// PreparedSimulationInputs owns all data shared by simulation attempts in one search.
// The catalog and nodes are immutable after construction; NewRun creates
// mutable pod copies and delegates topology and scheduling construction to the
// same provisioning path used by ordinary scheduling.
type PreparedSimulationInputs struct {
	factory          *SchedulerFactory
	stateNodes       state.StateNodes
	catalog          *simulationPodCatalog
	volumeData       map[types.UID]scheduling.VolumeData
	preparedTopology *scheduling.PreparedTopology
}

// SimulationRun pairs one scheduler with the exact pod copies it was built
// to solve. The pod slice is private so callers cannot accidentally use a
// scheduler with pods from another scenario or catalog.
type SimulationRun struct {
	scheduler *scheduling.Scheduler
	pods      []*corev1.Pod
}

// NewPreparedSimulationInputs captures the scheduler baseline, node snapshot, pod and
// namespace snapshots, catalog, and volume data needed by repeated scenarios.
func (p *Provisioner) NewPreparedSimulationInputs(ctx context.Context, pods []*corev1.Pod, stateNodes []*state.StateNode, opts ...scheduling.Options) (*PreparedSimulationInputs, error) {
	factory, err := p.NewSchedulerFactory(ctx, opts...)
	if err != nil {
		return nil, err
	}
	catalog, err := newSimulationPodCatalog(pods)
	if err != nil {
		return nil, err
	}

	volumeData, err := p.captureSimulationVolumeData(ctx, catalog)
	if err != nil {
		return nil, err
	}
	apiPods, namespaces, err := p.captureSimulationTopologyObjects(ctx)
	if err != nil {
		return nil, err
	}
	preparedTopology, err := scheduling.NewPreparedTopology(ctx, factory.inputs, stateNodes, apiPods, namespaces, opts...)
	if err != nil {
		return nil, fmt.Errorf("preparing topology snapshot, %w", err)
	}
	return &PreparedSimulationInputs{
		factory:          factory,
		stateNodes:       cloneStateNodes(stateNodes),
		catalog:          catalog,
		volumeData:       volumeData,
		preparedTopology: preparedTopology,
	}, nil
}

func (p *Provisioner) captureSimulationTopologyObjects(ctx context.Context) ([]*corev1.Pod, []*corev1.Namespace, error) {
	var podList corev1.PodList
	if err := p.kubeClient.List(ctx, &podList); err != nil {
		return nil, nil, fmt.Errorf("listing pods for topology snapshot, %w", err)
	}
	var namespaceList corev1.NamespaceList
	if err := p.kubeClient.List(ctx, &namespaceList); err != nil {
		return nil, nil, fmt.Errorf("listing namespaces for topology snapshot, %w", err)
	}
	apiPods := make([]*corev1.Pod, len(podList.Items))
	for i := range podList.Items {
		apiPods[i] = &podList.Items[i]
	}
	namespaces := make([]*corev1.Namespace, len(namespaceList.Items))
	for i := range namespaceList.Items {
		namespaces[i] = &namespaceList.Items[i]
	}
	return apiPods, namespaces, nil
}

func newSimulationPodCatalog(pods []*corev1.Pod) (*simulationPodCatalog, error) {
	catalog := &simulationPodCatalog{
		pods:       make([]*corev1.Pod, 0, len(pods)),
		byIdentity: map[string]int{},
	}
	usedUIDs := map[types.UID]struct{}{}
	for _, pod := range pods {
		if pod == nil {
			return nil, errors.New("simulation pod catalog cannot contain nil pod")
		}
		if pod.UID != "" {
			usedUIDs[pod.UID] = struct{}{}
		}
	}
	nextSyntheticUID := 0
	for _, pod := range pods {
		identity := simulationPodIdentity(pod)
		if existingIndex, ok := catalog.byIdentity[identity]; ok {
			if !apiequality.Semantic.DeepEqual(catalog.pods[existingIndex], pod) {
				return nil, fmt.Errorf("duplicate simulation pod identity %s has differing contents", identity)
			}
			continue
		}
		podCopy := pod.DeepCopy()
		if podCopy.UID == "" {
			for {
				podCopy.UID = types.UID(fmt.Sprintf("simulation-pod-%d", nextSyntheticUID))
				nextSyntheticUID++
				if _, exists := usedUIDs[podCopy.UID]; !exists {
					break
				}
			}
			usedUIDs[podCopy.UID] = struct{}{}
		}
		catalog.byIdentity[identity] = len(catalog.pods)
		catalog.pods = append(catalog.pods, podCopy)
	}
	return catalog, nil
}

func simulationPodIdentity(pod *corev1.Pod) string {
	if pod.UID != "" {
		return "uid:" + string(pod.UID)
	}
	return "name:" + pod.Namespace + "\x00" + pod.Name
}

func cloneStateNodes(nodes []*state.StateNode) state.StateNodes {
	return lo.Map(nodes, func(node *state.StateNode, _ int) *state.StateNode {
		if node == nil {
			return nil
		}
		return node.DeepCopy()
	})
}

func (p *Provisioner) captureSimulationVolumeData(ctx context.Context, catalog *simulationPodCatalog) (map[types.UID]scheduling.VolumeData, error) {
	result := make(map[types.UID]scheduling.VolumeData, len(catalog.pods))
	for _, pod := range catalog.pods {
		data := scheduling.VolumeData{}
		requirements, err := p.volumeTopology.GetRequirements(ctx, pod)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			data.RequirementError = err
		} else {
			data.Requirements = requirements
		}
		// Volume usage is independent of topology requirements. The ordinary
		// disruption path filters failed topology requirements from topology
		// construction but still passes the complete pod list to Solve.
		data.Volumes, data.UsageError = karpscheduling.GetVolumes(ctx, p.kubeClient, pod)
		result[pod.UID] = data
	}
	return result, nil
}

// PodIDsFor returns catalog-scoped IDs for the supplied pods in their input
// order. It is intended to be called once while a disruption search captures
// its candidate-to-pod mapping.
func (s *PreparedSimulationInputs) PodIDsFor(pods []*corev1.Pod) ([]SimulationPodID, error) {
	ids := make([]SimulationPodID, 0, len(pods))
	for _, pod := range pods {
		if pod == nil {
			return nil, errors.New("nil pod is outside simulation pod catalog")
		}
		index, ok := s.catalog.byIdentity[simulationPodIdentity(pod)]
		if !ok {
			return nil, fmt.Errorf("pod %s/%s is outside simulation pod catalog", pod.Namespace, pod.Name)
		}
		ids = append(ids, SimulationPodID{catalog: s.catalog, index: index})
	}
	return ids, nil
}

func (s *PreparedSimulationInputs) validateRemovedNodes(names []string) (sets.Set[string], error) {
	removed := sets.New[string](names...)
	nodeNames := sets.New[string]()
	for _, node := range s.stateNodes {
		if node == nil {
			return nil, errors.New("simulation node snapshot cannot contain nil node")
		}
		nodeNames.Insert(node.Name())
	}
	for _, name := range names {
		if !nodeNames.Has(name) {
			return nil, fmt.Errorf("node %q is outside simulation node snapshot", name)
		}
	}
	return removed, nil
}

func (s *PreparedSimulationInputs) selectSimulationPods(ids []SimulationPodID) ([]*corev1.Pod, []*corev1.Pod, map[types.UID]scheduling.VolumeData, error) {
	seen := sets.New[SimulationPodID]()
	pods := make([]*corev1.Pod, 0, len(ids))
	topologyPods := make([]*corev1.Pod, 0, len(ids))
	volumeData := make(map[types.UID]scheduling.VolumeData, len(ids))
	for _, id := range ids {
		if id.catalog != s.catalog || id.index < 0 || id.index >= len(s.catalog.pods) {
			return nil, nil, nil, errors.New("pod ID is outside simulation pod catalog")
		}
		if seen.Has(id) {
			return nil, nil, nil, fmt.Errorf("pod ID at catalog index %d appears more than once", id.index)
		}
		seen.Insert(id)
		pod := s.catalog.pods[id.index].DeepCopy()
		data, ok := s.volumeData[pod.UID]
		if !ok {
			return nil, nil, nil, fmt.Errorf("volume data for pod %s/%s was not captured", pod.Namespace, pod.Name)
		}
		pods = append(pods, pod)
		if data.RequirementError == nil {
			topologyPods = append(topologyPods, pod)
		}
		volumeData[pod.UID] = data
	}
	return pods, topologyPods, volumeData, nil
}

// NewRun creates a scheduler and its exact mutable solve pods from one
// catalog-scoped scenario. Pods with unresolved volume topology requirements
// are excluded from topology construction, matching ordinary provisioning;
// the complete selected pod set is still passed to Solve, matching the
// disruption simulation contract. Usage errors remain in the captured source
// and are surfaced when existing nodes are evaluated.
func (s *PreparedSimulationInputs) NewRun(ctx context.Context, scenario Scenario) (*SimulationRun, error) {
	removed, err := s.validateRemovedNodes(scenario.RemovedNodeNames)
	if err != nil {
		return nil, err
	}
	pods, topologyPods, volumeData, err := s.selectSimulationPods(scenario.PodIDs)
	if err != nil {
		return nil, err
	}

	stateNodes := lo.Filter(s.stateNodes, func(node *state.StateNode, _ int) bool {
		return !node.MarkedForDeletion() && !removed.Has(node.Name())
	})
	volumeSource := scheduling.NewCapturedVolumeSource(volumeData)
	topology, err := s.preparedTopology.Materialize(ctx, topologyPods, scenario.RemovedNodeNames...)
	if err != nil {
		return nil, fmt.Errorf("materializing topology, %w", err)
	}
	newScheduler, err := s.factory.newSchedulerWithTopology(ctx, cloneStateNodes(stateNodes), topology, volumeSource)
	if err != nil {
		return nil, fmt.Errorf("creating simulation scheduler, %w", err)
	}
	return &SimulationRun{scheduler: newScheduler, pods: pods}, nil
}

// Solve evaluates the attempt's private pod copies with its paired scheduler.
func (a *SimulationRun) Solve(ctx context.Context) (scheduling.Results, error) {
	return a.scheduler.Solve(ctx, a.pods)
}
