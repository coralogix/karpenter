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

package scheduling

import (
	"context"
	"fmt"
	"sort"

	"github.com/awslabs/operatorpkg/option"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// PreparedTopology is an immutable topology source built from a single view of
// nodes and API objects. Materialize creates an ordinary Topology with private
// mutable groups for one scheduling attempt.
//
// The source deliberately owns no pod catalog or scenario policy. Callers must
// pass the pods for an attempt to Materialize; that keeps scenario selection and
// validation at the provisioning boundary.
type PreparedTopology struct {
	preferencePolicy PreferencePolicy
	domainGroups     map[string]TopologyDomainGroup

	nodes         map[string]*state.StateNode
	activeNodes   []*state.StateNode
	apiPodsByNS   map[string][]*corev1.Pod
	namespaceLabs map[string]map[string]string

	topologyGroups        map[uint64]*preparedTopologyGroup
	inverseTopologyGroups map[uint64]*preparedInverseTopologyGroup
}

type preparedTopologyGroup struct {
	base             *TopologyGroup
	fixedDomains     sets.Set[string]
	domainProviders  map[string]sets.Set[string]
	podContributions map[types.UID][]string
}

type preparedInverseTopologyGroup struct {
	base            *TopologyGroup
	fixedDomains    sets.Set[string]
	domainProviders map[string]sets.Set[string]
	ownerDomains    map[types.UID]string
}

type preparedTopologySource struct {
	prepared *PreparedTopology
}

func (s *preparedTopologySource) resolveNamespaces(ctx context.Context, namespace string, namespaces []string, selector *metav1.LabelSelector) (sets.Set[string], error) {
	return s.prepared.resolveNamespaces(ctx, namespace, namespaces, selector)
}

func (s *preparedTopologySource) countDomains(ctx context.Context, group *TopologyGroup, stateNodes []*state.StateNode, excluded sets.Set[string]) error {
	return s.prepared.countDomains(ctx, group, excluded, stateNodes)
}

func (*preparedTopologySource) forEachInverseAffinity(context.Context, func(*corev1.Pod, *corev1.Node) error) error {
	// Existing inverse groups are prepared before materialization. There is no
	// API-backed initialization step left for an attempt.
	return nil
}

// NewPreparedTopology builds a topology source from already captured objects.
// stateNodes is the complete node snapshot, including deleting nodes. API pods
// and namespaces are copied so later API mutations cannot alter the source.
func NewPreparedTopology(
	ctx context.Context,
	inputs *NodePoolInputs,
	stateNodes []*state.StateNode,
	apiPods []*corev1.Pod,
	namespaces []*corev1.Namespace,
	opts ...Options,
) (*PreparedTopology, error) {
	if inputs == nil {
		return nil, fmt.Errorf("node pool inputs are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := newPreparedTopology(inputs, opts...)
	p.captureNodes(stateNodes)
	p.capturePods(apiPods)
	p.captureNamespaces(namespaces)
	if err := p.prepareGroups(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func newPreparedTopology(inputs *NodePoolInputs, opts ...Options) *PreparedTopology {
	return &PreparedTopology{
		preferencePolicy:      optionPreference(opts...),
		domainGroups:          inputs.domainGroups,
		nodes:                 map[string]*state.StateNode{},
		apiPodsByNS:           map[string][]*corev1.Pod{},
		namespaceLabs:         map[string]map[string]string{},
		topologyGroups:        map[uint64]*preparedTopologyGroup{},
		inverseTopologyGroups: map[uint64]*preparedInverseTopologyGroup{},
	}
}

func (p *PreparedTopology) captureNodes(stateNodes []*state.StateNode) {
	for _, n := range stateNodes {
		if n == nil {
			continue
		}
		copy := n.DeepCopy()
		p.nodes[copy.Name()] = copy
		if !copy.MarkedForDeletion() {
			p.activeNodes = append(p.activeNodes, copy)
		}
	}
}

func (p *PreparedTopology) capturePods(apiPods []*corev1.Pod) {
	for _, pod := range apiPods {
		if pod == nil {
			continue
		}
		copy := pod.DeepCopy()
		p.apiPodsByNS[copy.Namespace] = append(p.apiPodsByNS[copy.Namespace], copy)
	}
}

func (p *PreparedTopology) captureNamespaces(namespaces []*corev1.Namespace) {
	for _, namespace := range namespaces {
		if namespace == nil {
			continue
		}
		p.namespaceLabs[namespace.Name] = map[string]string{}
		for key, value := range namespace.Labels {
			p.namespaceLabs[namespace.Name][key] = value
		}
	}
}

func (p *PreparedTopology) prepareGroups(ctx context.Context) error {
	// Existing pods provide normal topology counts and inverse anti-affinity
	// owners. Definitions are built through the same rule helpers as Topology.
	for _, pods := range p.apiPodsByNS {
		for _, pod := range pods {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := p.preparePodGroups(ctx, pod); err != nil {
				return err
			}
		}
	}
	return nil
}

func (p *PreparedTopology) preparePodGroups(ctx context.Context, pod *corev1.Pod) error {
	groups := p.newForTopologies(pod)
	affinityGroups, err := p.newForAffinities(ctx, pod)
	if err != nil {
		return fmt.Errorf("building topology for pod %s/%s, %w", pod.Namespace, pod.Name, err)
	}
	for _, group := range append(groups, affinityGroups...) {
		hash := group.Hash()
		if _, ok := p.topologyGroups[hash]; !ok {
			p.topologyGroups[hash] = p.prepareTopologyGroup(group)
		}
	}
	inverseGroups, err := p.newForInverseAntiAffinity(ctx, pod)
	if err != nil {
		return fmt.Errorf("building inverse anti-affinity for pod %s/%s, %w", pod.Namespace, pod.Name, err)
	}
	for _, group := range inverseGroups {
		p.prepareInverseOwner(group, pod)
	}
	return nil
}

func (p *PreparedTopology) prepareInverseOwner(group *TopologyGroup, pod *corev1.Pod) {
	hash := group.Hash()
	prepared, ok := p.inverseTopologyGroups[hash]
	if !ok {
		prepared = p.prepareInverseTopologyGroup(group)
		p.inverseTopologyGroups[hash] = prepared
	}
	node := p.nodeForName(pod.Spec.NodeName)
	if node == nil || node.Node == nil {
		return
	}
	prepared.base.AddOwner(pod.UID)
	// updateInverseAffinities supplies node.Labels directly to the live
	// topology; preserve its lack of hostname fallback here.
	if domain, ok := node.Node.Labels[group.Key]; ok {
		prepared.base.Record(domain)
		prepared.ownerDomains[pod.UID] = domain
	}
}

// optionPreference is kept local to this file to avoid exposing preparation
// options as a second public options type.
func optionPreference(opts ...Options) PreferencePolicy {
	return option.Resolve(opts...).preferencePolicy
}

func (p *PreparedTopology) newForTopologies(pod *corev1.Pod) []*TopologyGroup {
	return newForTopologies(pod, p.preferencePolicy, p.domainGroups)
}

func (p *PreparedTopology) newForAffinities(ctx context.Context, pod *corev1.Pod) ([]*TopologyGroup, error) {
	return newForAffinities(ctx, pod, p.preferencePolicy, p.domainGroups, p.resolveNamespaces)
}

func (p *PreparedTopology) newForInverseAntiAffinity(ctx context.Context, pod *corev1.Pod) ([]*TopologyGroup, error) {
	return newForInverseAntiAffinity(ctx, pod, p.domainGroups, p.resolveNamespaces)
}

func (p *PreparedTopology) resolveNamespaces(ctx context.Context, namespace string, namespaces []string, selector *metav1.LabelSelector) (sets.Set[string], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(namespaces) == 0 && selector == nil {
		return sets.New(namespace), nil
	}
	if selector == nil {
		return sets.New(namespaces...), nil
	}
	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, fmt.Errorf("parsing selector, %w", err)
	}
	selected := sets.New[string]()
	for namespace, namespaceLabels := range p.namespaceLabs {
		if labelSelector.Matches(labels.Set(namespaceLabels)) {
			selected.Insert(namespace)
		}
	}
	selected.Insert(namespaces...)
	return selected, nil
}

func (p *PreparedTopology) prepareTopologyGroup(group *TopologyGroup) *preparedTopologyGroup {
	prepared := &preparedTopologyGroup{
		base:             cloneTopologyGroup(group),
		fixedDomains:     sets.New[string](),
		domainProviders:  map[string]sets.Set[string]{},
		podContributions: map[types.UID][]string{},
	}
	prepared.base.domains = map[string]int32{}
	prepared.base.emptyDomains = sets.New[string]()
	prepared.base.owners = map[types.UID]struct{}{}
	for domain := range group.domains {
		prepared.fixedDomains.Insert(domain)
		prepared.base.Register(domain)
	}
	for _, node := range p.activeNodes {
		p.addNodeProvider(prepared.base, prepared.domainProviders, node)
	}
	for _, pod := range p.matchingPods(group) {
		if IgnoredForTopology(pod) || p.nodeForName(pod.Spec.NodeName) == nil {
			continue
		}
		node := p.nodeForName(pod.Spec.NodeName)
		if node.Node == nil {
			continue
		}
		domain, ok := topologyPodDomain(group, node.Node, scheduling.NewLabelRequirements(node.Node.Labels))
		if !ok {
			continue
		}
		prepared.base.Record(domain)
		prepared.podContributions[pod.UID] = append(prepared.podContributions[pod.UID], domain)
	}
	return prepared
}

func (p *PreparedTopology) prepareInverseTopologyGroup(group *TopologyGroup) *preparedInverseTopologyGroup {
	prepared := &preparedInverseTopologyGroup{
		base:            cloneTopologyGroup(group),
		fixedDomains:    sets.New[string](),
		domainProviders: map[string]sets.Set[string]{},
		ownerDomains:    map[types.UID]string{},
	}
	for domain := range group.domains {
		prepared.fixedDomains.Insert(domain)
	}
	prepared.base.sparseHostnameDomains = group.Key == corev1.LabelHostname
	for _, node := range p.activeNodes {
		// Inverse hostname anti-affinity only needs domains that have an owner.
		// Active hostnames are supplied as a materialized-topology view so they
		// do not have to be copied into every prepared inverse group.
		if group.Key != corev1.LabelHostname {
			p.addNodeProvider(prepared.base, prepared.domainProviders, node)
		}
	}
	return prepared
}

func (p *PreparedTopology) addNodeProvider(group *TopologyGroup, providers map[string]sets.Set[string], node *state.StateNode) {
	if node == nil || node.Node == nil || !group.nodeFilter.Matches(node.Node.Spec.Taints, scheduling.NewLabelRequirements(node.Node.Labels)) {
		return
	}
	domain, ok := topologyDomain(group.Key, node.Node)
	if !ok {
		return
	}
	nodeName := node.Name()
	if providers != nil {
		if providers[domain] == nil {
			providers[domain] = sets.New[string]()
		}
		providers[domain].Insert(nodeName)
	}
	group.Register(domain)
}

func (p *PreparedTopology) matchingPods(group *TopologyGroup) []*corev1.Pod {
	var pods []*corev1.Pod
	for namespace := range group.namespaces {
		pods = append(pods, p.apiPodsByNS[namespace]...)
	}
	selector := group.selector
	pods = filterPods(pods, selector)
	sort.SliceStable(pods, func(i, j int) bool { return pods[i].Spec.NodeName < pods[j].Spec.NodeName })
	return pods
}

func (p *PreparedTopology) nodeForName(name string) *state.StateNode {
	if node := p.nodes[name]; node != nil {
		return node
	}
	for _, node := range p.nodes {
		if node.Node != nil && node.Node.Name == name {
			return node
		}
	}
	return nil
}

func filterPods(pods []*corev1.Pod, selector labels.Selector) []*corev1.Pod {
	result := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if selector.Matches(labels.Set(pod.Labels)) {
			result = append(result, pod)
		}
	}
	return result
}

func topologyDomain(key string, node *corev1.Node) (string, bool) {
	if node == nil {
		return "", false
	}
	if domain, ok := node.Labels[key]; ok {
		return domain, true
	}
	if key == corev1.LabelHostname {
		return node.Name, true
	}
	return "", false
}

// Materialize creates a private mutable topology for one scenario. removedNodeNames
// is applied to available node providers while retaining API pod observations,
// matching the live topology's treatment of pods bound to removed nodes.
func (p *PreparedTopology) Materialize(ctx context.Context, pods []*corev1.Pod, removedNodeNames ...string) (*Topology, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	removed, err := p.removedNodeSet(removedNodeNames)
	if err != nil {
		return nil, err
	}
	t := p.newMaterializedTopology(pods, removed)
	p.materializeExistingInverse(t, removed)
	for _, pod := range pods {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if pod == nil {
			continue
		}
		if err := p.materializePod(ctx, t, pod, removed); err != nil {
			return nil, err
		}
	}
	return t, nil
}

func (p *PreparedTopology) removedNodeSet(names []string) (sets.Set[string], error) {
	removed := sets.New[string]()
	for _, name := range names {
		node := p.nodeForName(name)
		if node == nil {
			return nil, fmt.Errorf("removed node %q is not in the node snapshot", name)
		}
		removed.Insert(node.Name())
	}
	return removed, nil
}

func (p *PreparedTopology) newMaterializedTopology(pods []*corev1.Pod, removed sets.Set[string]) *Topology {
	t := &Topology{
		preferencePolicy:      p.preferencePolicy,
		domainGroups:          p.domainGroups,
		topologyGroups:        map[uint64]*TopologyGroup{},
		inverseTopologyGroups: map[uint64]*TopologyGroup{},
		excludedPods:          sets.New[string](),
		activeHostnameDomains: sets.New[string](),
		source:                &preparedTopologySource{prepared: p},
	}
	for _, pod := range pods {
		if pod != nil {
			t.excludedPods.Insert(string(pod.UID))
		}
	}
	for _, node := range p.activeNodes {
		if !removed.Has(node.Name()) {
			t.stateNodes = append(t.stateNodes, node.DeepCopy())
			// Keep the prepared source's existing node-label semantics here. A
			// managed NodeClaim label is registered later by ExistingNode.materialize
			// through StateNode.HostName().
			if node.Node != nil {
				if hostname, ok := topologyDomain(corev1.LabelHostname, node.Node); ok {
					t.activeHostnameDomains.Insert(hostname)
				}
			}
		}
	}
	return t
}

func (p *PreparedTopology) materializeExistingInverse(t *Topology, removed sets.Set[string]) {
	for hash, group := range p.inverseTopologyGroups {
		if materialized := group.materialize(t.excludedPods, removed, t.activeHostnameDomains); materialized != nil {
			t.inverseTopologyGroups[hash] = materialized
		}
	}
}

func (p *PreparedTopology) materializePod(ctx context.Context, t *Topology, pod *corev1.Pod, removed sets.Set[string]) error {
	for _, group := range p.newForTopologies(pod) {
		p.materializeNormalGroup(t, group, pod.UID, removed)
	}
	affinityGroups, err := p.newForAffinities(ctx, pod)
	if err != nil {
		return fmt.Errorf("building affinities for pod %s/%s, %w", pod.Namespace, pod.Name, err)
	}
	for _, group := range affinityGroups {
		p.materializeNormalGroup(t, group, pod.UID, removed)
	}
	inverseGroups, err := p.newForInverseAntiAffinity(ctx, pod)
	if err != nil {
		return fmt.Errorf("building inverse anti-affinity for pod %s/%s, %w", pod.Namespace, pod.Name, err)
	}
	for _, group := range inverseGroups {
		p.materializeInverseGroup(t, group, pod.UID, removed)
	}
	return nil
}

func (p *PreparedTopology) materializeNormalGroup(t *Topology, group *TopologyGroup, owner types.UID, removed sets.Set[string]) {
	hash := group.Hash()
	if _, ok := t.topologyGroups[hash]; !ok {
		if prepared := p.topologyGroups[hash]; prepared != nil {
			t.topologyGroups[hash] = prepared.materialize(t.excludedPods, removed)
		} else {
			t.topologyGroups[hash] = p.prepareTopologyGroup(group).materialize(t.excludedPods, removed)
		}
	}
	t.topologyGroups[hash].AddOwner(owner)
}

func (p *PreparedTopology) materializeInverseGroup(t *Topology, group *TopologyGroup, owner types.UID, removed sets.Set[string]) {
	hash := group.Hash()
	if _, ok := t.inverseTopologyGroups[hash]; !ok {
		if prepared := p.inverseTopologyGroups[hash]; prepared != nil {
			t.inverseTopologyGroups[hash] = prepared.materialize(t.excludedPods, removed, t.activeHostnameDomains)
			if t.inverseTopologyGroups[hash] == nil {
				// Every scenario owner must have a group even when all
				// existing owners of the same group were excluded.
				t.inverseTopologyGroups[hash] = cloneTopologyGroup(group)
				t.inverseTopologyGroups[hash].sparseHostnameDomains = group.Key == corev1.LabelHostname
			}
		} else {
			t.inverseTopologyGroups[hash] = cloneTopologyGroup(group)
			t.inverseTopologyGroups[hash].sparseHostnameDomains = group.Key == corev1.LabelHostname
		}
	}
	t.inverseTopologyGroups[hash].AddOwner(owner)
}

func (g *preparedTopologyGroup) materialize(excluded sets.Set[string], removed sets.Set[string]) *TopologyGroup {
	result := cloneTopologyGroup(g.base)
	for domain, providers := range g.domainProviders {
		if allRemoved(providers, removed) {
			if !g.fixedDomains.Has(domain) && result.domains[domain] == 0 {
				result.Unregister(domain)
			}
		}
	}
	for uid, domains := range g.podContributions {
		if !excluded.Has(string(uid)) {
			continue
		}
		for _, domain := range domains {
			result.Unrecord(domain)
		}
	}
	return result
}

func (g *preparedInverseTopologyGroup) materialize(excluded sets.Set[string], removed sets.Set[string], activeHostnameDomains sets.Set[string]) *TopologyGroup {
	result := cloneTopologyGroup(g.base)
	g.removeExcludedOwners(result, excluded)
	g.removeRemovedDomains(result, removed)
	if g.base.sparseHostnameDomains {
		g.removeInactiveHostnameDomains(result, activeHostnameDomains)
	}
	if len(result.owners) == 0 {
		return nil
	}
	return result
}

func (g *preparedInverseTopologyGroup) removeExcludedOwners(result *TopologyGroup, excluded sets.Set[string]) {
	for uid, domain := range g.ownerDomains {
		if excluded.Has(string(uid)) {
			result.RemoveOwner(uid)
			result.Unrecord(domain)
		}
	}
}

func (g *preparedInverseTopologyGroup) removeRemovedDomains(result *TopologyGroup, removed sets.Set[string]) {
	for domain, providers := range g.domainProviders {
		if allRemoved(providers, removed) {
			if !g.fixedDomains.Has(domain) && result.domains[domain] == 0 {
				result.Unregister(domain)
			}
		}
	}
}

func (g *preparedInverseTopologyGroup) removeInactiveHostnameDomains(result *TopologyGroup, activeHostnameDomains sets.Set[string]) {
	for domain, count := range result.domains {
		if count == 0 && !g.fixedDomains.Has(domain) && !activeHostnameDomains.Has(domain) {
			result.Unregister(domain)
		}
	}
}

// allRemoved reports whether every provider for a domain was removed. An
// empty provider set is considered removed, matching the vacuous membership
// check used by sets.Set.Intersection in the previous implementation.
func allRemoved(providers, removed sets.Set[string]) bool {
	for provider := range providers {
		if !removed.Has(provider) {
			return false
		}
	}
	return true
}

// countDomains is the snapshot equivalent of Topology.countDomains. It is
// invoked only by a materialized topology, including after preference updates.
func (p *PreparedTopology) countDomains(ctx context.Context, group *TopologyGroup, excluded sets.Set[string], stateNodes []*state.StateNode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, node := range stateNodes {
		p.addNodeProvider(group, nil, node)
	}
	for _, pod := range p.matchingPods(group) {
		if IgnoredForTopology(pod) || excluded.Has(string(pod.UID)) {
			continue
		}
		node := p.nodeForName(pod.Spec.NodeName)
		if node == nil || node.Node == nil {
			continue
		}
		if domain, ok := topologyPodDomain(group, node.Node, scheduling.NewLabelRequirements(node.Node.Labels)); ok {
			group.Record(domain)
		}
	}
	return nil
}

func cloneTopologyGroup(group *TopologyGroup) *TopologyGroup {
	clone := *group
	clone.namespaces = make(sets.Set[string], len(group.namespaces))
	for namespace := range group.namespaces {
		clone.namespaces[namespace] = struct{}{}
	}
	clone.domains = make(map[string]int32, len(group.domains))
	for domain, count := range group.domains {
		clone.domains[domain] = count
	}
	clone.emptyDomains = make(sets.Set[string], len(group.emptyDomains))
	for domain := range group.emptyDomains {
		clone.emptyDomains[domain] = struct{}{}
	}
	clone.owners = make(map[types.UID]struct{}, len(group.owners))
	for owner := range group.owners {
		clone.owners[owner] = struct{}{}
	}
	clone.rawSelector = group.rawSelector.DeepCopy()
	return &clone
}
