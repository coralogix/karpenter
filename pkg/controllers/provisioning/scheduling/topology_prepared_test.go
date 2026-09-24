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
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/scheme"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/karpenter/pkg/controllers/state"
	pscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestPreparedTopologyMatchesLiveTopologyForSpread(t *testing.T) {
	ctx := context.Background()
	inputs := preparedTopologyTestInputs()
	nodes := []*state.StateNode{preparedTopologyTestNode("node-a", "zone-a"), preparedTopologyTestNode("node-b", "zone-b")}
	existing := preparedTopologyTestPod("existing", "node-a")
	incoming := preparedTopologyTestPod("incoming", "")
	incoming.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		TopologyKey:       corev1.LabelTopologyZone,
		MaxSkew:           1,
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
	}}
	namespaces := []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}}
	liveClient := fakeclient.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(existing, nodes[0].Node, nodes[1].Node, namespaces[0]).Build()
	live, err := NewTopology(ctx, liveClient, &state.Cluster{}, nodes, inputs, []*corev1.Pod{incoming})
	if err != nil {
		t.Fatalf("building live topology: %v", err)
	}
	prepared, err := NewPreparedTopology(ctx, inputs, nodes, []*corev1.Pod{existing}, namespaces)
	if err != nil {
		t.Fatalf("building prepared topology: %v", err)
	}
	materialized, err := prepared.Materialize(ctx, []*corev1.Pod{incoming})
	if err != nil {
		t.Fatalf("materializing topology: %v", err)
	}

	group := incomingTopologyGroup(t, incoming)
	if materialized.topologyGroups[group.Hash()].sparseHostnameDomains {
		t.Fatalf("normal topology group unexpectedly uses sparse hostname domains")
	}
	if got, want := materialized.topologyGroups[group.Hash()].domains, live.topologyGroups[group.Hash()].domains; !reflect.DeepEqual(got, want) {
		t.Fatalf("prepared spread counts differ from live counts: got %v, want %v", got, want)
	}
	if got := materialized.topologyGroups[group.Hash()].domains["zone-a"]; got != 1 {
		t.Fatalf("expected one existing pod in zone-a, got %d", got)
	}
	if got, want := materialized.topologyGroups[group.Hash()].emptyDomains, live.topologyGroups[group.Hash()].emptyDomains; !reflect.DeepEqual(got, want) {
		t.Fatalf("prepared spread domains differ from live domains: got %v, want %v", got, want)
	}
}

func TestPreparedTopologyRemovesNodeProvidersButRetainsBoundPodCounts(t *testing.T) {
	ctx := context.Background()
	inputs := preparedTopologyTestInputs()
	nodeA := preparedTopologyTestNode("node-a", "zone-a")
	nodeB := preparedTopologyTestNode("node-b", "zone-b")
	existing := preparedTopologyTestPod("existing", "node-a")
	incoming := preparedTopologyTestPod("incoming", "")
	incoming.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		TopologyKey:       corev1.LabelTopologyZone,
		MaxSkew:           1,
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
	}}
	namespaces := []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}}
	liveClient := fakeclient.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(existing, nodeA.Node, nodeB.Node, namespaces[0]).Build()
	live, err := NewTopology(ctx, liveClient, &state.Cluster{}, []*state.StateNode{nodeB}, inputs, []*corev1.Pod{incoming})
	if err != nil {
		t.Fatalf("building live topology: %v", err)
	}
	prepared, err := NewPreparedTopology(ctx, inputs, []*state.StateNode{nodeA, nodeB}, []*corev1.Pod{existing}, namespaces)
	if err != nil {
		t.Fatalf("building prepared topology: %v", err)
	}
	materialized, err := prepared.Materialize(ctx, []*corev1.Pod{incoming}, "node-a")
	if err != nil {
		t.Fatalf("materializing topology: %v", err)
	}
	group := incomingTopologyGroup(t, incoming)
	if got, want := materialized.topologyGroups[group.Hash()].domains, live.topologyGroups[group.Hash()].domains; !reflect.DeepEqual(got, want) {
		t.Fatalf("prepared removed-node counts differ from live counts: got %v, want %v", got, want)
	}
	if got := materialized.topologyGroups[group.Hash()].domains["zone-a"]; got != 1 {
		t.Fatalf("expected bound pod on removed node to remain counted, got %d", got)
	}
}

func TestPreparedTopologyUsesSnapshotForInverseAffinityAndRelaxation(t *testing.T) {
	ctx := context.Background()
	inputs := preparedTopologyTestInputs()
	node := preparedTopologyTestNode("node-a", "zone-a")
	existing := preparedTopologyTestPod("existing", "node-a")
	existing.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		TopologyKey:       corev1.LabelTopologyZone,
		MaxSkew:           1,
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
	}}
	existing.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
		TopologyKey: corev1.LabelTopologyZone,
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
			"app": "other",
		}},
	}}}}
	namespaces := []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default", Labels: map[string]string{"team": "blue"}}}}
	prepared, err := NewPreparedTopology(ctx, inputs, []*state.StateNode{node}, []*corev1.Pod{existing}, namespaces)
	if err != nil {
		t.Fatalf("building prepared topology: %v", err)
	}
	incoming := preparedTopologyTestPod("incoming", "")
	incoming.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
		Weight: 1,
		PodAffinityTerm: corev1.PodAffinityTerm{
			TopologyKey: corev1.LabelTopologyZone,
			NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				"team": "blue",
			}},
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}}}}
	materialized, err := prepared.Materialize(ctx, []*corev1.Pod{incoming})
	if err != nil {
		t.Fatalf("materializing topology: %v", err)
	}
	// The materialized topology has no client. This update creates a new group
	// and resolves its namespace selector and counts entirely from the snapshot.
	incoming.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{{
		TopologyKey: "example.com/region",
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
			"team": "blue",
		}},
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
	}}
	if err := materialized.Update(ctx, incoming); err != nil {
		t.Fatalf("updating prepared topology after preference relaxation: %v", err)
	}
	if len(materialized.inverseTopologyGroups) != 1 {
		t.Fatalf("expected one existing inverse group, got %d", len(materialized.inverseTopologyGroups))
	}
	liveClient := fakeclient.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(existing, node.Node, namespaces[0]).Build()
	live, err := NewTopology(ctx, liveClient, &state.Cluster{}, []*state.StateNode{node}, inputs, []*corev1.Pod{incoming})
	if err != nil {
		t.Fatalf("building live topology: %v", err)
	}
	if err := live.updateInverseAntiAffinity(ctx, existing, map[string]string{corev1.LabelTopologyZone: "zone-a"}); err != nil {
		t.Fatalf("adding live inverse affinity: %v", err)
	}
	group := newForInverseGroupForTest(t, existing)
	preparedGroup := materialized.inverseTopologyGroups[group.Hash()]
	liveGroup := live.inverseTopologyGroups[group.Hash()]
	if got, want := preparedGroup.domains, liveGroup.domains; !reflect.DeepEqual(got, want) {
		t.Fatalf("prepared inverse counts differ from live counts: got %v, want %v", got, want)
	}
	if !preparedGroup.IsOwnedBy(existing.UID) || !liveGroup.IsOwnedBy(existing.UID) {
		t.Fatalf("expected existing anti-affinity pod to own the inverse group")
	}
}

func TestPreparedTopologyMaterializationsOwnMutableGroups(t *testing.T) {
	ctx := context.Background()
	inputs := preparedTopologyTestInputs()
	node := preparedTopologyTestNode("node-a", "zone-a")
	existing := preparedTopologyTestPod("existing", "node-a")
	existing.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		TopologyKey:       corev1.LabelTopologyZone,
		MaxSkew:           1,
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
	}}
	namespaces := []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}}
	prepared, err := NewPreparedTopology(ctx, inputs, []*state.StateNode{node}, []*corev1.Pod{existing}, namespaces)
	if err != nil {
		t.Fatalf("building prepared topology: %v", err)
	}
	incoming := preparedTopologyTestPod("incoming", "")
	incoming.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
		TopologyKey:       corev1.LabelTopologyZone,
		MaxSkew:           1,
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
	}}
	first, err := prepared.Materialize(ctx, []*corev1.Pod{incoming})
	if err != nil {
		t.Fatalf("materializing first topology: %v", err)
	}
	second, err := prepared.Materialize(ctx, []*corev1.Pod{incoming})
	if err != nil {
		t.Fatalf("materializing second topology: %v", err)
	}
	group := incomingTopologyGroup(t, incoming)
	firstGroup := first.topologyGroups[group.Hash()]
	secondGroup := second.topologyGroups[group.Hash()]
	firstGroup.Record("zone-a")
	firstGroup.namespaces.Insert("other")
	firstGroup.emptyDomains.Insert("other")
	firstGroup.owners[types.UID("mutated")] = struct{}{}
	firstGroup.rawSelector.MatchLabels["app"] = "changed"
	if got := secondGroup.domains["zone-a"]; got != 1 {
		t.Fatalf("second materialization changed with first mutation, got %d", got)
	}
	assertUnchangedTopologyGroup(t, secondGroup, "second materialization")
	if got := prepared.topologyGroups[group.Hash()].base.domains["zone-a"]; got != 1 {
		t.Fatalf("prepared baseline changed with materialized mutation, got %d", got)
	}
	assertUnchangedTopologyGroup(t, prepared.topologyGroups[group.Hash()].base, "prepared baseline")
}

func TestPreparedTopologyInverseHostnameActiveDomainsMatchDenseBaseline(t *testing.T) {
	ctx := context.Background()
	inputs := preparedTopologyTestInputs()
	nodeA := preparedTopologyTestNode("node-a", "zone-a")
	nodeA.Node.Labels[corev1.LabelHostname] = "shared-host"
	nodeB := preparedTopologyTestNode("node-b", "zone-b")
	delete(nodeB.Node.Labels, corev1.LabelHostname)
	nodeC := preparedTopologyTestNode("node-c", "zone-c")
	nodeC.Node.Labels[corev1.LabelHostname] = "shared-host"
	owner := hostnameAntiAffinityPod("owner", "node-a")
	namespaces := []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}}
	prepared, err := NewPreparedTopology(ctx, inputs, []*state.StateNode{nodeA, nodeB, nodeC}, []*corev1.Pod{owner}, namespaces)
	if err != nil {
		t.Fatalf("building prepared topology: %v", err)
	}
	incoming := hostnameAntiAffinityPod("incoming", "")
	materialized, err := prepared.Materialize(ctx, []*corev1.Pod{incoming})
	if err != nil {
		t.Fatalf("materializing topology: %v", err)
	}
	groupHash := newForInverseGroupForTest(t, owner).Hash()
	preparedGroup := materialized.inverseTopologyGroups[groupHash]
	if preparedGroup == nil {
		t.Fatalf("expected prepared inverse hostname group")
	}
	if !preparedGroup.sparseHostnameDomains {
		t.Fatalf("prepared inverse hostname group did not use sparse hostname domains")
	}
	if _, ok := preparedGroup.domains["node-b"]; ok {
		t.Fatalf("active hostname fallback should not be copied into the inverse group: %v", preparedGroup.domains)
	}
	if got, want := materialized.activeHostnameDomains, sets.New("shared-host", "node-b"); !reflect.DeepEqual(got, want) {
		t.Fatalf("active hostname view = %v, want %v", got, want)
	}

	assertSparseInverseHostnameMatchesDense(t, materialized, preparedGroup, groupHash, incoming)
	materialized.Register(corev1.LabelHostname, "node-b")
	if _, ok := preparedGroup.domains["node-b"]; ok {
		t.Fatalf("registering an active hostname repopulated the sparse inverse group: %v", preparedGroup.domains)
	}
}

func assertSparseInverseHostnameMatchesDense(t *testing.T, materialized *Topology, preparedGroup *TopologyGroup, groupHash uint64, incoming *corev1.Pod) {
	t.Helper()
	// Reconstruct the old dense representation from the sparse group. The
	// effective requirements must remain identical for exact, multi-value, and
	// non-exact hostname requirements.
	denseGroup := cloneTopologyGroup(preparedGroup)
	for domain := range materialized.activeHostnameDomains {
		denseGroup.Register(domain)
	}
	tests := []struct {
		name         string
		podOperator  corev1.NodeSelectorOperator
		podValues    []string
		nodeOperator corev1.NodeSelectorOperator
		nodeValues   []string
	}{
		{name: "exact", podOperator: corev1.NodeSelectorOpIn, podValues: []string{"shared-host"}, nodeOperator: corev1.NodeSelectorOpIn, nodeValues: []string{"shared-host"}},
		{name: "multi-value", podOperator: corev1.NodeSelectorOpIn, podValues: []string{"shared-host", "node-b"}, nodeOperator: corev1.NodeSelectorOpIn, nodeValues: []string{"shared-host", "node-b"}},
		{name: "exists", podOperator: corev1.NodeSelectorOpExists, nodeOperator: corev1.NodeSelectorOpExists},
		{name: "not-in", podOperator: corev1.NodeSelectorOpNotIn, podValues: []string{"shared-host"}, nodeOperator: corev1.NodeSelectorOpNotIn, nodeValues: []string{"shared-host"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podDomains := pscheduling.NewRequirement(corev1.LabelHostname, tt.podOperator, tt.podValues...)
			nodeDomains := pscheduling.NewRequirement(corev1.LabelHostname, tt.nodeOperator, tt.nodeValues...)
			got, _ := preparedGroup.GetWithActiveDomains(incoming, podDomains, nodeDomains, materialized.activeHostnameDomains)
			want, _ := denseGroup.Get(incoming, podDomains, nodeDomains)
			if got := got.NodeSelectorRequirement(); !reflect.DeepEqual(got, want.NodeSelectorRequirement()) {
				t.Fatalf("active-domain result = %v, dense result = %v", got, want.NodeSelectorRequirement())
			}
		})
	}
	denseTopology := *materialized
	denseTopology.activeHostnameDomains = nil
	denseTopology.inverseTopologyGroups = map[uint64]*TopologyGroup{groupHash: denseGroup}
	podRequirements := pscheduling.NewRequirements(pscheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpExists))
	nodeRequirements := pscheduling.NewRequirements(pscheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpExists))
	sparseResult, err := materialized.AddRequirements(incoming, nil, podRequirements, nodeRequirements)
	if err != nil {
		t.Fatalf("sparse topology requirements: %v", err)
	}
	denseResult, err := denseTopology.AddRequirements(incoming, nil, podRequirements, nodeRequirements)
	if err != nil {
		t.Fatalf("dense topology requirements: %v", err)
	}
	if got, want := sparseResult.Get(corev1.LabelHostname).NodeSelectorRequirement(), denseResult.Get(corev1.LabelHostname).NodeSelectorRequirement(); !reflect.DeepEqual(got, want) {
		t.Fatalf("NewRun topology result = %v, dense result = %v", got, want)
	}
}

func TestPreparedTopologyInverseHostnameActiveDomainsRespectRemovedProviders(t *testing.T) {
	ctx := context.Background()
	inputs := preparedTopologyTestInputs()
	nodeA := preparedTopologyTestNode("node-a", "zone-a")
	nodeA.Node.Labels[corev1.LabelHostname] = "shared-host"
	nodeB := preparedTopologyTestNode("node-b", "zone-b")
	nodeB.Node.Labels[corev1.LabelHostname] = "shared-host"
	owner := hostnameAntiAffinityPod("owner", "node-a")
	namespaces := []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}}
	prepared, err := NewPreparedTopology(ctx, inputs, []*state.StateNode{nodeA, nodeB}, []*corev1.Pod{owner}, namespaces)
	if err != nil {
		t.Fatalf("building prepared topology: %v", err)
	}
	incoming := hostnameAntiAffinityPod("incoming", "")
	materialized, err := prepared.Materialize(ctx, []*corev1.Pod{incoming}, "node-a")
	if err != nil {
		t.Fatalf("materializing topology: %v", err)
	}
	if got, want := materialized.activeHostnameDomains, sets.New("shared-host"); !reflect.DeepEqual(got, want) {
		t.Fatalf("active hostname view after one provider removal = %v, want %v", got, want)
	}

	groupHash := newForInverseGroupForTest(t, owner).Hash()
	preparedGroup := materialized.inverseTopologyGroups[groupHash]
	denseGroup := cloneTopologyGroup(preparedGroup)
	for domain := range materialized.activeHostnameDomains {
		denseGroup.Register(domain)
	}
	podDomains := pscheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpExists)
	nodeDomains := pscheduling.NewRequirement(corev1.LabelHostname, corev1.NodeSelectorOpExists)
	got, _ := preparedGroup.GetWithActiveDomains(incoming, podDomains, nodeDomains, materialized.activeHostnameDomains)
	want, _ := denseGroup.Get(incoming, podDomains, nodeDomains)
	if got := got.NodeSelectorRequirement(); !reflect.DeepEqual(got, want.NodeSelectorRequirement()) {
		t.Fatalf("active-domain result after provider removal = %v, dense result = %v", got, want.NodeSelectorRequirement())
	}

	second, err := prepared.Materialize(ctx, []*corev1.Pod{incoming})
	if err != nil {
		t.Fatalf("materializing second topology: %v", err)
	}
	materialized.Register(corev1.LabelHostname, "new-host")
	if second.activeHostnameDomains.Has("new-host") {
		t.Fatalf("hostname registration leaked across materializations")
	}
}

func TestPreparedTopologyInverseHostnameOwnerAndProviderDeltas(t *testing.T) {
	ctx := context.Background()
	inputs := preparedTopologyTestInputs()
	nodeA := preparedTopologyTestNode("node-a", "zone-a")
	nodeA.Node.Labels[corev1.LabelHostname] = "shared-host"
	nodeB := preparedTopologyTestNode("node-b", "zone-b")
	nodeB.Node.Labels[corev1.LabelHostname] = "shared-host"
	ownerA := hostnameAntiAffinityPod("owner-a", "node-a")
	ownerB := hostnameAntiAffinityPod("owner-b", "node-b")
	namespaces := []*corev1.Namespace{{ObjectMeta: metav1.ObjectMeta{Name: "default"}}}
	prepared, err := NewPreparedTopology(ctx, inputs, []*state.StateNode{nodeA, nodeB}, []*corev1.Pod{ownerA, ownerB}, namespaces)
	if err != nil {
		t.Fatalf("building prepared topology: %v", err)
	}
	groupHash := newForInverseGroupForTest(t, ownerA).Hash()
	preparedGroup := prepared.inverseTopologyGroups[groupHash]
	materialized := preparedGroup.materialize(
		sets.New(string(ownerA.UID)),
		sets.New("node-a"),
		sets.New("shared-host"),
	)
	if materialized == nil {
		t.Fatalf("expected remaining inverse owner")
	}
	if got := materialized.domains["shared-host"]; got != 1 {
		t.Fatalf("remaining owner count = %d, want 1", got)
	}

	// A removed sole provider with a zero count must disappear from the dense
	// maps as well; the active-domain view must not rely on stale emptyDomains.
	nodeC := preparedTopologyTestNode("node-c", "zone-c")
	nodeD := preparedTopologyTestNode("node-d", "zone-d")
	delete(nodeD.Node.Labels, corev1.LabelHostname)
	ownerC := hostnameAntiAffinityPod("owner-c", "node-c")
	prepared, err = NewPreparedTopology(ctx, inputs, []*state.StateNode{nodeC, nodeD}, []*corev1.Pod{ownerC, hostnameAntiAffinityPod("owner-without-hostname", "node-d")}, namespaces)
	if err != nil {
		t.Fatalf("building sole-provider topology: %v", err)
	}
	groupHash = newForInverseGroupForTest(t, ownerC).Hash()
	materialized = prepared.inverseTopologyGroups[groupHash].materialize(
		sets.New(string(ownerC.UID)),
		sets.New("node-c"),
		sets.New("node-d"),
	)
	if materialized == nil {
		t.Fatalf("expected inverse owner without a node to remain")
	}
	if _, ok := materialized.domains["node-c"]; ok {
		t.Fatalf("removed zero-count hostname remained in dense domains: %v", materialized.domains)
	}
}

func assertUnchangedTopologyGroup(t *testing.T, group *TopologyGroup, name string) {
	t.Helper()
	if group.namespaces.Has("other") || group.emptyDomains.Has("other") || group.IsOwnedBy(types.UID("mutated")) {
		t.Fatalf("%s shares mutable group collections with first materialization", name)
	}
	if got := group.rawSelector.MatchLabels["app"]; got != "web" {
		t.Fatalf("%s shares raw selector with first, got app=%q", name, got)
	}
}

func TestPreparedTopologyMaterializeHonorsContext(t *testing.T) {
	prepared, err := NewPreparedTopology(context.Background(), preparedTopologyTestInputs(), nil, nil, nil)
	if err != nil {
		t.Fatalf("building prepared topology: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepared.Materialize(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled materialization, got %v", err)
	}
}

func TestAllRemoved(t *testing.T) {
	tests := []struct {
		name      string
		providers sets.Set[string]
		removed   sets.Set[string]
		want      bool
	}{
		{name: "nil providers", providers: nil, removed: nil, want: true},
		{name: "empty providers", providers: sets.New[string](), removed: sets.New("provider-a"), want: true},
		{name: "no providers removed", providers: sets.New("provider-a"), removed: nil, want: false},
		{name: "some providers removed", providers: sets.New("provider-a", "provider-b"), removed: sets.New("provider-a"), want: false},
		{name: "all providers removed", providers: sets.New("provider-a", "provider-b"), removed: sets.New("provider-a", "provider-b"), want: true},
		{name: "empty provider name removed", providers: sets.New(""), removed: sets.New(""), want: true},
		{name: "empty provider name retained", providers: sets.New(""), removed: nil, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := allRemoved(tt.providers, tt.removed); got != tt.want {
				t.Fatalf("allRemoved(%v, %v) = %t, want %t", tt.providers, tt.removed, got, tt.want)
			}
		})
	}
}

func preparedTopologyTestInputs() *NodePoolInputs {
	domains := NewTopologyDomainGroup()
	domains.Insert("zone-a")
	domains.Insert("zone-b")
	return &NodePoolInputs{domainGroups: map[string]TopologyDomainGroup{corev1.LabelTopologyZone: domains}}
}

func preparedTopologyTestNode(name, zone string) *state.StateNode {
	node := state.NewNode()
	node.Node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{corev1.LabelTopologyZone: zone, corev1.LabelHostname: name}}}
	return node
}

func preparedTopologyTestPod(name, nodeName string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID(name), Labels: map[string]string{"app": "web"}}, Spec: corev1.PodSpec{NodeName: nodeName, Containers: []corev1.Container{{Name: "container", Image: "image"}}}}
}

func hostnameAntiAffinityPod(name, nodeName string) *corev1.Pod {
	pod := preparedTopologyTestPod(name, nodeName)
	pod.Labels["app"] = "anti"
	pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
			TopologyKey: corev1.LabelHostname,
			LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
				"app": "anti",
			}},
		}},
	}}
	return pod
}

func incomingTopologyGroup(t *testing.T, pod *corev1.Pod) *TopologyGroup {
	t.Helper()
	groups := newForTopologies(pod, PreferencePolicyRespect, preparedTopologyTestInputs().domainGroups)
	if len(groups) != 1 {
		t.Fatalf("expected one incoming topology group, got %d", len(groups))
	}
	return groups[0]
}

func newForInverseGroupForTest(t *testing.T, pod *corev1.Pod) *TopologyGroup {
	t.Helper()
	groups, err := newForInverseAntiAffinity(context.Background(), pod, preparedTopologyTestInputs().domainGroups, func(context.Context, string, []string, *metav1.LabelSelector) (sets.Set[string], error) {
		return sets.New[string]("default"), nil
	})
	if err != nil || len(groups) != 1 {
		t.Fatalf("expected one inverse group, got %d (%v)", len(groups), err)
	}
	return groups[0]
}
