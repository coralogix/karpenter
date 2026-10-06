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
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestScoreBasedPriceSnapshotUsesFullInitializedActiveFleetWeightedByCPU(t *testing.T) {
	pool := "pool-a"
	capacityType := v1.CapacityTypeOnDemand
	types := map[string]*cloudprovider.InstanceType{
		"small":    scorePriceInstanceType("small", 2, map[string]float64{capacityType: 0.20}),
		"large":    scorePriceInstanceType("large", 8, map[string]float64{capacityType: 0.40}),
		"excluded": scorePriceInstanceType("excluded", 100, map[string]float64{capacityType: 100}),
	}
	source := scorePriceStateNode("source", pool, capacityType, "small", 2, true)
	nonCandidate := scorePriceStateNode("large-fleet-node", pool, capacityType, "large", 8, true)
	deleting := scorePriceStateNode("deleting", pool, capacityType, "excluded", 100, true)
	deleting.NodeClaim.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time}
	uninitialized := scorePriceStateNode("uninitialized", pool, capacityType, "excluded", 100, false)
	standbyNode := scorePriceStateNode("standby", pool, capacityType, "excluded", 100, true)
	standby.SetNodeClaimStandby(standbyNode.NodeClaim, true)
	activating := scorePriceStateNode("activating", pool, capacityType, "excluded", 100, true)
	standby.SetNodeClaimActivating(activating.NodeClaim, true)
	candidate := scorePriceCandidate(source, types)

	snapshot := newScoreBasedPriceSnapshot(state.StateNodes{source, nonCandidate, deleting, uninitialized, standbyNode, activating}, []*Candidate{candidate})
	group := snapshot.groups[scoreBasedPriceGroupKey{nodePool: pool, capacityType: capacityType}]
	if group == nil || !group.known {
		t.Fatal("expected a known price baseline")
	}
	if group.nodeCount != 2 || group.totalCPU != 10 || math.Abs(group.totalPrice-0.60) > 1e-12 {
		t.Fatalf("baseline fleet totals = nodes %d, CPU %v, price %v; want 2, 10, 0.60", group.nodeCount, group.totalCPU, group.totalPrice)
	}
	if math.Abs(group.baseline-0.06) > 1e-12 {
		t.Fatalf("CPU-weighted baseline = %v, want 0.06 USD/CPU/h", group.baseline)
	}
	score := snapshot.score(Command{Candidates: []*Candidate{candidate}})
	if !score.Valid || math.Abs(score.Contribution-(0.20/(2*0.06))) > 1e-12 {
		t.Fatalf("source normalized cost = %+v, want valid contribution %v", score, 0.20/(2*0.06))
	}
}

func TestScoreBasedPriceSnapshotSeparatesNodePoolAndCapacityType(t *testing.T) {
	type nodeSpec struct {
		name, pool, capacityType, instanceType string
		cpu                                    float64
		price                                  float64
	}
	specs := []nodeSpec{
		{name: "a-spot-source", pool: "pool-a", capacityType: v1.CapacityTypeSpot, instanceType: "a-spot-source", cpu: 4, price: 0.40},
		{name: "a-spot-peer", pool: "pool-a", capacityType: v1.CapacityTypeSpot, instanceType: "a-spot-peer", cpu: 4, price: 0.80},
		{name: "a-od-source", pool: "pool-a", capacityType: v1.CapacityTypeOnDemand, instanceType: "a-od-source", cpu: 4, price: 0.80},
		{name: "a-od-peer", pool: "pool-a", capacityType: v1.CapacityTypeOnDemand, instanceType: "a-od-peer", cpu: 4, price: 0.80},
		{name: "b-spot-source", pool: "pool-b", capacityType: v1.CapacityTypeSpot, instanceType: "b-spot-source", cpu: 1, price: 0.20},
		{name: "b-spot-peer", pool: "pool-b", capacityType: v1.CapacityTypeSpot, instanceType: "b-spot-peer", cpu: 3, price: 0.60},
	}
	allNodes := make(state.StateNodes, 0, len(specs))
	nodesByName := map[string]*state.StateNode{}
	typesByPool := map[string]map[string]*cloudprovider.InstanceType{}
	for _, spec := range specs {
		instanceType := scorePriceInstanceType(spec.instanceType, spec.cpu, map[string]float64{spec.capacityType: spec.price})
		if typesByPool[spec.pool] == nil {
			typesByPool[spec.pool] = map[string]*cloudprovider.InstanceType{}
		}
		typesByPool[spec.pool][spec.instanceType] = instanceType
		node := scorePriceStateNode(spec.name, spec.pool, spec.capacityType, spec.instanceType, spec.cpu, true)
		allNodes = append(allNodes, node)
		nodesByName[spec.name] = node
	}
	// Build candidates after all per-pool type maps are populated so each carries
	// the complete map used to resolve its pool's fleet nodes.
	candidates := []*Candidate{
		scorePriceCandidate(nodesByName["a-spot-source"], typesByPool["pool-a"]),
		scorePriceCandidate(nodesByName["a-od-source"], typesByPool["pool-a"]),
		scorePriceCandidate(nodesByName["b-spot-source"], typesByPool["pool-b"]),
	}
	snapshot := newScoreBasedPriceSnapshot(allNodes, candidates)
	replacement := scorePriceInstanceType("replacement", 8, map[string]float64{v1.CapacityTypeOnDemand: 0.70})
	score := snapshot.score(scorePriceReplacementCommand(candidates, replacement))
	if !score.Valid {
		t.Fatal("expected a valid score across three independent pool/capacity-type baselines")
	}
	if math.Abs(score.SavingsFraction-0.5) > 1e-12 || math.Abs(score.Contribution-(0.5*(2.0/3.0+1+1))) > 1e-12 {
		t.Fatalf("mixed-type replacement score = %+v, want savings fraction 0.5 and normalized contribution %v", score, 0.5*(2.0/3.0+1+1))
	}
	for key, want := range map[scoreBasedPriceGroupKey]float64{
		{nodePool: "pool-a", capacityType: v1.CapacityTypeSpot}:     0.15,
		{nodePool: "pool-a", capacityType: v1.CapacityTypeOnDemand}: 0.20,
		{nodePool: "pool-b", capacityType: v1.CapacityTypeSpot}:     0.20,
	} {
		if got := snapshot.groups[key].baseline; math.Abs(got-want) > 1e-12 {
			t.Errorf("baseline for %+v = %v, want %v", key, got, want)
		}
	}
}

func TestScoreBasedPriceContributionRetainsAveragePricedOnDemandToSpotSavings(t *testing.T) {
	const pool = "pool"
	types := map[string]*cloudprovider.InstanceType{
		"on-demand":      scorePriceInstanceType("on-demand", 4, map[string]float64{v1.CapacityTypeOnDemand: 0.80}),
		"on-demand-peer": scorePriceInstanceType("on-demand-peer", 4, map[string]float64{v1.CapacityTypeOnDemand: 0.80}),
		"spot":           scorePriceInstanceType("spot", 4, map[string]float64{v1.CapacityTypeSpot: 0.40}),
		"spot-peer":      scorePriceInstanceType("spot-peer", 4, map[string]float64{v1.CapacityTypeSpot: 0.40}),
	}
	source := scorePriceStateNode("on-demand-source", pool, v1.CapacityTypeOnDemand, "on-demand", 4, true)
	peer := scorePriceStateNode("on-demand-peer", pool, v1.CapacityTypeOnDemand, "on-demand-peer", 4, true)
	spot := scorePriceStateNode("spot-peer", pool, v1.CapacityTypeSpot, "spot-peer", 4, true)
	candidate := scorePriceCandidate(source, types)
	spotReplacement := types["spot"]
	snapshot := newScoreBasedPriceSnapshot(state.StateNodes{source, peer, spot}, []*Candidate{candidate})

	score := snapshot.score(scorePriceReplacementCommand([]*Candidate{candidate}, spotReplacement))
	if !score.Valid || math.Abs(score.SavingsFraction-0.5) > 1e-12 || math.Abs(score.NormalizedSourceCost-1) > 1e-12 || math.Abs(score.Contribution-0.5) > 1e-12 {
		t.Fatalf("average-priced OD to cheaper Spot score = %+v, want normalized source 1, savings fraction 0.5, contribution 0.5", score)
	}
}

func TestScoreBasedReplacementPriceUsesAvailableCompatibleOfferings(t *testing.T) {
	const pool = "pool"
	sourceType := scorePriceInstanceType("source", 2, map[string]float64{v1.CapacityTypeOnDemand: 0.50})
	sourceNode := scorePriceStateNode("source", pool, v1.CapacityTypeOnDemand, "source", 2, true)
	source := scorePriceCandidate(sourceNode, map[string]*cloudprovider.InstanceType{"source": sourceType})
	snapshot := newScoreBasedPriceSnapshot(state.StateNodes{sourceNode}, []*Candidate{source})
	offering := func(price float64, available bool, capacityType, zone string) *cloudprovider.Offering {
		return &cloudprovider.Offering{
			Available: available,
			Price:     price,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:  capacityType,
				corev1.LabelTopologyZone: zone,
			}),
		}
	}
	requirements := func(key string, values ...string) scheduling.Requirements {
		return scheduling.NewRequirements(scheduling.NewRequirement(key, corev1.NodeSelectorOpIn, values...))
	}
	tests := []struct {
		name            string
		offerings       cloudprovider.Offerings
		requirements    scheduling.Requirements
		wantReplacement float64
		wantSavings     float64
	}{
		{
			name: "unavailable cheaper offering",
			offerings: cloudprovider.Offerings{
				offering(0.01, false, v1.CapacityTypeOnDemand, "zone-a"),
				offering(0.40, true, v1.CapacityTypeOnDemand, "zone-a"),
			},
			requirements:    requirements(v1.CapacityTypeLabelKey, v1.CapacityTypeOnDemand),
			wantReplacement: 0.40,
			wantSavings:     0.10,
		},
		{
			name: "incompatible capacity type",
			offerings: cloudprovider.Offerings{
				offering(0.10, true, v1.CapacityTypeSpot, "zone-a"),
				offering(0.40, true, v1.CapacityTypeOnDemand, "zone-a"),
			},
			requirements:    requirements(v1.CapacityTypeLabelKey, v1.CapacityTypeOnDemand),
			wantReplacement: 0.40,
			wantSavings:     0.10,
		},
		{
			name: "incompatible zone",
			offerings: cloudprovider.Offerings{
				offering(0.05, true, v1.CapacityTypeOnDemand, "zone-b"),
				offering(0.45, true, v1.CapacityTypeOnDemand, "zone-a"),
			},
			requirements:    requirements(corev1.LabelTopologyZone, "zone-a"),
			wantReplacement: 0.45,
			wantSavings:     0.05,
		},
		{
			name: "unconstrained offerings",
			offerings: cloudprovider.Offerings{
				offering(0.10, true, v1.CapacityTypeSpot, "zone-a"),
				offering(0.40, true, v1.CapacityTypeOnDemand, "zone-a"),
			},
			requirements:    scheduling.NewRequirements(),
			wantReplacement: 0.10,
			wantSavings:     0.40,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			replacementType := &cloudprovider.InstanceType{Name: "replacement", Offerings: test.offerings}
			command := scorePriceReplacementCommand([]*Candidate{source}, replacementType)
			command.Results.NewNodeClaims[0].Requirements = test.requirements

			if got := consolidationSavingsUSDPerHour(command); math.Abs(got-test.wantSavings) > 1e-12 {
				t.Errorf("consolidationSavingsUSDPerHour() = %v, want %v", got, test.wantSavings)
			}
			if got := command.EstimatedSavings(); math.Abs(got-test.wantSavings) > 1e-12 {
				t.Errorf("Command.EstimatedSavings() = %v, want %v", got, test.wantSavings)
			}
			cost := snapshot.score(command)
			if !cost.Valid || cost.Status != scoreFactorApplied {
				t.Fatalf("cost score = %+v, want an applied valid score", cost)
			}
			if math.Abs(cost.SavingsUSDPerHour-test.wantSavings) > 1e-12 || math.Abs(cost.ReplacementUSDPerHour-test.wantReplacement) > 1e-12 {
				t.Errorf("cost score savings/replacement = %v/%v, want %v/%v", cost.SavingsUSDPerHour, cost.ReplacementUSDPerHour, test.wantSavings, test.wantReplacement)
			}
			wantContribution := test.wantSavings / source.Price
			if math.Abs(cost.Contribution-wantContribution) > 1e-12 {
				t.Errorf("normalized cost contribution = %v, want %v", cost.Contribution, wantContribution)
			}
		})
	}
}

func TestScoreBasedPriceContributionIsInvariantToCommonPriceScalingWithReplacement(t *testing.T) {
	const pool = "pool"
	types := map[string]*cloudprovider.InstanceType{
		"source-a": scorePriceInstanceType("source-a", 2, map[string]float64{v1.CapacityTypeOnDemand: 0.20}),
		"source-b": scorePriceInstanceType("source-b", 4, map[string]float64{v1.CapacityTypeOnDemand: 0.40}),
	}
	sourceA := scorePriceStateNode("source-a", pool, v1.CapacityTypeOnDemand, "source-a", 2, true)
	sourceB := scorePriceStateNode("source-b", pool, v1.CapacityTypeOnDemand, "source-b", 4, true)
	candidates := []*Candidate{scorePriceCandidate(sourceA, types), scorePriceCandidate(sourceB, types)}
	replacementType := scorePriceInstanceType("replacement", 4, map[string]float64{v1.CapacityTypeOnDemand: 0.30})
	command := scorePriceReplacementCommand(candidates, replacementType)
	first := newScoreBasedPriceSnapshot(state.StateNodes{sourceA, sourceB}, candidates).score(command)
	if !first.Valid || math.Abs(first.SavingsFraction-0.5) > 1e-12 || math.Abs(first.Contribution-1) > 1e-12 {
		t.Fatalf("initial normalized score = %+v, want fraction 0.5 and contribution 1", first)
	}

	for _, instanceType := range types {
		instanceType.Offerings[0].Price *= 10
	}
	replacementType.Offerings[0].Price *= 10
	for _, candidate := range candidates {
		candidate.Price *= 10
	}
	second := newScoreBasedPriceSnapshot(state.StateNodes{sourceA, sourceB}, candidates).score(command)
	if !second.Valid || math.Abs(second.SavingsFraction-first.SavingsFraction) > 1e-12 || math.Abs(second.Contribution-first.Contribution) > 1e-12 {
		t.Fatalf("score after common price scaling = %+v, want the same normalized score as %+v", second, first)
	}
}

func TestScoreBasedPriceContributionDistinguishesKnownZeroFromUnknownPrices(t *testing.T) {
	const pool = "pool"
	knownTypes := map[string]*cloudprovider.InstanceType{
		"zero":     scorePriceInstanceType("zero", 2, map[string]float64{v1.CapacityTypeReserved: 0}),
		"positive": scorePriceInstanceType("positive", 2, map[string]float64{v1.CapacityTypeOnDemand: 0.20}),
	}
	zeroNode := scorePriceStateNode("zero-source", pool, v1.CapacityTypeReserved, "zero", 2, true)
	positiveNode := scorePriceStateNode("positive-source", pool, v1.CapacityTypeOnDemand, "positive", 2, true)
	zero := scorePriceCandidate(zeroNode, knownTypes)
	positive := scorePriceCandidate(positiveNode, knownTypes)
	knownSnapshot := newScoreBasedPriceSnapshot(state.StateNodes{zeroNode, positiveNode}, []*Candidate{zero, positive})
	knownScore := knownSnapshot.score(Command{Candidates: []*Candidate{zero, positive}})
	if !knownScore.Valid || knownScore.Contribution != 1 {
		t.Fatalf("known zero-price source score = %+v, want zero source contribution and positive-source contribution 1", knownScore)
	}
	if got := knownScore.Groups[scoreBasedPriceGroupName(scoreBasedPriceGroupKey{nodePool: pool, capacityType: v1.CapacityTypeReserved})].NormalizedSourceCost; got != 0 {
		t.Fatalf("known zero-price source normalized cost = %v, want 0", got)
	}

	unknownTypes := map[string]*cloudprovider.InstanceType{"positive": knownTypes["positive"]}
	unknownNode := scorePriceStateNode("unknown-source", pool, v1.CapacityTypeReserved, "missing", 2, true)
	unknown := scorePriceCandidate(unknownNode, unknownTypes)
	unknown.Price = 0 // Candidate construction's legacy unknown-price representation.
	unknownSnapshot := newScoreBasedPriceSnapshot(state.StateNodes{unknownNode, positiveNode}, []*Candidate{unknown, positive})
	if score := unknownSnapshot.score(Command{Candidates: []*Candidate{unknown, positive}}); score.Valid {
		t.Fatalf("unknown source offering must invalidate a mixed positive-price move, got %+v", score)
	}
}

func TestScoreBasedPriceContributionRejectsUnknownFleetBaseline(t *testing.T) {
	const pool = "pool"
	types := map[string]*cloudprovider.InstanceType{
		"source": scorePriceInstanceType("source", 2, map[string]float64{v1.CapacityTypeOnDemand: 0.20}),
	}
	source := scorePriceStateNode("source", pool, v1.CapacityTypeOnDemand, "source", 2, true)
	unpricedPeer := scorePriceStateNode("unpriced-peer", pool, v1.CapacityTypeOnDemand, "unpriced", 2, true)
	candidate := scorePriceCandidate(source, types)
	snapshot := newScoreBasedPriceSnapshot(state.StateNodes{source, unpricedPeer}, []*Candidate{candidate})
	if score := snapshot.score(Command{Candidates: []*Candidate{candidate}}); score.Valid {
		t.Fatalf("positive-price source with an unknown fleet baseline must be rejected, got %+v", score)
	}
}

func scorePriceInstanceType(name string, cpu float64, prices map[string]float64) *cloudprovider.InstanceType {
	offerings := make(cloudprovider.Offerings, 0, len(prices))
	for capacityType, price := range prices {
		offerings = append(offerings, &cloudprovider.Offering{
			Available: true,
			Price:     price,
			Requirements: scheduling.NewLabelRequirements(map[string]string{
				v1.CapacityTypeLabelKey:  capacityType,
				corev1.LabelTopologyZone: "zone-a",
			}),
		})
	}
	return &cloudprovider.InstanceType{
		Name: name,
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse(resource.NewMilliQuantity(int64(cpu*1000), resource.DecimalSI).String()),
		},
		Offerings: offerings,
	}
}

func scorePriceStateNode(name, pool, capacityType, instanceType string, cpu float64, initialized bool) *state.StateNode {
	labels := map[string]string{
		v1.NodePoolLabelKey:            pool,
		corev1.LabelInstanceTypeStable: instanceType,
		v1.CapacityTypeLabelKey:        capacityType,
		corev1.LabelTopologyZone:       "zone-a",
		v1.NodeRegisteredLabelKey:      "true",
	}
	if initialized {
		labels[v1.NodeInitializedLabelKey] = "true"
	}
	return &state.StateNode{
		Node: &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
				corev1.ResourceCPU: *resource.NewMilliQuantity(int64(cpu*1000), resource.DecimalSI),
			}},
		},
		NodeClaim: &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim-" + name, Labels: labels}},
	}
}

func scorePriceCandidate(node *state.StateNode, instanceTypes map[string]*cloudprovider.InstanceType) *Candidate {
	instanceType := instanceTypes[node.Labels()[corev1.LabelInstanceTypeStable]]
	price, known := scoreBasedOfferingPrice(instanceType, node.Labels())
	if !known {
		price = 0
	}
	return &Candidate{
		StateNode:             node,
		instanceType:          instanceType,
		nodePoolInstanceTypes: instanceTypes,
		NodePool:              &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: node.Labels()[v1.NodePoolLabelKey]}},
		Price:                 price,
	}
}

func scorePriceReplacementCommand(candidates []*Candidate, replacementType *cloudprovider.InstanceType) Command {
	replacement := &pscheduling.NodeClaim{}
	replacement.InstanceTypeOptions = cloudprovider.InstanceTypes{replacementType}
	return Command{
		Candidates:   candidates,
		Replacements: []*Replacement{{}},
		Results:      pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{replacement}},
	}
}
