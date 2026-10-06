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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestScoreBasedUtilisationWeight(t *testing.T) {
	if got := scoreBasedUtilisationWeight(nil); got != defaultScoreBasedUtilisationWeight {
		t.Fatalf("nil NodePool weight = %v, want %v", got, defaultScoreBasedUtilisationWeight)
	}
	np := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		v1.ScoreBasedUtilisationWeightAnnotationKey: "2.5",
	}}}
	if got := scoreBasedUtilisationWeight(np); got != 2.5 {
		t.Fatalf("custom weight = %v, want 2.5", got)
	}
	np.Annotations[v1.ScoreBasedUtilisationWeightAnnotationKey] = "bad"
	if got := scoreBasedUtilisationWeight(np); got != defaultScoreBasedUtilisationWeight {
		t.Fatalf("invalid weight = %v, want default %v", got, defaultScoreBasedUtilisationWeight)
	}
}

func TestScoreBasedScoringConfigSignatureTracksRawAnnotationPresenceAndValues(t *testing.T) {
	candidate := &Candidate{NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool", Annotations: map[string]string{}}}}
	keys := []string{
		v1.ScoreBasedUtilisationWeightAnnotationKey,
		v1.ScoreBasedMemorySurplusAnnotationKey,
		v1.ScoreBasedMemoryBalanceWeightAnnotationKey,
		v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey,
		v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey,
	}

	empty, changed := scoreBasedScoringConfigChanged(nil, []*Candidate{candidate})
	if changed || len(empty) != 0 {
		t.Fatalf("initial absent configuration = changed %v, signature %v; want unchanged and empty", changed, empty)
	}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			candidate.NodePool.Annotations[key] = ""
			presentEmpty, changed := scoreBasedScoringConfigChanged(empty, []*Candidate{candidate})
			if !changed {
				t.Fatal("adding a present empty annotation should change the signature")
			}
			if same, unchangedChanged := scoreBasedScoringConfigChanged(presentEmpty, []*Candidate{candidate}); unchangedChanged || len(same) != 1 {
				t.Fatalf("unchanged present annotation = changed %v, signature %v; want unchanged", unchangedChanged, same)
			}

			candidate.NodePool.Annotations[key] = "1"
			presentValue, changed := scoreBasedScoringConfigChanged(presentEmpty, []*Candidate{candidate})
			if !changed {
				t.Fatal("changing the raw annotation value should change the signature")
			}

			delete(candidate.NodePool.Annotations, key)
			absentAgain, changed := scoreBasedScoringConfigChanged(presentValue, []*Candidate{candidate})
			if !changed || len(absentAgain) != 0 {
				t.Fatalf("removing the annotation = changed %v, signature %v; want changed and empty", changed, absentAgain)
			}
		})
		delete(candidate.NodePool.Annotations, key)
	}
}

func TestMoveSetScorerCombinesNormalizedCostAndDimensionlessUtilisation(t *testing.T) {
	offering := &cloudprovider.Offering{
		Price: 0.12,
		Requirements: scheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
			corev1.LabelTopologyZone: "us-east-1a",
		}),
	}
	instanceType := &cloudprovider.InstanceType{
		Name: "m5.large",
		Capacity: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("4"),
		},
		Offerings: cloudprovider.Offerings{offering},
	}
	node := state.NewNode()
	node.Node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				corev1.LabelInstanceTypeStable: instanceType.Name,
				v1.CapacityTypeLabelKey:        offering.Requirements.Get(v1.CapacityTypeLabelKey).Any(),
				corev1.LabelTopologyZone:       offering.Requirements.Get(corev1.LabelTopologyZone).Any(),
			},
		},
	}
	candidate := &Candidate{
		StateNode:    node,
		instanceType: instanceType,
		NodePool: &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool", Annotations: map[string]string{
			v1.ScoreBasedUtilisationWeightAnnotationKey:                  "0.5",
			v1.ScoreBasedMemorySurplusAnnotationKey:                      "0.2",
			v1.ScoreBasedMemoryBalanceWeightAnnotationKey:                "0",
			v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey: "0.5",
			v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey:    "2",
		}}},
		Price: 0.12,
	}
	cmd := Command{Candidates: []*Candidate{candidate}}
	key := scoreBasedPriceGroupKey{nodePool: "pool", capacityType: v1.CapacityTypeOnDemand}
	prices := &scoreBasedPriceSnapshot{
		groups: map[scoreBasedPriceGroupKey]*scoreBasedPriceGroup{
			key: {totalPrice: 0.12, totalCPU: 4, nodeCount: 1, known: true, baseline: 0.03},
		},
		nodes: map[string]scoreBasedNodePrice{
			"node-1": {group: key, cpu: 4, price: 0.12, known: true},
		},
	}
	configs := scoreBasedScoringConfigForCandidates([]*Candidate{candidate})
	concentration := newScoreBasedInstanceTypeConcentrationSnapshot(nil, configs)
	scorer := newMoveSetScorer(prices, configs, nil, concentration)
	first := scorer.Score(cmd)
	if first.Total != 1.5 {
		t.Fatalf("move-set score = %v, want normalized cost 1 plus utilization 0.5", first.Total)
	}
	candidate.NodePool.Annotations[v1.ScoreBasedUtilisationWeightAnnotationKey] = "10"
	candidate.NodePool.Annotations[v1.ScoreBasedMemorySurplusAnnotationKey] = "9"
	candidate.NodePool.Annotations[v1.ScoreBasedMemoryBalanceWeightAnnotationKey] = "5"
	candidate.NodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey] = "0.1"
	candidate.NodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey] = "100"
	second := scorer.Score(cmd)
	if second.Total != first.Total || second.Sources[0].UtilisationWeight != 0.5 || second.Factors.MemoryBalance.Pools["pool"].MemorySurplus != 0.2 || second.Factors.InstanceTypeConcentration.SourceNodes[0].Weight != 2 {
		t.Fatalf("score/config changed after annotations mutated: first=%+v second=%+v", first, second)
	}
}
