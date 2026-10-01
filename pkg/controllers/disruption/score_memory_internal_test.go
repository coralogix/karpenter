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
	"math"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	kubescheduling "sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestScoreBasedMemoryInternalCorrectionIsIndependentOfPoolSize(t *testing.T) {
	const poolName = "balanced-pool"
	candidate := scoreMemoryCandidateForTest(poolName, "source")
	var first float64
	for i, nodeCount := range []float64{5, 1000} {
		snapshot := scoreMemorySnapshotForTest(poolName, nodeCount*10, nodeCount*40, nodeCount*5, nodeCount*10,
			map[string]scoreBasedMemoryShape{"source": {cpu: 1, memory: 8}})
		score := snapshot.score(Command{Candidates: []*Candidate{candidate}})
		if !score.Enabled {
			t.Fatal("expected memory balance scoring to be enabled")
		}
		if i == 0 {
			first = score.Correction
			continue
		}
		if score.Correction != first {
			t.Fatalf("correction changed with pool size: 5 nodes=%v, 1000 nodes=%v", first, score.Correction)
		}
	}
}

func TestScoreBasedMemoryJointCorrectionScalesWithSourceNodeCount(t *testing.T) {
	const poolName = "memory-pool"
	first := scoreMemoryCandidateForTest(poolName, "first-source")
	second := scoreMemoryCandidateForTest(poolName, "second-source")
	snapshot := scoreMemorySnapshotForTest(poolName, 10, 40, 1, 2,
		map[string]scoreBasedMemoryShape{
			"first-source":  {cpu: 1, memory: 8},
			"second-source": {cpu: 1, memory: 8},
		})

	single := snapshot.score(Command{Candidates: []*Candidate{first}})
	joint := snapshot.score(Command{Candidates: []*Candidate{first, second}})
	if single.Correction != 1 || single.SourceNodeCount != 1 {
		t.Fatalf("single-node correction = %+v, want correction 1 with one source", single)
	}
	if joint.Correction != 2 || joint.SourceNodeCount != 2 {
		t.Fatalf("joint correction = %+v, want correction 2 with two sources", joint)
	}
	poolScore := joint.Pools[poolName]
	if poolScore.NormalizedDelta != 1 || poolScore.SourceNodeCount != 2 || poolScore.Contribution != 2 {
		t.Fatalf("joint pool correction details = %+v, want unscaled delta 1 and doubled contribution 2", poolScore)
	}
}

func TestScoreBasedMemoryInternalPressureReversesShapePreference(t *testing.T) {
	const poolName = "memory-pool"
	highMemory := scoreMemoryCandidateForTest(poolName, "high-memory")
	lowMemory := scoreMemoryCandidateForTest(poolName, "low-memory")
	shapes := map[string]scoreBasedMemoryShape{
		"high-memory": {cpu: 1, memory: 8},
		"low-memory":  {cpu: 1, memory: 1},
	}
	tests := []struct {
		name        string
		targetRatio float64
		want        int
	}{
		{name: "memory surplus favors removing high-memory shape", targetRatio: 2, want: 1},
		{name: "memory shortage reverses the preference", targetRatio: 8, want: -1},
		{name: "balanced target is neutral", targetRatio: 4, want: 0},
		{name: "deadband is neutral", targetRatio: 4 / 1.04, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := scoreMemorySnapshotForTest(poolName, 10, 40, 1, test.targetRatio, shapes)
			high := snapshot.score(Command{Candidates: []*Candidate{highMemory}}).Correction
			low := snapshot.score(Command{Candidates: []*Candidate{lowMemory}}).Correction
			switch test.want {
			case 1:
				if high <= low {
					t.Fatalf("high-memory correction %v should exceed low-memory correction %v", high, low)
				}
			case -1:
				if high >= low {
					t.Fatalf("high-memory correction %v should be below low-memory correction %v", high, low)
				}
			default:
				if high != 0 || low != 0 {
					t.Fatalf("balanced or deadband corrections = (%v, %v), want both zero", high, low)
				}
			}
		})
	}
}

func TestScoreBasedMemoryInternalReplacementAndStandbyDeltasAreBatched(t *testing.T) {
	const poolName = "source-pool"
	const nodeName = "source"
	sourceShape := scoreBasedMemoryShape{cpu: 2, memory: 16}
	shapes := map[string]scoreBasedMemoryShape{nodeName: sourceShape}
	snapshot := scoreMemorySnapshotForTest(poolName, 10, 40, 5, 10, shapes)
	candidate := scoreMemoryCandidateForTest(poolName, nodeName)
	replacement := scoreMemoryReplacementForTest(poolName, sourceShape)
	command := Command{Candidates: []*Candidate{candidate}, Replacements: []*Replacement{{NodeClaim: replacement}}}

	withoutStandby := snapshot.score(command)
	if withoutStandby.Correction != 0 || withoutStandby.RemovedCPU != sourceShape.cpu {
		t.Fatalf("identical source/replacement score = correction %v, removed CPU %v; want 0 and %v", withoutStandby.Correction, withoutStandby.RemovedCPU, sourceShape.cpu)
	}

	standbyNode := scoreMemoryStandbyNodeForTest(poolName, "standby")
	shapes["standby"] = sourceShape
	command.Results.ExistingNodes = []*scheduling.ExistingNode{standbyNode}
	withStandby := snapshot.score(command)
	if withStandby.Correction != -1 || withStandby.RemovedCPU != sourceShape.cpu {
		t.Fatalf("batched source/replacement/standby score = correction %v, removed CPU %v; want -1 and %v", withStandby.Correction, withStandby.RemovedCPU, sourceShape.cpu)
	}
}

func TestScoreBasedMemoryInternalReplacementUsesConservativeOptions(t *testing.T) {
	const poolName = "source-pool"
	source := scoreMemoryCandidateForTest(poolName, "source")
	snapshot := scoreMemorySnapshotForTest(poolName, 10, 40, 5, 10,
		map[string]scoreBasedMemoryShape{"source": {cpu: 2, memory: 8}})
	replacement := scoreMemoryReplacementForTest(poolName,
		scoreBasedMemoryShape{cpu: 2, memory: 16},
		scoreBasedMemoryShape{cpu: 2, memory: 4},
	)
	score := snapshot.score(Command{Candidates: []*Candidate{source}, Replacements: []*Replacement{{NodeClaim: replacement}}})
	if score.Correction != -1 {
		t.Fatalf("correction using the most adverse replacement option = %v, want -1", score.Correction)
	}
}

func TestScoreBasedMemoryBatchScoringUsesJointScoreWithOneCorrection(t *testing.T) {
	first := scoreBasedBatchTestCandidate("memory-first", "memory-pool")
	second := scoreBasedBatchTestCandidate("memory-second", "memory-pool")
	first.Price, second.Price = 1, 2
	for _, candidate := range []*Candidate{first, second} {
		candidate.Node.Status.Allocatable = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")}
	}
	priceKey := scoreBasedPriceGroupKey{nodePool: "memory-pool", capacityType: v1.CapacityTypeOnDemand}
	prices := &scoreBasedPriceSnapshot{
		groups: map[scoreBasedPriceGroupKey]*scoreBasedPriceGroup{
			priceKey: {totalPrice: 3, totalCPU: 4, nodeCount: 2, known: true, baseline: 0.75},
		},
		nodes: map[string]scoreBasedNodePrice{
			first.Name():  {group: priceKey, cpu: 2, price: 1, known: true},
			second.Name(): {group: priceKey, cpu: 2, price: 2, known: true},
		},
	}
	memorySnapshot := scoreMemorySnapshotForTest("memory-pool", 10, 40, 1, 2,
		map[string]scoreBasedMemoryShape{
			first.Name():  {cpu: 1, memory: 12}, // Single-node delta clamps to 1.
			second.Name(): {cpu: 1, memory: 4},
		})
	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		return Command{Candidates: candidates}, nil
	}
	baseScorer := func(command Command) moveSetScoring {
		costScore := prices.score(command)
		baseScore := costScore.Contribution + moveSetUtilisationScore(command)
		return moveSetScoring{Score: baseScore, BaseScore: baseScore, Cost: costScore}
	}
	withMemoryScorer := func(command Command) moveSetScoring {
		scoring := baseScorer(command)
		scoring.MemoryBalance = memorySnapshot.score(command)
		scoring.Score += scoring.MemoryBalance.Correction
		return scoring
	}
	makeSingletons := func(scorer func(Command) moveSetScoring) []*moveSetEvaluation {
		out := make([]*moveSetEvaluation, 0, 2)
		for _, candidate := range []*Candidate{first, second} {
			command := Command{Candidates: []*Candidate{candidate}}
			scoring := scorer(command)
			out = append(out, &moveSetEvaluation{
				Command: command, Score: scoring.Score, BaseScore: scoring.BaseScore,
				Cost: scoring.Cost, MemoryBalance: scoring.MemoryBalance,
			})
		}
		return out
	}
	disabledSingletons := makeSingletons(baseScorer)
	enabledSingletons := makeSingletons(withMemoryScorer)
	constituentBaseScore := disabledSingletons[0].BaseScore + disabledSingletons[1].BaseScore
	constituentMemoryCorrection := enabledSingletons[0].MemoryBalance.Correction + enabledSingletons[1].MemoryBalance.Correction

	disabledBatches := scoreMemoryGrowBatchesForTest(t, disabledSingletons, compute, baseScorer)
	disabled := scoreMemoryTwoCandidateBatchForTest(t, disabledBatches)
	if math.Abs(disabled.BaseScore-constituentBaseScore) > 1e-12 || math.Abs(disabled.Score-constituentBaseScore) > 1e-12 {
		t.Fatalf("memory-disabled batch base/score = %v/%v, want additive singleton base %v", disabled.BaseScore, disabled.Score, constituentBaseScore)
	}

	enabledBatches := scoreMemoryGrowBatchesForTest(t, enabledSingletons, compute, withMemoryScorer)
	enabled := scoreMemoryTwoCandidateBatchForTest(t, enabledBatches)
	if math.Abs(enabled.BaseScore-disabled.BaseScore) > 1e-12 || math.Abs(enabled.BaseScore-constituentBaseScore) > 1e-12 {
		t.Fatalf("enabled batch base score = %v, want the same additive base %v", enabled.BaseScore, constituentBaseScore)
	}
	if enabled.MemoryBalance.Correction != 2 || enabled.MemoryBalance.SourceNodeCount != 2 {
		t.Fatalf("enabled batch memory correction = %+v, want one joint correction 2 for two sources", enabled.MemoryBalance)
	}
	if enabled.Score != constituentBaseScore+enabled.MemoryBalance.Correction {
		t.Fatalf("enabled batch score = %v, want additive base %v plus joint correction %v", enabled.Score, constituentBaseScore, enabled.MemoryBalance.Correction)
	}
	if enabled.MemoryBalance.Correction == constituentMemoryCorrection {
		t.Fatalf("joint correction %v unexpectedly equals summed singleton corrections %v", enabled.MemoryBalance.Correction, constituentMemoryCorrection)
	}
}

func TestScoreBasedMemoryInternalAnnotationParsing(t *testing.T) {
	if got := scoreBasedMemoryConfigFor(nil); got != (scoreBasedMemoryConfig{surplus: defaultScoreBasedMemorySurplus, weight: defaultScoreBasedMemoryBalanceWeight}) {
		t.Fatalf("default config = %+v", got)
	}
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
		v1.ScoreBasedMemorySurplusAnnotationKey:       "NaN",
		v1.ScoreBasedMemoryBalanceWeightAnnotationKey: "+Inf",
	}}}
	if got := scoreBasedMemoryConfigFor(nodePool); got != (scoreBasedMemoryConfig{surplus: defaultScoreBasedMemorySurplus, weight: defaultScoreBasedMemoryBalanceWeight}) {
		t.Fatalf("non-finite annotations should use defaults, got %+v", got)
	}
	nodePool.Annotations[v1.ScoreBasedMemorySurplusAnnotationKey] = "-0.1"
	nodePool.Annotations[v1.ScoreBasedMemoryBalanceWeightAnnotationKey] = "-1"
	if got := scoreBasedMemoryConfigFor(nodePool); got != (scoreBasedMemoryConfig{surplus: defaultScoreBasedMemorySurplus, weight: defaultScoreBasedMemoryBalanceWeight}) {
		t.Fatalf("negative annotations should use defaults, got %+v", got)
	}
	nodePool.Annotations[v1.ScoreBasedMemorySurplusAnnotationKey] = "0"
	nodePool.Annotations[v1.ScoreBasedMemoryBalanceWeightAnnotationKey] = "0"
	if got := scoreBasedMemoryConfigFor(nodePool); got != (scoreBasedMemoryConfig{}) {
		t.Fatalf("explicit zero annotations should be honored, got %+v", got)
	}
}

func scoreMemoryGrowBatchesForTest(t *testing.T, singletons []*moveSetEvaluation, compute consolidationComputer, scorers ...func(Command) moveSetScoring) []*moveSetEvaluation {
	t.Helper()
	batches, _, timedOut, err := growScoreBasedBatches(context.Background(), singletons, map[string]int{"memory-pool": 2}, nil, compute, &moveSetSearchStats{}, scorers...)
	if err != nil {
		t.Fatalf("growScoreBasedBatches() error = %v", err)
	}
	if timedOut {
		t.Fatal("growScoreBasedBatches() timed out")
	}
	return batches
}

func scoreMemoryTwoCandidateBatchForTest(t *testing.T, batches []*moveSetEvaluation) *moveSetEvaluation {
	t.Helper()
	for _, batch := range batches {
		if len(batch.Command.Candidates) == 2 {
			return batch
		}
	}
	t.Fatalf("no feasible two-candidate batch in %v", batchSizes(batches))
	return nil
}

func scoreMemorySnapshotForTest(poolName string, capacityCPU, capacityMemory, requestCPU, requestMemory float64, shapes map[string]scoreBasedMemoryShape) *scoreBasedMemorySnapshot {
	capacityRatio := capacityMemory / capacityCPU
	targetRatio := requestMemory / requestCPU
	return &scoreBasedMemorySnapshot{
		configs: map[string]scoreBasedMemoryConfig{poolName: {weight: 1}},
		pools: map[string]*scoreBasedMemoryPool{poolName: {
			capacityCPU:    capacityCPU,
			capacityMemory: capacityMemory,
			requestCPU:     requestCPU,
			requestMemory:  requestMemory,
			capacityRatio:  capacityRatio,
			targetRatio:    targetRatio,
			pressure:       scoreBasedMemoryPressure(capacityRatio, targetRatio),
			known:          true,
			valid:          true,
			shapes:         shapes,
		}},
	}
}

func scoreMemoryCandidateForTest(poolName, name string) *Candidate {
	node := state.NewNode()
	node.Node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	return &Candidate{
		StateNode: node,
		NodePool:  &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: poolName}},
	}
}

func scoreMemoryReplacementForTest(poolName string, shapes ...scoreBasedMemoryShape) *scheduling.NodeClaim {
	instanceTypes := make(cloudprovider.InstanceTypes, 0, len(shapes))
	daemonGroups := make([]scheduling.DaemonOverheadGroup, 0, len(shapes))
	for i, shape := range shapes {
		instanceType := &cloudprovider.InstanceType{
			Name: fmt.Sprintf("shape-%d", i),
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%g", shape.cpu)),
				corev1.ResourceMemory: resource.MustParse(fmt.Sprintf("%g", shape.memory)),
			},
			Overhead: &cloudprovider.InstanceTypeOverhead{},
			Offerings: cloudprovider.Offerings{{
				Available: true,
				Requirements: kubescheduling.NewLabelRequirements(map[string]string{
					v1.CapacityTypeLabelKey:  v1.CapacityTypeOnDemand,
					corev1.LabelTopologyZone: "zone-a",
				}),
			}},
		}
		instanceTypes = append(instanceTypes, instanceType)
		daemonGroups = append(daemonGroups, scheduling.DaemonOverheadGroup{
			InstanceTypes:  []*cloudprovider.InstanceType{instanceType},
			DaemonOverhead: corev1.ResourceList{},
			HostPortUsage:  kubescheduling.NewHostPortUsage(),
		})
	}
	template := &scheduling.NodeClaimTemplate{
		NodeClaim:    v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: poolName}}},
		Requirements: kubescheduling.NewRequirements(),
	}
	return scheduling.NewNodeClaim(template, nil, daemonGroups, instanceTypes, nil, scheduling.ReservedOfferingModeFallback)
}

func scoreMemoryStandbyNodeForTest(poolName, name string) *scheduling.ExistingNode {
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.NodePoolLabelKey: poolName}}}
	standby.SetNodeClaimStandby(nodeClaim, true)
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{
		v1.NodePoolLabelKey:       poolName,
		v1.NodeRegisteredLabelKey: "true",
	}}}
	return &scheduling.ExistingNode{
		StateNode: &state.StateNode{Node: node, NodeClaim: nodeClaim},
		Pods:      []*corev1.Pod{{}},
	}
}
