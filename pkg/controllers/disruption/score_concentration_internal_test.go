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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestScoreBasedInstanceTypeConcentrationSnapshotAndFrozenSourceScore(t *testing.T) {
	nodePool, nodes, command, snapshot := scoreBasedConcentrationSnapshotFixture()
	assertConcentrationInventory(t, snapshot)
	firstScore := snapshot.score(command)
	assertConcentrationScoreAtThreshold(t, firstScore)

	// The current search keeps its captured threshold and weight if live metadata changes later.
	nodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey] = "0.49"
	assertConcentrationSnapshotFrozen(t, snapshot.score(command))
	newConfigs := map[string]scoreBasedInstanceTypeConcentrationConfig{nodePool.Name: scoreBasedInstanceTypeConcentrationConfigFor(nodePool)}
	updated := newScoreBasedInstanceTypeConcentrationSnapshot(nodes, newConfigs).score(command)
	assertConcentrationScoreBelowThreshold(t, updated)
}

func scoreBasedConcentrationSnapshotFixture() (*v1.NodePool, state.StateNodes, Command, *scoreBasedInstanceTypeConcentrationSnapshot) {
	const poolName = "concentrated-pool"
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: poolName, Annotations: map[string]string{
		v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey: "0.5",
		v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey:    "2",
	}}}

	typeA1 := scorePriceStateNode("a-active", poolName, v1.CapacityTypeOnDemand, "type-a", 2, true)
	typeAStandby := scorePriceStateNode("a-standby", poolName, v1.CapacityTypeSpot, "type-a", 2, true)
	standby.SetNodeClaimStandby(typeAStandby.NodeClaim, true)
	typeAActivating := scorePriceStateNode("a-activating", poolName, v1.CapacityTypeOnDemand, "type-a", 2, true)
	standby.SetNodeClaimActivating(typeAActivating.NodeClaim, true)
	typeB1 := scorePriceStateNode("b-one", poolName, v1.CapacityTypeOnDemand, "type-b", 2, true)
	typeB2 := scorePriceStateNode("b-two", poolName, v1.CapacityTypeSpot, "type-b", 2, true)
	unknownType := scorePriceStateNode("unknown-type", poolName, v1.CapacityTypeOnDemand, "type-c", 2, true)
	delete(unknownType.Node.Labels, corev1.LabelInstanceTypeStable)
	delete(unknownType.NodeClaim.Labels, corev1.LabelInstanceTypeStable)
	otherPoolNode := scorePriceStateNode("other-pool-node", "other-pool", v1.CapacityTypeOnDemand, "type-a", 2, true)
	deletingByClaim := scorePriceStateNode("deleting-claim", poolName, v1.CapacityTypeOnDemand, "type-a", 2, true)
	deletingByClaim.NodeClaim.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	deletingByNode := scorePriceStateNode("deleting-node", poolName, v1.CapacityTypeOnDemand, "type-a", 2, true)
	deletingByNode.Node.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	uninitialized := scorePriceStateNode("uninitialized", poolName, v1.CapacityTypeOnDemand, "type-a", 2, false)
	unmanaged := state.NewNode()
	unmanaged.Node = typeA1.Node.DeepCopy()

	nodes := state.StateNodes{typeA1, typeAStandby, typeAActivating, typeB1, typeB2, unknownType, otherPoolNode, deletingByClaim, deletingByNode, uninitialized, unmanaged}
	configs := map[string]scoreBasedInstanceTypeConcentrationConfig{
		poolName:     scoreBasedInstanceTypeConcentrationConfigFor(nodePool),
		"other-pool": {threshold: 0.5, weight: 1},
	}
	snapshot := newScoreBasedInstanceTypeConcentrationSnapshot(nodes, configs)
	candidateA1 := scorePriceCandidate(typeA1, nil)
	candidateA1.NodePool = nodePool
	candidateA2 := scorePriceCandidate(typeAActivating, nil)
	candidateA2.NodePool = nodePool
	candidateB := scorePriceCandidate(typeB1, nil)
	candidateB.NodePool = nodePool
	candidateUnknown := scorePriceCandidate(unknownType, nil)
	candidateUnknown.NodePool = nodePool
	command := Command{Candidates: []*Candidate{candidateA1, candidateA2, candidateB, candidateUnknown}}
	return nodePool, nodes, command, snapshot
}

func assertConcentrationInventory(t *testing.T, snapshot *scoreBasedInstanceTypeConcentrationSnapshot) {
	t.Helper()
	const poolName = "concentrated-pool"
	pool := snapshot.pools[poolName]
	if pool.nodeCount != 6 || pool.instanceCounts["type-a"] != 3 || pool.instanceCounts["type-b"] != 2 {
		t.Fatalf("pool type counts = total %d, type-a %d, type-b %d, want 6, 3, 2", pool.nodeCount, pool.instanceCounts["type-a"], pool.instanceCounts["type-b"])
	}
	if otherPool := snapshot.pools["other-pool"]; otherPool.nodeCount != 1 || pool.nodeCount == otherPool.nodeCount {
		t.Fatalf("concentration inventory was not isolated by pool: pool=%d other-pool=%d", pool.nodeCount, otherPool.nodeCount)
	}
}

func assertConcentrationScoreAtThreshold(t *testing.T, score scoreBasedInstanceTypeConcentrationScore) {
	t.Helper()
	if score.Correction != 0 || score.SourceNodes[0].Share != 0.5 || score.SourceNodes[0].Qualifies {
		t.Fatalf("a type exactly at the threshold received a bonus: %+v", score)
	}
	if score.SourceNodes[3].Contribution != 0 || score.SourceNodes[3].Share != 0 {
		t.Fatalf("a source without an instance-type label received a bonus: %+v", score.SourceNodes[3])
	}
}

func assertConcentrationSnapshotFrozen(t *testing.T, score scoreBasedInstanceTypeConcentrationScore) {
	t.Helper()
	if score.Correction != 0 {
		t.Fatalf("an existing snapshot changed after annotation mutation: correction = %v, want 0", score.Correction)
	}
}

func assertConcentrationScoreBelowThreshold(t *testing.T, score scoreBasedInstanceTypeConcentrationScore) {
	t.Helper()
	if score.Correction != 4 || !score.SourceNodes[0].Qualifies || score.SourceNodes[2].Qualifies {
		t.Fatalf("below-threshold additive source score = %+v, want two type-a bonuses totaling 4", score)
	}
	if score.SourceNodes[3].Contribution != 0 {
		t.Fatalf("a source without an instance-type label received a bonus: %+v", score.SourceNodes[3])
	}
}

func TestScoreBasedInstanceTypeConcentrationAnnotationValidationAndCacheInvalidation(t *testing.T) {
	nodePool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool", Annotations: map[string]string{
		v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey: "1",
		v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey:    "NaN",
	}}}
	config := scoreBasedInstanceTypeConcentrationConfigFor(nodePool)
	if config.threshold != defaultScoreBasedInstanceTypeConcentrationThreshold || config.weight != defaultScoreBasedInstanceTypeConcentrationWeight {
		t.Fatalf("invalid concentration annotations resolved to %+v, want default threshold/weight", config)
	}

	clk := clocktesting.NewFakeClock(time.Now())
	kubeClient := fake.NewClientBuilder().WithScheme(scheme.Scheme).Build()
	cluster := state.NewCluster(clk, kubeClient, nil)
	method := NewScoreBasedConsolidation(MakeConsolidation(clk, cluster, kubeClient, nil, nil, events.NewRecorder(&record.FakeRecorder{}), nil, nil))
	candidate := scoreBasedTestCandidate(true)
	method.markConsolidated()
	assertCacheInvalidates := func(change func()) {
		t.Helper()
		method.markConsolidated()
		change()
		if _, err := method.ComputeCommands(context.Background(), map[string]int{candidate.NodePool.Name: 0}, candidate); err != nil {
			t.Fatalf("ComputeCommands() after concentration setting change = %v", err)
		}
		if method.IsConsolidated() {
			t.Fatal("changing concentration annotations should invalidate the consolidated cache")
		}
	}
	if _, err := method.ComputeCommands(context.Background(), nil, candidate); err != nil {
		t.Fatalf("ComputeCommands() with no concentration annotations = %v", err)
	}
	if !method.IsConsolidated() {
		t.Fatal("absent concentration annotations should preserve the consolidated cache")
	}
	assertCacheInvalidates(func() {
		candidate.NodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey] = "1"
	})
	assertCacheInvalidates(func() {
		candidate.NodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey] = "0.4"
	})
	assertCacheInvalidates(func() {
		delete(candidate.NodePool.Annotations, v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey)
		delete(candidate.NodePool.Annotations, v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey)
	})
}
