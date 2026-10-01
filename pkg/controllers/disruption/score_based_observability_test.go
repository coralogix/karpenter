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
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

//nolint:gocyclo // Keep the serialized schema assertions together across all paths, per-node fields, and validation fallback.
func TestLogScoreBasedCompactionMoveSelectedJSON(t *testing.T) {
	first := scoreBasedLogTestCandidate("node-a", "claim-a", "pool-a", "m5.large", v1.CapacityTypeOnDemand, 2)
	second := scoreBasedLogTestCandidate("node-b", "claim-b", "pool-b", "c6i.large", v1.CapacityTypeSpot, 3)
	third := scoreBasedLogTestCandidate("node-c", "claim-c", "pool-c", "m5.xlarge", v1.CapacityTypeOnDemand, 1.5)
	fourth := scoreBasedLogTestCandidate("node-d", "claim-d", "pool-d", "c6a.large", v1.CapacityTypeSpot, 0.8)
	replacementType := &cloudprovider.InstanceType{
		Offerings: cloudprovider.Offerings{&cloudprovider.Offering{Price: 1.25}},
	}
	cases := []struct {
		name            string
		path            scoreBasedMoveSetSearchPath
		command         Command
		score           float64
		singleScores    []float64
		selectedIndex   int
		wantMethod      string
		wantDecision    string
		wantSourceCost  float64
		wantSavings     float64
		wantReplacement float64
	}{
		{
			name:    "single node search",
			path:    scoreBasedSearchPathSingleNode,
			command: Command{Candidates: []*Candidate{first}},
			score:   7.5, singleScores: []float64{7.5}, wantMethod: "joint_move_set_score",
			wantDecision: "delete", wantSourceCost: 2, wantSavings: 2,
		},
		{
			name:    "batch fallback preserves original singleton scores",
			path:    scoreBasedSearchPathBatch,
			command: Command{Candidates: []*Candidate{first, second, third, fourth}},
			score:   18, singleScores: []float64{6, 5, 4, 3}, selectedIndex: 1,
			wantMethod: "additive_single_node_scores", wantDecision: "delete", wantSourceCost: 7.3, wantSavings: 7.3,
		},
		{
			name: "two to one pair search omits singleton score",
			path: scoreBasedSearchPathPair2To1,
			command: Command{
				Candidates:   []*Candidate{first, second},
				Replacements: []*Replacement{{Name: "replacement"}},
				Results: pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{{
					NodeClaimTemplate: pscheduling.NodeClaimTemplate{InstanceTypeOptions: cloudprovider.InstanceTypes{replacementType}},
				}}},
			},
			score: 8, wantMethod: "joint_move_set_score", wantDecision: "replace",
			wantSourceCost: 5, wantSavings: 3.75, wantReplacement: 1.25,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eval := &moveSetEvaluation{
				Command:                    tc.command,
				Score:                      tc.score,
				searchPath:                 tc.path,
				sourceNodeSingleMoveScores: tc.singleScores,
			}
			evals := []*moveSetEvaluation{eval}
			if tc.selectedIndex > 0 {
				evals = append([]*moveSetEvaluation{{Command: Command{Candidates: []*Candidate{first}}, Score: 100}}, evals...)
			}

			var output bytes.Buffer
			encoder := zapcore.EncoderConfig{
				MessageKey:  "message",
				LevelKey:    "level",
				EncodeLevel: zapcore.LowercaseLevelEncoder,
			}
			logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(encoder), zapcore.AddSync(&output), zapcore.InfoLevel))
			defer func() { _ = logger.Sync() }()
			ctx := log.IntoContext(context.Background(), zapr.NewLogger(logger))
			logScoreBasedCompactionMoveSelected(ctx, evals, tc.selectedIndex, 44, 7)

			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatalf("decoding emitted JSON log %q: %v", output.String(), err)
			}
			if record["message"] != "score-based consolidation move selected" {
				t.Fatalf("message = %v, want selection summary", record["message"])
			}
			move := nestedLogMap(t, record, "move")
			if move["searchPath"] != string(tc.path) || move["simulatedDecision"] != tc.wantDecision {
				t.Errorf("move path/decision = %v/%v, want %s/%s", move["searchPath"], move["simulatedDecision"], tc.path, tc.wantDecision)
			}
			if move["sourceNodeCount"] != float64(len(tc.command.Candidates)) || move["replacementNodeCount"] != float64(len(tc.command.Replacements)) {
				t.Errorf("move source/replacement counts = %v/%v, want %d/%d", move["sourceNodeCount"], move["replacementNodeCount"], len(tc.command.Candidates), len(tc.command.Replacements))
			}
			score := nestedLogMap(t, move, "score")
			if score["rankingScore"] != tc.score || score["method"] != tc.wantMethod {
				t.Errorf("move score = %v, want value %v and method %q", score, tc.score, tc.wantMethod)
			}
			cost := nestedLogMap(t, move, "cost")
			if cost["sourceUSDPerHour"] != tc.wantSourceCost || cost["estimatedSavingsUSDPerHour"] != tc.wantSavings || cost["estimatedReplacementUSDPerHour"] != tc.wantReplacement {
				t.Errorf("move cost = %v, want source/savings/replacement %v/%v/%v", cost, tc.wantSourceCost, tc.wantSavings, tc.wantReplacement)
			}
			nodes, ok := move["sourceNodes"].([]any)
			if !ok || len(nodes) != len(tc.command.Candidates) {
				t.Fatalf("sourceNodes = %T %v, want %d serialized nodes", move["sourceNodes"], move["sourceNodes"], len(tc.command.Candidates))
			}
			for i, candidate := range tc.command.Candidates {
				node := nodes[i].(map[string]any)
				if node["nodeName"] != candidate.Name() || node["nodeClaimName"] != candidate.NodeClaim.Name || node["nodePoolName"] != candidate.NodePool.Name || node["instanceType"] != candidate.Labels()[corev1.LabelInstanceTypeStable] || node["capacityType"] != candidate.Labels()[v1.CapacityTypeLabelKey] || node["costUSDPerHour"] != candidate.Price {
					t.Errorf("sourceNodes[%d] = %v, missing expected identity/type/cost for %s", i, node, candidate.Name())
				}
				nodeScore := nestedLogMap(t, node, "score")
				if nodeScore["searchPriorityScore"] != candidate.Price {
					t.Errorf("sourceNodes[%d].score.searchPriorityScore = %v, want %v", i, nodeScore["searchPriorityScore"], candidate.Price)
				}
				if i < len(tc.singleScores) {
					if nodeScore["singleNodeMoveScore"] != tc.singleScores[i] {
						t.Errorf("sourceNodes[%d].score.singleNodeMoveScore = %v, want %v", i, nodeScore["singleNodeMoveScore"], tc.singleScores[i])
					}
				} else if _, exists := nodeScore["singleNodeMoveScore"]; exists {
					t.Errorf("sourceNodes[%d] unexpectedly has singleNodeMoveScore: %v", i, nodeScore)
				}
			}
			search := nestedLogMap(t, record, "search")
			if search["eligibleCandidateCount"] != float64(7) || search["evaluatedMoveSetCount"] != float64(44) || search["retainedMoveSetCount"] != float64(len(evals)) {
				t.Errorf("search summary = %v, want eligible/evaluated/retained 7/44/%d", search, len(evals))
			}
			selection := nestedLogMap(t, record, "selection")
			wantReason := "highest_ranked_validated_move"
			if tc.selectedIndex > 0 {
				wantReason = "fallback_after_validation"
			}
			if selection["rank"] != float64(tc.selectedIndex+1) || selection["rejectedHigherRankedMoveSetCount"] != float64(tc.selectedIndex) || selection["reason"] != wantReason {
				t.Errorf("selection = %v, want rank/rejected/reason %d/%d/%s", selection, tc.selectedIndex+1, tc.selectedIndex, wantReason)
			}
		})
	}
}

func TestGrowScoreBasedBatchesRetainsOriginalSingletonScoresInCommandOrder(t *testing.T) {
	first := scoreBasedLogTestCandidate("node-a", "claim-a", "pool-a", "m5.large", v1.CapacityTypeOnDemand, 2)
	second := scoreBasedLogTestCandidate("node-b", "claim-b", "pool-a", "c6i.large", v1.CapacityTypeSpot, 3)
	singletons := []*moveSetEvaluation{
		{Command: Command{Candidates: []*Candidate{first}}, Score: 4},
		{Command: Command{Candidates: []*Candidate{second}}, Score: 3},
	}
	compute := func(_ context.Context, candidates ...*Candidate) (Command, error) {
		if len(candidates) == 1 {
			return Command{Candidates: candidates}, nil
		}
		return Command{Candidates: []*Candidate{second, first}}, nil
	}
	batches, _, timedOut, err := growScoreBasedBatches(context.Background(), singletons, map[string]int{"pool-a": 2}, nil, compute, &moveSetSearchStats{})
	if err != nil || timedOut {
		t.Fatalf("growScoreBasedBatches() error/timedOut = %v/%v, want nil/false", err, timedOut)
	}
	if len(batches) != 1 {
		t.Fatalf("retained batches = %d, want exactly one", len(batches))
	}
	batch := batches[0]
	if batch.searchPath != scoreBasedSearchPathBatch {
		t.Errorf("batch searchPath = %q, want %q", batch.searchPath, scoreBasedSearchPathBatch)
	}
	if batch.Score != 7 {
		t.Errorf("batch ranking score = %v, want sum of original singleton scores 7", batch.Score)
	}
	if len(batch.sourceNodeSingleMoveScores) != 2 || batch.sourceNodeSingleMoveScores[0] != 3 || batch.sourceNodeSingleMoveScores[1] != 4 {
		t.Errorf("batch source-node singleton scores = %v, want [3 4] in returned command order", batch.sourceNodeSingleMoveScores)
	}
}

func scoreBasedLogTestCandidate(nodeName, claimName, poolName, instanceType, capacityType string, price float64) *Candidate {
	node := state.NewNode()
	node.Node = &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: nodeName,
		Labels: map[string]string{
			corev1.LabelInstanceTypeStable: instanceType,
			v1.CapacityTypeLabelKey:        capacityType,
		},
	}}
	node.NodeClaim = &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: claimName}}
	return &Candidate{
		StateNode: node,
		NodePool:  &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: poolName}},
		Price:     price,
	}
}

func nestedLogMap(t *testing.T, value map[string]any, key string) map[string]any {
	t.Helper()
	child, ok := value[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %T %v, want serialized object", key, value[key], value[key])
	}
	return child
}
