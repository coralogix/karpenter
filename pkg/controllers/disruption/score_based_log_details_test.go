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
	"math"
	"testing"

	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

//nolint:gocyclo // Keep the three real scoring paths and captured JSON values in one focused test.
func TestSelectedMoveJSONUsesCapturedJointScoreDetails(t *testing.T) {
	const poolName = "score-pool"
	candidates, priceSnapshot := scoreBasedLogDetailCandidates(poolName)
	for _, candidate := range candidates {
		candidate.NodePool.Annotations = map[string]string{
			v1.ScoreBasedUtilisationWeightAnnotationKey:                  "2",
			v1.ScoreBasedMemorySurplusAnnotationKey:                      "0",
			v1.ScoreBasedMemoryBalanceWeightAnnotationKey:                "1",
			v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey: "0.3",
			v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey:    "2",
		}
	}
	configs := map[string]scoreBasedMemoryConfig{poolName: {surplus: 0, weight: 1}}
	concentrationConfigs := scoreBasedInstanceTypeConcentrationConfigs(candidates)
	nodeSnapshot := make(state.StateNodes, 0, len(candidates))
	for _, candidate := range candidates {
		nodeSnapshot = append(nodeSnapshot, candidate.StateNode)
	}
	concentrationSnapshot := newScoreBasedInstanceTypeConcentrationSnapshot(nodeSnapshot, concentrationConfigs)
	memorySnapshot := scoreMemorySnapshotForTest(poolName, 100, 400, 100, 100, map[string]scoreBasedMemoryShape{
		candidates[0].Name(): {cpu: 2, memory: 16},
		candidates[1].Name(): {cpu: 4, memory: 16},
		candidates[2].Name(): {cpu: 4, memory: 16},
	})
	commands := []struct {
		name string
		path scoreBasedMoveSetSearchPath
		cmd  Command
	}{
		{name: "single removal", path: scoreBasedSearchPathSingleNode, cmd: Command{Candidates: []*Candidate{candidates[0]}}},
		{name: "two to one replacement", path: scoreBasedSearchPathPair2To1, cmd: scoreBasedLogDetailReplacementCommand(t, poolName, candidates[:2])},
		{name: "three node batch", path: scoreBasedSearchPathBatch, cmd: Command{Candidates: candidates}},
	}
	evals := make([]*moveSetEvaluation, 0, len(commands))
	for _, test := range commands {
		eval := scoreBasedLogDetailEvaluation(test.cmd, test.path, priceSnapshot, memorySnapshot, configs, concentrationSnapshot)
		evals = append(evals, eval)
		assertScoreBasedLogDetailReconciliation(t, eval)
	}
	singletonMemorySum := evals[0].MemoryBalance.Correction +
		scoreBasedMemoryScoreForCommand(Command{Candidates: []*Candidate{candidates[1]}}, memorySnapshot, configs).Correction +
		scoreBasedMemoryScoreForCommand(Command{Candidates: []*Candidate{candidates[2]}}, memorySnapshot, configs).Correction
	if got, want := evals[2].MemoryBalance.Correction, singletonMemorySum; math.Abs(got-want) < 1e-12 {
		t.Fatalf("joint three-node memory correction %v equals singleton sum %v, want a joint correction", got, want)
	}
	if math.Abs(evals[0].MemoryBalance.Correction-1) > 1e-12 || math.Abs(evals[2].MemoryBalance.Correction-0.6) > 1e-12 {
		t.Fatalf("single/batch memory corrections = %v/%v, want 1/0.6", evals[0].MemoryBalance.Correction, evals[2].MemoryBalance.Correction)
	}
	if math.Abs(evals[0].InstanceTypeConcentration.Correction-2) > 1e-12 || math.Abs(evals[2].InstanceTypeConcentration.Correction-6) > 1e-12 {
		t.Fatalf("single/batch concentration corrections = %v/%v, want 2/6", evals[0].InstanceTypeConcentration.Correction, evals[2].InstanceTypeConcentration.Correction)
	}

	// The emitted summary must describe the values used when scoring completed,
	// even if mutable candidates and their annotations change before logging.
	for _, candidate := range candidates {
		candidate.Price = 99
		candidate.Node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("99")
		candidate.NodePool.Annotations[v1.ScoreBasedUtilisationWeightAnnotationKey] = "9"
		candidate.NodePool.Annotations[v1.ScoreBasedMemoryBalanceWeightAnnotationKey] = "9"
		candidate.NodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey] = "0.9"
		candidate.NodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey] = "9"
	}
	for i, test := range commands {
		t.Run(test.name, func(t *testing.T) {
			record := captureScoreBasedSelectedMove(t, []*moveSetEvaluation{evals[i]}, 0, false)
			move := scoreBasedTestLogMap(t, record, "move")
			if move["searchPath"] != string(test.path) {
				t.Errorf("searchPath = %v, want %q", move["searchPath"], test.path)
			}
			score := scoreBasedTestLogMap(t, move, "score")
			if score["rankingScore"] != evals[i].Score || score["baseScore"] != evals[i].BaseScore || score["costContribution"] != evals[i].Cost.Contribution || score["utilisationContribution"] != evals[i].UtilisationContribution || score["memoryBalanceCorrection"] != evals[i].MemoryBalance.Correction || score["instanceTypeConcentrationCorrection"] != evals[i].InstanceTypeConcentration.Correction {
				t.Errorf("selected score summary = %v, want captured eval details", score)
			}
			concentration := scoreBasedTestLogMap(t, score, "instanceTypeConcentration")
			if concentration["correction"] != evals[i].InstanceTypeConcentration.Correction || concentration["sourceNodes"] != nil {
				t.Errorf("aggregate concentration score = %v, want captured aggregate without duplicate source details", concentration)
			}
			if _, found := score["selectedMinusAlternative"]; found {
				t.Errorf("selected score unexpectedly has selectedMinusAlternative: %v", score)
			}
			cost := scoreBasedTestLogMap(t, move, "cost")
			if cost["sourceUSDPerHour"] != evals[i].Cost.SourceUSDPerHour || cost["estimatedReplacementUSDPerHour"] != evals[i].Cost.ReplacementUSDPerHour || cost["estimatedSavingsUSDPerHour"] != evals[i].Cost.SavingsUSDPerHour {
				t.Errorf("cost summary = %v, want captured cost details", cost)
			}
			assertSelectedMoveSourceScores(t, move, test.cmd, evals[i])
		})
	}
}

//nolint:gocyclo // Keep validation fallback, alternative capping, and compact schema assertions together.
func TestSelectedMoveAlternativesAreCappedAndShowValidationOutcome(t *testing.T) {
	evals := make([]*moveSetEvaluation, 5)
	for i := range evals {
		name := "candidate-" + string(rune('a'+i))
		if i == 0 {
			name = "high-score"
		}
		candidate := scoreBasedLogTestCandidate(name, name, "pool", "instance", v1.CapacityTypeOnDemand, float64(5-i))
		evals[i] = &moveSetEvaluation{
			Command:    Command{Candidates: []*Candidate{candidate}},
			Score:      float64(5 - i),
			BaseScore:  float64(5 - i),
			searchPath: scoreBasedSearchPathSingleNode,
		}
	}
	fakeRecorder := record.NewFakeRecorder(10)
	cmd, selectedIdx, err := selectFirstStillValidCommand(context.Background(), rejectFirstCommandValidator{}, events.NewRecorder(fakeRecorder), evals)
	if err != nil {
		t.Fatalf("selectFirstStillValidCommand() error = %v", err)
	}
	if selectedIdx != 1 || len(cmd.Candidates) != 1 || cmd.Candidates[0].Name() != evals[1].Command.Candidates[0].Name() {
		t.Fatalf("selected index/command = %d/%v, want fallback at index 1", selectedIdx, cmd.Candidates)
	}

	record := captureScoreBasedSelectedMove(t, evals, selectedIdx, true)
	selection := scoreBasedTestLogMap(t, record, "selection")
	if selection["rank"] != float64(2) || selection["reason"] != "fallback_after_validation" || selection["validationStatus"] != "validated" {
		t.Errorf("selection details = %v, want validated fallback at rank 2", selection)
	}
	if selection["validationReason"] != "" {
		t.Errorf("selected validationReason = %v, want empty", selection["validationReason"])
	}
	search := scoreBasedTestLogMap(t, record, "search")
	if search["timedOut"] != true {
		t.Errorf("search.timedOut = %v, want true", search["timedOut"])
	}
	alternatives, ok := record["alternatives"].([]any)
	if !ok || len(alternatives) != 3 {
		t.Fatalf("alternatives = %T %v, want the three-item cap", record["alternatives"], record["alternatives"])
	}
	wantRanks := []float64{1, 3, 4}
	wantStatuses := []string{"failed", "not_attempted", "not_attempted"}
	for i, rawAlternative := range alternatives {
		alternative := rawAlternative.(map[string]any)
		if alternative["rank"] != wantRanks[i] {
			t.Errorf("alternatives[%d].rank = %v, want %v", i, alternative["rank"], wantRanks[i])
		}
		validation := scoreBasedTestLogMap(t, alternative, "validation")
		if validation["status"] != wantStatuses[i] {
			t.Errorf("alternatives[%d].validation.status = %v, want %q", i, validation["status"], wantStatuses[i])
		}
		if i == 0 && validation["reason"] != "scheduling" {
			t.Errorf("failed alternative validation.reason = %v, want scheduling", validation["reason"])
		}
		score := scoreBasedTestLogMap(t, alternative, "score")
		for _, key := range []string{"rankingScore", "costContribution", "utilisationContribution", "memoryBalanceCorrection", "instanceTypeConcentrationCorrection"} {
			if _, found := score[key]; !found {
				t.Errorf("alternative score is missing %q: %v", key, score)
			}
		}
		for _, removed := range []string{"total", "selectedMinusAlternative"} {
			if _, found := score[removed]; found {
				t.Errorf("alternative score unexpectedly includes %q: %v", removed, score)
			}
		}
		if _, found := alternative["cost"]; found {
			t.Errorf("alternative duplicates cost summary outside score: %v", alternative)
		}
		if memory, found := alternative["memoryBalance"]; found {
			compactMemory, ok := memory.(map[string]any)
			if !ok || len(compactMemory) != 2 || compactMemory["status"] == nil {
				t.Errorf("alternative memory summary = %v, want only compact status/reason", memory)
			}
		}
	}
}

func scoreBasedLogDetailCandidates(poolName string) ([]*Candidate, *scoreBasedPriceSnapshot) {
	specs := []struct {
		name, instanceType string
		cpu, price         float64
	}{
		{name: "source-a", instanceType: "type-a", cpu: 2, price: 0.2},
		{name: "source-b", instanceType: "type-b", cpu: 4, price: 0.4},
		{name: "source-c", instanceType: "type-c", cpu: 4, price: 0.4},
	}
	instanceTypes := make(map[string]*cloudprovider.InstanceType, len(specs))
	nodes := make(state.StateNodes, 0, len(specs))
	for _, spec := range specs {
		instanceTypes[spec.instanceType] = scorePriceInstanceType(spec.instanceType, spec.cpu, map[string]float64{v1.CapacityTypeOnDemand: spec.price})
		nodes = append(nodes, scorePriceStateNode(spec.name, poolName, v1.CapacityTypeOnDemand, spec.instanceType, spec.cpu, true))
	}
	candidates := make([]*Candidate, 0, len(nodes))
	for _, node := range nodes {
		candidates = append(candidates, scorePriceCandidate(node, instanceTypes))
	}
	return candidates, newScoreBasedPriceSnapshot(nodes, candidates)
}

func scoreBasedLogDetailReplacementCommand(t *testing.T, poolName string, candidates []*Candidate) Command {
	t.Helper()
	replacement := scoreMemoryReplacementForTest(poolName, scoreBasedMemoryShape{cpu: 1, memory: 4})
	replacement.InstanceTypeOptions[0].Offerings[0].Price = 0.3
	return Command{
		Candidates:   candidates,
		Replacements: []*Replacement{{NodeClaim: replacement}},
		Results:      pscheduling.Results{NewNodeClaims: []*pscheduling.NodeClaim{replacement}},
	}
}

func scoreBasedLogDetailEvaluation(command Command, path scoreBasedMoveSetSearchPath, prices *scoreBasedPriceSnapshot, memory *scoreBasedMemorySnapshot, configs map[string]scoreBasedMemoryConfig, concentrationSnapshot *scoreBasedInstanceTypeConcentrationSnapshot) *moveSetEvaluation {
	baseScore, cost, utilisation, sourceScores := scoreMoveSetPriorityDetails(command, prices)
	memoryBalance := scoreBasedMemoryScoreForCommand(command, memory, configs)
	concentration := concentrationSnapshot.score(command)
	for i := range sourceScores {
		if i < len(concentration.SourceNodes) {
			sourceScores[i].InstanceTypeConcentration = concentration.SourceNodes[i]
		}
	}
	return &moveSetEvaluation{
		Command:                   command,
		Score:                     baseScore + memoryBalance.Correction + concentration.Correction,
		BaseScore:                 baseScore,
		Cost:                      cost,
		MemoryBalance:             memoryBalance,
		InstanceTypeConcentration: concentration,
		SourceNodeScores:          sourceScores,
		UtilisationContribution:   utilisation,
		searchPath:                path,
	}
}

func assertScoreBasedLogDetailReconciliation(t *testing.T, eval *moveSetEvaluation) {
	t.Helper()
	var costContribution, utilisationContribution float64
	for i, source := range eval.SourceNodeScores {
		if math.Abs(source.CostContribution-eval.Cost.SavingsFraction*source.NormalizedSourceCost) > 1e-12 {
			t.Errorf("source %d cost contribution %v does not reconcile with normalized cost %v and savings fraction %v", i, source.CostContribution, source.NormalizedSourceCost, eval.Cost.SavingsFraction)
		}
		if math.Abs(source.CPUSlackFraction-(source.AllocatableCPU-source.RequestedCPU)/source.AllocatableCPU) > 1e-12 {
			t.Errorf("source %d CPU slack fraction %v does not reconcile with allocatable/requested CPU %v/%v", i, source.CPUSlackFraction, source.AllocatableCPU, source.RequestedCPU)
		}
		if math.Abs(source.UtilisationContribution-source.CPUSlackFraction*source.UtilisationWeight) > 1e-12 {
			t.Errorf("source %d utilisation contribution %v does not reconcile with slack %v and weight %v", i, source.UtilisationContribution, source.CPUSlackFraction, source.UtilisationWeight)
		}
		costContribution += source.CostContribution
		utilisationContribution += source.UtilisationContribution
	}
	if math.Abs(costContribution-eval.Cost.Contribution) > 1e-12 || math.Abs(utilisationContribution-eval.UtilisationContribution) > 1e-12 {
		t.Errorf("per-source cost/utilisation totals = %v/%v, want move totals %v/%v", costContribution, utilisationContribution, eval.Cost.Contribution, eval.UtilisationContribution)
	}
	var concentrationContribution float64
	for _, source := range eval.SourceNodeScores {
		concentrationContribution += source.InstanceTypeConcentration.Contribution
	}
	if math.Abs(concentrationContribution-eval.InstanceTypeConcentration.Correction) > 1e-12 {
		t.Errorf("per-source concentration contributions = %v, want aggregate %v", concentrationContribution, eval.InstanceTypeConcentration.Correction)
	}
	if math.Abs(eval.BaseScore-(eval.Cost.Contribution+eval.UtilisationContribution)) > 1e-12 || math.Abs(eval.Score-(eval.BaseScore+eval.MemoryBalance.Correction+eval.InstanceTypeConcentration.Correction)) > 1e-12 {
		t.Errorf("base/total scores = %v/%v, want cost + utilisation %v and base + memory + concentration %v", eval.BaseScore, eval.Score, eval.Cost.Contribution+eval.UtilisationContribution, eval.BaseScore+eval.MemoryBalance.Correction+eval.InstanceTypeConcentration.Correction)
	}
}

func assertSelectedMoveSourceScores(t *testing.T, move map[string]any, command Command, eval *moveSetEvaluation) {
	t.Helper()
	nodes, ok := move["sourceNodes"].([]any)
	if !ok || len(nodes) != len(command.Candidates) {
		t.Fatalf("move.sourceNodes = %T %v, want %d nodes", move["sourceNodes"], move["sourceNodes"], len(command.Candidates))
	}
	for i, source := range eval.SourceNodeScores {
		node := nodes[i].(map[string]any)
		if node["nodeName"] != command.Candidates[i].Name() || node["costUSDPerHour"] != source.PriceUSDPerHour {
			t.Errorf("sourceNodes[%d] identity/price = %v/%v, want %q/%v", i, node["nodeName"], node["costUSDPerHour"], command.Candidates[i].Name(), source.PriceUSDPerHour)
		}
		score := scoreBasedTestLogMap(t, node, "score")
		checks := map[string]float64{
			"allocatableCPU":          source.AllocatableCPU,
			"requestedCPU":            source.RequestedCPU,
			"baselineUSDPerCPUHour":   source.BaselineUSDPerCPUHour,
			"normalizedSourceCost":    source.NormalizedSourceCost,
			"costContribution":        source.CostContribution,
			"cpuSlackFraction":        source.CPUSlackFraction,
			"utilisationWeight":       source.UtilisationWeight,
			"utilisationContribution": source.UtilisationContribution,
		}
		for key, want := range checks {
			if got := score[key]; got != want {
				t.Errorf("sourceNodes[%d].score.%s = %v, want captured value %v", i, key, got, want)
			}
		}
		concentration := scoreBasedTestLogMap(t, score, "instanceTypeConcentration")
		concentrationChecks := map[string]any{
			"nodePool":              source.InstanceTypeConcentration.NodePool,
			"instanceType":          source.InstanceTypeConcentration.InstanceType,
			"poolNodeCount":         float64(source.InstanceTypeConcentration.PoolNodeCount),
			"instanceTypeNodeCount": float64(source.InstanceTypeConcentration.InstanceTypeNodeCount),
			"share":                 source.InstanceTypeConcentration.Share,
			"threshold":             source.InstanceTypeConcentration.Threshold,
			"weight":                source.InstanceTypeConcentration.Weight,
			"qualifies":             source.InstanceTypeConcentration.Qualifies,
			"contribution":          source.InstanceTypeConcentration.Contribution,
		}
		for key, want := range concentrationChecks {
			if got := concentration[key]; got != want {
				t.Errorf("sourceNodes[%d].score.instanceTypeConcentration.%s = %v, want captured value %v", i, key, got, want)
			}
		}
	}
}

func captureScoreBasedSelectedMove(t *testing.T, evals []*moveSetEvaluation, selectedIndex int, timedOut bool) map[string]any {
	t.Helper()
	var output bytes.Buffer
	encoder := zapcore.EncoderConfig{MessageKey: "message", LevelKey: "level", EncodeLevel: zapcore.LowercaseLevelEncoder}
	logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(encoder), zapcore.AddSync(&output), zapcore.InfoLevel))
	t.Cleanup(func() { _ = logger.Sync() })
	ctx := log.IntoContext(context.Background(), zapr.NewLogger(logger))
	logScoreBasedCompactionMoveSelected(ctx, evals, selectedIndex, 44, 7, timedOut)
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decoding selected-move log %q: %v", output.String(), err)
	}
	if record["message"] != "score-based consolidation move selected" {
		t.Fatalf("log message = %v, want selected-move summary", record["message"])
	}
	return record
}

func scoreBasedTestLogMap(t *testing.T, value map[string]any, key string) map[string]any {
	t.Helper()
	child, ok := value[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %T %v, want serialized object", key, value[key], value[key])
	}
	return child
}
