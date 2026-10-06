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
	scoringConfigs := scoreBasedScoringConfigForCandidates(candidates)
	nodeSnapshot := make(state.StateNodes, 0, len(candidates))
	for _, candidate := range candidates {
		nodeSnapshot = append(nodeSnapshot, candidate.StateNode)
	}
	concentrationSnapshot := newScoreBasedInstanceTypeConcentrationSnapshot(nodeSnapshot, scoringConfigs)
	memorySnapshot := scoreMemorySnapshotForTest(poolName, 100, 400, 100, 100, map[string]scoreBasedMemoryShape{
		candidates[0].Name(): {cpu: 2, memory: 16},
		candidates[1].Name(): {cpu: 4, memory: 16},
		candidates[2].Name(): {cpu: 4, memory: 16},
	})
	scorer := newMoveSetScorer(priceSnapshot, scoringConfigs, memorySnapshot, concentrationSnapshot)
	// A search uses the settings captured when its scorer was constructed, even
	// if annotations change before individual move sets are evaluated.
	for _, candidate := range candidates {
		candidate.NodePool.Annotations[v1.ScoreBasedUtilisationWeightAnnotationKey] = "9"
		candidate.NodePool.Annotations[v1.ScoreBasedMemorySurplusAnnotationKey] = "0.8"
		candidate.NodePool.Annotations[v1.ScoreBasedMemoryBalanceWeightAnnotationKey] = "9"
		candidate.NodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationThresholdAnnotationKey] = "0.9"
		candidate.NodePool.Annotations[v1.ScoreBasedInstanceTypeConcentrationWeightAnnotationKey] = "9"
	}
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
		eval := scoreBasedLogDetailEvaluation(test.cmd, test.path, scorer)
		evals = append(evals, eval)
		assertScoreBasedLogDetailReconciliation(t, eval)
		assertScoreBasedCapturedScoringConfig(t, eval, poolName)
	}
	singletonMemorySum := evals[0].Score.Factors.MemoryBalance.Contribution +
		scoreBasedMemoryScoreForCommand(Command{Candidates: []*Candidate{candidates[1]}}, memorySnapshot, scoringConfigs).Contribution +
		scoreBasedMemoryScoreForCommand(Command{Candidates: []*Candidate{candidates[2]}}, memorySnapshot, scoringConfigs).Contribution
	if got, want := evals[2].Score.Factors.MemoryBalance.Contribution, singletonMemorySum; math.Abs(got-want) < 1e-12 {
		t.Fatalf("joint three-node memory correction %v equals singleton sum %v, want a joint correction", got, want)
	}
	if math.Abs(evals[0].Score.Factors.MemoryBalance.Contribution-1) > 1e-12 || math.Abs(evals[2].Score.Factors.MemoryBalance.Contribution-0.6) > 1e-12 {
		t.Fatalf("single/batch memory contributions = %v/%v, want 1/0.6", evals[0].Score.Factors.MemoryBalance.Contribution, evals[2].Score.Factors.MemoryBalance.Contribution)
	}
	if math.Abs(evals[0].Score.Factors.InstanceTypeConcentration.Contribution-2) > 1e-12 || math.Abs(evals[2].Score.Factors.InstanceTypeConcentration.Contribution-6) > 1e-12 {
		t.Fatalf("single/batch concentration contributions = %v/%v, want 2/6", evals[0].Score.Factors.InstanceTypeConcentration.Contribution, evals[2].Score.Factors.InstanceTypeConcentration.Contribution)
	}

	// The emitted summary must describe the values used when scoring completed,
	// even if mutable candidates and their annotations change before logging.
	for _, candidate := range candidates {
		candidate.Price = 99
		candidate.Node.Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("99")
		candidate.Node.Name = "mutated-" + candidate.Node.Name
		candidate.NodeClaim.Name = "mutated-" + candidate.NodeClaim.Name
		candidate.NodePool.Name = "mutated-" + candidate.NodePool.Name
		candidate.Node.Labels[corev1.LabelInstanceTypeStable] = "mutated-instance"
		candidate.Node.Labels[v1.CapacityTypeLabelKey] = "mutated-capacity"
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
			if score["total"] != evals[i].Score.Total {
				t.Errorf("selected score total = %v, want captured total %v", score["total"], evals[i].Score.Total)
			}
			factors := scoreBasedTestLogMap(t, score, "factors")
			assertScoreBasedLogFactor(t, factors, "cost", evals[i].Score.Factors.Cost.Contribution, evals[i].Score.Factors.Cost.Status, evals[i].Score.Factors.Cost.Reason)
			assertScoreBasedLogFactor(t, factors, "utilisation", evals[i].Score.Factors.Utilisation.Contribution, evals[i].Score.Factors.Utilisation.Status, evals[i].Score.Factors.Utilisation.Reason)
			assertScoreBasedLogFactor(t, factors, "memoryBalance", evals[i].Score.Factors.MemoryBalance.Contribution, evals[i].Score.Factors.MemoryBalance.Status, evals[i].Score.Factors.MemoryBalance.Reason)
			assertScoreBasedLogFactor(t, factors, "instanceTypeConcentration", evals[i].Score.Factors.InstanceTypeConcentration.Contribution, evals[i].Score.Factors.InstanceTypeConcentration.Status, evals[i].Score.Factors.InstanceTypeConcentration.Reason)
			concentration := scoreBasedTestLogMap(t, factors, "instanceTypeConcentration")
			concentrationDetails := scoreBasedTestLogMap(t, concentration, "details")
			if concentrationDetails["sources"] == nil {
				t.Errorf("concentration details = %v, want captured per-source details", concentrationDetails)
			}
			if _, found := score["baseScore"]; found {
				t.Errorf("selected score unexpectedly has baseScore: %v", score)
			}
			cost := scoreBasedTestLogMap(t, factors, "cost")
			costDetails := scoreBasedTestLogMap(t, cost, "details")
			if costDetails["sourceUSDPerHour"] != evals[i].Score.Factors.Cost.SourceUSDPerHour || costDetails["replacementUSDPerHour"] != evals[i].Score.Factors.Cost.ReplacementUSDPerHour || costDetails["savingsUSDPerHour"] != evals[i].Score.Factors.Cost.SavingsUSDPerHour {
				t.Errorf("cost details = %v, want captured source/replacement/savings", costDetails)
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
		source := scoreBasedSourceScore{
			NodeName:      candidate.Name(),
			NodeClaimName: candidate.NodeClaim.Name,
			NodePoolName:  candidate.NodePool.Name,
			InstanceType:  candidate.Labels()[corev1.LabelInstanceTypeStable],
			CapacityType:  candidate.Labels()[v1.CapacityTypeLabelKey],
		}
		evals[i] = &moveSetEvaluation{
			Command: Command{Candidates: []*Candidate{candidate}},
			Score: moveSetScore{
				Total: float64(5 - i),
				Factors: moveSetScoreFactors{
					Cost:                      scoreBasedCostScore{Contribution: float64(5 - i), Status: scoreFactorApplied, Enabled: true, Valid: true},
					Utilisation:               scoreBasedUtilisationScore{Status: scoreFactorDisabled},
					MemoryBalance:             scoreBasedMemoryScore{Status: scoreFactorDisabled},
					InstanceTypeConcentration: scoreBasedInstanceTypeConcentrationScore{Status: scoreFactorDisabled},
				},
				Sources: []scoreBasedSourceScore{source},
			},
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
		if score["total"] != 6-wantRanks[i] {
			t.Errorf("alternative score.total = %v, want score for rank %v", score["total"], wantRanks[i])
		}
		factors := scoreBasedTestLogMap(t, score, "factors")
		for _, name := range []string{"cost", "utilisation", "memoryBalance", "instanceTypeConcentration"} {
			factor := scoreBasedTestLogMap(t, factors, name)
			if _, found := factor["contribution"]; !found || factor["status"] == nil {
				t.Errorf("alternative factor %q is missing common fields: %v", name, factor)
			}
			if name != "cost" {
				if _, found := factor["details"]; found {
					t.Errorf("alternative factor %q unexpectedly includes detailed inputs: %v", name, factor)
				}
			}
		}
		costDetails := scoreBasedTestLogMap(t, scoreBasedTestLogMap(t, factors, "cost"), "details")
		if len(costDetails) != 3 {
			t.Errorf("alternative cost details = %v, want only source/replacement/savings hourly prices", costDetails)
		}
		for _, key := range []string{"sourceUSDPerHour", "replacementUSDPerHour", "savingsUSDPerHour"} {
			if _, found := costDetails[key]; !found {
				t.Errorf("alternative cost details are missing %q: %v", key, costDetails)
			}
		}
		for _, omitted := range []string{"groups", "sources", "savingsFraction", "normalizedSourceCost"} {
			if _, found := costDetails[omitted]; found {
				t.Errorf("alternative cost details unexpectedly include %q: %v", omitted, costDetails)
			}
		}
		if _, found := alternative["estimatedSavingsUSDPerHour"]; found {
			t.Errorf("alternative duplicates a factor value outside score: %v", alternative)
		}
		alternativeSources, ok := alternative["sourceNodes"].([]any)
		if !ok {
			t.Fatalf("alternative sourceNodes = %T %v, want captured sources", alternative["sourceNodes"], alternative["sourceNodes"])
		}
		evalIndex := int(wantRanks[i]) - 1
		if len(alternativeSources) != len(evals[evalIndex].Score.Sources) {
			t.Errorf("alternative sourceNodes = %v, want captured sources", alternative["sourceNodes"])
		}
		for j, rawSource := range alternativeSources {
			source := rawSource.(map[string]any)
			wantName := evals[evalIndex].Score.Sources[j].NodeName
			if source["nodeName"] != wantName {
				t.Errorf("alternative sourceNodes[%d].nodeName = %v, want captured %q", j, source["nodeName"], wantName)
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

func scoreBasedLogDetailEvaluation(command Command, path scoreBasedMoveSetSearchPath, scorer *moveSetScorer) *moveSetEvaluation {
	return &moveSetEvaluation{
		Command:    command,
		Score:      scorer.Score(command),
		searchPath: path,
	}
}

func assertScoreBasedLogDetailReconciliation(t *testing.T, eval *moveSetEvaluation) {
	t.Helper()
	factors := eval.Score.Factors
	var costContribution, utilisationContribution float64
	for i, source := range eval.Score.Sources {
		if math.Abs(source.CostContribution-factors.Cost.SavingsFraction*source.NormalizedSourceCost) > 1e-12 {
			t.Errorf("source %d cost contribution %v does not reconcile with normalized cost %v and savings fraction %v", i, source.CostContribution, source.NormalizedSourceCost, factors.Cost.SavingsFraction)
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
	if math.Abs(costContribution-factors.Cost.Contribution) > 1e-12 || math.Abs(utilisationContribution-factors.Utilisation.Contribution) > 1e-12 {
		t.Errorf("per-source cost/utilisation totals = %v/%v, want move totals %v/%v", costContribution, utilisationContribution, factors.Cost.Contribution, factors.Utilisation.Contribution)
	}
	var concentrationContribution float64
	for _, source := range eval.Score.Sources {
		concentrationContribution += source.InstanceTypeConcentration.Contribution
	}
	if math.Abs(concentrationContribution-factors.InstanceTypeConcentration.Contribution) > 1e-12 {
		t.Errorf("per-source concentration contributions = %v, want aggregate %v", concentrationContribution, factors.InstanceTypeConcentration.Contribution)
	}
	wantTotal := factors.Cost.Contribution + factors.Utilisation.Contribution + factors.MemoryBalance.Contribution + factors.InstanceTypeConcentration.Contribution
	if math.Abs(eval.Score.Total-wantTotal) > 1e-12 {
		t.Errorf("total score = %v, want sum of the production score factors %v", eval.Score.Total, wantTotal)
	}
}

func assertScoreBasedCapturedScoringConfig(t *testing.T, eval *moveSetEvaluation, poolName string) {
	t.Helper()
	for _, source := range eval.Score.Sources {
		if source.UtilisationWeight != 2 {
			t.Errorf("source %q utilisation weight = %v, want captured annotation weight 2", source.NodeName, source.UtilisationWeight)
		}
		if source.InstanceTypeConcentration.Threshold != 0.3 || source.InstanceTypeConcentration.Weight != 2 {
			t.Errorf("source %q concentration threshold/weight = %v/%v, want captured annotations 0.3/2", source.NodeName, source.InstanceTypeConcentration.Threshold, source.InstanceTypeConcentration.Weight)
		}
	}
	if poolScore, ok := eval.Score.Factors.MemoryBalance.Pools[poolName]; !ok || poolScore.MemorySurplus != 0 {
		t.Errorf("memory pool score = %v/%v, want captured surplus 0", poolScore, ok)
	}
}

func assertSelectedMoveSourceScores(t *testing.T, move map[string]any, command Command, eval *moveSetEvaluation) {
	t.Helper()
	nodes, ok := move["sourceNodes"].([]any)
	if !ok || len(nodes) != len(command.Candidates) || len(nodes) != len(eval.Score.Sources) {
		t.Fatalf("move.sourceNodes = %T %v, want %d nodes", move["sourceNodes"], move["sourceNodes"], len(command.Candidates))
	}
	assertSelectedMoveSourceIdentities(t, nodes, eval.Score.Sources)
	score := scoreBasedTestLogMap(t, move, "score")
	factors := scoreBasedTestLogMap(t, score, "factors")
	assertSelectedMoveCostSources(t, factors, eval.Score.Sources)
	assertSelectedMoveUtilisationSources(t, factors, eval.Score.Sources)
	assertSelectedMoveConcentrationSources(t, factors, eval.Score.Sources)
}

func assertSelectedMoveSourceIdentities(t *testing.T, nodes []any, sources []scoreBasedSourceScore) {
	t.Helper()
	for i, source := range sources {
		assertLoggedSourceFields(t, "source identity", i, nodes[i].(map[string]any), map[string]any{
			"nodeName":      source.NodeName,
			"nodeClaimName": source.NodeClaimName,
			"nodePoolName":  source.NodePoolName,
			"instanceType":  source.InstanceType,
			"capacityType":  source.CapacityType,
		})
	}
}

func assertSelectedMoveCostSources(t *testing.T, factors map[string]any, sources []scoreBasedSourceScore) {
	t.Helper()
	want := make([]map[string]any, 0, len(sources))
	for _, source := range sources {
		want = append(want, map[string]any{
			"nodeName":              source.NodeName,
			"priceUSDPerHour":       source.PriceUSDPerHour,
			"allocatableCPU":        source.AllocatableCPU,
			"baselineUSDPerCPUHour": source.BaselineUSDPerCPUHour,
			"normalizedSourceCost":  source.NormalizedSourceCost,
			"contribution":          source.CostContribution,
		})
	}
	assertSelectedMoveFactorSources(t, factors, "cost", want)
}

func assertSelectedMoveUtilisationSources(t *testing.T, factors map[string]any, sources []scoreBasedSourceScore) {
	t.Helper()
	want := make([]map[string]any, 0, len(sources))
	for _, source := range sources {
		want = append(want, map[string]any{
			"nodeName":       source.NodeName,
			"allocatableCPU": source.AllocatableCPU,
			"requestedCPU":   source.RequestedCPU,
			"slackFraction":  source.CPUSlackFraction,
			"weight":         source.UtilisationWeight,
			"contribution":   source.UtilisationContribution,
		})
	}
	assertSelectedMoveFactorSources(t, factors, "utilisation", want)
}

func assertSelectedMoveConcentrationSources(t *testing.T, factors map[string]any, sources []scoreBasedSourceScore) {
	t.Helper()
	want := make([]map[string]any, 0, len(sources))
	for _, source := range sources {
		concentration := source.InstanceTypeConcentration
		want = append(want, map[string]any{
			"nodeName":              source.NodeName,
			"nodePool":              concentration.NodePool,
			"instanceType":          concentration.InstanceType,
			"poolNodeCount":         float64(concentration.PoolNodeCount),
			"instanceTypeNodeCount": float64(concentration.InstanceTypeNodeCount),
			"share":                 concentration.Share,
			"threshold":             concentration.Threshold,
			"weight":                concentration.Weight,
			"qualifies":             concentration.Qualifies,
			"contribution":          concentration.Contribution,
		})
	}
	assertSelectedMoveFactorSources(t, factors, "instanceTypeConcentration", want)
}

func assertSelectedMoveFactorSources(t *testing.T, factors map[string]any, factorName string, want []map[string]any) {
	t.Helper()
	factor := scoreBasedTestLogMap(t, factors, factorName)
	details := scoreBasedTestLogMap(t, factor, "details")
	actual, ok := details["sources"].([]any)
	if !ok || len(actual) != len(want) {
		t.Fatalf("score.factors.%s.details.sources = %T %v, want %d captured sources", factorName, details["sources"], details["sources"], len(want))
	}
	for i, rawSource := range actual {
		assertLoggedSourceFields(t, factorName+" source", i, rawSource.(map[string]any), want[i])
	}
}

func assertLoggedSourceFields(t *testing.T, detail string, sourceIndex int, actual, want map[string]any) {
	t.Helper()
	for key, expected := range want {
		if actual[key] != expected {
			t.Errorf("%s[%d].%s = %v, want captured value %v", detail, sourceIndex, key, actual[key], expected)
		}
	}
}

func assertScoreBasedLogFactor(t *testing.T, factors map[string]any, name string, contribution float64, status scoreFactorStatus, reason string) {
	t.Helper()
	logged := scoreBasedTestLogMap(t, factors, name)
	if logged["contribution"] != contribution || logged["status"] != string(status) {
		t.Errorf("score.factors.%s contribution/status = %v/%v, want %v/%s", name, logged["contribution"], logged["status"], contribution, status)
	}
	loggedReason, _ := logged["reason"].(string)
	if loggedReason != reason {
		t.Errorf("score.factors.%s reason = %v, want %q", name, logged["reason"], reason)
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
