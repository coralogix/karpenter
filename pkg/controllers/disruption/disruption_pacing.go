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
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

type disruptionPacingConfig struct {
	rate             float64
	maxNodesPerBatch int // 0 means unlimited
	configured       bool
	blocked          bool
	error            string
}

type disruptionPacingPolicy struct {
	scope  string
	config disruptionPacingConfig
}

// DisruptionPacingSuccessfulStart records a command after StartCommand succeeds.
type DisruptionPacingSuccessfulStart struct {
	Command   *Command
	StartTime time.Time
}

// DisruptionPacing applies a per-NodePool rate and batch cap to non-empty disruption
// candidates. It remains in-memory and is reset when the controller restarts.
type DisruptionPacing struct {
	clock clock.Clock

	mu                sync.Mutex
	nextEligible      map[string]time.Time
	policies          map[string]disruptionPacingPolicy
	policyInitialized bool
}

func NewDisruptionPacing(clk clock.Clock) *DisruptionPacing {
	return &DisruptionPacing{
		clock:        clk,
		nextEligible: map[string]time.Time{},
		policies:     map[string]disruptionPacingPolicy{},
	}
}

// Refresh validates every managed NodePool, including pools with no current
// candidates. It returns true when pacing policy has changed.
func (p *DisruptionPacing) Refresh(nodePools []*v1.NodePool, recorder events.Recorder) bool {
	policies, scopeConfigs := resolveDisruptionPacingPolicies(nodePools)

	p.mu.Lock()
	changed := p.policyInitialized && !sameDisruptionPacingPolicies(p.policies, policies)
	p.migrateCooldownsLocked(policies, scopeConfigs)
	p.policies = policies
	p.policyInitialized = true
	p.mu.Unlock()

	return changed
}

func (p *DisruptionPacing) migrateCooldownsLocked(policies map[string]disruptionPacingPolicy, scopeConfigs map[string]disruptionPacingConfig) {
	oldDeadlines := p.nextEligible
	oldPolicies := p.policies

	migrated := make(map[string]time.Time, len(scopeConfigs))
	for scope := range scopeConfigs {
		if deadline, ok := oldDeadlines[scope]; ok {
			migrated[scope] = deadline
		}
	}
	for poolName, policy := range policies {
		oldPolicy, ok := oldPolicies[poolName]
		if !ok || oldPolicy.scope == policy.scope {
			continue
		}
		if deadline, ok := oldDeadlines[oldPolicy.scope]; ok {
			migrated[policy.scope] = laterPacingDeadline(migrated[policy.scope], deadline)
		}
	}
	p.nextEligible = migrated
}

func laterPacingDeadline(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func sameDisruptionPacingPolicies(left, right map[string]disruptionPacingPolicy) bool {
	if len(left) != len(right) {
		return false
	}
	for name, a := range left {
		b, ok := right[name]
		if !ok || a.scope != b.scope || a.config.rate != b.config.rate ||
			a.config.maxNodesPerBatch != b.config.maxNodesPerBatch || a.config.configured != b.config.configured ||
			a.config.blocked != b.config.blocked || a.config.error != b.config.error {
			return false
		}
	}
	return true
}

func resolveDisruptionPacingPolicies(nodePools []*v1.NodePool) (map[string]disruptionPacingPolicy, map[string]disruptionPacingConfig) {
	policies := make(map[string]disruptionPacingPolicy, len(nodePools))
	configs := map[string]disruptionPacingConfig{}
	for _, np := range nodePools {
		if np == nil {
			continue
		}
		config := disruptionPacingConfigFor(np)
		scope := poolPacingScope(np.Name)
		policies[np.Name] = disruptionPacingPolicy{scope: scope, config: config}
		configs[scope] = config
	}
	return policies, configs
}

func poolPacingScope(poolName string) string {
	return "pool/" + poolName
}

func disruptionPacingConfigFor(np *v1.NodePool) disruptionPacingConfig {
	if np == nil || np.Annotations == nil {
		return disruptionPacingConfig{}
	}
	config := disruptionPacingConfig{}
	if raw, present := np.Annotations[v1.DisruptionPacingPerMinuteAnnotationKey]; present {
		if rate, err := parsePacingRate(raw); err != nil {
			config.error = fmt.Sprintf("invalid per-minute value %q", raw)
		} else {
			config.rate = rate
			config.configured = true
		}
	}
	if raw, present := np.Annotations[v1.DisruptionPacingPerBatchAnnotationKey]; present {
		if cap, err := parsePacingCap(raw); err != nil {
			if config.error == "" {
				config.error = fmt.Sprintf("invalid per-batch value %q", raw)
			}
		} else {
			config.maxNodesPerBatch = cap
		}
	}
	return config
}

func parsePacingRate(raw string) (float64, error) {
	rate, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 {
		return 0, fmt.Errorf("rate must be finite and positive")
	}
	return rate, nil
}

func parsePacingCap(raw string) (int, error) {
	cap, err := strconv.Atoi(raw)
	if err != nil || cap <= 0 {
		return 0, fmt.Errorf("batch cap must be a positive integer")
	}
	return cap, nil
}

// Scope returns the effective pacing scope for batch accounting during planning.
func (p *DisruptionPacing) Scope(np *v1.NodePool) string {
	if np == nil {
		return ""
	}
	return poolPacingScope(np.Name)
}

// CandidateAllowed is advisory planning. Whole-command admission is checked
// immediately before StartCommand.
func (p *DisruptionPacing) CandidateAllowed(np *v1.NodePool, selectedCount int) bool {
	if p == nil || np == nil {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	policy := p.policyForLocked(np)
	config := policy.config
	if config.blocked {
		return false
	}
	if !config.configured {
		return true
	}
	if config.maxNodesPerBatch > 0 && selectedCount >= config.maxNodesPerBatch {
		return false
	}
	return !p.clock.Now().Before(p.nextEligible[policy.scope])
}

func (p *DisruptionPacing) policyForLocked(np *v1.NodePool) disruptionPacingPolicy {
	if policy, ok := p.policies[np.Name]; ok {
		return policy
	}
	if np == nil {
		return disruptionPacingPolicy{}
	}
	return disruptionPacingPolicy{scope: poolPacingScope(np.Name), config: disruptionPacingConfigFor(np)}
}

// CommandAllowed checks whether a whole command could be admitted right now.
// It does not reserve cooldown state or mutate deadlines.
func (p *DisruptionPacing) CommandAllowed(cmd Command) bool {
	if p == nil {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.planCommandAdmissionLocked(cmd, map[string]int{}, p.clock.Now())
	return ok
}

// AdmitCommands admits whole commands against current cooldowns and aggregate
// per-scope batch caps. It aggregates commands from one ComputeCommands pass,
// which lets multiple static-drift commands share one NodePool batch cap. The
// singleton controller charges the pass before the next one and refreshes policy
// only between passes.
func (p *DisruptionPacing) AdmitCommands(commands []Command) []int {
	if p == nil {
		accepted := make([]int, len(commands))
		for i := range commands {
			accepted[i] = i
		}
		return accepted
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.clock.Now()
	passCounts := map[string]int{}
	accepted := make([]int, 0, len(commands))
	for index, cmd := range commands {
		admission, ok := p.planCommandAdmissionLocked(cmd, passCounts, now)
		if !ok {
			continue
		}
		accepted = append(accepted, index)
		recordAdmittedCommand(admission, passCounts)
	}
	return accepted
}

type disruptionPacingCommandAdmission struct {
	poolCounts   map[string]int
	poolPolicies map[string]disruptionPacingPolicy
	scopeCounts  map[string]int
	scopeConfigs map[string]disruptionPacingConfig
}

func (p *DisruptionPacing) planCommandAdmissionLocked(cmd Command, passCounts map[string]int, now time.Time) (disruptionPacingCommandAdmission, bool) {
	if !p.candidatePoolsInPolicySnapshotLocked(cmd) {
		return disruptionPacingCommandAdmission{}, false
	}

	poolCounts, poolObjects := candidateCountsAndPools(&cmd)
	admission := disruptionPacingCommandAdmission{
		poolCounts: poolCounts, poolPolicies: map[string]disruptionPacingPolicy{}, scopeCounts: map[string]int{}, scopeConfigs: map[string]disruptionPacingConfig{},
	}
	for poolName, count := range poolCounts {
		policy, ok := p.policyForAdmissionLocked(poolName, poolObjects[poolName])
		if !ok || policy.config.blocked {
			return disruptionPacingCommandAdmission{}, false
		}
		admission.poolPolicies[poolName] = policy
		if policy.config.configured {
			admission.scopeCounts[policy.scope] += count
			admission.scopeConfigs[policy.scope] = policy.config
		}
	}
	for scope, count := range admission.scopeCounts {
		if !p.scopeHasAdmissionCapacityLocked(scope, count, admission.scopeConfigs[scope], passCounts, now) {
			return disruptionPacingCommandAdmission{}, false
		}
	}
	return admission, true
}

func (p *DisruptionPacing) candidatePoolsInPolicySnapshotLocked(cmd Command) bool {
	if !p.policyInitialized {
		return true
	}
	for _, candidate := range cmd.Candidates {
		if candidate == nil || candidate.NodePool == nil {
			continue
		}
		if _, ok := p.policies[candidate.NodePool.Name]; !ok {
			return false
		}
	}
	return true
}

func (p *DisruptionPacing) policyForAdmissionLocked(poolName string, np *v1.NodePool) (disruptionPacingPolicy, bool) {
	if policy, ok := p.policies[poolName]; ok {
		return policy, true
	}
	if p.policyInitialized {
		return disruptionPacingPolicy{}, false
	}
	return p.policyForLocked(np), true
}

func (p *DisruptionPacing) scopeHasAdmissionCapacityLocked(scope string, count int, config disruptionPacingConfig, passCounts map[string]int, now time.Time) bool {
	if passCounts[scope] == 0 && now.Before(p.nextEligible[scope]) {
		return false
	}
	return config.maxNodesPerBatch == 0 || passCounts[scope]+count <= config.maxNodesPerBatch
}

func recordAdmittedCommand(command disruptionPacingCommandAdmission, passCounts map[string]int) {
	for poolName, count := range command.poolCounts {
		policy := command.poolPolicies[poolName]
		if policy.config.configured {
			passCounts[policy.scope] += count
		}
	}
}

// ChargePass updates scope deadlines from successful StartCommand results in
// one disruption-method pass.
func (p *DisruptionPacing) ChargePass(starts []DisruptionPacingSuccessfulStart) {
	if p == nil || len(starts) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for scope, charge := range aggregateSuccessfulStartCharges(p, starts) {
		eligible := charge.start.Add(pacingCooldown(charge.count, charge.rate))
		p.nextEligible[scope] = laterPacingDeadline(p.nextEligible[scope], eligible)
	}
}

func aggregateSuccessfulStartCharges(p *DisruptionPacing, starts []DisruptionPacingSuccessfulStart) map[string]disruptionPacingCharge {
	charges := map[string]disruptionPacingCharge{}
	for _, start := range starts {
		if start.Command == nil {
			continue
		}
		poolCounts, poolObjects := candidateCountsAndPools(start.Command)
		for poolName, count := range poolCounts {
			if count <= 0 {
				continue
			}
			policy, ok := p.policyForAdmissionLocked(poolName, poolObjects[poolName])
			if !ok || !policy.config.configured {
				continue
			}
			charge := charges[policy.scope]
			charge.count += count
			if start.StartTime.After(charge.start) {
				charge.start = start.StartTime
			}
			if charge.rate == 0 || policy.config.rate < charge.rate {
				charge.rate = policy.config.rate
			}
			charges[policy.scope] = charge
		}
	}
	return charges
}

type disruptionPacingCharge struct {
	count int
	start time.Time
	rate  float64
}

func pacingCooldown(count int, rate float64) time.Duration {
	if count <= 0 || rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return 0
	}
	nanoseconds := float64(count) * float64(time.Minute) / rate
	if math.IsInf(nanoseconds, 1) || math.IsNaN(nanoseconds) || nanoseconds >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	if nanoseconds < 1 {
		return time.Nanosecond
	}
	return time.Duration(math.Ceil(nanoseconds))
}

func candidateCountsAndPools(cmd *Command) (map[string]int, map[string]*v1.NodePool) {
	counts := map[string]int{}
	pools := map[string]*v1.NodePool{}
	if cmd == nil {
		return counts, pools
	}
	for _, candidate := range cmd.Candidates {
		if candidate == nil || candidate.NodePool == nil || len(candidate.reschedulablePods) == 0 {
			continue
		}
		counts[candidate.NodePool.Name]++
		pools[candidate.NodePool.Name] = candidate.NodePool
	}
	return counts, pools
}
