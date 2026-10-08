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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	validationutil "k8s.io/apimachinery/pkg/util/validation"
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

// DisruptionPacing applies a shared rate and batch cap to non-empty disruption
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
// candidates. It returns true when effective membership or pacing policy has
// changed and publishes warning events for invalid grouped configurations.
func (p *DisruptionPacing) Refresh(nodePools []*v1.NodePool, recorder events.Recorder) bool {
	policies, scopeConfigs, issues := resolveDisruptionPacingPolicies(nodePools)

	p.mu.Lock()
	changed := p.policyInitialized && !sameDisruptionPacingPolicies(p.policies, policies)
	p.migrateCooldownsLocked(policies, scopeConfigs)
	p.policies = policies
	p.policyInitialized = true
	p.mu.Unlock()

	if recorder != nil {
		for _, issue := range issues {
			for _, np := range issue.nodePools {
				recorder.Publish(events.Event{
					InvolvedObject: np,
					Type:           corev1.EventTypeWarning,
					Reason:         "InvalidDisruptionPacing",
					Message:        issue.message,
					DedupeValues:   []string{string(np.UID), issue.scope, issue.message},
				})
			}
		}
	}
	return changed
}

type disruptionPacingIssue struct {
	scope     string
	nodePools []*v1.NodePool
	message   string
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

type disruptionPacingPolicyResolution struct {
	policies      map[string]disruptionPacingPolicy
	configs       map[string]disruptionPacingConfig
	groupMembers  map[string][]*v1.NodePool
	groupSettings map[string]map[string]disruptionPacingConfig
	issues        []disruptionPacingIssue
}

func resolveDisruptionPacingPolicies(nodePools []*v1.NodePool) (map[string]disruptionPacingPolicy, map[string]disruptionPacingConfig, []disruptionPacingIssue) {
	resolution := disruptionPacingPolicyResolution{
		policies:      make(map[string]disruptionPacingPolicy, len(nodePools)),
		configs:       map[string]disruptionPacingConfig{},
		groupMembers:  map[string][]*v1.NodePool{},
		groupSettings: map[string]map[string]disruptionPacingConfig{},
	}
	for _, np := range nodePools {
		if np != nil {
			resolution.addNodePool(np)
		}
	}
	for _, scope := range sortedPacingGroupScopes(resolution.groupMembers) {
		resolution.resolveGroup(scope)
	}
	resolution.addInvalidGroupScopes()
	return resolution.policies, resolution.configs, resolution.issues
}

func (r *disruptionPacingPolicyResolution) addNodePool(np *v1.NodePool) {
	config := disruptionPacingConfigFor(np)
	groupRaw, grouped := np.Annotations[v1.DisruptionPacingGroupAnnotationKey]
	if !grouped {
		scope := "pool/" + np.Name
		r.policies[np.Name] = disruptionPacingPolicy{scope: scope, config: config}
		r.configs[scope] = config
		return
	}

	scope := "invalid-group/" + np.Name
	validGroup := groupRaw != "" && len(validationutil.IsDNS1123Label(groupRaw)) == 0
	if validGroup {
		scope = "group/" + groupRaw
		r.groupMembers[scope] = append(r.groupMembers[scope], np)
		if r.groupSettings[scope] == nil {
			r.groupSettings[scope] = map[string]disruptionPacingConfig{}
		}
		r.groupSettings[scope][np.Name] = config
	} else {
		config.blocked = true
		config.error = fmt.Sprintf("invalid group value %q", groupRaw)
		r.issues = append(r.issues, disruptionPacingIssue{
			scope: scope, nodePools: []*v1.NodePool{np},
			message: fmt.Sprintf("NodePool %q has invalid disruption pacing group %q with settings {%s}; non-empty disruption pacing is blocked", np.Name, groupRaw, pacingAnnotationSettings(np)),
		})
	}
	r.policies[np.Name] = disruptionPacingPolicy{scope: scope, config: config}
}

func sortedPacingGroupScopes(groups map[string][]*v1.NodePool) []string {
	scopes := make([]string, 0, len(groups))
	for scope := range groups {
		scopes = append(scopes, scope)
	}
	sort.Strings(scopes)
	return scopes
}

func (r *disruptionPacingPolicyResolution) resolveGroup(scope string) {
	members := r.groupMembers[scope]
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })
	common, errors := comparePacingGroupSettings(members, r.groupSettings[scope])
	if len(errors) == 0 {
		r.acceptGroup(scope, members, common)
		return
	}
	r.blockGroup(scope, members, errors)
}

func comparePacingGroupSettings(members []*v1.NodePool, settings map[string]disruptionPacingConfig) (disruptionPacingConfig, []string) {
	var common disruptionPacingConfig
	var errors []string
	for i, np := range members {
		config := settings[np.Name]
		if config.error != "" {
			errors = append(errors, fmt.Sprintf("%s: %s", np.Name, config.error))
		}
		if !config.configured {
			errors = append(errors, fmt.Sprintf("%s: a finite positive per-minute rate is required", np.Name))
		}
		if i == 0 {
			common = config
		} else if !samePacingValues(common, config) {
			errors = append(errors, fmt.Sprintf("%s: settings differ from group members", np.Name))
		}
	}
	return common, errors
}

func (r *disruptionPacingPolicyResolution) acceptGroup(scope string, members []*v1.NodePool, common disruptionPacingConfig) {
	common.blocked = false
	r.configs[scope] = common
	for _, np := range members {
		policy := r.policies[np.Name]
		policy.config = common
		r.policies[np.Name] = policy
	}
}

func (r *disruptionPacingPolicyResolution) blockGroup(scope string, members []*v1.NodePool, errors []string) {
	memberSettings := make([]string, 0, len(members))
	messageErrors := strings.Join(errors, "; ")
	for _, np := range members {
		memberSettings = append(memberSettings, fmt.Sprintf("%s={%s}", np.Name, pacingAnnotationSettings(np)))
		policy := r.policies[np.Name]
		policy.config.blocked = true
		policy.config.configured = false
		policy.config.error = messageErrors
		r.policies[np.Name] = policy
	}
	message := fmt.Sprintf("disruption pacing group %q is invalid; non-empty consolidation and drift are blocked for member NodePools: %s (errors: %s)", strings.TrimPrefix(scope, "group/"), strings.Join(memberSettings, ", "), messageErrors)
	r.issues = append(r.issues, disruptionPacingIssue{scope: scope, nodePools: members, message: message})
	r.configs[scope] = disruptionPacingConfig{blocked: true, error: messageErrors}
}

func (r *disruptionPacingPolicyResolution) addInvalidGroupScopes() {
	// Invalid group values use a per-pool blocking scope, while valid groups
	// have already been collected above.
	for _, policy := range r.policies {
		if strings.HasPrefix(policy.scope, "invalid-group/") {
			r.configs[policy.scope] = policy.config
		}
	}
}

func samePacingValues(left, right disruptionPacingConfig) bool {
	return left.configured && right.configured && left.rate == right.rate && left.maxNodesPerBatch == right.maxNodesPerBatch && left.error == "" && right.error == ""
}

func pacingAnnotationSettings(np *v1.NodePool) string {
	if np == nil || np.Annotations == nil {
		return ""
	}
	keys := []string{
		v1.DisruptionPacingGroupAnnotationKey,
		v1.DisruptionPacingPerMinuteAnnotationKey,
		v1.DisruptionPacingPerBatchAnnotationKey,
	}
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		if value, ok := np.Annotations[key]; ok {
			values = append(values, fmt.Sprintf("%s=%q", key, value))
		}
	}
	return strings.Join(values, ",")
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

// Scope returns the effective pacing scope. It is also used by planning code
// to account tentative selections across NodePools in the same group.
func (p *DisruptionPacing) Scope(np *v1.NodePool) string {
	if p == nil || np == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if policy, ok := p.policies[np.Name]; ok {
		return policy.scope
	}
	if group, ok := np.Annotations[v1.DisruptionPacingGroupAnnotationKey]; ok && group != "" && len(validationutil.IsDNS1123Label(group)) == 0 {
		return "group/" + group
	}
	if _, ok := np.Annotations[v1.DisruptionPacingGroupAnnotationKey]; ok {
		return "invalid-group/" + np.Name
	}
	return "pool/" + np.Name
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
	if p.policyInitialized {
		if _, grouped := np.Annotations[v1.DisruptionPacingGroupAnnotationKey]; grouped {
			return disruptionPacingPolicy{scope: "invalid-group/" + np.Name, config: disruptionPacingConfig{blocked: true}}
		}
		// A previously unseen ungrouped pool has no shared membership; parse its
		// annotations directly until the next complete managed-pool snapshot.
		return disruptionPacingPolicy{scope: "pool/" + np.Name, config: disruptionPacingConfigFor(np)}
	}
	if _, grouped := np.Annotations[v1.DisruptionPacingGroupAnnotationKey]; grouped {
		group := np.Annotations[v1.DisruptionPacingGroupAnnotationKey]
		if group == "" || len(validationutil.IsDNS1123Label(group)) != 0 {
			return disruptionPacingPolicy{scope: "invalid-group/" + np.Name, config: disruptionPacingConfig{blocked: true}}
		}
		config := disruptionPacingConfigFor(np)
		if !config.configured || config.error != "" {
			config.blocked = true
		}
		return disruptionPacingPolicy{scope: "group/" + group, config: config}
	}
	return disruptionPacingPolicy{scope: "pool/" + np.Name, config: disruptionPacingConfigFor(np)}
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
// which lets multiple static-drift commands share one group batch cap. The
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
