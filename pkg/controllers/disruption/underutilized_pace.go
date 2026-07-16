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
	"strconv"
	"sync"
	"time"

	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
)

type underutilizedPaceConfig struct {
	rate               float64
	maxNodesPerCommand int
	configured         bool
	invalid            bool
}

// UnderutilizedConsolidationPace tracks per-NodePool eligibility for underutilized consolidation.
type UnderutilizedConsolidationPace struct {
	clock        clock.Clock
	recorder     events.Recorder
	mu           sync.Mutex
	nextEligible map[string]time.Time
}

// PaceCharge captures per-NodePool state before a successful admission so it can be rolled back.
type PaceCharge struct {
	entries map[string]paceChargeEntry
}

type paceChargeEntry struct {
	hadPrevious  bool
	previous     time.Time
	chargedUntil time.Time
}

func NewUnderutilizedConsolidationPace(clk clock.Clock, recorder events.Recorder) *UnderutilizedConsolidationPace {
	return &UnderutilizedConsolidationPace{
		clock:        clk,
		recorder:     recorder,
		nextEligible: map[string]time.Time{},
	}
}

func underutilizedPaceConfigFor(np *v1.NodePool) underutilizedPaceConfig {
	if np == nil || np.Annotations == nil {
		return underutilizedPaceConfig{}
	}
	rateRaw, rateOK := np.Annotations[v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey]
	maxRaw, maxOK := np.Annotations[v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey]
	if !rateOK && !maxOK {
		return underutilizedPaceConfig{}
	}
	if rateOK != maxOK {
		return underutilizedPaceConfig{configured: true, invalid: true}
	}
	rate, err := strconv.ParseFloat(rateRaw, 64)
	if err != nil || rate <= 0 {
		return underutilizedPaceConfig{configured: true, invalid: true}
	}
	maxNodes, err := strconv.Atoi(maxRaw)
	if err != nil || maxNodes <= 0 {
		return underutilizedPaceConfig{configured: true, invalid: true}
	}
	return underutilizedPaceConfig{
		rate:               rate,
		maxNodesPerCommand: maxNodes,
		configured:         true,
	}
}

func (p *UnderutilizedConsolidationPace) publishMisconfigured(np *v1.NodePool) {
	if p == nil || p.recorder == nil || np == nil {
		return
	}
	p.recorder.Publish(disruptionevents.UnderutilizedPaceMisconfigured(np))
}

func (p *UnderutilizedConsolidationPace) publishMisconfiguredForCommand(cmd *Command) {
	if cmd == nil {
		return
	}
	_, pools := candidateCountsAndPools(cmd)
	for _, np := range pools {
		if underutilizedPaceConfigFor(np).invalid {
			p.publishMisconfigured(np)
		}
	}
}

func (p *UnderutilizedConsolidationPace) canAdmitLocked(np *v1.NodePool) bool {
	cfg := underutilizedPaceConfigFor(np)
	if cfg.invalid {
		return false
	}
	if !cfg.configured {
		return true
	}
	next, ok := p.nextEligible[np.Name]
	if !ok {
		return true
	}
	return !p.clock.Now().Before(next)
}

func (p *UnderutilizedConsolidationPace) canAdmitCommandLocked(cmd *Command) bool {
	if cmd == nil {
		return true
	}
	counts, pools := candidateCountsAndPools(cmd)
	for name, count := range counts {
		np := pools[name]
		cfg := underutilizedPaceConfigFor(np)
		if cfg.invalid {
			return false
		}
		if !cfg.configured {
			continue
		}
		if count > cfg.maxNodesPerCommand {
			return false
		}
		if !p.canAdmitLocked(np) {
			return false
		}
	}
	return true
}

func (p *UnderutilizedConsolidationPace) chargeCommandLocked(cmd *Command) PaceCharge {
	charge := PaceCharge{entries: map[string]paceChargeEntry{}}
	if cmd == nil {
		return charge
	}
	now := p.clock.Now()
	counts, pools := candidateCountsAndPools(cmd)
	for name, count := range counts {
		np := pools[name]
		cfg := underutilizedPaceConfigFor(np)
		if !cfg.configured || cfg.invalid {
			continue
		}
		entry := paceChargeEntry{}
		if previous, ok := p.nextEligible[name]; ok {
			entry.hadPrevious = true
			entry.previous = previous
		}

		base := now
		if next, ok := p.nextEligible[name]; ok && next.After(base) {
			base = next
		}
		cooldown := time.Duration(float64(count) * float64(time.Minute) / cfg.rate)
		chargedUntil := base.Add(cooldown)
		p.nextEligible[name] = chargedUntil
		entry.chargedUntil = chargedUntil
		charge.entries[name] = entry
	}
	return charge
}

// candidateAllowed reports whether another candidate from np may be selected during planning.
// selectedCount is the number of candidates already selected for this NodePool.
//
// This is a best-effort planning filter only. TryAdmitCommand is the authoritative
// atomic check-and-charge under a single lock at command admission time.
func (p *UnderutilizedConsolidationPace) candidateAllowed(np *v1.NodePool, selectedCount int) bool {
	if p == nil {
		return true
	}
	cfg := underutilizedPaceConfigFor(np)
	if cfg.invalid {
		p.publishMisconfigured(np)
		return false
	}
	if !cfg.configured {
		return true
	}
	if selectedCount >= cfg.maxNodesPerCommand {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.canAdmitLocked(np)
}

// TryAdmitCommand atomically charges all participating NodePools for the command's candidate nodes.
//
// Planning may have already filtered candidates via candidateAllowed, but admission
// re-checks eligibility under the lock before charging.
func (p *UnderutilizedConsolidationPace) TryAdmitCommand(cmd *Command) (PaceCharge, bool) {
	if p == nil {
		return PaceCharge{}, true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.canAdmitCommandLocked(cmd) {
		p.publishMisconfiguredForCommand(cmd)
		return PaceCharge{}, false
	}
	return p.chargeCommandLocked(cmd), true
}

// Release rolls back a prior TryAdmitCommand charge.
func (p *UnderutilizedConsolidationPace) Release(charge PaceCharge) {
	if p == nil || charge.entries == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for name, entry := range charge.entries {
		next, ok := p.nextEligible[name]
		if !ok || !next.Equal(entry.chargedUntil) {
			continue
		}
		if entry.hadPrevious {
			p.nextEligible[name] = entry.previous
			continue
		}
		delete(p.nextEligible, name)
	}
}

func candidateCountsAndPools(cmd *Command) (map[string]int, map[string]*v1.NodePool) {
	counts := map[string]int{}
	pools := map[string]*v1.NodePool{}
	for _, candidate := range cmd.Candidates {
		if candidate == nil || candidate.NodePool == nil {
			continue
		}
		counts[candidate.NodePool.Name]++
		pools[candidate.NodePool.Name] = candidate.NodePool
	}
	return counts, pools
}
