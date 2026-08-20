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
)

type underutilizedPaceConfig struct {
	rate               float64
	maxNodesPerCommand int // 0 = no per-command cap
	configured         bool
}

// UnderutilizedConsolidationPace tracks per-NodePool eligibility for underutilized consolidation.
type UnderutilizedConsolidationPace struct {
	clock        clock.Clock
	mu           sync.Mutex
	nextEligible map[string]time.Time
}

func NewUnderutilizedConsolidationPace(clk clock.Clock) *UnderutilizedConsolidationPace {
	return &UnderutilizedConsolidationPace{
		clock:        clk,
		nextEligible: map[string]time.Time{},
	}
}

func underutilizedPaceConfigFor(np *v1.NodePool) underutilizedPaceConfig {
	if np == nil || np.Annotations == nil {
		return underutilizedPaceConfig{}
	}
	rateRaw, rateOK := np.Annotations[v1.MaxUnderutilizedNodeDisruptionsPerMinuteAnnotationKey]
	if !rateOK {
		return underutilizedPaceConfig{}
	}
	rate, err := strconv.ParseFloat(rateRaw, 64)
	if err != nil || rate <= 0 {
		return underutilizedPaceConfig{}
	}
	cfg := underutilizedPaceConfig{
		rate:       rate,
		configured: true,
	}
	if maxRaw, maxOK := np.Annotations[v1.MaxUnderutilizedNodesPerConsolidationAnnotationKey]; maxOK {
		if maxNodes, err := strconv.Atoi(maxRaw); err == nil && maxNodes > 0 {
			cfg.maxNodesPerCommand = maxNodes
		}
	}
	return cfg
}

func (p *UnderutilizedConsolidationPace) canAdmitLocked(np *v1.NodePool) bool {
	cfg := underutilizedPaceConfigFor(np)
	if !cfg.configured {
		return true
	}
	next, ok := p.nextEligible[np.Name]
	if !ok {
		return true
	}
	return !p.clock.Now().Before(next)
}

func (p *UnderutilizedConsolidationPace) chargeCommandLocked(cmd *Command) {
	if cmd == nil {
		return
	}
	now := p.clock.Now()
	counts, pools := candidateCountsAndPools(cmd)
	for name, count := range counts {
		np := pools[name]
		cfg := underutilizedPaceConfigFor(np)
		if !cfg.configured {
			continue
		}

		base := now
		if next, ok := p.nextEligible[name]; ok && next.After(base) {
			base = next
		}
		cooldown := time.Duration(float64(count) * float64(time.Minute) / cfg.rate)
		p.nextEligible[name] = base.Add(cooldown)
	}
}

// candidateAllowed reports whether another candidate from np may be selected during planning.
// selectedCount is the number of candidates already selected for this NodePool.
func (p *UnderutilizedConsolidationPace) candidateAllowed(np *v1.NodePool, selectedCount int) bool {
	if p == nil {
		return true
	}
	cfg := underutilizedPaceConfigFor(np)
	if !cfg.configured {
		return true
	}
	if cfg.maxNodesPerCommand > 0 && selectedCount >= cfg.maxNodesPerCommand {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.canAdmitLocked(np)
}

// Charge records a successfully started consolidation command against participating NodePools.
func (p *UnderutilizedConsolidationPace) Charge(cmd *Command) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.chargeCommandLocked(cmd)
}

func candidateCountsAndPools(cmd *Command) (map[string]int, map[string]*v1.NodePool) {
	counts := map[string]int{}
	pools := map[string]*v1.NodePool{}
	for _, candidate := range cmd.Candidates {
		if candidate == nil || candidate.NodePool == nil || len(candidate.reschedulablePods) == 0 {
			continue
		}
		counts[candidate.NodePool.Name]++
		pools[candidate.NodePool.Name] = candidate.NodePool
	}
	return counts, pools
}
