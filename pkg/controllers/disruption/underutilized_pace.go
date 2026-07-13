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

	"github.com/samber/lo"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

type underutilizedPaceConfig struct {
	rate       float64
	configured bool
	invalid    bool
}

// UnderutilizedConsolidationPace tracks per-NodePool admission times for underutilized consolidation.
type UnderutilizedConsolidationPace struct {
	clock        clock.Clock
	mu           sync.Mutex
	lastAdmitted map[string]time.Time
}

func NewUnderutilizedConsolidationPace(clk clock.Clock) *UnderutilizedConsolidationPace {
	return &UnderutilizedConsolidationPace{
		clock:        clk,
		lastAdmitted: map[string]time.Time{},
	}
}

func maxUnderutilizedConsolidationsPerMinute(np *v1.NodePool) underutilizedPaceConfig {
	if np == nil || np.Annotations == nil {
		return underutilizedPaceConfig{}
	}
	raw, ok := np.Annotations[v1.MaxUnderutilizedConsolidationsPerMinuteAnnotationKey]
	if !ok {
		return underutilizedPaceConfig{}
	}
	rate, err := strconv.ParseFloat(raw, 64)
	if err != nil || rate <= 0 {
		return underutilizedPaceConfig{configured: true, invalid: true}
	}
	return underutilizedPaceConfig{rate: rate, configured: true}
}

func (p *UnderutilizedConsolidationPace) minInterval(np *v1.NodePool) (time.Duration, underutilizedPaceConfig) {
	cfg := maxUnderutilizedConsolidationsPerMinute(np)
	if !cfg.configured || cfg.invalid {
		return 0, cfg
	}
	return time.Duration(float64(time.Minute) / cfg.rate), cfg
}

func (p *UnderutilizedConsolidationPace) canAdmitLocked(np *v1.NodePool) bool {
	interval, cfg := p.minInterval(np)
	if cfg.invalid {
		return false
	}
	if !cfg.configured {
		return true
	}
	last, ok := p.lastAdmitted[np.Name]
	if !ok {
		return true
	}
	return p.clock.Since(last) >= interval
}

// CanAdmit reports whether underutilized consolidation may start for the NodePool now.
func (p *UnderutilizedConsolidationPace) CanAdmit(np *v1.NodePool) bool {
	if p == nil || np == nil {
		return true
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.canAdmitLocked(np)
}

// TryAdmit atomically admits the command for all NodePools involved.
func (p *UnderutilizedConsolidationPace) TryAdmit(nodePools ...*v1.NodePool) bool {
	if p == nil {
		return true
	}
	unique := lo.UniqBy(lo.Filter(nodePools, func(np *v1.NodePool, _ int) bool { return np != nil }), func(np *v1.NodePool) string {
		return np.Name
	})

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, np := range unique {
		if !p.canAdmitLocked(np) {
			return false
		}
	}

	now := p.clock.Now()
	for _, np := range unique {
		_, cfg := p.minInterval(np)
		if cfg.configured && !cfg.invalid {
			p.lastAdmitted[np.Name] = now
		}
	}
	return true
}

func nodePoolsFromCommand(cmd Command) []*v1.NodePool {
	seen := map[string]*v1.NodePool{}
	for _, candidate := range cmd.Candidates {
		if candidate.NodePool != nil {
			seen[candidate.NodePool.Name] = candidate.NodePool
		}
	}
	return lo.Values(seen)
}
