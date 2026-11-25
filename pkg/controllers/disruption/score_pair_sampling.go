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
	"cmp"
	"container/heap"
	"maps"
	"math"
	"math/rand"
	"slices"

	"github.com/samber/lo"
)

const (
	sameNodePoolPairQuota = scoreBasedPairLimit / 2
	minSamplingWeight     = 0.05
)

// Same-NodePool pairs get sameNodePoolPairQuota of the places, because their pods already run on that pool, so they are
// likelier to fit on one replacement. Either kind's unused places go to the other.
func (s moveSearch) choosePairs(candidates []*Candidate) [][]*Candidate {
	byPool := lo.GroupBy(lo.Map(candidates, func(c *Candidate, _ int) weightedCandidate {
		return weightedCandidate{Candidate: c, weight: weighForSampling(c)}
	}), func(c weightedCandidate) string { return c.NodePool.Name })
	names := slices.Sorted(maps.Keys(byPool))
	samePool, mixedPools := &pairSample{random: s.random}, &pairSample{random: s.random}
	for i, p := range names {
		for _, q := range names[i:] {
			a, b := byPool[p], byPool[q]
			if !s.pairAllowed(a[0].Candidate, b[len(b)-1].Candidate) {
				continue
			}
			sample := lo.Ternary(p == q, samePool, mixedPools)
			for i, x := range a {
				for j := lo.Ternary(p == q, i+1, 0); j < len(b); j++ {
					sample.offer(x, b[j])
				}
			}
		}
	}
	same := min(len(samePool.pairs), max(sameNodePoolPairQuota, scoreBasedPairLimit-len(mixedPools.pairs)))
	return append(samePool.best(same), mixedPools.best(scoreBasedPairLimit-same)...)
}

type weightedCandidate struct {
	*Candidate
	weight float64
}

func weighForSampling(c *Candidate) float64 {
	allocatable := cpuCores(c.Allocatable())
	if allocatable <= 0 {
		return minSamplingWeight
	}
	return math.Max(minSamplingWeight, 1-cpuCores(c.PodRequests())/allocatable)
}

// pairAllowed is validation's pre-compute check without the nomination check, which depends on the node. The rest
// depends only on the pair's NodePools, so it runs once per pool pair.
func (s moveSearch) pairAllowed(a, b *Candidate) bool {
	pair := []*Candidate{a, b}
	return withinBudgets(pair, s.budgets) && s.pacing.CommandAllowed(Command{Candidates: pair})
}

func withinBudgets(sources []*Candidate, budgets map[string]int) bool {
	perPool := lo.CountValuesBy(sources, func(c *Candidate) string { return c.NodePool.Name })
	for pool, count := range perPool {
		if count > budgets[pool] {
			return false
		}
	}
	return true
}

// pairSample is a weighted sample without replacement (Efraimidis–Spirakis): each offered pair gets the key
// ln(u)/weight and the scoreBasedPairLimit pairs with the largest keys are kept. Any number of the largest keys is
// again such a sample.
type pairSample struct {
	random *rand.Rand
	pairs  keyedPairs
}

type keyedPair struct {
	key  float64
	pair []*Candidate
}

func (s *pairSample) offer(a, b weightedCandidate) {
	key := math.Log(s.random.Float64()) / (a.weight * b.weight)
	if len(s.pairs) < scoreBasedPairLimit {
		heap.Push(&s.pairs, keyedPair{key: key, pair: []*Candidate{a.Candidate, b.Candidate}})
		return
	}
	if key > s.pairs[0].key {
		s.pairs[0] = keyedPair{key: key, pair: []*Candidate{a.Candidate, b.Candidate}}
		heap.Fix(&s.pairs, 0)
	}
}

func (s *pairSample) best(n int) [][]*Candidate {
	sorted := slices.SortedFunc(slices.Values(s.pairs), func(a, b keyedPair) int { return cmp.Compare(b.key, a.key) })
	return lo.Map(sorted[:min(n, len(sorted))], func(p keyedPair, _ int) []*Candidate { return p.pair })
}

type keyedPairs []keyedPair

func (h keyedPairs) Len() int           { return len(h) }
func (h keyedPairs) Less(i, j int) bool { return h[i].key < h[j].key }
func (h keyedPairs) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *keyedPairs) Push(x any)        { *h = append(*h, x.(keyedPair)) }
func (h *keyedPairs) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
