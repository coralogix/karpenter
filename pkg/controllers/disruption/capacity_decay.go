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
	"math"
	"slices"
	"strings"
	"time"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/utils/insufficientcapacity"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

// Capacity decay (ADR-0011) replaces the fixed reclamation delay on the NodePools that set a half-life.
const (
	CapacityDecayHalfLifeAnnotationKey                     = "karpenter.coralogix.net/reclamation-capacity-decay-half-life"
	CapacityDecayInsufficientCapacityHalfLifeAnnotationKey = "karpenter.coralogix.net/reclamation-capacity-decay-insufficient-capacity-half-life"
	CapacityDecayInsufficientCapacityWindowAnnotationKey   = "karpenter.coralogix.net/reclamation-capacity-decay-insufficient-capacity-window"
	CapacityDecayPoolsAnnotationKey                        = "karpenter.coralogix.net/reclamation-capacity-decay-pools"

	InvalidCapacityDecayHalfLife                     = "InvalidReclamationCapacityDecayHalfLife"
	InvalidCapacityDecayInsufficientCapacityHalfLife = "InvalidReclamationCapacityDecayInsufficientCapacityHalfLife"
	InvalidCapacityDecayInsufficientCapacityWindow   = "InvalidReclamationCapacityDecayInsufficientCapacityWindow"

	defaultInsufficientCapacityWindow = time.Hour
)

var (
	NodePoolReclamationKeptCPU = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "reclamation_kept_cpu",
			Help:      "The kept capacity in CPU cores of a NodePool with capacity decay on. Labeled by NodePool.",
		},
		[]string{metrics.NodePoolLabel},
	)
	keptCPUMetricStore = metrics.NewStore()
)

type keptCapacity struct {
	cpu          float64
	ownActiveCPU float64
	updated      time.Time
}

// ObservePass raises a pool's kept capacity at once to a higher active capacity, and otherwise decays the excess
// over active capacity with the pool's half-life. The first update starts at the larger of the active capacity and the
// pool's own active and standby capacity, so a restart only delays reclamation. Pools with capacity decay off are dropped.
func (r *Reclamation) ObservePass(nodePools map[string]*v1.NodePool) {
	now := r.clock.Now()
	activeCPUByPool, standbyCPUByPool := map[string]float64{}, map[string]float64{}
	for node := range r.cluster.Nodes() {
		if !node.Initialized() || node.MarkedForDeletion() {
			continue
		}
		pool := node.Labels()[v1.NodePoolLabelKey]
		if standby.HasStandbyTaint(node.Node) {
			standbyCPUByPool[pool] += countCores(node)
		} else {
			activeCPUByPool[pool] += countCores(node)
		}
	}
	kept := map[string]keptCapacity{}
	for name, np := range nodePools {
		halfLife := r.chooseHalfLife(np, now)
		if halfLife == nil {
			continue
		}
		active := lo.Sum(lo.Map(listCapacityDecayPools(np), func(pool string, _ int) float64 { return activeCPUByPool[pool] }))
		cpu := max(active, activeCPUByPool[name]+standbyCPUByPool[name])
		if prev, ok := r.keptCapacityByPool[name]; ok {
			cpu = max(active, active+(prev.cpu-active)*computeRemainingExcessFraction(now.Sub(prev.updated), *halfLife))
		}
		kept[name] = keptCapacity{cpu: cpu, ownActiveCPU: activeCPUByPool[name], updated: now}
	}
	r.keptCapacityByPool = kept
	keptCPUMetricStore.ReplaceAll(lo.MapValues(kept, func(k keptCapacity, pool string) []*metrics.StoreMetric {
		return []*metrics.StoreMetric{{GaugeMetric: NodePoolReclamationKeptCPU, Value: k.cpu, Labels: map[string]string{metrics.NodePoolLabel: pool}}}
	}))
}

func countCores(node *state.StateNode) float64 {
	cpu := node.Allocatable()[corev1.ResourceCPU]
	return cpu.AsApproximateFloat64()
}

func (r *Reclamation) chooseHalfLife(np *v1.NodePool, now time.Time) *time.Duration {
	if !scoreBasedStandbyPool(np) {
		return nil
	}
	halfLife := readDurationAnnotation(np, r.recorder, CapacityDecayHalfLifeAnnotationKey, InvalidCapacityDecayHalfLife, v1.ReclamationDelayAnnotationKey)
	if halfLife == nil {
		return nil
	}
	scarceHalfLife := readDurationAnnotation(np, r.recorder, CapacityDecayInsufficientCapacityHalfLifeAnnotationKey, InvalidCapacityDecayInsufficientCapacityHalfLife, CapacityDecayHalfLifeAnnotationKey)
	window := lo.FromPtrOr(readDurationAnnotation(np, r.recorder, CapacityDecayInsufficientCapacityWindowAnnotationKey, InvalidCapacityDecayInsufficientCapacityWindow, defaultInsufficientCapacityWindow.String()), defaultInsufficientCapacityWindow)
	return lo.Ternary(scarceHalfLife != nil && now.Sub(insufficientcapacity.LastFailure(np.Name)) < window, scarceHalfLife, halfLife)
}

func listCapacityDecayPools(np *v1.NodePool) []string {
	listed := lo.Map(strings.Split(np.Annotations[CapacityDecayPoolsAnnotationKey], ","), func(name string, _ int) string { return strings.TrimSpace(name) })
	return lo.Uniq(append(lo.Compact(listed), np.Name))
}

func computeRemainingExcessFraction(elapsed, halfLife time.Duration) float64 {
	if halfLife == 0 {
		return 0
	}
	return math.Exp2(-elapsed.Seconds() / halfLife.Seconds())
}

// dropKeptStandby drops the candidates that capacity decay keeps, and orders the rest of each such pool oldest first.
// Candidates of other pools pass unchanged.
func (r *Reclamation) dropKeptStandby(candidates []*Candidate) []*Candidate {
	var result []*Candidate
	for pool, poolCandidates := range lo.GroupBy(candidates, func(c *Candidate) string { return c.NodePool.Name }) {
		kept, on := r.keptCapacityByPool[pool]
		if !on {
			result = append(result, poolCandidates...)
			continue
		}
		result = append(result, r.selectBeyondAllowance(pool, kept.cpu-kept.ownActiveCPU, poolCandidates)...)
	}
	return result
}

// selectBeyondAllowance spends the pool's standby allowance first on standby nodes that reclamation may not delete, then on
// the newest candidates while it covers at least half of each one's cores. It returns the other candidates, oldest first.
func (r *Reclamation) selectBeyondAllowance(pool string, allowance float64, candidates []*Candidate) []*Candidate {
	reclaimable := sets.New(lo.Map(candidates, func(c *Candidate, _ int) string { return c.ProviderID() })...)
	for node := range r.cluster.Nodes() {
		if node.Labels()[v1.NodePoolLabelKey] == pool && node.Initialized() && !node.MarkedForDeletion() && standby.HasStandbyTaint(node.Node) && !reclaimable.Has(node.ProviderID()) {
			allowance -= countCores(node)
		}
	}
	slices.SortFunc(candidates, func(a, b *Candidate) int {
		aStandby, _ := standby.Timestamp(a.Node)
		bStandby, _ := standby.Timestamp(b.Node)
		return cmp.Or(aStandby.Compare(bStandby), strings.Compare(a.Name(), b.Name()))
	})
	reclaimed := len(candidates)
	for ; reclaimed > 0 && allowance >= countCores(candidates[reclaimed-1].StateNode)/2; reclaimed-- {
		allowance -= countCores(candidates[reclaimed-1].StateNode)
	}
	return candidates[:reclaimed]
}
