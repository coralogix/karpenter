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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/cxtracing"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const (
	standbyLoopSpanName      = "karpenter.disruption.standby"
	standbyLoopPollingPeriod = time.Second
)

// StandbyLoop runs standby marking and reclamation, with the standby-node metric, capacity decay sampling and standby
// repair, apart from the disruption loop, so that score-based consolidation does not delay them (ADR-0012).
type StandbyLoop struct {
	queue         *Queue
	kubeClient    client.Client
	cluster       *state.Cluster
	recorder      events.Recorder
	clock         clock.Clock
	cloudProvider cloudprovider.CloudProvider
	marking       *StandbyMarking
	reclamation   *Reclamation
}

func NewStandbyLoop(clk clock.Clock, kubeClient client.Client, cp cloudprovider.CloudProvider, recorder events.Recorder, cluster *state.Cluster, queue *Queue) *StandbyLoop {
	return &StandbyLoop{
		queue:         queue,
		kubeClient:    kubeClient,
		cluster:       cluster,
		recorder:      recorder,
		clock:         clk,
		cloudProvider: cp,
		marking:       NewStandbyMarking(cluster),
		reclamation:   NewReclamation(cluster, recorder, clk),
	}
}

func (l *StandbyLoop) Name() string {
	return "disruption.standby"
}

func (l *StandbyLoop) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(l.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(l))
}

// Reconcile runs one pass and starts the next at once when the pass started a command. Errors are logged rather than
// returned, so a failed command is retried on the next pass instead of after the controller's backoff. Conflicts are
// expected when another controller updates a Node at the same time, so they are logged at info level.
func (l *StandbyLoop) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, l.Name())
	ctx, endSpan := cxtracing.Start(ctx, standbyLoopSpanName)
	defer endSpan()

	if !l.cluster.Synced(ctx) {
		return reconciler.Result{RequeueAfter: standbyLoopPollingPeriod}, nil
	}
	started, err := l.runPass(ctx)
	for _, err := range multierr.Errors(err) {
		if errors.IsConflict(err) {
			log.FromContext(ctx).Info("standby pass hit a conflict, retrying on the next pass", "error", err)
			continue
		}
		log.FromContext(ctx).Error(err, "standby pass failed")
	}
	if started {
		return reconciler.Result{RequeueAfter: singleton.RequeueImmediately}, nil
	}
	return reconciler.Result{RequeueAfter: standbyLoopPollingPeriod}, nil
}

// runPass runs everything in one goroutine: ObservePass and reclamation share keptCapacityByPool, which has no lock.
func (l *StandbyLoop) runPass(ctx context.Context) (bool, error) {
	nodePoolMap, instanceTypes, err := BuildNodePoolMap(ctx, l.kubeClient, l.cloudProvider)
	if err != nil {
		return false, fmt.Errorf("building node pool map, %w", err)
	}
	recordStandbyNodes(l.cluster, nodePoolMap)
	l.reclamation.ObservePass(nodePoolMap)
	errs := []error{repairStandbyState(ctx, l.kubeClient, l.cluster, l.recorder, l.clock, nodePoolMap)}

	candidates, err := l.buildCandidates(ctx, nodePoolMap, instanceTypes)
	if err != nil {
		return false, multierr.Combine(append(errs, err)...)
	}
	for _, m := range []Method{l.marking, l.reclamation} {
		started, err := traceMethod(ctx, standbyLoopSpanName, m, func(ctx context.Context, m Method) (bool, error) {
			return l.disrupt(ctx, m, candidates)
		})
		errs = append(errs, err)
		if started {
			return true, multierr.Combine(errs...)
		}
	}
	return false, multierr.Combine(errs...)
}

// buildCandidates builds candidates only for nodes that may be standby work. Marking and reclamation still apply their
// full checks to these candidates.
func (l *StandbyLoop) buildCandidates(ctx context.Context, nodePoolMap map[string]*v1.NodePool, instanceTypes map[string]map[string]*cloudprovider.InstanceType) ([]*Candidate, error) {
	pdbs, err := pdb.NewLimits(ctx, l.kubeClient)
	if err != nil {
		return nil, fmt.Errorf("tracking PodDisruptionBudgets, %w", err)
	}
	return lo.FilterMap(l.cluster.DeepCopyNodes(), func(n *state.StateNode, _ int) (*Candidate, bool) {
		if !mayBeStandbyWork(n, nodePoolMap) {
			return nil, false
		}
		c, err := NewCandidate(ctx, l.kubeClient, l.recorder, l.clock, n, pdbs, nodePoolMap, instanceTypes, l.queue, GracefulDisruptionClass)
		return c, err == nil
	}), nil
}

// mayBeStandbyWork reports whether a node is in a pool that can hold standby nodes and is on standby or has no
// non-DaemonSet pod with a positive eviction cost in cluster state, counting terminating and mirror pods, so these pods
// delay or prevent marking.
func mayBeStandbyWork(n *state.StateNode, nodePoolMap map[string]*v1.NodePool) bool {
	return scoreBasedStandbyPool(nodePoolMap[n.Labels()[v1.NodePoolLabelKey]]) &&
		(standby.HasStandbyTaint(n.Node) || n.DisruptionCost() <= PerNodeBaseDisruptionCost)
}

func (l *StandbyLoop) disrupt(ctx context.Context, m Method, candidates []*Candidate) (bool, error) {
	reason := strings.ToLower(string(m.Reason()))
	defer metrics.Measure(EvaluationDurationSeconds, map[string]string{metrics.ReasonLabel: reason, ConsolidationTypeLabel: m.ConsolidationType()})()
	candidates = lo.Filter(candidates, func(c *Candidate, _ int) bool { return m.ShouldDisrupt(ctx, c) })
	EligibleNodes.Set(float64(len(candidates)), map[string]string{metrics.ReasonLabel: reason})
	if len(candidates) == 0 {
		return false, nil
	}
	var budgets map[string]int
	if usesDisruptionBudget(m) {
		var err error
		if budgets, err = BuildDisruptionBudgetMapping(ctx, l.cluster, l.clock, l.kubeClient, l.cloudProvider, l.recorder, m.Reason()); err != nil {
			return false, fmt.Errorf("building disruption budgets, %w", err)
		}
	}
	cmds, err := m.ComputeCommands(ctx, budgets, candidates...)
	if err != nil {
		return false, fmt.Errorf("computing disruption decision, %w", err)
	}
	started := false
	var errs []error
	for _, cmd := range cmds {
		cmd.Method, cmd.CreationTimestamp, cmd.ID = m, l.clock.Now(), uuid.New()
		if err := l.queue.StartCommand(ctx, &cmd); err != nil {
			errs = append(errs, fmt.Errorf("disrupting candidates, %w", err))
			continue
		}
		started = true
	}
	return started, multierr.Combine(errs...)
}
