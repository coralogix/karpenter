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
	stderrors "errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/singleton"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/multierr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/cxtracing"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

type Controller struct {
	queue             *Queue
	kubeClient        client.Client
	apiReader         client.Reader
	cluster           *state.Cluster
	provisioner       *provisioning.Provisioner
	recorder          events.Recorder
	clock             clock.Clock
	cloudProvider     cloudprovider.CloudProvider
	clusterCost       *cost.ClusterCost
	underutilizedPace *UnderutilizedConsolidationPace
	methods           []Method
	mu                sync.Mutex
	lastRun           map[string]time.Time
}

// pollingPeriod that we inspect cluster to look for opportunities to disrupt
const pollingPeriod = 10 * time.Second

type ControllerOptions struct {
	methods           []Method
	underutilizedPace *UnderutilizedConsolidationPace
}

func WithMethods(methods ...Method) option.Function[ControllerOptions] {
	return func(o *ControllerOptions) {
		o.methods = methods
	}
}

func WithUnderutilizedPace(pace *UnderutilizedConsolidationPace) option.Function[ControllerOptions] {
	return func(o *ControllerOptions) { o.underutilizedPace = pace }
}

func NewController(clk clock.Clock, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cp cloudprovider.CloudProvider, recorder events.Recorder, cluster *state.Cluster, queue *Queue, clusterCost *cost.ClusterCost, opts ...option.Function[ControllerOptions]) *Controller {

	o := option.Resolve(opts...)
	pace := o.underutilizedPace
	if pace == nil {
		pace = NewUnderutilizedConsolidationPace(clk)
	}
	methods := o.methods
	if methods == nil {
		methods = NewMethods(clk, cluster, kubeClient, provisioner, cp, recorder, queue, pace)
	}
	return &Controller{
		queue:             queue,
		clock:             clk,
		kubeClient:        kubeClient,
		apiReader:         kubeClient,
		cluster:           cluster,
		provisioner:       provisioner,
		recorder:          recorder,
		cloudProvider:     cp,
		clusterCost:       clusterCost,
		underutilizedPace: pace,
		lastRun:           map[string]time.Time{},
		methods:           methods,
	}
}

func NewMethods(clk clock.Clock, cluster *state.Cluster, kubeClient client.Client, provisioner *provisioning.Provisioner, cp cloudprovider.CloudProvider, recorder events.Recorder, queue *Queue, paces ...*UnderutilizedConsolidationPace) []Method {
	var pace *UnderutilizedConsolidationPace
	if len(paces) > 0 {
		pace = paces[0]
	}
	c := MakeConsolidation(clk, cluster, kubeClient, provisioner, cp, recorder, queue, pace)
	return []Method{
		// Delete empty nodes across all consolidation policies (WhenEmpty, WhenEmptyOrUnderutilized, Balanced).
		NewEmptiness(c),
		// Reclaim empty capacity in NodePools that opt in to score-based consolidation.
		NewScoreBasedReclamation(c),
		// Terminate and create replacement for drifted NodeClaims in Static NodePool
		NewStaticDrift(cluster, provisioner, cp),
		// Terminate any NodeClaims that have drifted from provisioning specifications, allowing the pods to reschedule.
		NewDrift(kubeClient, cluster, provisioner, recorder, clk),
		// Attempt to identify multiple NodeClaims that we can consolidate simultaneously to reduce pod churn
		NewMultiNodeConsolidation(c),
		// Compact non-empty nodes in NodePools that opt in via annotation.
		NewScoreBasedConsolidation(c),
		// And finally fall back our single NodeClaim consolidation to further reduce cluster cost.
		NewSingleNodeConsolidation(c),
	}
}

func (c *Controller) Name() string {
	return "disruption"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	c.apiReader = m.GetAPIReader()
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	// Give each disruption iteration its own trace and sampling decision.
	ctx, end := cxtracing.Start(cxtracing.WithoutSpan(ctx), "karpenter.disruption.loop")
	defer end()
	ctx = injection.WithControllerName(ctx, c.Name())

	// this won't catch if the reconciler loop hangs forever, but it will catch other issues
	c.logAbnormalRuns(ctx)
	defer c.logAbnormalRuns(ctx)
	c.recordRun("disruption-loop")

	// Log if there are any budgets that are misconfigured that weren't caught by validation.
	// Only validate the first reason, since CEL validation will catch invalid disruption reasons
	c.logInvalidBudgets(ctx)

	// We need to ensure that our internal cluster state mechanism is synced before we proceed
	// with making any scheduling decision off of our state nodes. Otherwise, we have the potential to make
	// a scheduling decision based on a smaller subset of nodes in our cluster state than actually exist.
	if !c.cluster.Synced(ctx) {
		return reconciler.Result{RequeueAfter: time.Second}, nil
	}

	if err := c.cleanupStaleDisruptionState(ctx); err != nil {
		if errors.IsConflict(err) {
			return reconciler.Result{Requeue: true}, nil
		}
		return reconciler.Result{}, err
	}

	// Attempt different disruption methods. We'll only let one method perform an action
	for _, m := range c.methods {
		c.recordRun(fmt.Sprintf("%T", m))
		methodCtx, endMethod := cxtracing.Start(ctx, disruptionMethodSpanName(m),
			attribute.String("method", fmt.Sprintf("%T", m)),
			attribute.String("reason", string(m.Reason())),
		)
		success, err := c.disrupt(methodCtx, m)
		endMethod()
		if success {
			DisruptionLoopIterationsTotal.Inc(map[string]string{disruptionLoopOutcomeLabel: disruptionLoopOutcomeCommand})
		}
		if err != nil {
			if errors.IsConflict(err) {
				return reconciler.Result{Requeue: true}, nil
			}
			return reconciler.Result{}, serrors.Wrap(fmt.Errorf("disrupting, %w", err), strings.ToLower(string(m.Reason())), "reason")
		}
		if success {
			return reconciler.Result{RequeueAfter: singleton.RequeueImmediately}, nil
		}
	}

	// All methods did nothing, so return nothing to do
	DisruptionLoopIterationsTotal.Inc(map[string]string{disruptionLoopOutcomeLabel: disruptionLoopOutcomeNoAction})
	return reconciler.Result{RequeueAfter: pollingPeriod}, nil
}

func disruptionMethodSpanName(method Method) string {
	switch method.(type) {
	case *Emptiness:
		return "karpenter.disruption.emptiness"
	case *ScoreBasedReclamation:
		return scoreBasedReclamationSpan
	case *StaticDrift:
		return "karpenter.disruption.static_drift"
	case *Drift:
		return "karpenter.disruption.drift"
	case *MultiNodeConsolidation:
		return "karpenter.disruption.multi_node_consolidation"
	case *ScoreBasedConsolidation:
		return scoreBasedConsolidationSpan
	case *SingleNodeConsolidation:
		return "karpenter.disruption.single_node_consolidation"
	default:
		return "karpenter.disruption.method"
	}
}

func (c *Controller) cleanupStaleDisruptionState(ctx context.Context) error {
	// Karpenter taints nodes with a karpenter.sh/disruption taint as part of the disruption process while it progresses in memory.
	// If Karpenter restarts or fails with an error during a disruption action, some nodes can be left tainted.
	// Idempotently remove this taint from candidates that are not in the orchestration queue before continuing.
	outdatedNodes := lo.Reject(c.cluster.DeepCopyNodes(), func(s *state.StateNode, _ int) bool {
		return c.queue.HasAny(s.ProviderID()) || s.MarkedForDeletion() || standby.IsNodeClaimActivating(s.NodeClaim)
	})
	standbyNodes := lo.Filter(outdatedNodes, func(s *state.StateNode, _ int) bool { return standby.IsNodeClaimStandby(s.NodeClaim) })
	if err := c.ensureStandbyTaints(ctx, standbyNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("restoring standby taint, %w", err), "taint", standby.NodeTaintKey)
	}
	if err := state.RequireNoScheduleTaint(ctx, c.kubeClient, false, outdatedNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("removing taint from nodes, %w", err), "taint", pretty.Taint(v1.DisruptedNoScheduleTaint))
	}
	if err := state.ClearNodeClaimsCondition(ctx, c.kubeClient, c.clock, v1.ConditionTypeDisruptionReason, outdatedNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("removing condition from nodeclaims, %w", err), "condition", v1.ConditionTypeDisruptionReason)
	}
	return nil
}

func (c *Controller) ensureStandbyTaints(ctx context.Context, nodes ...*state.StateNode) error {
	for _, stateNode := range nodes {
		if err := c.ensureStandbyTaint(ctx, stateNode); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) ensureStandbyTaint(ctx context.Context, stateNode *state.StateNode) error {
	if stateNode.Node == nil || stateNode.NodeClaim == nil {
		return nil
	}
	// Cluster state is informer-backed and may still report a standby marker
	// after provisioning has activated this node. Read the live Node first so
	// its optimistic-lock version protects against activation starting after
	// this snapshot, then check the live NodeClaim marker.
	node := &corev1.Node{}
	if err := c.apiReader.Get(ctx, client.ObjectKeyFromObject(stateNode.Node), node); err != nil {
		return client.IgnoreNotFound(err)
	}
	// Activation marks the live Node before removing the standby taint. The
	// optimistic-lock patch below makes a concurrent marker update conflict,
	// so the next reconciliation can observe activation.
	if node.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" {
		return nil
	}
	nodeClaim := &v1.NodeClaim{}
	if err := c.apiReader.Get(ctx, client.ObjectKeyFromObject(stateNode.NodeClaim), nodeClaim); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !standby.IsNodeClaimStandby(nodeClaim) || standby.IsNodeClaimActivating(nodeClaim) {
		return nil
	}
	stored := node.DeepCopy()
	standby.SetNodeTaint(node, true)
	if equality.Semantic.DeepEqual(stored, node) {
		return nil
	}
	return c.kubeClient.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (c *Controller) disrupt(ctx context.Context, disruption Method) (bool, error) {
	defer metrics.Measure(EvaluationDurationSeconds, map[string]string{
		metrics.ReasonLabel:    strings.ToLower(string(disruption.Reason())),
		ConsolidationTypeLabel: disruption.ConsolidationType(),
	})()
	candidates, nodePoolTotals, err := GetCandidatesWithTotals(ctx, c.cluster, c.kubeClient, c.recorder, c.clock, c.cloudProvider, disruption.ShouldDisrupt, disruption.Class(), c.queue, c.clusterCost)
	if err != nil {
		return false, fmt.Errorf("determining candidates, %w", err)
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.Int("candidate_count", len(candidates)))
	EligibleNodes.Set(float64(len(candidates)), map[string]string{
		metrics.ReasonLabel: strings.ToLower(string(disruption.Reason())),
	})

	// If there are no candidates, move to the next disruption
	if len(candidates) == 0 {
		logScoreBasedNoEligibleCandidates(ctx, disruption)
		// Reclamation normally refreshes its inventory while computing commands.
		// Refresh here as well because this controller skips ComputeCommands when there are no
		// candidates, but configured pools must still publish zero-valued inventory series.
		if scoreBased, ok := disruption.(*ScoreBasedReclamation); ok {
			if _, err := scoreBased.emptyReclamationNodeCounts(ctx); err != nil {
				return false, fmt.Errorf("updating score-based reclamation inventory, %w", err)
			}
		}
		return false, nil
	}
	// Pass precomputed NodePool totals to consolidation methods for balanced scoring
	if setter, ok := disruption.(NodePoolTotalsSetter); ok {
		setter.SetNodePoolTotals(nodePoolTotals)
	}
	disruptionBudgetMapping, err := c.budgetMappingForMethod(ctx, disruption)
	if err != nil {
		return false, fmt.Errorf("building disruption budgets, %w", err)
	}
	// Determine the disruption action
	cmds, computeErr := disruption.ComputeCommands(ctx, disruptionBudgetMapping, candidates...)
	cmds = lo.Filter(cmds, func(c Command, _ int) bool { return c.Decision() != NoOpDecision })
	if len(cmds) == 0 {
		if computeErr != nil {
			return false, fmt.Errorf("computing disruption decision, %w", computeErr)
		}
		return false, nil
	}

	// A method may return commands selected before a later part of the decision failed.
	// Start those commands, then report the decision error so the controller still retries.
	started, startErr := c.startCommands(ctx, disruption, cmds)
	if computeErr != nil {
		computeErr = fmt.Errorf("computing disruption decision, %w", computeErr)
	}
	if startErr != nil {
		startErr = fmt.Errorf("disrupting candidates, %w", startErr)
	}
	if err := multierr.Combine(computeErr, startErr); err != nil {
		return started > 0, err
	}
	return started > 0, nil
}

func (c *Controller) budgetMappingForMethod(ctx context.Context, disruption Method) (map[string]int, error) {
	if _, budgetExempt := disruption.(*ScoreBasedReclamation); budgetExempt {
		return map[string]int{}, nil
	}
	return BuildDisruptionBudgetMapping(ctx, c.cluster, c.clock, c.kubeClient, c.cloudProvider, c.recorder, disruption.Reason())
}

func (c *Controller) startCommands(ctx context.Context, disruption Method, cmds []Command) (int, error) {
	var started atomic.Int64
	errs := make([]error, len(cmds))
	paced := disruption.Reason() == v1.DisruptionReasonUnderutilized
	workqueue.ParallelizeUntil(ctx, len(cmds), len(cmds), func(i int) {
		cmd := cmds[i]

		// Assign common fields
		cmd.CreationTimestamp = c.clock.Now()
		cmd.ID = uuid.New()
		cmd.Method = disruption

		// Attempt to disrupt
		if err := c.queue.StartCommand(ctx, &cmd); err != nil {
			errs[i] = fmt.Errorf("disrupting candidates, %w", err)
			return
		}
		if paced {
			c.underutilizedPace.Charge(&cmd)
		}
		started.Add(1)
	})
	return int(started.Load()), multierr.Combine(errs...)
}

func (c *Controller) recordRun(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastRun[s] = c.clock.Now()
}

func (c *Controller) logAbnormalRuns(ctx context.Context) {
	const AbnormalTimeLimit = 15 * time.Minute
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, runTime := range c.lastRun {
		if timeSince := c.clock.Since(runTime); timeSince > AbnormalTimeLimit {
			log.FromContext(ctx).V(1).Info("abnormal time between runs", "name", name, "time_since", timeSince)
		}
	}
}

// logInvalidBudgets will log if there are any invalid schedules detected
func (c *Controller) logInvalidBudgets(ctx context.Context) {
	nps, err := nodepoolutils.ListManaged(ctx, c.kubeClient, c.cloudProvider)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed listing nodepools")
		return
	}
	var buf bytes.Buffer
	for _, np := range nps {
		// Use a dummy value of 100 since we only care if this errors.
		for _, method := range c.methods {
			if _, err := np.GetAllowedDisruptionsByReason(c.clock, 100, method.Reason()); err != nil {
				fmt.Fprintf(&buf, "invalid disruption budgets in nodepool %s, %s", np.Name, err)
				break // Prevent duplicate error message
			}
		}
	}
	if buf.Len() > 0 {
		log.FromContext(ctx).Error(stderrors.New(buf.String()), "detected disruption budget errors")
	}
}
