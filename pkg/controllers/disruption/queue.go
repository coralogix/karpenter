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
	stderrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	operatorlogging "sigs.k8s.io/karpenter/pkg/operator/logging"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const (
	queueBaseDelay          = 1 * time.Second
	queueMaxDelay           = 10 * time.Second
	minRetryDuration        = 10 * time.Minute
	maxRetryDuration        = 1 * time.Hour
	maxConcurrentReconciles = 100
	retryDurationScale      = 80 * time.Millisecond
)

type UnrecoverableError struct {
	error
}

func NewUnrecoverableError(err error) *UnrecoverableError {
	return &UnrecoverableError{error: err}
}

func IsUnrecoverableError(err error) bool {
	if err == nil {
		return false
	}
	var unrecoverableError *UnrecoverableError
	return stderrors.As(err, &unrecoverableError)
}

func (q *Queue) GetMaxRetryDuration() time.Duration {
	q.RLock()
	numCommands := len(q.ProviderIDToCommand)
	q.RUnlock()
	retryDuration := retryDurationScale * time.Duration(numCommands)
	return lo.Clamp(retryDuration, minRetryDuration, maxRetryDuration)
}

type Queue struct {
	sync.RWMutex
	ProviderIDToCommand map[string]*Command // providerID -> command, maps a candidate to its command
	source              chan event.TypedGenericEvent[*v1.NodeClaim]
	kubeClient          client.Client
	recorder            events.Recorder
	cluster             *state.Cluster
	clock               clock.Clock
	provisioner         *provisioning.Provisioner
	evictionQueue       *terminator.Queue
}

// SetEvictionQueue supplies the shared queue used by the node termination controller for pod evictions.
func (q *Queue) SetEvictionQueue(evictionQueue *terminator.Queue) {
	q.evictionQueue = evictionQueue
}

// NewQueue creates a queue that will asynchronously orchestrate disruption commands
func NewQueue(kubeClient client.Client, recorder events.Recorder, cluster *state.Cluster, clock clock.Clock,
	provisioner *provisioning.Provisioner,
) *Queue {
	queue := &Queue{
		// nolint:staticcheck
		// We need to implement a deprecated interface since Command currently doesn't implement "comparable"
		source:              make(chan event.TypedGenericEvent[*v1.NodeClaim], 10000),
		ProviderIDToCommand: map[string]*Command{},
		kubeClient:          kubeClient,
		recorder:            recorder,
		cluster:             cluster,
		clock:               clock,
		provisioner:         provisioner,
	}
	return queue
}

func (q *Queue) Name() string {
	return "disruption.queue"
}

func (q *Queue) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(q.Name()).
		WatchesRawSource(source.Channel(q.source, &handler.TypedEnqueueRequestForObject[*v1.NodeClaim]{})).
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedMaxOfRateLimiter[reconcile.Request](
				workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](queueBaseDelay, queueMaxDelay),
				&workqueue.TypedBucketRateLimiter[reconcile.Request]{Limiter: rate.NewLimiter(rate.Limit(100), 1000)},
			),
			MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), 100, 1000),
		}).
		Complete(reconcile.AsReconciler(m.GetClient(), q))
}

func (q *Queue) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, q.Name())
	q.RLock()
	cmd, exists := q.ProviderIDToCommand[nodeClaim.Status.ProviderID]
	q.RUnlock()
	if !exists {
		log.FromContext(ctx).Error(fmt.Errorf("no command found"), "")
		return reconcile.Result{}, nil
	}
	baseLogContext := ctx
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues(cmd.LogValues()...))

	if err := q.waitOrTerminate(ctx, baseLogContext, cmd); err != nil {
		// If recoverable, re-queue and try again.
		if !IsUnrecoverableError(err) {
			return reconcile.Result{RequeueAfter: queueBaseDelay}, nil
		}
		// If the command failed, bail on the action.
		// 1. Emit metrics for launch failures
		// 2. Ensure cluster state no longer thinks these nodes are deleting
		// 3. Remove it from the Queue's internal data structure
		failedLaunches := lo.Filter(cmd.Replacements, func(r *Replacement, _ int) bool {
			return !r.Initialized
		})
		DisruptionQueueFailuresTotal.Add(float64(len(failedLaunches)), map[string]string{
			decisionLabel:          string(cmd.Decision()),
			metrics.ReasonLabel:    pretty.ToSnakeCase(string(cmd.Reason())),
			ConsolidationTypeLabel: string(cmd.Decision()),
		})
		if cmd.Action == EvacuateAction && q.evictionQueue != nil {
			q.evictionQueue.Cancel(cmd.evictionPods...)
		}
		stateNodes := lo.Map(cmd.Candidates, func(c *Candidate, _ int) *state.StateNode { return c.StateNode })
		var cleanupErr error
		if cmd.Action == EvacuateAction {
			cleanupErr = q.rollbackEvacuation(ctx, cmd.Candidates)
		} else {
			cleanupErr = state.RequireNoScheduleTaint(ctx, q.kubeClient, false, stateNodes...)
			cleanupErr = multierr.Combine(cleanupErr, state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, stateNodes...))
		}
		multiErr := multierr.Combine(err, cleanupErr)
		if cmd.Action == EvacuateAction {
			q.recordEvacuationOutcome(baseLogContext, cmd, evacuationOutcomeFailed, multiErr)
		} else {
			log.FromContext(ctx).Error(multiErr, "failed terminating nodes while executing a disruption command")
		}
	} else {
		cmd.Succeeded = true
		if cmd.Action == EvacuateAction {
			q.recordEvacuationOutcome(baseLogContext, cmd, evacuationOutcomeCompleted, nil)
		} else {
			log.FromContext(ctx).V(1).Info("command succeeded")
		}
	}
	q.CompleteCommand(cmd)
	return reconcile.Result{}, nil
}

func (q *Queue) recordEvacuationOutcome(ctx context.Context, cmd *Command, outcome string, err error) {
	duration := q.clock.Since(cmd.CreationTimestamp)
	reason := pretty.ToSnakeCase(string(cmd.Reason()))
	EvacuationCommandsTotal.Inc(map[string]string{
		evacuationOutcomeLabel: outcome,
		metrics.ReasonLabel:    reason,
	})
	EvacuationDurationSeconds.Observe(duration.Seconds(), map[string]string{
		metrics.ReasonLabel: reason,
	})

	logger := log.FromContext(ctx).WithValues(
		"command-id", cmd.ID.String(),
		"outcome", outcome,
		metrics.ReasonLabel, reason,
		"disrupted-node-count", len(cmd.Candidates),
		"duration", duration,
	)
	if err != nil {
		logger.Error(err, "evacuation failed")
		return
	}
	logger.Info("evacuation completed; source nodes handed off to standby")
}

// waitOrTerminate will wait until launched nodeclaims are ready.
// Once the replacements are ready, it will terminate the candidates.
// nolint:gocyclo
func (q *Queue) waitOrTerminate(ctx context.Context, callbackCtx context.Context, cmd *Command) (err error) {
	// We use the number of commands in the queue as a proxy for cloud provider traffic.
	// As the number of commands increase, we expect more delays and scale the retry duration accordingly.
	retryDuration := q.GetMaxRetryDuration()
	// Wrap an error in an unrecoverable error if it timed out
	defer func() {
		if q.clock.Since(cmd.CreationTimestamp) > retryDuration {
			err = NewUnrecoverableError(serrors.Wrap(fmt.Errorf("command reached timeout, %w", err), "duration", q.clock.Since(cmd.CreationTimestamp)))
		}
	}()
	waitErrs := make([]error, len(cmd.Replacements))
	for i := range cmd.Replacements {
		// If we know the node claim is Initialized, no need to check again.
		if cmd.Replacements[i].Initialized {
			continue
		}
		// Get the nodeclaim
		nodeClaim := &v1.NodeClaim{}
		if err := q.kubeClient.Get(ctx, types.NamespacedName{Name: cmd.Replacements[i].Name}, nodeClaim); err != nil {
			// The NodeClaim got deleted after an initial eventual consistency delay
			// This means that there was an ICE error or the Node initializationTTL expired
			// In this case, the error is unrecoverable, so don't requeue.
			if errors.IsNotFound(err) && !q.cluster.NodeClaimExists(cmd.Replacements[i].Name) {
				return NewUnrecoverableError(fmt.Errorf("replacement was deleted, %w", err))
			}
			waitErrs[i] = fmt.Errorf("getting node claim, %w", err)
			continue
		}
		// We emitted this event when disruption was blocked on launching/termination.
		// This does not block other forms of deprovisioning, but we should still emit this.
		q.recorder.Publish(disruptionevents.Launching(nodeClaim, string(cmd.Reason())))
		initializedStatus := nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized)
		if !initializedStatus.IsTrue() {
			q.recorder.Publish(disruptionevents.WaitingOnReadiness(nodeClaim))
			waitErrs[i] = serrors.Wrap(fmt.Errorf("nodeclaim not initialized"), "NodeClaim", klog.KRef("", nodeClaim.Name))
			continue
		}
		cmd.Replacements[i].Initialized = true
	}
	// If we have any errors, don't continue
	if err := multierr.Combine(waitErrs...); err != nil {
		return fmt.Errorf("waiting for replacement initialization, %w", err)
	}
	if cmd.Action == EvacuateAction {
		return q.evacuate(ctx, cmd)
	}
	if cmd.BeforeDelete != nil {
		if err := cmd.BeforeDelete(ctx); err != nil {
			return fmt.Errorf("validating nodes before deletion, %w", err)
		}
	}

	// All replacements have been provisioned.
	// All we need to do now is get a successful delete call for each node claim,
	// then the termination controller will handle the eventual deletion of the nodes.
	errs := make([]error, len(cmd.Candidates))
	workqueue.ParallelizeUntil(ctx, len(cmd.Candidates), len(cmd.Candidates), func(i int) {
		candidate := cmd.Candidates[i]
		providerID := candidate.ProviderID()
		q.Lock()
		alreadyDeleted := cmd.deletedCandidates != nil && cmd.deletedCandidates[providerID]
		callbackDone := cmd.deleteSuccessCallbacks != nil && cmd.deleteSuccessCallbacks[providerID]
		q.Unlock()
		if alreadyDeleted {
			if cmd.OnDeleteSuccess != nil && !callbackDone {
				if err := cmd.OnDeleteSuccess(ctx, candidate); err != nil {
					errs[i] = err
					return
				}
				q.Lock()
				if cmd.deleteSuccessCallbacks == nil {
					cmd.deleteSuccessCallbacks = map[string]bool{}
				}
				cmd.deleteSuccessCallbacks[providerID] = true
				q.Unlock()
			}
			return
		}
		if err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
			return q.kubeClient.Delete(ctx, candidate.NodeClaim)
		}); err != nil {
			errs[i] = client.IgnoreNotFound(err)
			return
		}
		q.Lock()
		if cmd.deletedCandidates == nil {
			cmd.deletedCandidates = map[string]bool{}
		}
		cmd.deletedCandidates[providerID] = true
		q.Unlock()
		q.recorder.Publish(disruptionevents.Terminating(candidate.Node, candidate.NodeClaim, string(cmd.Reason()))...)
		metrics.NodeClaimsDisruptedTotal.Inc(map[string]string{
			metrics.ReasonLabel:       pretty.ToSnakeCase(string(cmd.Reason())),
			metrics.NodePoolLabel:     candidate.NodeClaim.Labels[v1.NodePoolLabelKey],
			metrics.CapacityTypeLabel: candidate.NodeClaim.Labels[v1.CapacityTypeLabelKey],
		})
		if cmd.OnDeleteSuccess != nil {
			if err := cmd.OnDeleteSuccess(ctx, candidate); err != nil {
				errs[i] = err
				return
			}
			q.Lock()
			if cmd.deleteSuccessCallbacks == nil {
				cmd.deleteSuccessCallbacks = map[string]bool{}
			}
			cmd.deleteSuccessCallbacks[providerID] = true
			q.Unlock()
		}
	})
	// If there were any deletion failures, we should requeue.
	// In the case where we requeue, but the timeout for the command is reached, we'll mark this as a failure.
	if err := multierr.Combine(errs...); err != nil {
		return err
	}
	if cmd.OnSuccess != nil {
		return cmd.OnSuccess(callbackCtx)
	}
	return nil
}

// evacuate asks the shared termination queue to evict non-daemon pods, then hands off an empty node as standby.
func (q *Queue) evacuate(ctx context.Context, cmd *Command) error {
	if q.evictionQueue == nil {
		return NewUnrecoverableError(fmt.Errorf("evacuation requires the shared pod eviction queue"))
	}
	waiting, err := q.queueEvacuationPods(ctx, cmd)
	if err != nil {
		return err
	}
	if waiting > 0 {
		return fmt.Errorf("waiting for %d non-daemon pod(s) to leave evacuation nodes", waiting)
	}
	q.evictionQueue.Cancel(cmd.evictionPods...)
	return q.completeEvacuation(ctx, cmd)
}

func (q *Queue) queueEvacuationPods(ctx context.Context, cmd *Command) (int, error) {
	var waiting int
	queued := map[terminator.QueueKey]struct{}{}
	for _, pod := range cmd.evictionPods {
		queued[terminator.NewQueueKey(pod)] = struct{}{}
	}
	for _, candidate := range cmd.Candidates {
		if candidate.Node == nil || candidate.NodeClaim == nil {
			return 0, NewUnrecoverableError(fmt.Errorf("evacuation requires a registered node and NodeClaim"))
		}
		pods, err := nodeutils.GetPods(ctx, q.kubeClient, candidate.Node)
		if err != nil {
			return 0, fmt.Errorf("listing pods on evacuation node, %w", err)
		}
		nonDaemonPods := nonDaemonPods(pods)
		waiting += len(nonDaemonPods)
		evictable := lo.Filter(nonDaemonPods, func(p *corev1.Pod, _ int) bool { return podutils.IsEvictable(p) })
		q.addEvictablePods(cmd, queued, evictable)
	}
	return waiting, nil
}

func nonDaemonPods(pods []*corev1.Pod) []*corev1.Pod {
	return lo.Filter(pods, func(p *corev1.Pod, _ int) bool { return !podutils.IsOwnedByDaemonSet(p) })
}

func (q *Queue) addEvictablePods(cmd *Command, queued map[terminator.QueueKey]struct{}, pods []*corev1.Pod) {
	if len(pods) == 0 {
		return
	}
	q.evictionQueue.Add(pods...)
	for _, pod := range pods {
		key := terminator.NewQueueKey(pod)
		if _, exists := queued[key]; exists {
			continue
		}
		cmd.evictionPods = append(cmd.evictionPods, pod)
		queued[key] = struct{}{}
	}
}

func (q *Queue) completeEvacuation(ctx context.Context, cmd *Command) error {
	for _, candidate := range cmd.Candidates {
		if err := q.persistStandby(ctx, candidate); err != nil {
			return fmt.Errorf("persisting standby state, %w", err)
		}
	}
	stateNodes := lo.Map(cmd.Candidates, func(c *Candidate, _ int) *state.StateNode { return c.StateNode })
	if err := state.RequireNoScheduleTaint(ctx, q.kubeClient, false, stateNodes...); err != nil {
		return fmt.Errorf("removing temporary disruption taint after evacuation, %w", err)
	}
	if err := state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, stateNodes...); err != nil {
		return fmt.Errorf("clearing disruption reason after evacuation, %w", err)
	}
	return nil
}

// persistStandby writes the NodeClaim marker and standby taint before removing the temporary disruption taint.
func (q *Queue) persistStandby(ctx context.Context, candidate *Candidate) error {
	if err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := q.kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
			return err
		}
		stored := nodeClaim.DeepCopy()
		standby.SetNodeClaimStandby(nodeClaim, true)
		if equality.Semantic.DeepEqual(stored, nodeClaim) {
			return nil
		}
		return q.kubeClient.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	}); err != nil {
		return err
	}
	return q.ensureStandbyTaint(ctx, candidate.Node)
}

func (q *Queue) ensureStandbyTaint(ctx context.Context, sourceNode *corev1.Node) error {
	if sourceNode == nil {
		return fmt.Errorf("cannot restore standby taint without a Node")
	}
	return retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
		node := &corev1.Node{}
		if err := q.kubeClient.Get(ctx, client.ObjectKeyFromObject(sourceNode), node); err != nil {
			return err
		}
		stored := node.DeepCopy()
		standby.SetNodeTaint(node, true)
		if equality.Semantic.DeepEqual(stored, node) {
			return nil
		}
		return q.kubeClient.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	})
}

func (q *Queue) rollbackEvacuation(ctx context.Context, candidates []*Candidate) error {
	var uncordon []*state.StateNode
	var errs []error
	for _, candidate := range candidates {
		current := &v1.NodeClaim{}
		if err := q.kubeClient.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), current); err != nil {
			if errors.IsNotFound(err) {
				uncordon = append(uncordon, candidate.StateNode)
				continue
			}
			errs = append(errs, fmt.Errorf("checking standby marker before evacuation rollback, %w", err))
			continue
		}
		if standby.IsNodeClaimStandby(current) {
			if err := q.ensureStandbyTaint(ctx, candidate.Node); err != nil {
				// Keep the disruption taint until standby state is safe to preserve.
				errs = append(errs, fmt.Errorf("restoring standby taint before evacuation rollback, %w", err))
				continue
			}
		}
		uncordon = append(uncordon, candidate.StateNode)
	}
	uncordonErr := state.RequireNoScheduleTaint(ctx, q.kubeClient, false, uncordon...)
	stateNodes := lo.Map(candidates, func(c *Candidate, _ int) *state.StateNode { return c.StateNode })
	conditionErr := state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, stateNodes...)
	return multierr.Combine(append(errs, uncordonErr, conditionErr)...)
}

// markDisrupted taints the node and adds the Disrupted condition to the NodeClaim for a candidate that is about to be disrupted
// For static NodeClaims, we mark NodeClaims as pendingdisruption in statenodepool
func (q *Queue) markDisrupted(ctx context.Context, cmd *Command) ([]*Candidate, error) {
	errs := make([]error, len(cmd.Candidates))
	workqueue.ParallelizeUntil(ctx, len(cmd.Candidates), len(cmd.Candidates), func(i int) {
		if err := state.RequireNoScheduleTaint(ctx, q.kubeClient, true, cmd.Candidates[i].StateNode); err != nil {
			errs[i] = serrors.Wrap(fmt.Errorf("tainting nodes, %w", err), "taint", pretty.Taint(v1.DisruptedNoScheduleTaint))
			return
		}
		// refresh nodeclaim before updating status
		nodeClaim := &v1.NodeClaim{}
		if err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, func() error {
			if e := q.kubeClient.Get(ctx, client.ObjectKeyFromObject(cmd.Candidates[i].NodeClaim), nodeClaim); e != nil {
				return e
			}
			stored := nodeClaim.DeepCopy()
			nodeClaim.StatusConditions(status.WithClock(q.clock)).SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(cmd.Reason()), string(cmd.Reason()))
			return q.kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFrom(stored))
		}); err != nil {
			errs[i] = client.IgnoreNotFound(err)
			return
		}
	})
	var markedCandidates []*Candidate
	for i := range errs {
		if errs[i] != nil {
			continue
		}
		markedCandidates = append(markedCandidates, cmd.Candidates[i])

		// Mark all StaticNodeClaims as pendingdisruption in nodepoolstate
		if cmd.Candidates[i].OwnedByStaticNodePool() {
			q.cluster.NodePoolState.MarkNodeClaimPendingDisruption(cmd.Candidates[i].NodePool.Name, cmd.Candidates[i].NodeClaim.Name)
		}
	}
	return markedCandidates, multierr.Combine(errs...)
}

// createReplacementNodeClaims creates replacement NodeClaims
func (q *Queue) createReplacementNodeClaims(ctx context.Context, cmd *Command) error {
	nodeClaimNames, err := q.provisioner.CreateNodeClaims(ctx, lo.Map(cmd.Replacements, func(r *Replacement, _ int) *pscheduling.NodeClaim { return r.NodeClaim }), provisioning.WithReason(strings.ToLower(string(cmd.Reason()))))
	if err != nil {
		return err
	}
	if len(nodeClaimNames) != len(cmd.Replacements) {
		// shouldn't ever occur since a partially failed CreateNodeClaims should return an error
		return serrors.Wrap(fmt.Errorf("expected replacement count did not equal actual replacement count"), "expected-count", len(cmd.Replacements), "actual-count", len(nodeClaimNames))
	}
	for i, name := range nodeClaimNames {
		cmd.Replacements[i].Name = name
	}
	return nil
}

func (q *Queue) activateStandbyDestinations(ctx context.Context, cmd *Command) error {
	stateNodes := lo.FilterMap(cmd.Results.ExistingNodes, func(existing *pscheduling.ExistingNode, _ int) (*state.StateNode, bool) {
		if len(existing.Pods) == 0 || existing.StateNode == nil ||
			(!standby.IsNodeClaimStandby(existing.NodeClaim) && !standby.IsNodeClaimActivating(existing.NodeClaim)) {
			return nil, false
		}
		return existing.StateNode, true
	})
	if len(stateNodes) == 0 {
		return nil
	}
	return q.provisioner.ActivateStandbyNodes(ctx, standby.ActivationSourceCompaction, stateNodes...)
}

func (q *Queue) rollbackStart(ctx context.Context, cmd *Command, candidates []*Candidate, cause error) error {
	if cmd.Action == EvacuateAction && q.evictionQueue != nil {
		q.evictionQueue.Cancel(cmd.evictionPods...)
	}
	stateNodes := lo.Map(candidates, func(c *Candidate, _ int) *state.StateNode { return c.StateNode })
	rollbackErr := multierr.Combine(
		state.RequireNoScheduleTaint(ctx, q.kubeClient, false, stateNodes...),
		state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, stateNodes...),
	)
	q.cluster.UnmarkForDeletion(lo.Map(candidates, func(c *Candidate, _ int) string { return c.ProviderID() })...)
	return multierr.Combine(cause, rollbackErr)
}

// StartCommand will do the following:
// 1. Taint candidate nodes
// 2. Spin up replacement nodes
// 3. Add Command to the queue to wait to delete the candidates.
func (q *Queue) StartCommand(ctx context.Context, cmd *Command) error {
	// First check if we can add the command.
	providerIDs := lo.Map(cmd.Candidates, func(c *Candidate, _ int) string {
		return c.ProviderID()
	})
	if q.HasAny(providerIDs...) {
		return fmt.Errorf("candidate is being disrupted")
	}
	if err := q.activateStandbyDestinations(ctx, cmd); err != nil {
		return fmt.Errorf("activating standby destinations, %w", err)
	}

	log.FromContext(ctx).WithValues(append([]any{
		"command", cmd.String(),
	}, cmd.LogValues()...)...).Info("disrupting node(s)")

	// Cordon the old nodes before we launch the replacements to prevent new pods from scheduling to the old nodes
	markedCandidates, markDisruptedErr := q.markDisrupted(ctx, cmd)
	// If we get a failure marking some nodes as disrupted, if we are launching replacements, we shouldn't continue
	// with disrupting the candidates. If it's just a delete operation, we can proceed
	if markDisruptedErr != nil && (len(cmd.Replacements) > 0 || len(markedCandidates) == 0) {
		return q.rollbackStart(ctx, cmd, markedCandidates, serrors.Wrap(fmt.Errorf("marking disrupted, %w", markDisruptedErr), "command-id", cmd.ID))
	}

	// Update the command to only consider the successfully MarkDisrupted candidates
	cmd.Candidates = markedCandidates

	if err := q.createReplacementNodeClaims(ctx, cmd); err != nil {
		// If we failed to launch the replacement, don't disrupt.  If this is some permanent failure,
		// we don't want to disrupt workloads with no way to provision new nodes for them.
		return q.rollbackStart(ctx, cmd, cmd.Candidates, serrors.Wrap(fmt.Errorf("launching replacement nodeclaim, %w", err), "command-id", cmd.ID))
	}
	// IMPORTANT
	// We must MarkForDeletion AFTER we launch the replacements and not before
	// The reason for this is to avoid producing double-launches
	// If we MarkForDeletion before we create replacements, it's possible for the provisioner
	// to recognize that it needs to launch capacity for terminating pods, causing us to launch
	// capacity for these pods twice instead of just once
	q.cluster.MarkForDeletion(lo.Map(cmd.Candidates, func(c *Candidate, _ int) string { return c.ProviderID() })...)

	// Nominate each node for scheduling and emit pod nomination events
	// We emit all nominations before we exit the disruption loop as
	// we want to ensure that nodes that are nominated are respected in the subsequent
	// disruption reconciliation. This is essential in correctly modeling multiple
	// disruption commands in parallel.
	// This will only nominate nodes for 2 * batchingWindow. Once the candidates are
	// tainted with the Karpenter taint, the provisioning controller will continue
	// to do scheduling simulations and nominate the pods on the candidate nodes until
	// the node is cleaned up.
	cmd.Results.Record(log.IntoContext(ctx, operatorlogging.NopLogger), q.recorder, q.cluster)

	q.Lock()
	for _, c := range cmd.Candidates {
		q.ProviderIDToCommand[c.ProviderID()] = cmd
	}
	// IMPORTANT
	// We are adding the first nodeclaim in the list of candidates into the reconciliation queue
	// This invariant SHOULD NOT be relied on anywhere else besides within this file.
	q.source <- event.TypedGenericEvent[*v1.NodeClaim]{Object: cmd.Candidates[0].NodeClaim}
	q.Unlock()

	// An action is only performed and pods/nodes are only disrupted after a successful add to the queue
	nodePools := lo.Uniq(lo.Map(cmd.Candidates, func(c *Candidate, _ int) string {
		return c.NodePool.Name
	}))
	for _, nodePool := range nodePools {
		NodepoolDecisionsPerformed.Inc(map[string]string{
			metrics.NodePoolLabel:  nodePool,
			decisionLabel:          string(cmd.Decision()),
			metrics.ReasonLabel:    strings.ToLower(string(cmd.Reason())),
			ConsolidationTypeLabel: cmd.ConsolidationType(),
		})
	}
	DecisionsPerformedTotal.Inc(map[string]string{
		decisionLabel:          string(cmd.Decision()),
		metrics.ReasonLabel:    strings.ToLower(string(cmd.Reason())),
		ConsolidationTypeLabel: cmd.ConsolidationType(),
	})
	return nil
}

// HasAny checks to see if the candidate is part of an currently executing command.
func (q *Queue) HasAny(ids ...string) bool {
	q.RLock()
	defer q.RUnlock()

	// If the mapping has at least one of the candidates' providerIDs, return true.
	for _, id := range ids {
		if _, exists := q.ProviderIDToCommand[id]; exists {
			return true
		}
	}
	return false
}

// For TESTING ONLY
// This function is not thread safe as it returns pointers to commands.
// If you edit these commands returned, you can create race conditions.
func (q *Queue) GetCommands() []*Command {
	q.RLock()
	defer q.RUnlock()

	return lo.UniqValues(q.ProviderIDToCommand)
}

// CompleteCommand fully clears the queue of all references of a hash/command
func (q *Queue) CompleteCommand(cmd *Command) {
	if !cmd.Succeeded || cmd.Action == EvacuateAction {
		q.cluster.UnmarkForDeletion(lo.Map(cmd.Candidates, func(c *Candidate, _ int) string { return c.ProviderID() })...)
	}
	// Remove all candidates linked to the command
	q.Lock()
	defer q.Unlock()
	for _, c := range cmd.Candidates {
		delete(q.ProviderIDToCommand, c.ProviderID())
	}
}

func (q *Queue) IsEmpty() bool {
	q.RLock()
	defer q.RUnlock()
	return len(q.ProviderIDToCommand) == 0
}
