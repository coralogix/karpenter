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
	"errors"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/serrors"
	"go.uber.org/multierr"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	terminatorevents "sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator/events"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

type evacuationProgress uint8

const (
	evacuationMarkedStandby evacuationProgress = iota + 1
	evacuationComplete
)

func (q *Queue) evacuate(ctx context.Context, cmd *Command) error {
	if cmd.evacuatedSources == nil {
		cmd.evacuatedSources = map[string]evacuationProgress{}
	}
	var errs error
	for _, candidate := range cmd.Candidates {
		if cmd.evacuatedSources[candidate.ProviderID()] == evacuationComplete {
			continue
		}
		done, err := q.evacuateSource(ctx, cmd, candidate)
		errs = multierr.Append(errs, err)
		if done {
			cmd.evacuatedSources[candidate.ProviderID()] = evacuationComplete
		}
	}
	return errs
}

//nolint:gocyclo // Eviction and standby cleanup share the source's progress.
func (q *Queue) evacuateSource(ctx context.Context, cmd *Command, candidate *Candidate) (bool, error) {
	if cmd.evacuatedSources[candidate.ProviderID()] != evacuationMarkedStandby {
		pods, err := nodeutils.GetPods(ctx, q.kubeClient, candidate.Node)
		if err != nil {
			return false, fmt.Errorf("listing evacuation pods, %w", err)
		}
		waiting := lo.Filter(pods, func(p *corev1.Pod, _ int) bool {
			return !podutils.IsOwnedByDaemonSet(p) && (podutils.IsReschedulable(p) || podutils.IsWaitingEviction(p, q.clock))
		})
		if len(waiting) > 0 {
			ordinary := lo.Reject(waiting, func(p *corev1.Pod, _ int) bool {
				return p.Spec.PriorityClassName == "system-cluster-critical" || p.Spec.PriorityClassName == "system-node-critical"
			})
			if len(ordinary) > 0 {
				waiting = ordinary
			}
			evictable := lo.Filter(waiting, func(p *corev1.Pod, _ int) bool { return podutils.IsEvictable(p, q.clock, q.recorder) })
			var errs error
			for _, pod := range evictable {
				errs = multierr.Append(errs, q.evictEvacuationPod(ctx, candidate.Node, pod, string(cmd.Reason())))
			}
			return false, multierr.Append(errs, fmt.Errorf("waiting for %d evacuation pods", len(waiting)))
		}
		// Persist standby before removing the temporary disruption taint.
		marked, err := standby.MarkIfPresent(ctx, q.kubeClient, candidate.Node, q.clock.Now())
		if err != nil {
			return false, fmt.Errorf("marking evacuated node standby, %w", err)
		}
		if !marked {
			return false, NewUnrecoverableError(fmt.Errorf("evacuation source is being deleted"))
		}
		cmd.evacuatedSources[candidate.ProviderID()] = evacuationMarkedStandby
		if len(cmd.evacuatedSources) == len(cmd.Candidates) && cmd.allSourcesStandbyAt.IsZero() {
			cmd.allSourcesStandbyAt = q.clock.Now()
		}
		metrics.NodeClaimsEvacuatedTotal.Inc(map[string]string{metrics.NodePoolLabel: candidate.NodeClaim.Labels[v1.NodePoolLabelKey], metrics.CapacityTypeLabel: candidate.NodeClaim.Labels[v1.CapacityTypeLabelKey]})
	}
	if err := state.RequireNoScheduleTaint(ctx, q.kubeClient, false, candidate.StateNode); err != nil {
		return false, err
	}
	if err := state.ClearNodeClaimsCondition(ctx, q.kubeClient, q.clock, v1.ConditionTypeDisruptionReason, candidate.StateNode); err != nil {
		return false, err
	}
	return true, nil
}

func (q *Queue) evictEvacuationPod(ctx context.Context, node *corev1.Node, pod *corev1.Pod, reason string) error {
	err := q.kubeClient.SubResource("eviction").Create(ctx, pod, &policyv1.Eviction{
		DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &pod.UID}},
	})
	if err == nil {
		q.recorder.Publish(terminatorevents.EvictPod(pod, reason))
		return nil
	}
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	var status apierrors.APIStatus
	multiplePDBs := errors.As(err, &status) && status.Status().Message == "This pod has more than one PodDisruptionBudget, which the eviction subresource does not support."
	if apierrors.IsTooManyRequests(err) || multiplePDBs {
		message := "evicting pod violates a PDB"
		if multiplePDBs {
			message = "eviction does not support multiple PDBs"
		}
		q.recorder.Publish(terminatorevents.NodeFailedToDrain(node, serrors.Wrap(errors.New(message), "Pod", klog.KRef(pod.Namespace, pod.Name))))
		return nil
	}
	return err
}

func (c Command) isScoreBasedEvacuation() bool {
	return c.Method != nil && c.ConsolidationType() == ScoreBasedConsolidationType
}

// evacuationTimeout returns how long a score-based evacuation command may run before it times out.
func evacuationTimeout(np *v1.NodePool, recorder events.Recorder) time.Duration {
	return readDurationAnnotation(np, recorder, v1.EvacuationTimeoutAnnotationKey, 5*time.Minute, events.InvalidEvacuationTimeout)
}
