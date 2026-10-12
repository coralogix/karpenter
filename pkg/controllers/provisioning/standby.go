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

package provisioning

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func (p *Provisioner) solveWithStandbyActivation(
	ctx context.Context,
	pods []*corev1.Pod,
	nodes state.StateNodes,
	deletingPodUIDs sets.Set[types.UID],
	opts []pscheduling.Options,
) (pscheduling.Results, error) {
	s, err := p.NewScheduler(ctx, pods, cloneStateNodes(nodes.Active()), deletingPodUIDs, opts...)
	if err != nil {
		if errors.Is(err, ErrNodePoolsNotFound) {
			log.FromContext(ctx).Info("no dynamic nodepools found")
			p.cluster.MarkPodSchedulingDecisions(ctx, lo.SliceToMap(pods, func(p *corev1.Pod) (*corev1.Pod, error) {
				return p, fmt.Errorf("no dynamic nodepools found")
			}), nil, nil)
			return pscheduling.Results{}, nil
		}
		return pscheduling.Results{}, fmt.Errorf("creating scheduler, %w", err)
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	results, err := s.Solve(timeoutCtx, pods)
	cancel()
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return pscheduling.Results{}, err
	}
	results = results.TruncateInstanceTypes(ctx, pscheduling.MaxInstanceTypes)
	selected := selectedStandbyNodes(results)
	if len(selected) == 0 {
		return results, nil
	}
	actResult, actErr := p.activateScheduledStandbyNodes(ctx, selected)
	if actErr != nil {
		_ = standbypkg.ApplyActivationResult(ctx, p.cluster, selected, actResult)
		return pscheduling.Results{}, fmt.Errorf("activating standby capacity, %w", actErr)
	}
	return results, nil
}

func (p *Provisioner) resumeStandbyActivations(ctx context.Context, nodes state.StateNodes) error {
	activating := standbypkg.FilterActivating(nodes)
	if len(activating) == 0 {
		return nil
	}
	actResult, err := p.standbyCoordinator.Activate(ctx, standby.ActivationSourceRecovery, activating...)
	if err != nil {
		_ = standbypkg.ApplyActivationResult(ctx, p.cluster, activating, actResult)
		return err
	}
	return standbypkg.ApplyActivationResult(ctx, p.cluster, activating, actResult)
}

func selectedStandbyNodes(results pscheduling.Results) []*state.StateNode {
	return lo.FilterMap(results.ExistingNodes, func(node *pscheduling.ExistingNode, _ int) (*state.StateNode, bool) {
		if node == nil || len(node.Pods) == 0 || !standby.IsNodeClaimStandby(node.NodeClaim) {
			return nil, false
		}
		return node.StateNode, true
	})
}

func (p *Provisioner) activateScheduledStandbyNodes(ctx context.Context, nodes []*state.StateNode) (standbypkg.ActivationResult, error) {
	if len(nodes) == 0 {
		return standbypkg.ActivationResult{}, nil
	}
	actResult, err := p.standbyCoordinator.Activate(ctx, standby.ActivationSourceProvisioning, nodes...)
	if applyErr := standbypkg.ApplyActivationResult(ctx, p.cluster, nodes, actResult); applyErr != nil && err == nil {
		err = applyErr
	}
	return actResult, err
}
