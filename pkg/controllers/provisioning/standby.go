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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	standbypkg "sigs.k8s.io/karpenter/pkg/standby"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const maxStandbyActivationSchedulingAttempts = 3

func (p *Provisioner) solveWithStandbyActivation(
	ctx context.Context,
	pods []*corev1.Pod,
	nodes state.StateNodes,
	deletingPodUIDs sets.Set[types.UID],
	opts []pscheduling.Options,
) (pscheduling.Results, error) {
	var results pscheduling.Results
	for attempt := 0; attempt < maxStandbyActivationSchedulingAttempts; attempt++ {
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
		results, err = s.Solve(timeoutCtx, pods)
		cancel()
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return pscheduling.Results{}, err
		}
		results = results.TruncateInstanceTypes(ctx, pscheduling.MaxInstanceTypes)
		selected := selectedStandbyNodes(results)
		if len(selected) == 0 {
			return results, nil
		}
		actErr := p.activateScheduledStandbyNodes(ctx, results)
		if actErr == nil {
			return results, nil
		}
		if !lo.SomeBy(selected, func(node *state.StateNode) bool {
			return standbypkg.ActivationComplete(node.Node, node.NodeClaim)
		}) {
			return pscheduling.Results{}, fmt.Errorf("activating standby capacity, %w", actErr)
		}
		nodes = p.cluster.DeepCopyNodes()
		if attempt == maxStandbyActivationSchedulingAttempts-1 {
			return pscheduling.Results{}, fmt.Errorf("activating standby capacity, %w", actErr)
		}
	}
	return results, nil
}

func (p *Provisioner) resumeStandbyActivations(ctx context.Context, nodes state.StateNodes) error {
	activating := standbypkg.FilterActivating(nodes)
	if len(activating) == 0 {
		return nil
	}
	if err := p.standbyCoordinator.Activate(ctx, standby.ActivationSourceRecovery, activating...); err != nil {
		return err
	}
	return p.refreshActivatedStandbyNodes(ctx, activating)
}

func selectedStandbyNodes(results pscheduling.Results) []*state.StateNode {
	return lo.FilterMap(results.ExistingNodes, func(node *pscheduling.ExistingNode, _ int) (*state.StateNode, bool) {
		if node == nil || len(node.Pods) == 0 || !standby.IsNodeClaimStandby(node.NodeClaim) {
			return nil, false
		}
		return node.StateNode, true
	})
}

func (p *Provisioner) activateScheduledStandbyNodes(ctx context.Context, results pscheduling.Results) error {
	selected := selectedStandbyNodes(results)
	if len(selected) == 0 {
		return nil
	}
	err := p.standbyCoordinator.Activate(ctx, standby.ActivationSourceProvisioning, selected...)
	if refreshErr := p.refreshActivatedStandbyNodes(ctx, selected); refreshErr != nil {
		return refreshErr
	}
	return err
}

func (p *Provisioner) refreshActivatedStandbyNodes(ctx context.Context, nodes []*state.StateNode) error {
	var errs []error
	for _, stateNode := range nodes {
		if stateNode == nil || stateNode.NodeClaim == nil {
			continue
		}
		if !standbypkg.ActivationComplete(stateNode.Node, stateNode.NodeClaim) {
			continue
		}
		nodeClaim := &v1.NodeClaim{}
		if err := p.kubeClient.Get(ctx, client.ObjectKeyFromObject(stateNode.NodeClaim), nodeClaim); err != nil {
			errs = append(errs, fmt.Errorf("refreshing activated nodeclaim %q, %w", stateNode.NodeClaim.Name, err))
			continue
		}
		p.cluster.UpdateNodeClaim(nodeClaim)
		stateNode.NodeClaim = nodeClaim
		if stateNode.Node == nil {
			continue
		}
		node := &corev1.Node{}
		if err := p.kubeClient.Get(ctx, types.NamespacedName{Name: stateNode.Node.Name}, node); err != nil {
			errs = append(errs, fmt.Errorf("refreshing activated node %q, %w", stateNode.Node.Name, err))
			continue
		}
		if err := p.cluster.UpdateNode(ctx, node); err != nil {
			errs = append(errs, fmt.Errorf("updating cluster state for activated node %q, %w", node.Name, err))
			continue
		}
		stateNode.Node = node
	}
	if len(errs) > 0 {
		return fmt.Errorf("refreshing activated standby nodes, %v", errs)
	}
	return nil
}
