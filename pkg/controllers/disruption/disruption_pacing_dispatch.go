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
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
)

type commandStartResult struct {
	err       error
	attempted bool
	started   bool
	command   *Command
	startTime time.Time
}

func (c *Controller) dispatchCommands(ctx context.Context, method Method, commands []Command) (bool, error) {
	paced := isDisruptionPaced(method)
	accepted, err := c.admitCommands(ctx, method, commands, paced)
	if err != nil {
		return false, err
	}
	if len(accepted) == 0 {
		return false, nil
	}

	results := c.startCommands(ctx, method, commands, accepted, c.queue.StartCommand)
	successfulStarts, errs, started := c.collectCommandResults(method, results)
	if paced {
		c.disruptionPacing.ChargePass(successfulStarts)
	}
	if err := multierr.Combine(errs...); err != nil {
		return false, fmt.Errorf("disrupting candidates, %w", err)
	}
	return started > 0, nil
}

func (c *Controller) admitCommands(ctx context.Context, method Method, commands []Command, paced bool) ([]int, error) {
	for i := range commands {
		commands[i].Method = method
	}
	if !paced {
		accepted := make([]int, len(commands))
		for i := range commands {
			accepted[i] = i
		}
		return accepted, nil
	}

	if err := c.refreshDisruptionPacingForAdmission(ctx); err != nil {
		if staticDrift, ok := method.(*StaticDrift); ok {
			for _, command := range commands {
				c.releaseStaticNodeCountReservation(staticDrift, command)
			}
		}
		return nil, fmt.Errorf("refreshing disruption pacing policy before dispatch, %w", err)
	}

	accepted := c.disruptionPacing.AdmitCommands(commands)
	if staticDrift, ok := method.(*StaticDrift); ok {
		releaseRejectedStaticReservations(c, staticDrift, commands, accepted)
	}
	return accepted, nil
}

func (c *Controller) startCommands(ctx context.Context, method Method, commands []Command, accepted []int, startCommand func(context.Context, *Command) error) []commandStartResult {
	results := make([]commandStartResult, len(accepted))
	for i, index := range accepted {
		results[i].command = &commands[index]
	}
	workqueue.ParallelizeUntil(ctx, len(accepted), len(accepted), func(i int) {
		command := commands[accepted[i]]
		results[i].attempted = true

		command.CreationTimestamp = c.clock.Now()
		command.ID = uuid.New()
		if err := startCommand(ctx, &command); err != nil {
			if staticDrift, ok := method.(*StaticDrift); ok && !command.staticNodeCountReservationHandedOff {
				c.releaseStaticNodeCountReservation(staticDrift, command)
			}
			results[i].err = fmt.Errorf("disrupting candidates, %w", err)
			return
		}
		results[i] = commandStartResult{
			attempted: true,
			started:   true,
			command:   &command,
			startTime: c.clock.Now(),
		}
	})
	return results
}

func (c *Controller) collectCommandResults(method Method, results []commandStartResult) ([]DisruptionPacingSuccessfulStart, []error, int) {
	successfulStarts := make([]DisruptionPacingSuccessfulStart, 0, len(results))
	errs := make([]error, len(results))
	started := 0
	for i, result := range results {
		errs[i] = result.err
		if !result.attempted {
			if staticDrift, ok := method.(*StaticDrift); ok && result.command != nil {
				c.releaseStaticNodeCountReservation(staticDrift, *result.command)
			}
			continue
		}
		if !result.started {
			continue
		}
		started++
		successfulStarts = append(successfulStarts, DisruptionPacingSuccessfulStart{
			Command:   result.command,
			StartTime: result.startTime,
		})
	}
	return successfulStarts, errs, started
}

func releaseRejectedStaticReservations(controller *Controller, method *StaticDrift, commands []Command, accepted []int) {
	acceptedSet := make(map[int]struct{}, len(accepted))
	for _, index := range accepted {
		acceptedSet[index] = struct{}{}
	}
	for i, command := range commands {
		if _, ok := acceptedSet[i]; !ok {
			controller.releaseStaticNodeCountReservation(method, command)
		}
	}
}

func isDisruptionPaced(method Method) bool {
	return method.Reason() == v1.DisruptionReasonUnderutilized || method.Reason() == v1.DisruptionReasonDrifted
}

func (c *Controller) refreshDisruptionPacing(ctx context.Context) error {
	return c.refreshDisruptionPacingFrom(ctx, c.kubeClient)
}

func (c *Controller) refreshDisruptionPacingForAdmission(ctx context.Context) error {
	reader := c.nodePoolReader
	if reader == nil {
		reader = c.kubeClient
	}
	return c.refreshDisruptionPacingFrom(ctx, reader)
}

func (c *Controller) refreshDisruptionPacingFrom(ctx context.Context, reader client.Reader) error {
	nodePools, err := listManagedNodePools(ctx, reader, c.cloudProvider)
	if err != nil {
		return fmt.Errorf("listing managed NodePools, %w", err)
	}
	if c.disruptionPacing.Refresh(nodePools, c.recorder) {
		c.cluster.MarkUnconsolidated()
	}
	return nil
}

func listManagedNodePools(ctx context.Context, reader client.Reader, cp cloudprovider.CloudProvider) ([]*v1.NodePool, error) {
	nodePoolList := &v1.NodePoolList{}
	if err := reader.List(ctx, nodePoolList); err != nil {
		return nil, err
	}
	return lo.FilterMap(nodePoolList.Items, func(np v1.NodePool, _ int) (*v1.NodePool, bool) {
		return &np, nodepoolutils.IsManaged(&np, cp)
	}), nil
}

func (c *Controller) releaseStaticNodeCountReservation(method *StaticDrift, command Command) {
	if method == nil || len(command.Candidates) == 0 {
		return
	}
	counts := map[string]int64{}
	for _, candidate := range command.Candidates {
		if candidate != nil && candidate.NodePool != nil {
			counts[candidate.NodePool.Name]++
		}
	}
	for poolName, count := range counts {
		c.cluster.NodePoolState.ReleaseNodeCount(poolName, count)
	}
}
