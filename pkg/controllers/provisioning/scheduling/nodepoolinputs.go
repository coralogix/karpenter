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

package scheduling

import (
	"context"

	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	karpopts "sigs.k8s.io/karpenter/pkg/operator/options"
)

type NodePoolInputs struct {
	nodePools     []*v1.NodePool
	instanceTypes map[string][]*cloudprovider.InstanceType
	domainGroups  map[string]TopologyDomainGroup
	// nodeClaimTemplates is immutable once built and is shared by every Scheduler created from these inputs,
	// including the concurrent simulations of a score-based search.
	nodeClaimTemplates []*NodeClaimTemplate
}

func NewNodePoolInputs(ctx context.Context, recorder events.Recorder, nodePools []*v1.NodePool,
	instanceTypes map[string][]*cloudprovider.InstanceType, opts ...Options,
) *NodePoolInputs {
	_, stop := MeasureNewSchedulerPhase(ctx, PhaseBuildDomainGroups)
	domainGroups := buildDomainGroups(nodePools, instanceTypes)
	stop()

	minValuesPolicy := option.Resolve(opts...).minValuesPolicy
	_, stop = MeasureNewSchedulerPhase(ctx, PhaseFilterInstanceTypes)
	nodeClaimTemplates := newNodeClaimTemplates(ctx, recorder, nodePools, instanceTypes, minValuesPolicy)
	stop()

	return &NodePoolInputs{
		nodePools:          nodePools,
		instanceTypes:      instanceTypes,
		domainGroups:       domainGroups,
		nodeClaimTemplates: nodeClaimTemplates,
	}
}

func newNodeClaimTemplates(ctx context.Context, recorder events.Recorder, nodePools []*v1.NodePool,
	instanceTypes map[string][]*cloudprovider.InstanceType, minValuesPolicy karpopts.MinValuesPolicy,
) []*NodeClaimTemplate {
	// Pre-filter instance types eligible for NodePools to reduce work done during scheduling loops for pods.
	// if no templates remain, we still want to build the scheduler so that Karpenter can ack pods which can schedule to existing and in-flight capacity
	return lo.FilterMap(nodePools, func(np *v1.NodePool, _ int) (*NodeClaimTemplate, bool) {
		var err error
		nct := NewNodeClaimTemplate(np)
		nct.InstanceTypeOptions, _, err = filterInstanceTypesByRequirements(instanceTypes[np.Name], nct.Requirements, corev1.ResourceList{}, corev1.ResourceList{}, corev1.ResourceList{}, minValuesPolicy == karpopts.MinValuesPolicyBestEffort)
		if len(nct.InstanceTypeOptions) == 0 {
			if instanceTypeFilterErr, ok := lo.ErrorsAs[InstanceTypeFilterError](err); ok && instanceTypeFilterErr.minValuesIncompatibleErr != nil {
				recorder.Publish(NoCompatibleInstanceTypes(np, true))
				log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Info("skipping, nodepool requirements filtered out all instance types", "minValuesIncompatibleErr", instanceTypeFilterErr.minValuesIncompatibleErr)
			} else {
				recorder.Publish(NoCompatibleInstanceTypes(np, false))
				log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Info("skipping, nodepool requirements filtered out all instance types")
			}
			return nil, false
		}
		return nct, true
	})
}
