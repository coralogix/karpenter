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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/pdb"
)

// reclamationValidator refreshes only selected empty-node candidates without applying
// NodePool disruption budgets. It retains NewCandidate's queue, Node, PDB, and pod annotation gates.
type reclamationValidator struct {
	validation
	reclamation *Reclamation
}

func (s *reclamationValidator) Validate(ctx context.Context, cmd Command, validationPeriod time.Duration) (Command, error) {
	if validationPeriod > 0 {
		select {
		case <-ctx.Done():
			return Command{}, errors.New("context canceled")
		case <-s.clock.After(validationPeriod):
		}
	}
	validatedCandidates, err := s.validateCandidates(ctx, cmd.Candidates...)
	if err != nil {
		return Command{}, err
	}
	// Refresh a second time to catch changes while the selected batch was being validated.
	validatedCandidates, err = s.validateCandidates(ctx, validatedCandidates...)
	if err != nil {
		return Command{}, err
	}
	cmd.Candidates = validatedCandidates
	return cmd, nil
}

func (s *reclamationValidator) validateCandidates(ctx context.Context, candidates ...*Candidate) ([]*Candidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	pdbLimits, err := pdb.NewLimits(ctx, s.kubeClient)
	if err != nil {
		return nil, fmt.Errorf("constructing reclamation validation candidates, tracking PodDisruptionBudgets, %w", err)
	}

	validated, err := s.refreshSelectedCandidates(ctx, candidates, pdbLimits)
	if err != nil {
		return nil, err
	}
	if err := s.validateCandidateSet(candidates, validated); err != nil {
		return nil, err
	}
	return validated, nil
}

func (s *reclamationValidator) refreshSelectedCandidates(ctx context.Context, candidates []*Candidate, pdbLimits pdb.Limits) ([]*Candidate, error) {
	stateNodes := s.selectedStateNodes(candidates)
	pools := reclamationValidationPoolCache{
		nodePools:     map[string]*v1.NodePool{},
		instanceTypes: map[string]map[string]*cloudprovider.InstanceType{},
	}
	validated := make([]*Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		if !reclamationCandidateHasIdentity(candidate) {
			continue
		}
		poolName := candidate.NodePool.Name
		pool, instanceTypes, err := pools.get(ctx, s, poolName)
		if err != nil {
			return nil, err
		}
		if pool == nil {
			continue
		}
		stateNode := stateNodes[candidate.ProviderID()]
		if stateNode == nil {
			continue
		}
		fresh, eligible := s.refreshSelectedCandidate(ctx, candidate, stateNode, pool, instanceTypes, pdbLimits)
		if eligible {
			validated = append(validated, fresh)
		}
	}
	return validated, nil
}

type reclamationValidationPoolCache struct {
	nodePools     map[string]*v1.NodePool
	instanceTypes map[string]map[string]*cloudprovider.InstanceType
}

func (c *reclamationValidationPoolCache) get(ctx context.Context, validator *reclamationValidator, name string) (*v1.NodePool, map[string]*cloudprovider.InstanceType, error) {
	if pool, ok := c.nodePools[name]; ok {
		return pool, c.instanceTypes[name], nil
	}
	pool, instanceTypes, eligible, err := validator.reclamationPool(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	if !eligible {
		c.nodePools[name] = nil
		return nil, nil, nil
	}
	c.nodePools[name] = pool
	c.instanceTypes[name] = instanceTypes
	return pool, instanceTypes, nil
}

func (s *reclamationValidator) validateCandidateSet(candidates, validated []*Candidate) error {
	if len(validated) != len(candidates) {
		invalid := len(candidates) - len(validated)
		FailedValidationsTotal.Add(float64(invalid), map[string]string{ConsolidationTypeLabel: ReclamationType})
		return NewChurnValidationError(fmt.Errorf("%d reclamation candidates are no longer valid", invalid))
	}
	for _, candidate := range validated {
		if s.cluster.IsNodeNominated(candidate.ProviderID()) {
			FailedValidationsTotal.Inc(map[string]string{ConsolidationTypeLabel: ReclamationType})
			return NewBudgetValidationError(fmt.Errorf("a reclamation candidate was nominated during validation"))
		}
	}
	return nil
}

// selectedStateNodes takes small snapshots while the cluster iterator holds its read lock. It
// preserves in-memory nominations and deletion marks without deep-copying unrelated fleet state.
func (s *reclamationValidator) selectedStateNodes(candidates []*Candidate) map[string]*state.StateNode {
	selected := make(map[string]*Candidate, len(candidates))
	for _, candidate := range candidates {
		if reclamationCandidateHasIdentity(candidate) {
			selected[candidate.ProviderID()] = candidate
		}
	}
	result := make(map[string]*state.StateNode, len(selected))
	for node := range s.cluster.Nodes() {
		providerID := node.ProviderID()
		candidate := selected[providerID]
		if !stateNodeMatchesReclamationCandidate(node, candidate) {
			continue
		}
		result[providerID] = node.DeepCopy()
	}
	return result
}

func reclamationCandidateHasIdentity(candidate *Candidate) bool {
	return candidate != nil && candidate.StateNode != nil && candidate.Node != nil && candidate.NodeClaim != nil && candidate.NodePool != nil
}

func stateNodeMatchesReclamationCandidate(node *state.StateNode, candidate *Candidate) bool {
	return candidate != nil && node != nil && node.Node != nil && node.NodeClaim != nil &&
		node.Node.UID == candidate.Node.UID && node.NodeClaim.UID == candidate.NodeClaim.UID
}

func (s *reclamationValidator) reclamationPool(ctx context.Context, name string) (*v1.NodePool, map[string]*cloudprovider.InstanceType, bool, error) {
	pool := &v1.NodePool{}
	if err := s.reclamation.apiReader().Get(ctx, types.NamespacedName{Name: name}, pool); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil, false, nil
		}
		return nil, nil, false, fmt.Errorf("getting NodePool for reclamation validation, %w", err)
	}
	if !standbyLifecycleEnabled(pool) || !nodepool.IsManaged(pool, s.cloudProvider) {
		return nil, nil, false, nil
	}
	typesForPool, err := s.cloudProvider.GetInstanceTypes(ctx, pool)
	if err != nil {
		if cloudprovider.IsUnevaluatedNodePoolError(err) {
			log.FromContext(ctx).WithValues("NodePool", pool.Name).Error(err, "skipping, node overlays are not applied")
		} else {
			log.FromContext(ctx).Error(err, "failed listing instance types", "nodepool", pool.Name)
		}
		return nil, nil, false, nil
	}
	if len(typesForPool) == 0 {
		return nil, nil, false, nil
	}
	instanceTypeMap := make(map[string]*cloudprovider.InstanceType, len(typesForPool))
	for _, instanceType := range typesForPool {
		instanceTypeMap[instanceType.Name] = instanceType
	}
	return pool, instanceTypeMap, true, nil
}

func (s *reclamationValidator) refreshSelectedCandidate(ctx context.Context, candidate *Candidate, stateNode *state.StateNode, pool *v1.NodePool,
	instanceTypes map[string]*cloudprovider.InstanceType, pdbLimits pdb.Limits,
) (*Candidate, bool) {
	reader := s.reclamation.apiReader()
	nodeClaim := &v1.NodeClaim{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(candidate.NodeClaim), nodeClaim); err != nil {
		return nil, false
	}
	node := &corev1.Node{}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(candidate.Node), node); err != nil {
		return nil, false
	}
	// Keep the selected objects' identity fixed across validation. A same-name replacement cannot
	// become the object authorized by the original reclamation decision.
	if nodeClaim.UID != candidate.NodeClaim.UID || node.UID != candidate.Node.UID ||
		nodeClaim.Status.ProviderID != candidate.ProviderID() || node.Spec.ProviderID != candidate.ProviderID() {
		return nil, false
	}
	stateNode = stateNode.ShallowCopy()
	stateNode.NodeClaim = nodeClaim
	stateNode.Node = node
	fresh, err := NewCandidate(ctx, s.kubeClient, s.recorder, s.clock, stateNode, pdbLimits,
		map[string]*v1.NodePool{pool.Name: pool}, map[string]map[string]*cloudprovider.InstanceType{pool.Name: instanceTypes}, s.queue, GracefulDisruptionClass)
	if err != nil {
		// GetCandidates treats candidate-construction failures as candidates that are no longer valid.
		return nil, false
	}
	if !s.reclamation.ShouldDisrupt(ctx, fresh) {
		return nil, false
	}
	return fresh, true
}
