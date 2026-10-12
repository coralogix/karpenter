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

package standby

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
)

// ActivationPair is a live Node and NodeClaim that completed activation with stable identity.
type ActivationPair struct {
	ProviderID string
	Node       *corev1.Node
	NodeClaim  *v1.NodeClaim
}

// ActivationResult contains standby destinations that finished activation during the call.
type ActivationResult struct {
	Activated []ActivationPair
}

// ApplyActivationResult updates cluster state and caller snapshots for activated pairs.
func ApplyActivationResult(ctx context.Context, cluster *state.Cluster, nodes []*state.StateNode, result ActivationResult) error {
	if cluster == nil || len(result.Activated) == 0 {
		return nil
	}
	byProvider := make(map[string]ActivationPair, len(result.Activated))
	for _, pair := range result.Activated {
		byProvider[pair.ProviderID] = pair
	}
	var errs []error
	for _, stateNode := range nodes {
		if stateNode == nil {
			continue
		}
		pair, ok := byProvider[stateNode.ProviderID()]
		if !ok {
			continue
		}
		cluster.UpdateNodeClaim(pair.NodeClaim)
		stateNode.NodeClaim = pair.NodeClaim
		if pair.Node == nil {
			continue
		}
		if err := cluster.UpdateNode(ctx, pair.Node); err != nil {
			errs = append(errs, fmt.Errorf("updating cluster state for activated node %q, %w", pair.Node.Name, err))
			continue
		}
		stateNode.Node = pair.Node
	}
	if len(errs) > 0 {
		return fmt.Errorf("applying standby activation result, %v", errs)
	}
	return nil
}
