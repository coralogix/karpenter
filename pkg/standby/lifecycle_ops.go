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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

// readLivePair reads the Node before its NodeClaim, then verifies the original objects and provider ID.
func (c *Coordinator) readLivePair(ctx context.Context, nodeRef *corev1.Node, nodeClaimRef *v1.NodeClaim, providerID string) (*corev1.Node, *v1.NodeClaim, bool, error) {
	if nodeRef == nil || nodeClaimRef == nil {
		return nil, nil, false, fmt.Errorf("standby transition requires a Node and NodeClaim")
	}
	node := &corev1.Node{}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(nodeRef), node); err != nil {
		return nil, nil, false, client.IgnoreNotFound(err)
	}
	nodeClaim := &v1.NodeClaim{}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaimRef), nodeClaim); err != nil {
		return nil, nil, false, client.IgnoreNotFound(err)
	}
	if node.UID != nodeRef.UID || nodeClaim.UID != nodeClaimRef.UID ||
		node.Spec.ProviderID != providerID || nodeClaim.Status.ProviderID != providerID {
		return nil, nil, false, nil
	}
	return node, nodeClaim, true, nil
}

func (c *Coordinator) patchNodeClaimStandby(ctx context.Context, nodeClaim *v1.NodeClaim, standby bool, since time.Time) error {
	if nodeClaim == nil {
		return fmt.Errorf("cannot update standby marker on a missing NodeClaim")
	}
	stored := nodeClaim.DeepCopy()
	utilstandby.SetNodeClaimStandby(nodeClaim, standby, since)
	return c.writer.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (c *Coordinator) patchNodeStandbyTaint(ctx context.Context, node *corev1.Node, standby bool) error {
	if node == nil {
		return fmt.Errorf("cannot update standby taint on a missing Node")
	}
	stored := node.DeepCopy()
	utilstandby.SetNodeTaint(node, standby)
	return c.writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (c *Coordinator) applyNodeStandbyTaint(ctx context.Context, node *corev1.Node) error {
	stored := node.DeepCopy()
	utilstandby.SetNodeTaint(node, true)
	if equality.Semantic.DeepEqual(stored, node) {
		return nil
	}
	return c.writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

// EnsureStandbyTaintForClaim restores a missing taint only on the original live pair.
func (c *Coordinator) EnsureStandbyTaintForClaim(ctx context.Context, nodeRef *corev1.Node, nodeClaimRef *v1.NodeClaim, providerID string) error {
	node, nodeClaim, matches, err := c.readLivePair(ctx, nodeRef, nodeClaimRef, providerID)
	if err != nil || !matches {
		return err
	}
	if !node.DeletionTimestamp.IsZero() || !nodeClaim.DeletionTimestamp.IsZero() ||
		nodeClaim.StatusConditions().Get(v1.ConditionTypeInstanceTerminating).IsTrue() ||
		!utilstandby.IsNodeClaimStandby(nodeClaim) {
		return nil
	}
	return c.applyNodeStandbyTaint(ctx, node)
}
