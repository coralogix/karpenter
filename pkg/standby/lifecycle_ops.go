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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/standbymetrics"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

type activationIdentity struct {
	nodeName      string
	nodeUID       types.UID
	nodeClaimName string
	nodeClaimUID  types.UID
	providerID    string
}

func activationIdentityFor(node *corev1.Node, nodeClaim *v1.NodeClaim, providerID string) (activationIdentity, error) {
	if node == nil || nodeClaim == nil {
		return activationIdentity{}, fmt.Errorf("standby activation requires a Node and NodeClaim")
	}
	if node.UID == "" || nodeClaim.UID == "" {
		return activationIdentity{}, fmt.Errorf("standby activation requires stable Node and NodeClaim identities")
	}
	if providerID == "" {
		return activationIdentity{}, fmt.Errorf("standby activation requires a provider ID")
	}
	return activationIdentity{
		nodeName:      node.Name,
		nodeUID:       node.UID,
		nodeClaimName: nodeClaim.Name,
		nodeClaimUID:  nodeClaim.UID,
		providerID:    providerID,
	}, nil
}

func (id activationIdentity) nodeRef() *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: id.nodeName, UID: id.nodeUID}}
}

func (id activationIdentity) nodeClaimRef() *v1.NodeClaim {
	return &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: id.nodeClaimName, UID: id.nodeClaimUID}}
}

func (id activationIdentity) nodeMatches(node *corev1.Node) bool {
	return node != nil && node.UID == id.nodeUID && node.Spec.ProviderID == id.providerID
}

func (id activationIdentity) nodeClaimMatches(nodeClaim *v1.NodeClaim) bool {
	return nodeClaim != nil && nodeClaim.UID == id.nodeClaimUID && nodeClaim.Status.ProviderID == id.providerID
}

func (id activationIdentity) assertNode(node *corev1.Node) error {
	if id.nodeMatches(node) {
		return nil
	}
	return fmt.Errorf("standby activation target Node %q was replaced", id.nodeName)
}

func (id activationIdentity) assertNodeClaim(nodeClaim *v1.NodeClaim) error {
	if id.nodeClaimMatches(nodeClaim) {
		return nil
	}
	return fmt.Errorf("standby activation target NodeClaim %q was replaced", id.nodeClaimName)
}

// readLivePair reads the Node before its NodeClaim, then verifies that both still identify the
// objects from the caller's snapshot. The ordering lets an optimistic Node patch protect a
// concurrent activation marker written before the NodeClaim check.
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

func (c *Coordinator) ensureStandbyTaintForClaim(ctx context.Context, nodeRef *corev1.Node, nodeClaimRef *v1.NodeClaim) error {
	if nodeRef == nil || nodeClaimRef == nil {
		return nil
	}
	node := &corev1.Node{}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(nodeRef), node); err != nil {
		return client.IgnoreNotFound(err)
	}
	if utilstandby.HasNodeActivationMarker(node) {
		return nil
	}
	nodeClaim := &v1.NodeClaim{}
	if err := c.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaimRef), nodeClaim); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !utilstandby.IsNodeClaimStandby(nodeClaim) || utilstandby.IsNodeClaimActivating(nodeClaim) {
		return nil
	}
	return c.applyNodeStandbyTaint(ctx, node)
}

func (c *Coordinator) activateNode(ctx context.Context, source utilstandby.ActivationSource, stateNode *state.StateNode) (*ActivationPair, error) {
	if stateNode == nil || stateNode.NodeClaim == nil {
		return nil, nil
	}
	if !utilstandby.IsNodeClaimStandby(stateNode.NodeClaim) && !utilstandby.IsNodeClaimActivating(stateNode.NodeClaim) &&
		!utilstandby.HasNodeActivationMarker(stateNode.Node) {
		return nil, nil
	}
	nodeSnapshot := stateNode.Node
	nodeClaimSnapshot := stateNode.NodeClaim
	providerID := stateNode.ProviderID()
	identity, err := activationIdentityFor(nodeSnapshot, nodeClaimSnapshot, providerID)
	if err != nil {
		return nil, err
	}
	if err := c.runActivation(ctx, identity, nodeSnapshot, source, func(resolvedSource utilstandby.ActivationSource) {
		standbymetrics.RecordActivation(ctx, stateNode, resolvedSource)
	}); err != nil {
		return nil, err
	}
	return c.confirmedActivationPair(ctx, nodeSnapshot, nodeClaimSnapshot, providerID)
}

func (c *Coordinator) confirmedActivationPair(ctx context.Context, nodeRef *corev1.Node, nodeClaimRef *v1.NodeClaim, providerID string) (*ActivationPair, error) {
	node, nodeClaim, matches, err := c.readLivePair(ctx, nodeRef, nodeClaimRef, providerID)
	if err != nil {
		return nil, err
	}
	if !matches {
		return nil, fmt.Errorf("activated standby destination %q was replaced", nodeClaimRef.Name)
	}
	if !node.DeletionTimestamp.IsZero() || !nodeClaim.DeletionTimestamp.IsZero() {
		return nil, fmt.Errorf("activated standby destination %q is deleting", nodeClaimRef.Name)
	}
	if !ActivationComplete(node, nodeClaim) {
		return nil, fmt.Errorf("standby activation for NodeClaim %q has not completed", nodeClaimRef.Name)
	}
	return &ActivationPair{ProviderID: providerID, Node: node, NodeClaim: nodeClaim}, nil
}

func (c *Coordinator) runActivation(ctx context.Context, id activationIdentity, nodeSnapshot *corev1.Node, requestedSource utilstandby.ActivationSource, onActivated func(utilstandby.ActivationSource)) error {
	_, _, matches, err := c.readLivePair(ctx, id.nodeRef(), id.nodeClaimRef(), id.providerID)
	if err != nil {
		return err
	}
	if !matches {
		return fmt.Errorf("standby activation target NodeClaim %q was replaced", id.nodeClaimName)
	}
	started, source, err := c.reserveActivation(ctx, id, requestedSource)
	if err != nil {
		return fmt.Errorf("reserving nodeclaim activation, %w", err)
	}
	if !started && !utilstandby.HasNodeActivationMarker(nodeSnapshot) {
		return nil
	}
	if nodeSnapshot == nil {
		return fmt.Errorf("nodeclaim has no registered node")
	}
	if err := c.setNodeActivationMarker(ctx, id, true); err != nil {
		return fmt.Errorf("marking node activation, %w", err)
	}
	activated, err := c.removeNodeStandbyTaint(ctx, id)
	if err != nil {
		return fmt.Errorf("removing standby taint, %w", err)
	}
	if activated && onActivated != nil {
		onActivated(source)
	}
	return c.completeActivation(ctx, id)
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

func (c *Coordinator) reserveActivation(ctx context.Context, id activationIdentity, requestedSource utilstandby.ActivationSource) (bool, utilstandby.ActivationSource, error) {
	var started bool
	source := utilstandby.ActivationSourceRecovery
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := c.reader.Get(ctx, client.ObjectKeyFromObject(id.nodeClaimRef()), nodeClaim); err != nil {
			return err
		}
		if err := id.assertNodeClaim(nodeClaim); err != nil {
			return err
		}
		if !nodeClaim.DeletionTimestamp.IsZero() {
			return fmt.Errorf("nodeclaim is deleting")
		}
		if utilstandby.IsNodeClaimActivating(nodeClaim) {
			started = true
			source = normalizeActivationSource(utilstandby.NodeClaimActivationSource(nodeClaim))
			return nil
		}
		if !utilstandby.IsNodeClaimStandby(nodeClaim) {
			return nil
		}
		stored := nodeClaim.DeepCopy()
		source = normalizeActivationSource(requestedSource)
		utilstandby.SetNodeClaimActivating(nodeClaim, true)
		utilstandby.SetNodeClaimActivationSource(nodeClaim, source)
		if err := c.writer.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		started = true
		return nil
	})
	return started, source, err
}

func (c *Coordinator) setNodeActivationMarker(ctx context.Context, id activationIdentity, activating bool) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node := &corev1.Node{}
		if err := c.reader.Get(ctx, types.NamespacedName{Name: id.nodeName}, node); err != nil {
			return err
		}
		if err := id.assertNode(node); err != nil {
			return err
		}
		if activating == utilstandby.HasNodeActivationMarker(node) {
			return nil
		}
		stored := node.DeepCopy()
		if activating {
			if node.Annotations == nil {
				node.Annotations = map[string]string{}
			}
			node.Annotations[utilstandby.NodeClaimActivatingAnnotationKey] = "true"
		} else if node.Annotations != nil {
			delete(node.Annotations, utilstandby.NodeClaimActivatingAnnotationKey)
		}
		return c.writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	})
}

func (c *Coordinator) removeNodeStandbyTaint(ctx context.Context, id activationIdentity) (bool, error) {
	var removed bool
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node := &corev1.Node{}
		if err := c.reader.Get(ctx, types.NamespacedName{Name: id.nodeName}, node); err != nil {
			return err
		}
		if err := id.assertNode(node); err != nil {
			return err
		}
		if !utilstandby.HasNodeTaint(node) {
			return nil
		}
		if err := c.patchNodeStandbyTaint(ctx, node, false); err != nil {
			return err
		}
		removed = true
		return nil
	})
	return removed, err
}

func (c *Coordinator) completeActivation(ctx context.Context, id activationIdentity) error {
	if err := c.finishActivation(ctx, id); err != nil {
		return fmt.Errorf("clearing activation markers, %w", err)
	}
	if err := c.setNodeActivationMarker(ctx, id, false); err != nil {
		return fmt.Errorf("clearing node activation marker, %w", err)
	}
	return nil
}

func (c *Coordinator) finishActivation(ctx context.Context, id activationIdentity) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := c.reader.Get(ctx, client.ObjectKeyFromObject(id.nodeClaimRef()), nodeClaim); err != nil {
			return err
		}
		if err := id.assertNodeClaim(nodeClaim); err != nil {
			return err
		}
		if !utilstandby.IsNodeClaimStandby(nodeClaim) && !utilstandby.IsNodeClaimActivating(nodeClaim) && utilstandby.NodeClaimActivationSource(nodeClaim) == "" {
			return nil
		}
		stored := nodeClaim.DeepCopy()
		utilstandby.SetNodeClaimStandby(nodeClaim, false, time.Time{})
		utilstandby.SetNodeClaimActivating(nodeClaim, false)
		utilstandby.SetNodeClaimActivationSource(nodeClaim, "")
		return c.writer.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	})
}

func normalizeActivationSource(source utilstandby.ActivationSource) utilstandby.ActivationSource {
	switch source {
	case utilstandby.ActivationSourceProvisioning, utilstandby.ActivationSourceCompaction:
		return source
	default:
		return utilstandby.ActivationSourceRecovery
	}
}
