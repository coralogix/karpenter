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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Lifecycle coordinates the live API mutations that move capacity between active and standby.
// Construct it at the call site so controllers use the current API reader, including one
// registered after their construction.
type Lifecycle struct {
	reader client.Reader
	writer client.Client
}

func NewLifecycle(reader client.Reader, writer client.Client) Lifecycle {
	if reader == nil {
		reader = writer
	}
	return Lifecycle{reader: reader, writer: writer}
}

// ReadLivePair reads the Node before its NodeClaim, then verifies that both still identify the
// objects from the caller's snapshot. The ordering lets an optimistic Node patch protect a
// concurrent activation marker written before the NodeClaim check.
func (l Lifecycle) ReadLivePair(ctx context.Context, nodeRef *corev1.Node, nodeClaimRef *v1.NodeClaim, providerID string) (*corev1.Node, *v1.NodeClaim, bool, error) {
	if nodeRef == nil || nodeClaimRef == nil {
		return nil, nil, false, fmt.Errorf("standby transition requires a Node and NodeClaim")
	}
	node := &corev1.Node{}
	if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeRef), node); err != nil {
		return nil, nil, false, client.IgnoreNotFound(err)
	}
	nodeClaim := &v1.NodeClaim{}
	if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaimRef), nodeClaim); err != nil {
		return nil, nil, false, client.IgnoreNotFound(err)
	}
	if node.UID != nodeRef.UID || nodeClaim.UID != nodeClaimRef.UID || node.Spec.ProviderID != providerID {
		return nil, nil, false, nil
	}
	return node, nodeClaim, true, nil
}

// EnsureNodeStandbyTaint restores the taint from a live Node read. Callers decide whether and
// how to retry, which keeps evacuation's retry behavior distinct from periodic cleanup.
func (l Lifecycle) EnsureNodeStandbyTaint(ctx context.Context, nodeRef *corev1.Node) error {
	if nodeRef == nil {
		return fmt.Errorf("cannot restore standby taint without a Node")
	}
	node := &corev1.Node{}
	if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeRef), node); err != nil {
		return err
	}
	return l.ensureNodeStandbyTaint(ctx, node)
}

// EnsureStandbyTaintForClaim restores a standby taint only while the live NodeClaim remains
// standby. It reads the Node first and uses that resourceVersion for the patch, so an activation
// marker written concurrently causes a conflict instead of re-tainting activated capacity.
func (l Lifecycle) EnsureStandbyTaintForClaim(ctx context.Context, nodeRef *corev1.Node, nodeClaimRef *v1.NodeClaim) error {
	if nodeRef == nil || nodeClaimRef == nil {
		return nil
	}
	node := &corev1.Node{}
	if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeRef), node); err != nil {
		return client.IgnoreNotFound(err)
	}
	if HasNodeActivationMarker(node) {
		return nil
	}
	nodeClaim := &v1.NodeClaim{}
	if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaimRef), nodeClaim); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !IsNodeClaimStandby(nodeClaim) || IsNodeClaimActivating(nodeClaim) {
		return nil
	}
	return l.ensureNodeStandbyTaint(ctx, node)
}

// Activate runs the shared activation protocol. onActivated is called immediately after the
// standby taint is removed, matching the point at which activation metrics are recorded.
func (l Lifecycle) Activate(ctx context.Context, nodeSnapshot *corev1.Node, nodeClaimSnapshot *v1.NodeClaim, requestedSource ActivationSource, onActivated func(ActivationSource)) error {
	if nodeClaimSnapshot == nil {
		return fmt.Errorf("reserving nodeclaim activation, missing NodeClaim")
	}
	started, source, err := l.reserveActivation(ctx, nodeClaimSnapshot, requestedSource)
	if err != nil {
		return fmt.Errorf("reserving nodeclaim activation, %w", err)
	}
	if !started && !HasNodeActivationMarker(nodeSnapshot) {
		return nil
	}
	if nodeSnapshot == nil {
		return fmt.Errorf("nodeclaim has no registered node")
	}
	if err := l.setNodeActivationMarker(ctx, nodeSnapshot.Name, true); err != nil {
		return fmt.Errorf("marking node activation, %w", err)
	}
	activated, err := l.removeNodeStandbyTaint(ctx, nodeSnapshot.Name)
	if err != nil {
		return fmt.Errorf("removing standby taint, %w", err)
	}
	if activated && onActivated != nil {
		onActivated(source)
	}
	return l.completeActivation(ctx, nodeSnapshot.Name, nodeClaimSnapshot)
}

// PatchNodeClaimStandby changes the marker on a freshly-read NodeClaim using its resourceVersion.
// It always patches, even when the marker is unchanged; callers own idempotency checks so this
// write can remain a concurrency barrier for a transition.
func (l Lifecycle) PatchNodeClaimStandby(ctx context.Context, nodeClaim *v1.NodeClaim, standby bool, since time.Time) error {
	if nodeClaim == nil {
		return fmt.Errorf("cannot update standby marker on a missing NodeClaim")
	}
	stored := nodeClaim.DeepCopy()
	SetNodeClaimStandby(nodeClaim, standby, since)
	return l.writer.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

// PatchNodeStandbyTaint changes the standby taint on a live Node using its resourceVersion. It
// always patches, even when the taint is unchanged; callers own idempotency checks so this write
// can remain a concurrency barrier for a transition.
func (l Lifecycle) PatchNodeStandbyTaint(ctx context.Context, node *corev1.Node, standby bool) error {
	if node == nil {
		return fmt.Errorf("cannot update standby taint on a missing Node")
	}
	stored := node.DeepCopy()
	SetNodeTaint(node, standby)
	return l.writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (l Lifecycle) ensureNodeStandbyTaint(ctx context.Context, node *corev1.Node) error {
	stored := node.DeepCopy()
	SetNodeTaint(node, true)
	if equality.Semantic.DeepEqual(stored, node) {
		return nil
	}
	return l.writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (l Lifecycle) reserveActivation(ctx context.Context, nodeClaimRef *v1.NodeClaim, requestedSource ActivationSource) (bool, ActivationSource, error) {
	var started bool
	source := ActivationSourceRecovery
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaimRef), nodeClaim); err != nil {
			return err
		}
		if !nodeClaim.DeletionTimestamp.IsZero() {
			return fmt.Errorf("nodeclaim is deleting")
		}
		if IsNodeClaimActivating(nodeClaim) {
			started = true
			source = normalizeActivationSource(NodeClaimActivationSource(nodeClaim))
			return nil
		}
		if !IsNodeClaimStandby(nodeClaim) {
			return nil
		}
		stored := nodeClaim.DeepCopy()
		source = normalizeActivationSource(requestedSource)
		SetNodeClaimActivating(nodeClaim, true)
		SetNodeClaimActivationSource(nodeClaim, source)
		if err := l.writer.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		started = true
		return nil
	})
	return started, source, err
}

func (l Lifecycle) setNodeActivationMarker(ctx context.Context, nodeName string, activating bool) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node := &corev1.Node{}
		if err := l.reader.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
			return err
		}
		if activating == HasNodeActivationMarker(node) {
			return nil
		}
		stored := node.DeepCopy()
		if activating {
			if node.Annotations == nil {
				node.Annotations = map[string]string{}
			}
			node.Annotations[NodeClaimActivatingAnnotationKey] = "true"
		} else if node.Annotations != nil {
			delete(node.Annotations, NodeClaimActivatingAnnotationKey)
		}
		return l.writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	})
}

func (l Lifecycle) removeNodeStandbyTaint(ctx context.Context, nodeName string) (bool, error) {
	var removed bool
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node := &corev1.Node{}
		if err := l.reader.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
			return err
		}
		if !HasNodeTaint(node) {
			return nil
		}
		if err := l.PatchNodeStandbyTaint(ctx, node, false); err != nil {
			return err
		}
		removed = true
		return nil
	})
	return removed, err
}

func (l Lifecycle) completeActivation(ctx context.Context, nodeName string, nodeClaimRef *v1.NodeClaim) error {
	if err := l.finishActivation(ctx, nodeClaimRef); err != nil {
		return fmt.Errorf("clearing activation markers, %w", err)
	}
	if err := l.setNodeActivationMarker(ctx, nodeName, false); err != nil {
		return fmt.Errorf("clearing node activation marker, %w", err)
	}
	return nil
}

func (l Lifecycle) finishActivation(ctx context.Context, nodeClaimRef *v1.NodeClaim) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaimRef), nodeClaim); err != nil {
			return err
		}
		if !IsNodeClaimStandby(nodeClaim) && !IsNodeClaimActivating(nodeClaim) && NodeClaimActivationSource(nodeClaim) == "" {
			return nil
		}
		stored := nodeClaim.DeepCopy()
		SetNodeClaimStandby(nodeClaim, false, time.Time{})
		SetNodeClaimActivating(nodeClaim, false)
		SetNodeClaimActivationSource(nodeClaim, "")
		return l.writer.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	})
}

func normalizeActivationSource(source ActivationSource) ActivationSource {
	switch source {
	case ActivationSourceProvisioning, ActivationSourceCompaction:
		return source
	default:
		return ActivationSourceRecovery
	}
}
