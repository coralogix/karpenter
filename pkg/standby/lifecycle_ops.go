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
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

type lifecycle struct {
	reader client.Reader
	writer client.Client
}

func (l lifecycle) readLivePair(ctx context.Context, nodeRef *corev1.Node, nodeClaimRef *v1.NodeClaim, providerID string) (*corev1.Node, *v1.NodeClaim, bool, error) {
	return utilstandby.NewLifecycle(l.reader, l.writer).ReadLivePair(ctx, nodeRef, nodeClaimRef, providerID)
}

func (l lifecycle) ensureNodeStandbyTaint(ctx context.Context, nodeRef *corev1.Node) error {
	if nodeRef == nil {
		return fmt.Errorf("cannot restore standby taint without a Node")
	}
	node := &corev1.Node{}
	if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeRef), node); err != nil {
		return err
	}
	return l.applyNodeStandbyTaint(ctx, node)
}

func (l lifecycle) ensureStandbyTaintForClaim(ctx context.Context, nodeRef *corev1.Node, nodeClaimRef *v1.NodeClaim) error {
	if nodeRef == nil || nodeClaimRef == nil {
		return nil
	}
	node := &corev1.Node{}
	if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeRef), node); err != nil {
		return client.IgnoreNotFound(err)
	}
	if utilstandby.HasNodeActivationMarker(node) {
		return nil
	}
	nodeClaim := &v1.NodeClaim{}
	if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaimRef), nodeClaim); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !utilstandby.IsNodeClaimStandby(nodeClaim) || utilstandby.IsNodeClaimActivating(nodeClaim) {
		return nil
	}
	return l.applyNodeStandbyTaint(ctx, node)
}

func (l lifecycle) activate(ctx context.Context, nodeSnapshot *corev1.Node, nodeClaimSnapshot *v1.NodeClaim, requestedSource utilstandby.ActivationSource, onActivated func(utilstandby.ActivationSource)) error {
	if nodeClaimSnapshot == nil {
		return fmt.Errorf("reserving nodeclaim activation, missing NodeClaim")
	}
	started, source, err := l.reserveActivation(ctx, nodeClaimSnapshot, requestedSource)
	if err != nil {
		return fmt.Errorf("reserving nodeclaim activation, %w", err)
	}
	if !started && !utilstandby.HasNodeActivationMarker(nodeSnapshot) {
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

func (l lifecycle) patchNodeClaimStandby(ctx context.Context, nodeClaim *v1.NodeClaim, standby bool, since time.Time) error {
	if nodeClaim == nil {
		return fmt.Errorf("cannot update standby marker on a missing NodeClaim")
	}
	stored := nodeClaim.DeepCopy()
	utilstandby.SetNodeClaimStandby(nodeClaim, standby, since)
	return l.writer.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (l lifecycle) patchNodeStandbyTaint(ctx context.Context, node *corev1.Node, standby bool) error {
	if node == nil {
		return fmt.Errorf("cannot update standby taint on a missing Node")
	}
	stored := node.DeepCopy()
	utilstandby.SetNodeTaint(node, standby)
	return l.writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (l lifecycle) applyNodeStandbyTaint(ctx context.Context, node *corev1.Node) error {
	stored := node.DeepCopy()
	utilstandby.SetNodeTaint(node, true)
	if equality.Semantic.DeepEqual(stored, node) {
		return nil
	}
	return l.writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func (l lifecycle) reserveActivation(ctx context.Context, nodeClaimRef *v1.NodeClaim, requestedSource utilstandby.ActivationSource) (bool, utilstandby.ActivationSource, error) {
	var started bool
	source := utilstandby.ActivationSourceRecovery
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaimRef), nodeClaim); err != nil {
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
		if err := l.writer.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); err != nil {
			return err
		}
		started = true
		return nil
	})
	return started, source, err
}

func (l lifecycle) setNodeActivationMarker(ctx context.Context, nodeName string, activating bool) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node := &corev1.Node{}
		if err := l.reader.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
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
		return l.writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	})
}

func (l lifecycle) removeNodeStandbyTaint(ctx context.Context, nodeName string) (bool, error) {
	var removed bool
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node := &corev1.Node{}
		if err := l.reader.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
			return err
		}
		if !utilstandby.HasNodeTaint(node) {
			return nil
		}
		if err := l.patchNodeStandbyTaint(ctx, node, false); err != nil {
			return err
		}
		removed = true
		return nil
	})
	return removed, err
}

func (l lifecycle) completeActivation(ctx context.Context, nodeName string, nodeClaimRef *v1.NodeClaim) error {
	if err := l.finishActivation(ctx, nodeClaimRef); err != nil {
		return fmt.Errorf("clearing activation markers, %w", err)
	}
	if err := l.setNodeActivationMarker(ctx, nodeName, false); err != nil {
		return fmt.Errorf("clearing node activation marker, %w", err)
	}
	return nil
}

func (l lifecycle) finishActivation(ctx context.Context, nodeClaimRef *v1.NodeClaim) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		nodeClaim := &v1.NodeClaim{}
		if err := l.reader.Get(ctx, client.ObjectKeyFromObject(nodeClaimRef), nodeClaim); err != nil {
			return err
		}
		if !utilstandby.IsNodeClaimStandby(nodeClaim) && !utilstandby.IsNodeClaimActivating(nodeClaim) && utilstandby.NodeClaimActivationSource(nodeClaim) == "" {
			return nil
		}
		stored := nodeClaim.DeepCopy()
		utilstandby.SetNodeClaimStandby(nodeClaim, false, time.Time{})
		utilstandby.SetNodeClaimActivating(nodeClaim, false)
		utilstandby.SetNodeClaimActivationSource(nodeClaim, "")
		return l.writer.Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
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
