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

// Package standby reads and writes the standby marks: the node taint and the NodeClaim timestamp.
package standby

import (
	"context"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// IsMarked reports whether the node carries the standby taint or its NodeClaim carries a standby timestamp.
func IsMarked(node *corev1.Node, nodeClaim *v1.NodeClaim) bool {
	return HasStandbyTaint(node) || (nodeClaim != nil && nodeClaim.Annotations[v1.StandbyNodeClaimAnnotationKey] != "")
}

func Timestamp(nodeClaim *v1.NodeClaim) (time.Time, bool) {
	if nodeClaim == nil {
		return time.Time{}, false
	}
	raw := nodeClaim.Annotations[v1.StandbyNodeClaimAnnotationKey]
	if raw == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return ts.UTC(), true
}

// Mark writes the timestamp before the taint, so a failed taint write leaves no taint rather than a taint with an old timestamp.
func Mark(ctx context.Context, kubeClient client.Client, node *corev1.Node, nodeClaim *v1.NodeClaim, now time.Time) error {
	if err := setTimestamp(ctx, kubeClient, nodeClaim, now); err != nil {
		return err
	}
	return addTaint(ctx, kubeClient, node)
}

func Clear(ctx context.Context, kubeClient client.Client, node *corev1.Node, nodeClaim *v1.NodeClaim) error {
	if err := removeTaint(ctx, kubeClient, node); err != nil {
		return err
	}
	return clearTimestamp(ctx, kubeClient, nodeClaim)
}

func setTimestamp(ctx context.Context, kubeClient client.Client, nodeClaim *v1.NodeClaim, at time.Time) error {
	if nodeClaim == nil {
		return nil
	}
	return retryIgnoringNotFound(func() error {
		stored := &v1.NodeClaim{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), stored); err != nil {
			return err
		}
		patch := stored.DeepCopy()
		if patch.Annotations == nil {
			patch.Annotations = map[string]string{}
		}
		patch.Annotations[v1.StandbyNodeClaimAnnotationKey] = at.UTC().Format(time.RFC3339Nano)
		return kubeClient.Patch(ctx, patch, client.MergeFrom(stored))
	})
}

func clearTimestamp(ctx context.Context, kubeClient client.Client, nodeClaim *v1.NodeClaim) error {
	if nodeClaim == nil {
		return nil
	}
	return retryIgnoringNotFound(func() error {
		stored := &v1.NodeClaim{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(nodeClaim), stored); err != nil {
			return err
		}
		if stored.Annotations[v1.StandbyNodeClaimAnnotationKey] == "" {
			return nil
		}
		patch := stored.DeepCopy()
		delete(patch.Annotations, v1.StandbyNodeClaimAnnotationKey)
		return kubeClient.Patch(ctx, patch, client.MergeFrom(stored))
	})
}

func addTaint(ctx context.Context, kubeClient client.Client, node *corev1.Node) error {
	return patchTaints(ctx, kubeClient, node, func(taints []corev1.Taint) []corev1.Taint {
		return append(WithoutStandbyTaint(taints), v1.StandbyNoScheduleTaint)
	})
}

func removeTaint(ctx context.Context, kubeClient client.Client, node *corev1.Node) error {
	return patchTaints(ctx, kubeClient, node, WithoutStandbyTaint)
}

func patchTaints(ctx context.Context, kubeClient client.Client, node *corev1.Node, update func([]corev1.Taint) []corev1.Taint) error {
	if node == nil {
		return nil
	}
	return retryIgnoringNotFound(func() error {
		stored := &corev1.Node{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(node), stored); err != nil {
			return err
		}
		if !stored.DeletionTimestamp.IsZero() {
			return nil
		}
		patch := stored.DeepCopy()
		patch.Spec.Taints = update(patch.Spec.Taints)
		if HasStandbyTaint(patch) == HasStandbyTaint(stored) {
			return nil
		}
		return kubeClient.Patch(ctx, patch, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
	})
}

func HasStandbyTaint(node *corev1.Node) bool {
	return node != nil && lo.ContainsBy(node.Spec.Taints, isStandbyTaint)
}

func WithoutStandbyTaint(taints []corev1.Taint) []corev1.Taint {
	return lo.Reject(taints, func(taint corev1.Taint, _ int) bool { return isStandbyTaint(taint) })
}

func isStandbyTaint(taint corev1.Taint) bool {
	return taint.MatchTaint(&v1.StandbyNoScheduleTaint)
}

func retryIgnoringNotFound(op func() error) error {
	return retry.OnError(retry.DefaultBackoff, func(err error) bool { return client.IgnoreNotFound(err) != nil }, op)
}
