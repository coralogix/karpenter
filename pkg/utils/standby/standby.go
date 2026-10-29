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

// Package standby reads and writes the standby marks: the Node taint and timestamp.
package standby

import (
	"context"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func HasAnnotation(node *corev1.Node) bool {
	_, ok := node.Annotations[v1.StandbyNodeAnnotationKey]
	return ok
}

func Timestamp(node *corev1.Node) (time.Time, bool) {
	raw := node.Annotations[v1.StandbyNodeAnnotationKey]
	if raw == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return ts.UTC(), true
}

// Mark writes the standby timestamp and taint together.
func Mark(ctx context.Context, kubeClient client.Client, node *corev1.Node, now time.Time) error {
	return patchNode(ctx, kubeClient, node, func(patch *corev1.Node) {
		if patch.Annotations == nil {
			patch.Annotations = map[string]string{}
		}
		patch.Annotations[v1.StandbyNodeAnnotationKey] = now.UTC().Format(time.RFC3339Nano)
		patch.Spec.Taints = append(WithoutStandbyTaint(patch.Spec.Taints), v1.StandbyNoScheduleTaint)
	})
}

// Clear removes both standby marks in one Node update.
func Clear(ctx context.Context, kubeClient client.Client, node *corev1.Node) error {
	return patchNode(ctx, kubeClient, node, func(patch *corev1.Node) {
		delete(patch.Annotations, v1.StandbyNodeAnnotationKey)
		patch.Spec.Taints = WithoutStandbyTaint(patch.Spec.Taints)
	})
}

func patchNode(ctx context.Context, kubeClient client.Client, node *corev1.Node, update func(*corev1.Node)) error {
	return retryIgnoringNotFound(func() error {
		stored := &corev1.Node{}
		if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(node), stored); err != nil {
			return err
		}
		if !stored.DeletionTimestamp.IsZero() {
			return nil
		}
		patch := stored.DeepCopy()
		update(patch)
		if equality.Semantic.DeepEqual(patch, stored) {
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
