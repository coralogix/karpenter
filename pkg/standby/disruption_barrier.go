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

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func patchDisruptionTaint(ctx context.Context, writer client.Client, node *corev1.Node, add bool) error {
	if node == nil {
		return fmt.Errorf("cannot taint a missing Node")
	}
	stored := node.DeepCopy()
	hasTaint := false
	for _, taint := range node.Spec.Taints {
		if taint.MatchTaint(&v1.DisruptedNoScheduleTaint) {
			hasTaint = true
			break
		}
	}
	if add && !hasTaint {
		node.Spec.Taints = append(node.Spec.Taints, v1.DisruptedNoScheduleTaint)
	}
	if !add && hasTaint {
		node.Spec.Taints = lo.Reject(node.Spec.Taints, func(taint corev1.Taint, _ int) bool {
			return taint.MatchTaint(&v1.DisruptedNoScheduleTaint)
		})
	}
	if equality.Semantic.DeepEqual(stored, node) {
		return nil
	}
	return writer.Patch(ctx, node, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{}))
}

func removeDisruptionTaint(ctx context.Context, reader client.Reader, writer client.Client, nodeName string) error {
	node := &corev1.Node{}
	if err := reader.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
		return client.IgnoreNotFound(err)
	}
	return patchDisruptionTaint(ctx, writer, node, false)
}
