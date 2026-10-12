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
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Lifecycle performs live reads used by standby transition callers.
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
