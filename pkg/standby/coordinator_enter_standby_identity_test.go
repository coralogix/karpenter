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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestEnterStandbyEmptyMarkingSkipsWhenNodeReplacedAfterMarker(t *testing.T) {
	ctx := context.Background()
	h := newEnterStandbyTestHarness(t)
	swapped := false
	h.client.afterPatch = func(patchCtx context.Context, object client.Object) error {
		claim, ok := object.(*karpv1.NodeClaim)
		if !ok || swapped || !utilstandby.IsNodeClaimStandby(claim) {
			return nil
		}
		swapped = true
		return replaceActiveNodeSameName(patchCtx, h.client, h.node, types.UID("replacement-node-uid"))
	}
	result, err := h.coordinator.EnterStandby(ctx, h.request(), h.options())
	if err != nil {
		t.Fatalf("EnterStandby() error = %v", err)
	}
	if result.Outcome == OutcomeCompleted {
		t.Fatalf("EnterStandby() outcome = %q, want skip without completing on replaced node", result.Outcome)
	}
	replacementNode, _ := readActivationObjects(ctx, h.client, h.node.Name, h.nodeClaim.Name)
	if replacementNode.UID == h.node.UID {
		t.Fatal("expected a same-name node replacement")
	}
	if utilstandby.HasNodeTaint(replacementNode) {
		t.Fatal("replacement node should not receive the standby taint")
	}
}

func TestEnterStandbyPostEvacuationSkipsWhenNodeReplacedAfterMarker(t *testing.T) {
	ctx := context.Background()
	standbySince := time.Date(2026, time.October, 3, 10, 0, 0, 0, time.UTC)
	h := newCoordinatorTestFixture(t, "evac-node", "evac-claim", nil)
	swapped := false
	h.client.afterPatch = func(patchCtx context.Context, object client.Object) error {
		claim, ok := object.(*karpv1.NodeClaim)
		if !ok || swapped || !utilstandby.IsNodeClaimStandby(claim) {
			return nil
		}
		swapped = true
		return replaceActiveNodeSameName(patchCtx, h.client, h.node, types.UID("replacement-node-uid"))
	}
	result, err := h.coordinator.EnterStandby(ctx, h.enterStandbyRequest(), EnterStandbyOptions{BarrierTaintPresent: true, Since: standbySince})
	if err != nil {
		t.Fatalf("EnterStandby() error = %v", err)
	}
	if result.Outcome == OutcomeCompleted {
		t.Fatalf("EnterStandby() outcome = %q, want skip without completing on replaced node", result.Outcome)
	}
	replacementNode, _ := readActivationObjects(ctx, h.client, h.node.Name, h.nodeClaim.Name)
	if replacementNode.UID == h.node.UID {
		t.Fatal("expected a same-name node replacement")
	}
	if utilstandby.HasNodeTaint(replacementNode) {
		t.Fatal("replacement node should not receive the standby taint")
	}
}
