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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	utilstandby "sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestRepairStandbyNodeRestoresMissingTaintOnlyWhenEmpty(t *testing.T) {
	t.Run("empty node remains standby", func(t *testing.T) {
		h := newCoordinatorTestFixture(t, "empty-node", "empty-claim", func(_ *corev1.Node, claim *v1.NodeClaim) {
			utilstandby.SetNodeClaimStandby(claim, true, time.Now())
		})
		result, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
		require.NoError(t, err)
		require.Equal(t, OutcomeRecovered, result.Outcome)
		require.True(t, utilstandby.IsNodeClaimStandby(h.readClaim(h.ctx)))
		require.True(t, utilstandby.HasNodeTaint(h.readNode(h.ctx)))
	})

	t.Run("occupied node returns to active", func(t *testing.T) {
		h := newCoordinatorTestFixture(t, "occupied-node", "occupied-claim", func(_ *corev1.Node, claim *v1.NodeClaim) {
			utilstandby.SetNodeClaimStandby(claim, true, time.Now())
		})
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "protected-workload", Namespace: "default", Annotations: map[string]string{v1.DoNotDisruptAnnotationKey: "true"}},
			Spec:       corev1.PodSpec{NodeName: h.node.Name},
		}
		require.NoError(t, h.client.Create(h.ctx, pod))

		result, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
		require.NoError(t, err)
		require.Equal(t, OutcomeRecovered, result.Outcome)
		require.False(t, utilstandby.IsNodeClaimStandby(h.readClaim(h.ctx)))
		require.False(t, utilstandby.HasNodeTaint(h.readNode(h.ctx)))
	})
}

func TestRepairStandbyNodeRemovesOrphanTaintWithoutOccupancyGate(t *testing.T) {
	h := newCoordinatorTestFixture(t, "orphan-node", "orphan-claim", nil)
	node := h.readNode(h.ctx)
	utilstandby.SetNodeTaint(node, true)
	require.NoError(t, h.client.Update(h.ctx, node))
	workload := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bound-workload", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name}}
	require.NoError(t, h.client.Create(h.ctx, workload))

	result, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
	require.NoError(t, err)
	require.Equal(t, OutcomeRecovered, result.Outcome)
	require.False(t, utilstandby.HasNodeTaint(h.readNode(h.ctx)))
}

func TestRepairStandbyNodeRetriesMarkerWriteFailureOnNextPass(t *testing.T) {
	h := newOccupiedStandbyFixture(t, "claim-write")
	failed := false
	h.client.beforeNodeClaimPatch = func(*v1.NodeClaim) error {
		if failed {
			return nil
		}
		failed = true
		return errors.New("injected marker write failure")
	}

	_, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
	require.ErrorContains(t, err, "injected marker write failure")
	require.True(t, utilstandby.IsNodeClaimStandby(h.readClaim(h.ctx)))
	require.True(t, utilstandby.HasNodeTaint(h.readNode(h.ctx)))

	result, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
	require.NoError(t, err)
	require.Equal(t, OutcomeRecovered, result.Outcome)
	require.False(t, utilstandby.IsNodeClaimStandby(h.readClaim(h.ctx)))
	require.False(t, utilstandby.HasNodeTaint(h.readNode(h.ctx)))
}

func TestRepairStandbyNodePreservesConcurrentNodeUpdateAfterMarkerRemoval(t *testing.T) {
	h := newOccupiedStandbyFixture(t, "node-conflict")
	failed := false
	h.client.beforeNodePatch = func(node *corev1.Node) error {
		if failed {
			return nil
		}
		failed = true
		live := &corev1.Node{}
		if err := h.client.Get(h.ctx, client.ObjectKeyFromObject(node), live); err != nil {
			return err
		}
		live.Spec.Taints = append(live.Spec.Taints, corev1.Taint{Key: "example.com/concurrent", Effect: corev1.TaintEffectNoSchedule})
		return h.client.Update(h.ctx, live)
	}

	_, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
	require.True(t, apierrors.IsConflict(err), "stale optimistic Node patch should conflict: %v", err)
	require.False(t, utilstandby.IsNodeClaimStandby(h.readClaim(h.ctx)))
	storedNode := h.readNode(h.ctx)
	require.True(t, utilstandby.HasNodeTaint(storedNode), "failed removal must leave the standby taint for the next pass")
	require.Contains(t, storedNode.Spec.Taints, corev1.Taint{Key: "example.com/concurrent", Effect: corev1.TaintEffectNoSchedule})

	result, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
	require.NoError(t, err)
	require.Equal(t, OutcomeRecovered, result.Outcome)
	storedNode = h.readNode(h.ctx)
	require.False(t, utilstandby.HasNodeTaint(storedNode))
	require.Contains(t, storedNode.Spec.Taints, corev1.Taint{Key: "example.com/concurrent", Effect: corev1.TaintEffectNoSchedule})
}

func TestRepairStandbyNodeRepairsOrphanTaintAfterTaintWriteFailure(t *testing.T) {
	h := newOccupiedStandbyFixture(t, "taint-write")
	failed := false
	h.client.beforeNodePatch = func(*corev1.Node) error {
		if failed {
			return nil
		}
		failed = true
		return errors.New("injected taint write failure")
	}

	_, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
	require.ErrorContains(t, err, "injected taint write failure")
	require.False(t, utilstandby.IsNodeClaimStandby(h.readClaim(h.ctx)), "the marker write should have succeeded")
	require.True(t, utilstandby.HasNodeTaint(h.readNode(h.ctx)))

	result, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
	require.NoError(t, err)
	require.Equal(t, OutcomeRecovered, result.Outcome)
	require.False(t, utilstandby.HasNodeTaint(h.readNode(h.ctx)))
}

func TestRepairStandbyNodeSkipsReplacedPairAndDeletingClaim(t *testing.T) {
	for _, kind := range []string{"Node", "NodeClaim"} {
		t.Run("same name "+kind+" replacement", func(t *testing.T) {
			h := newCoordinatorTestFixture(t, "replacement-node", "replacement-claim", func(node *corev1.Node, claim *v1.NodeClaim) {
				utilstandby.SetNodeClaimStandby(claim, true, time.Now())
				utilstandby.SetNodeTaint(node, true)
			})
			if kind == "Node" {
				oldNode := h.readNode(h.ctx)
				require.NoError(t, h.client.Delete(h.ctx, oldNode))
				replacement := oldNode.DeepCopy()
				replacement.UID = types.UID("new-node-uid")
				replacement.ResourceVersion = ""
				require.NoError(t, h.client.Create(h.ctx, replacement))
				require.NotEqual(t, h.node.UID, h.readNode(h.ctx).UID)
			} else {
				oldClaim := h.readClaim(h.ctx)
				require.NoError(t, h.client.Delete(h.ctx, oldClaim))
				replacement := oldClaim.DeepCopy()
				replacement.UID = types.UID("new-claim-uid")
				replacement.ResourceVersion = ""
				require.NoError(t, h.client.Create(h.ctx, replacement))
				require.NotEqual(t, h.nodeClaim.UID, h.readClaim(h.ctx).UID)
			}

			result, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
			require.NoError(t, err)
			require.Equal(t, OutcomeSkipped, result.Outcome)
			require.True(t, utilstandby.IsNodeClaimStandby(h.readClaim(h.ctx)))
			require.True(t, utilstandby.HasNodeTaint(h.readNode(h.ctx)))
		})
	}

	t.Run("deleting claim", func(t *testing.T) {
		h := newCoordinatorTestFixture(t, "deleting-node", "deleting-claim", func(node *corev1.Node, claim *v1.NodeClaim) {
			utilstandby.SetNodeClaimStandby(claim, true, time.Now())
			utilstandby.SetNodeTaint(node, true)
			claim.Finalizers = []string{"test.karpenter.sh/preserve"}
			deleting := metav1.NewTime(time.Now())
			claim.DeletionTimestamp = &deleting
		})
		result, err := h.coordinator.RepairStandbyNode(h.ctx, h.enterStandbyRequest())
		require.NoError(t, err)
		require.Equal(t, OutcomeSkipped, result.Outcome)
		require.True(t, utilstandby.IsNodeClaimStandby(h.readClaim(h.ctx)))
		require.True(t, utilstandby.HasNodeTaint(h.readNode(h.ctx)))
	})
}

func newOccupiedStandbyFixture(t *testing.T, suffix string) *coordinatorTestFixture {
	t.Helper()
	h := newCoordinatorTestFixture(t, "occupied-"+suffix, "occupied-"+suffix+"-claim", func(node *corev1.Node, claim *v1.NodeClaim) {
		utilstandby.SetNodeClaimStandby(claim, true, time.Now())
		utilstandby.SetNodeTaint(node, true)
	})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: h.node.Name}}
	require.NoError(t, h.client.Create(h.ctx, pod))
	return h
}
