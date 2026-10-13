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

package disruption

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

func TestStandbyMarkingFiltersToEligibleEmptyActiveNodes(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		standby   bool
		occupied  bool
		annotated bool
		want      bool
	}{
		{name: "eligible active empty node", annotated: true, want: true},
		{name: "occupied node", annotated: true, occupied: true},
		{name: "already standby node", annotated: true, standby: true},
		{name: "unconfigured pool", annotated: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate, kubeClient := standbyMarkingTestCandidate(tc.annotated, tc.standby, tc.occupied)
			method := NewStandbyMarking(consolidation{kubeClient: kubeClient})
			require.Equal(t, tc.want, method.ShouldDisrupt(ctx, candidate))
		})
	}
}

func TestStandbyMarkingRejectsBufferReservedNode(t *testing.T) {
	ctx := context.Background()
	candidate, kubeClient := standbyMarkingTestCandidate(true, false, false)
	clk := clocktesting.NewFakeClock(time.Now())
	cluster := state.NewCluster(clk, kubeClient, nil)
	cluster.UpdateBufferPodCounts(map[string]int{candidate.ProviderID(): 1})
	method := NewStandbyMarking(consolidation{kubeClient: kubeClient, cluster: cluster})
	require.False(t, method.ShouldDisrupt(ctx, candidate))
}

func standbyMarkingTestCandidate(annotated, markStandby, occupied bool) (*Candidate, client.Client) {
	poolAnnotations := map[string]string{}
	if annotated {
		poolAnnotations[v1.ScoreBasedConsolidationAnnotationKey] = ""
	}
	nodePool := &v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Annotations: poolAnnotations},
		Spec:       v1.NodePoolSpec{Disruption: v1.Disruption{ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized}},
	}
	nodeClaim := &v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{
		Name:   "claim",
		Labels: map[string]string{v1.NodePoolLabelKey: nodePool.Name},
	}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "node",
		Labels: map[string]string{
			v1.NodePoolLabelKey:        nodePool.Name,
			v1.NodeInitializedLabelKey: "true",
		},
	}}
	node.Spec.ProviderID = "provider-id"
	if markStandby {
		standby.SetNodeClaimStandby(nodeClaim, true, time.Now())
		standby.SetNodeTaint(node, true)
	}
	objects := []client.Object{nodePool, nodeClaim, node}
	if occupied {
		objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "default"}, Spec: corev1.PodSpec{NodeName: node.Name}})
	}
	kubeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(object client.Object) []string {
			pod := object.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		WithObjects(objects...).Build()
	stateNode := &state.StateNode{Node: node, NodeClaim: nodeClaim}
	return &Candidate{StateNode: stateNode, NodePool: nodePool}, kubeClient
}
