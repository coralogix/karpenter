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

package provisioning_test

import (
	"context"
	"reflect"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

type resourceCountingClient struct {
	client.Client
	mu    sync.Mutex
	gets  map[reflect.Type]int
	lists map[reflect.Type]int
}

func (c *resourceCountingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.mu.Lock()
	c.gets[reflect.TypeOf(obj)]++
	c.mu.Unlock()
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *resourceCountingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	c.mu.Lock()
	c.lists[reflect.TypeOf(list)]++
	c.mu.Unlock()
	return c.Client.List(ctx, list, opts...)
}

func (c *resourceCountingClient) getCount(typ reflect.Type) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets[typ]
}

func (c *resourceCountingClient) listCount(typ reflect.Type) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lists[typ]
}

func (c *resourceCountingClient) counts() (map[reflect.Type]int, map[reflect.Type]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gets := make(map[reflect.Type]int, len(c.gets))
	for typ, count := range c.gets {
		gets[typ] = count
	}
	lists := make(map[reflect.Type]int, len(c.lists))
	for typ, count := range c.lists {
		lists[typ] = count
	}
	return gets, lists
}

var _ = It("uses captured volume and topology data across attempts", func() {
	nodePool := test.NodePool()
	pod := test.Pod(test.PodOptions{
		PersistentVolumeClaims: []string{"captured-volume-claim"},
		NodeSelector:           map[string]string{"example.com/unsatisfiable": "true"},
		PodAntiPreferences: []corev1.WeightedPodAffinityTerm{{
			Weight: 1,
			PodAffinityTerm: corev1.PodAffinityTerm{
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "captured"}},
				TopologyKey:   corev1.LabelTopologyZone,
			},
		}},
	})
	ExpectApplied(ctx, env.Client, nodePool, pod)

	countingClient := &resourceCountingClient{Client: env.Client, gets: map[reflect.Type]int{}, lists: map[reflect.Type]int{}}
	countingProvisioner := provisioning.NewProvisioner(countingClient, test.NewEventRecorder(), cloudProvider, cluster, env.Clock, nil, nil)
	inputs, err := countingProvisioner.NewPreparedSimulationInputs(ctx, []*corev1.Pod{pod}, nil)
	Expect(err).To(Succeed())
	podIDs, err := inputs.PodIDsFor([]*corev1.Pod{pod})
	Expect(err).To(Succeed())

	claimType := reflect.TypeOf(&corev1.PersistentVolumeClaim{})
	capturedGets := countingClient.getCount(claimType)
	Expect(capturedGets).To(BeNumerically(">", 0))
	capturedGetCounts, capturedListCounts := countingClient.counts()
	podListType := reflect.TypeOf(&corev1.PodList{})
	namespaceListType := reflect.TypeOf(&corev1.NamespaceList{})
	capturedPodLists := countingClient.listCount(podListType)
	capturedNamespaceLists := countingClient.listCount(namespaceListType)
	Expect(capturedPodLists).To(Equal(1))
	Expect(capturedNamespaceLists).To(Equal(1))
	for i := 0; i < 2; i++ {
		attempt, attemptErr := inputs.NewRun(ctx, provisioning.Scenario{PodIDs: podIDs})
		Expect(attemptErr).To(Succeed())
		_, solveErr := attempt.Solve(ctx)
		Expect(solveErr).To(Succeed())
	}
	Expect(countingClient.getCount(claimType)).To(Equal(capturedGets))
	Expect(countingClient.listCount(podListType)).To(Equal(capturedPodLists))
	Expect(countingClient.listCount(namespaceListType)).To(Equal(capturedNamespaceLists))
	getCounts, listCounts := countingClient.counts()
	Expect(getCounts).To(Equal(capturedGetCounts))
	Expect(listCounts).To(Equal(capturedListCounts))

})
