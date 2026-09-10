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
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
)

type resourceCountingClient struct {
	client.Client
	mu   sync.Mutex
	gets map[reflect.Type]int
}

func (c *resourceCountingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.mu.Lock()
	c.gets[reflect.TypeOf(obj)]++
	c.mu.Unlock()
	return c.Client.Get(ctx, key, obj, opts...)
}

func (c *resourceCountingClient) getCount(typ reflect.Type) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets[typ]
}

var _ = It("uses captured volume data across attempts", func() {
	nodePool := test.NodePool()
	pod := test.Pod(test.PodOptions{PersistentVolumeClaims: []string{"captured-volume-claim"}})
	ExpectApplied(ctx, env.Client, nodePool, pod)

	countingClient := &resourceCountingClient{Client: env.Client, gets: map[reflect.Type]int{}}
	countingProvisioner := provisioning.NewProvisioner(countingClient, test.NewEventRecorder(), cloudProvider, cluster, fakeClock)
	inputs, err := countingProvisioner.NewPreparedSimulationInputs(ctx, []*corev1.Pod{pod}, nil)
	Expect(err).To(Succeed())
	podIDs, err := inputs.PodIDsFor([]*corev1.Pod{pod})
	Expect(err).To(Succeed())

	claimType := reflect.TypeOf(&corev1.PersistentVolumeClaim{})
	capturedGets := countingClient.getCount(claimType)
	Expect(capturedGets).To(BeNumerically(">", 0))
	for i := 0; i < 2; i++ {
		attempt, attemptErr := inputs.NewRun(ctx, provisioning.Scenario{PodIDs: podIDs})
		Expect(attemptErr).To(Succeed())
		_, solveErr := attempt.Solve(ctx)
		Expect(solveErr).To(Succeed())
	}
	Expect(countingClient.getCount(claimType)).To(Equal(capturedGets))

})
