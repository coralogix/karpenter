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

package terminator

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCancelWaitsForInFlightEviction(t *testing.T) {
	retryableErr := errors.New("temporary eviction failure")
	evictionClient := &gatedEvictionClient{
		evictionWriter: &gatedEvictionSubResource{
			started:      make(chan struct{}, 1),
			release:      make(chan error, 1),
			retryableErr: retryableErr,
		},
	}
	queue := NewQueue(evictionClient, nil)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "default",
		Name:      "test-pod",
		UID:       types.UID("test-pod-uid"),
	}}
	queue.Add(pod)
	defer func() {
		select {
		case evictionClient.evictionWriter.release <- retryableErr:
		default:
		}
	}()

	reconcileDone := make(chan error, 1)
	go func() {
		_, err := queue.Reconcile(context.Background(), pod)
		reconcileDone <- err
	}()
	select {
	case <-evictionClient.evictionWriter.started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for eviction request to start")
	}

	cancelDone := make(chan struct{})
	go func() {
		queue.Cancel(pod)
		close(cancelDone)
	}()

	// Cancel removes membership before waiting for the in-flight API request. Seeing
	// the public membership change establishes that Cancel has removed the pod.
	require.Eventually(t, func() bool { return !queue.Has(pod) }, 5*time.Second, time.Millisecond,
		"cancellation removes queue membership")

	// The eviction is still gated, so cancellation must not have returned yet.
	select {
	case <-cancelDone:
		t.Fatal("Cancel returned while the eviction request was still in flight")
	case <-time.After(100 * time.Millisecond):
	}

	evictionClient.evictionWriter.release <- retryableErr
	select {
	case err := <-reconcileDone:
		require.ErrorIs(t, err, retryableErr)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for eviction reconciliation to finish")
	}
	select {
	case <-cancelDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancellation to finish")
	}

	_, err := queue.Reconcile(context.Background(), pod)
	require.NoError(t, err)
	require.Equal(t, int32(1), evictionClient.evictionWriter.calls.Load())
}

type gatedEvictionClient struct {
	client.Client
	evictionWriter *gatedEvictionSubResource
}

func (c *gatedEvictionClient) SubResource(string) client.SubResourceClient {
	return c.evictionWriter
}

type gatedEvictionSubResource struct {
	client.SubResourceClient
	started      chan struct{}
	release      chan error
	retryableErr error
	calls        atomic.Int32
}

func (s *gatedEvictionSubResource) Create(_ context.Context, _, _ client.Object, _ ...client.SubResourceCreateOption) error {
	if s.calls.Add(1) == 1 {
		s.started <- struct{}{}
		return <-s.release
	}
	return s.retryableErr
}
