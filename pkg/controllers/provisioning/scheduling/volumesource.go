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

package scheduling

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// VolumeSource supplies the volume topology requirements and per-pod volume usage
// to a scheduling attempt. Live and captured sources implement the same contract,
// so the scheduler does not need an optional volume map or a second construction path.
type VolumeSource interface {
	Requirements(*corev1.Pod) scheduling.Requirements
	Volumes(context.Context, *corev1.Pod) (scheduling.Volumes, error)
}

type liveVolumeSource struct {
	kubeClient   client.Client
	requirements map[types.UID]scheduling.Requirements
}

// NewLiveVolumeSource adapts the existing API-backed volume lookup to VolumeSource.
func NewLiveVolumeSource(kubeClient client.Client, requirements map[types.UID]scheduling.Requirements) VolumeSource {
	return &liveVolumeSource{kubeClient: kubeClient, requirements: requirements}
}

func (s *liveVolumeSource) Requirements(pod *corev1.Pod) scheduling.Requirements {
	return cloneRequirements(s.requirements[pod.UID])
}

func (s *liveVolumeSource) Volumes(ctx context.Context, pod *corev1.Pod) (scheduling.Volumes, error) {
	return scheduling.GetVolumes(ctx, s.kubeClient, pod)
}

type capturedVolumeSource struct {
	data map[types.UID]VolumeData
}

// VolumeData is the frozen volume state for a pod. UsageError is returned by
// Volumes so callers can preserve a volume lookup failure without reading the
// API again. RequirementError is kept separate so simulation preparation can
// exclude pods whose topology requirements could not be resolved while still
// retaining usage errors for the scheduler's existing-node path.
type VolumeData struct {
	Requirements     scheduling.Requirements
	RequirementError error
	Volumes          scheduling.Volumes
	UsageError       error
}

// NewCapturedVolumeSource adapts volume data captured during simulation preparation.
func NewCapturedVolumeSource(data map[types.UID]VolumeData) VolumeSource {
	dataCopy := make(map[types.UID]VolumeData, len(data))
	for uid, value := range data {
		dataCopy[uid] = VolumeData{
			Requirements:     cloneRequirements(value.Requirements),
			RequirementError: value.RequirementError,
			Volumes:          value.Volumes.DeepCopy(),
			UsageError:       value.UsageError,
		}
	}
	return &capturedVolumeSource{data: dataCopy}
}

func (s *capturedVolumeSource) Requirements(pod *corev1.Pod) scheduling.Requirements {
	return cloneRequirements(s.data[pod.UID].Requirements)
}

func (s *capturedVolumeSource) Volumes(_ context.Context, pod *corev1.Pod) (scheduling.Volumes, error) {
	data, ok := s.data[pod.UID]
	if !ok {
		return nil, fmt.Errorf("volume data for pod %s/%s was not captured", pod.Namespace, pod.Name)
	}
	if data.UsageError != nil {
		return nil, data.UsageError
	}
	return data.Volumes.DeepCopy(), nil
}

func cloneRequirements(requirements scheduling.Requirements) scheduling.Requirements {
	if requirements == nil {
		return nil
	}
	return scheduling.NewNodeSelectorRequirementsWithMinValues(requirements.NodeSelectorRequirements()...)
}
