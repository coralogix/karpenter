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
	corev1 "k8s.io/api/core/v1"
)

// memoryGiBWeight weights memory relative to CPU cores in the workload-size denominator.
const memoryGiBWeight = 0.125

const gibibyte = 1024 * 1024 * 1024

// nodePriorityScore is a search-guidance heuristic.
// Uses non-daemon pod CPU/memory requests from cluster state via PodRequests().
func nodePriorityScore(c *Candidate) float64 {
	if c == nil || c.StateNode == nil {
		return 0
	}
	nodePrice := getCandidatePrices([]*Candidate{c})
	workloadSize := nonDaemonWorkloadSize(c.PodRequests())
	if workloadSize <= 0 {
		return nodePrice
	}
	return nodePrice / workloadSize
}

func nonDaemonWorkloadSize(requests corev1.ResourceList) float64 {
	cores := requests.Cpu().AsApproximateFloat64()
	gibibytes := requests.Memory().AsApproximateFloat64() / gibibyte
	return cores + gibibytes*memoryGiBWeight
}
