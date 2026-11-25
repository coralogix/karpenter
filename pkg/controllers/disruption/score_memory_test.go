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
	"math"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

// Shapes are in CPU cores and memory units of the pool's ratio, so a source of 4 CPU and 128 memory in a pool with
// q = 16 has the shape delta 128/16 − 4 = 4.
func TestMemoryBalanceScore(t *testing.T) {
	cases := []struct {
		name    string
		pool    memoryBalancePool
		sources []nodeShape
		added   []nodeShape
		want    float64
	}{
		{"pressure in the deadband", memoryBalancePool{MemoryPerCPU: 4, TargetMemoryPerCPU: 4.1, Weight: 1}, []nodeShape{{"a", 4, 128}}, nil, 0},
		{"saturated positive pressure, move reduces the deviation", memoryBalancePool{MemoryPerCPU: 16, TargetMemoryPerCPU: 4, Weight: 2}, []nodeShape{{"a", 4, 128}}, nil, 2},
		{"saturated negative pressure, move reduces the deviation", memoryBalancePool{MemoryPerCPU: 1, TargetMemoryPerCPU: 4, Weight: 1}, []nodeShape{{"a", 4, 0}}, nil, 1},
		{"move increases the deviation", memoryBalancePool{MemoryPerCPU: 4 * math.Sqrt2, TargetMemoryPerCPU: 4, Weight: 1}, []nodeShape{{"a", 4, 4 * math.Sqrt2}}, nil, -0.375},
		{"unknown ratio", memoryBalancePool{TargetMemoryPerCPU: 4, Weight: 1}, []nodeShape{{"a", 4, 128}}, nil, 0},
		{"reactivated standby shape counted as added", memoryBalancePool{MemoryPerCPU: 16, TargetMemoryPerCPU: 4, Weight: 1}, []nodeShape{{"a", 4, 128}}, []nodeShape{{"a", 2, 64}}, 0.5},
		{"pool that only receives a node is not affected", memoryBalancePool{MemoryPerCPU: 16, TargetMemoryPerCPU: 4, Weight: 1}, []nodeShape{{"b", 4, 128}}, []nodeShape{{"a", 2, 64}}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := scoreMemoryBalance(map[string]memoryBalancePool{"a": tc.pool}, tc.sources, tc.added)
			if math.Abs(got.Contribution-tc.want) > 1e-9 {
				t.Fatalf("memory balance = %v (%+v), want %v", got.Contribution, got.Pools, tc.want)
			}
		})
	}
}

func TestInvalidMemoryBalanceAnnotationsUseDefaultsAndWarnOnce(t *testing.T) {
	fakeRecorder := record.NewFakeRecorder(10)
	recorder := events.NewRecorder(fakeRecorder)
	pool := &v1.NodePool{ObjectMeta: metav1.ObjectMeta{Name: "pool", UID: "pool", Annotations: map[string]string{
		v1.MemoryBalanceWeightAnnotationKey: "-1",
		v1.MemorySurplusAnnotationKey:       "NaN",
	}}}

	for range 2 {
		weight, surplus := readMemoryBalanceSettings(pool, recorder)
		if weight != 0 || surplus != 0.2 {
			t.Fatalf("weight %v and surplus %v, want defaults 0 and 0.2", weight, surplus)
		}
	}

	warnings := strings.Join(collectFakeRecorderEvents(fakeRecorder), "\n")
	if strings.Count(warnings, "Warning InvalidMemoryBalanceWeight") != 1 || strings.Count(warnings, "Warning InvalidMemorySurplus") != 1 {
		t.Fatalf("events = %v, want one warning for each invalid annotation", warnings)
	}
}
