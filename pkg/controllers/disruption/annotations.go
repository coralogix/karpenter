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
	"fmt"
	"math"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
)

// readDurationAnnotation returns the annotation's duration, or nil when the annotation is absent or invalid. An invalid
// value raises an event naming fallbackDescription as what is used instead.
func readDurationAnnotation(np *v1.NodePool, recorder events.Recorder, key, reason, fallbackDescription string) *time.Duration {
	raw, ok := np.Annotations[key]
	if !ok {
		return nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < 0 {
		recorder.Publish(disruptionevents.InvalidDuration(np, key, raw, fallbackDescription, reason))
		return nil
	}
	return &duration
}

// readFloatAnnotation accepts finite values that pass valid; anything else uses fallback and raises a deduplicated
// Warning event on the NodePool.
func readFloatAnnotation(np *v1.NodePool, recorder events.Recorder, key string, fallback float64, reason string, valid func(float64) bool) float64 {
	raw, ok := np.Annotations[key]
	if !ok {
		return fallback
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || !valid(value) {
		recorder.Publish(events.Event{InvolvedObject: np, Type: corev1.EventTypeWarning, Reason: reason, Message: fmt.Sprintf("Invalid value %q for annotation %q; using %g", raw, key, fallback), DedupeValues: []string{string(np.UID), key, raw}, DedupeTimeout: 15 * time.Minute})
		return fallback
	}
	return value
}
