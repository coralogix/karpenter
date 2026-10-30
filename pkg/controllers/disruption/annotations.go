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
	"time"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/events"
)

func readDurationAnnotation(np *v1.NodePool, recorder events.Recorder, key string, fallback time.Duration, reason string) time.Duration {
	raw, ok := np.Annotations[key]
	if !ok {
		return fallback
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration < 0 {
		recorder.Publish(disruptionevents.InvalidDuration(np, key, raw, fallback, reason))
		return fallback
	}
	return duration
}
