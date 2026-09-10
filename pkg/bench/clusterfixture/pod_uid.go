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

package clusterfixture

import (
	"crypto/sha256"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ensurePodUIDs preserves dumped UIDs and assigns stable, unique UIDs to
// fixture pods that do not have one. Scheduler caches and topology ownership
// are keyed by pod UID, so an empty or duplicate UID makes distinct pods share
// scheduler state.
func ensurePodUIDs(pods []*corev1.Pod) {
	used := make(map[types.UID]struct{}, len(pods))
	for i, pod := range pods {
		uid := pod.UID
		if _, exists := used[uid]; uid == "" || exists {
			for attempt := 0; ; attempt++ {
				uid = fixturePodUID(pod, i, attempt)
				if _, exists := used[uid]; !exists {
					break
				}
			}
			pod.UID = uid
		}
		used[uid] = struct{}{}
	}
}

func fixturePodUID(pod *corev1.Pod, index, attempt int) types.UID {
	key := fmt.Sprintf("%s/%s/%d/%d", pod.Namespace, pod.Name, index, attempt)
	return types.UID(fmt.Sprintf("fixture-pod-%x", sha256.Sum256([]byte(key))))
}
