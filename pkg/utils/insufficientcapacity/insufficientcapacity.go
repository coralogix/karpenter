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

// Package insufficientcapacity records when each NodePool last had a NodeClaim fail to launch for insufficient
// capacity. The record is in memory, so a restart forgets it.
package insufficientcapacity

import (
	"sync"
	"time"
)

var (
	mu          sync.Mutex
	lastFailure = map[string]time.Time{}
)

func Record(nodePool string, at time.Time) {
	mu.Lock()
	defer mu.Unlock()
	lastFailure[nodePool] = at
}

// LastFailure returns the time of nodePool's last recorded failure, or the zero time when there is none.
func LastFailure(nodePool string) time.Time {
	mu.Lock()
	defer mu.Unlock()
	return lastFailure[nodePool]
}

func Reset() {
	mu.Lock()
	defer mu.Unlock()
	clear(lastFailure)
}
