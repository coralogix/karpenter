//go:build test_performance

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

package scheduling_test

import (
	"testing"

	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// Sink prevents the compiler from eliminating the benchmarked call.
var labelRequirements scheduling.Requirements

var nodeLabels = map[string]string{
	"beta.kubernetes.io/arch":                                    "arm64",
	"beta.kubernetes.io/instance-type":                           "c7g.12xlarge",
	"beta.kubernetes.io/os":                                      "linux",
	"failure-domain.beta.kubernetes.io/region":                   "us-west-2",
	"failure-domain.beta.kubernetes.io/zone":                     "us-west-2a",
	"k8s.io/cloud-provider-aws":                                  "b3449ef82a450428705a5fc30d0d7db9",
	"karpenter-managed":                                          "true",
	"karpenter.k8s.aws/ec2nodeclass":                             "shared-node-class",
	"karpenter.k8s.aws/instance-capability-flex":                 "false",
	"karpenter.k8s.aws/instance-category":                        "c",
	"karpenter.k8s.aws/instance-cpu":                             "48",
	"karpenter.k8s.aws/instance-cpu-manufacturer":                "aws",
	"karpenter.k8s.aws/instance-cpu-sustained-clock-speed-mhz":   "2600",
	"karpenter.k8s.aws/instance-ebs-bandwidth":                   "15000",
	"karpenter.k8s.aws/instance-encryption-in-transit-supported": "true",
	"karpenter.k8s.aws/instance-family":                          "c7g",
	"karpenter.k8s.aws/instance-generation":                      "7",
	"karpenter.k8s.aws/instance-hypervisor":                      "nitro",
	"karpenter.k8s.aws/instance-memory":                          "98304",
	"karpenter.k8s.aws/instance-network-bandwidth":               "22500",
	"karpenter.k8s.aws/instance-size":                            "12xlarge",
	"karpenter.k8s.aws/instance-tenancy":                         "default",
	"karpenter.sh/capacity-type":                                 "spot",
	"karpenter.sh/do-not-sync-taints":                            "true",
	"karpenter.sh/initialized":                                   "true",
	"karpenter.sh/nodepool":                                      "spot-node-pool",
	"karpenter.sh/registered":                                    "true",
	"kubernetes.io/arch":                                         "arm64",
	"kubernetes.io/hostname":                                     "ip-11-111-111-111.us-west-2.compute.internal",
	"kubernetes.io/os":                                           "linux",
	"node.kubernetes.io/instance-type":                           "c7g.12xlarge",
	"topology.ebs.csi.aws.com/zone":                              "us-west-2a",
	"topology.k8s.aws/zone-id":                                   "usw2-az2",
	"topology.kubernetes.io/region":                              "us-west-2",
	"topology.kubernetes.io/zone":                                "us-west-2a",
}

func BenchmarkNewLabelRequirements(b *testing.B) {
	for i := 0; i < b.N; i++ {
		labelRequirements = scheduling.NewLabelRequirements(nodeLabels)
	}
}
