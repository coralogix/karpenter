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
	"context"
	"fmt"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	clock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	fakecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/test"
	testv1alpha1 "sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// Options configures fixture hydration.
type Options struct {
	AssumeScoreBasedAllPools bool
}

// Env is a hydrated in-memory cluster for disruption benchmarks.
type Env struct {
	Client        client.Client
	Cluster       *state.Cluster
	CloudProvider *fakecloudprovider.CloudProvider
	Provisioner   *provisioning.Provisioner
	Recorder      *test.EventRecorder
	Clock         *clock.FakeClock

	NodeCount int
	PodCount  int

	fixture *Fixture
}

// BuildEnv hydrates cluster state from a fixture.
func (f *Fixture) BuildEnv(ctx context.Context, opts Options) (*Env, error) {
	f.normalize()
	if opts.AssumeScoreBasedAllPools {
		f.annotateScoreBasedPools()
	}

	objects := f.clientObjects()
	objects = append(objects, &testv1alpha1.TestNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "default"},
	})
	kubeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithStatusSubresource(&v1.NodeClaim{}, &v1.NodePool{}).
		WithObjects(objects...).
		WithIndex(&corev1.Pod{}, "spec.nodeName", func(obj client.Object) []string {
			pod := obj.(*corev1.Pod)
			if pod.Spec.NodeName == "" {
				return nil
			}
			return []string{pod.Spec.NodeName}
		}).
		WithInterceptorFuncs(newReadCache(scheme.Scheme, f).interceptorFuncs()).
		Build()

	fakeClock := clock.NewFakeClock(time.Now())
	recorder := test.NewEventRecorder()
	cloudProvider := fakecloudprovider.NewCloudProvider()
	f.Catalog.ApplyToCloudProvider(cloudProvider)

	cluster := state.NewCluster(fakeClock, kubeClient, cloudProvider)
	env := &Env{
		Client:        kubeClient,
		Cluster:       cluster,
		CloudProvider: cloudProvider,
		Recorder:      recorder,
		Clock:         fakeClock,
		NodeCount:     len(f.Nodes),
		PodCount:      len(f.Pods),
		fixture:       f,
	}
	if err := env.replayClusterState(ctx); err != nil {
		return nil, err
	}
	return env, nil
}

// Reset restores cluster state after scheduling mutations.
func (e *Env) Reset(ctx context.Context) error {
	for n := range e.Cluster.Nodes() {
		n.ClearNomination()
	}
	e.Cluster.MarkUnconsolidated()
	return nil
}

func (e *Env) replayClusterState(ctx context.Context) error {
	for _, nc := range e.fixture.NodeClaims {
		e.Cluster.UpdateNodeClaim(nc.DeepCopy())
	}
	for _, node := range e.fixture.Nodes {
		pods := e.fixture.podsByNode[node.Name]
		if err := e.Cluster.UpdateNodeWithPods(ctx, node.DeepCopy(), pods); err != nil {
			return fmt.Errorf("updating node %q in cluster state: %w", node.Name, err)
		}
	}
	for _, pod := range e.fixture.Pods {
		if pod.Spec.NodeName != "" || podutils.IsTerminal(pod) {
			continue
		}
		if err := e.Cluster.UpdatePod(ctx, pod.DeepCopy()); err != nil {
			return fmt.Errorf("updating pod %s/%s in cluster state: %w", pod.Namespace, pod.Name, err)
		}
	}
	for _, ds := range e.fixture.DaemonSets {
		if err := e.Cluster.UpdateDaemonSet(ctx, ds.DeepCopy()); err != nil {
			return fmt.Errorf("updating daemonset %s/%s: %w", ds.Namespace, ds.Name, err)
		}
	}
	return nil
}

func (f *Fixture) normalizeNodeClassRefs() {
	benchRef := &v1.NodeClassReference{
		Group: testv1alpha1.Group,
		Kind:  "TestNodeClass",
		Name:  "default",
	}
	benchLabelKey := v1.NodeClassLabelKey(benchRef.GroupKind())

	for _, np := range f.NodePools {
		np.Spec.Template.Spec.NodeClassRef = benchRef
	}
	for _, nc := range f.NodeClaims {
		nc.Spec.NodeClassRef = benchRef
	}
	for _, node := range f.Nodes {
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		node.Labels[benchLabelKey] = benchRef.Name
	}
}

func (f *Fixture) filterTerminating() {
	f.Pods = filterLiveObjects(f.Pods)
	f.Pods = lo.Filter(f.Pods, func(p *corev1.Pod, _ int) bool {
		return !podutils.IsTerminal(p)
	})
	f.Nodes = filterLiveObjects(f.Nodes)
	f.DaemonSets = filterLiveObjects(f.DaemonSets)
	f.PDBs = filterLiveObjects(f.PDBs)
	f.NodePools = filterLiveObjects(f.NodePools)
	f.NodeClaims = filterLiveObjects(f.NodeClaims)
}

func filterLiveObjects[T client.Object](objs []T) []T {
	return lo.Filter(objs, func(o T, _ int) bool {
		return o.GetDeletionTimestamp() == nil
	})
}

func (f *Fixture) slimPodsForBench() {
	for i, pod := range f.Pods {
		f.Pods[i] = slimPodForBench(pod)
	}
}

func slimPodForBench(pod *corev1.Pod) *corev1.Pod {
	slim := pod.DeepCopy()
	slim.ManagedFields = nil
	slim.Status = corev1.PodStatus{Phase: pod.Status.Phase}
	slim.Spec.Volumes = nil
	slim.Spec.EphemeralContainers = nil
	slim.Spec.Containers = slimContainers(slim.Spec.Containers)
	slim.Spec.InitContainers = slimContainers(slim.Spec.InitContainers)
	return slim
}

func slimContainers(containers []corev1.Container) []corev1.Container {
	if len(containers) == 0 {
		return nil
	}
	out := make([]corev1.Container, len(containers))
	for i, c := range containers {
		out[i] = corev1.Container{
			Name:      c.Name,
			Resources: c.Resources,
		}
	}
	return out
}

func (f *Fixture) indexPodsByNode() {
	f.podsByNode = map[string][]*corev1.Pod{}
	for _, pod := range f.Pods {
		if pod.Spec.NodeName == "" {
			continue
		}
		f.podsByNode[pod.Spec.NodeName] = append(f.podsByNode[pod.Spec.NodeName], pod)
	}
}

func (f *Fixture) normalize() {
	f.filterTerminating()
	f.slimPodsForBench()
	f.indexPodsByNode()
	f.normalizeNodeClassRefs()
	for _, nc := range f.NodeClaims {
		nc.StatusConditions().SetTrue(v1.ConditionTypeLaunched)
		nc.StatusConditions().SetTrue(v1.ConditionTypeRegistered)
		nc.StatusConditions().SetTrue(v1.ConditionTypeInitialized)
		nc.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
	}
	for _, np := range f.NodePools {
		if np.Spec.Disruption.ConsolidateAfter.Duration == nil {
			np.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("0s")
		}
		if np.Spec.Disruption.ConsolidationPolicy == "" {
			np.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmptyOrUnderutilized
		}
	}
}

// StateNodeForProviderID returns a deep copy of the cluster state node for providerID.
func (e *Env) StateNodeForProviderID(providerID string) *state.StateNode {
	for n := range e.Cluster.Nodes() {
		if n.ProviderID() == providerID {
			return n.DeepCopy()
		}
	}
	return nil
}

func (f *Fixture) annotateScoreBasedPools() {
	for _, np := range f.NodePools {
		if np.Annotations == nil {
			np.Annotations = map[string]string{}
		}
		if np.Spec.Replicas != nil {
			continue
		}
		np.Annotations[v1.ScoreBasedConsolidationAnnotationKey] = ""
		if np.Spec.Disruption.ConsolidateAfter.Duration == nil {
			np.Spec.Disruption.ConsolidateAfter = v1.MustParseNillableDuration("0s")
		}
		if np.Spec.Disruption.ConsolidationPolicy == "" {
			np.Spec.Disruption.ConsolidationPolicy = v1.ConsolidationPolicyWhenEmptyOrUnderutilized
		}
	}
}

func (f *Fixture) clientObjects() []client.Object {
	var objects []client.Object
	for _, obj := range f.Nodes {
		objects = append(objects, obj.DeepCopy())
	}
	for _, obj := range f.Pods {
		objects = append(objects, obj.DeepCopy())
	}
	for _, obj := range f.DaemonSets {
		objects = append(objects, obj.DeepCopy())
	}
	for _, obj := range f.PDBs {
		objects = append(objects, obj.DeepCopy())
	}
	for _, obj := range f.NodePools {
		objects = append(objects, obj.DeepCopy())
	}
	for _, obj := range f.NodeClaims {
		objects = append(objects, obj.DeepCopy())
	}
	return objects
}
