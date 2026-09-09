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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/kubernetes/scheme"
	resourcehelper "k8s.io/component-helpers/resource"
	volumehelpers "k8s.io/component-helpers/storage/volume"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	provisioningscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/test"
)

func miniFixtureDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve test file path")
	}
	return filepath.Join(filepath.Dir(file), "testdata", "mini")
}

func TestLoadMiniFixture(t *testing.T) {
	fixture, err := Load(miniFixtureDir(t))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(fixture.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(fixture.Nodes))
	}
	if len(fixture.Pods) != 2 {
		t.Fatalf("pods = %d, want 2", len(fixture.Pods))
	}
	if len(fixture.Namespaces) != 1 || fixture.Namespaces[0].Labels["team"] != "platform" {
		t.Fatalf("namespaces = %#v, want platform-labeled default namespace", fixture.Namespaces)
	}
	if fixture.Catalog == nil || len(fixture.Catalog.InstanceTypes) == 0 {
		t.Fatal("expected synthesized instance type catalog")
	}
}

func TestBuildEnvAndReset(t *testing.T) {
	ctx := options.ToContext(context.Background(), test.Options())
	fixture, err := Load(miniFixtureDir(t))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	env, err := fixture.BuildEnv(ctx, Options{AssumeScoreBasedAllPools: true})
	if err != nil {
		t.Fatalf("BuildEnv() error = %v", err)
	}
	if env.NodeCount != 2 {
		t.Fatalf("NodeCount = %d, want 2", env.NodeCount)
	}
	if env.StateNodeForProviderID("aws:///us-west-2a/i-nodea") == nil {
		t.Fatal("expected state node for node-a provider ID")
	}

	env.Cluster.NominateNodeForPod(ctx, "aws:///us-west-2a/i-nodea")
	if err := env.Reset(ctx); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}
	if env.Cluster.IsNodeNominated("aws:///us-west-2a/i-nodea") {
		t.Fatal("expected nominations to be cleared after Reset()")
	}
}

func TestBuildCatalog(t *testing.T) {
	fixture, err := Load(miniFixtureDir(t))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	catalog := BuildCatalog(fixture)
	if len(catalog.InstanceTypes) < 2 {
		t.Fatalf("instance types = %d, want at least 2", len(catalog.InstanceTypes))
	}
	if len(catalog.NodePoolInstanceTypes["bench-pool"]) == 0 {
		t.Fatal("expected node pool instance type mapping")
	}
}

func TestReadCacheReusesListResults(t *testing.T) {
	ctx := context.Background()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-a", Namespace: "default"},
		Spec:       corev1.PodSpec{NodeName: "node-a"},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}}
	fixture := &Fixture{
		Pods:  []*corev1.Pod{pod},
		Nodes: []*corev1.Node{node},
	}
	kubeClient := fakeclient.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(pod, node).
		WithInterceptorFuncs(newReadCache(scheme.Scheme, fixture).interceptorFuncs()).
		Build()

	first := &corev1.PodList{}
	second := &corev1.PodList{}
	if err := kubeClient.List(ctx, first); err != nil {
		t.Fatalf("first List() error = %v", err)
	}
	if err := kubeClient.List(ctx, second); err != nil {
		t.Fatalf("second List() error = %v", err)
	}
	if len(first.Items) != 1 || len(second.Items) != 1 {
		t.Fatalf("items = %d and %d, want 1 each", len(first.Items), len(second.Items))
	}

	gotNode := &corev1.Node{}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: "node-a"}, gotNode); err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	if err := kubeClient.Get(ctx, client.ObjectKey{Name: "node-a"}, &corev1.Node{}); err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
}

func TestBuildEnvSkipsTerminatingPods(t *testing.T) {
	ctx := options.ToContext(context.Background(), test.Options())
	fixture, err := Load(miniFixtureDir(t))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	now := metav1.Now()
	fixture.Pods[0].DeletionTimestamp = &now

	env, err := fixture.BuildEnv(ctx, Options{AssumeScoreBasedAllPools: true})
	if err != nil {
		t.Fatalf("BuildEnv() error = %v", err)
	}
	if env.PodCount != 1 {
		t.Fatalf("PodCount = %d, want 1 after filtering terminating pod", env.PodCount)
	}
}

func TestSlimPodPreservesSchedulingFields(t *testing.T) {
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: "data",
				}},
			}},
			Containers: []corev1.Container{{
				Name:  "app",
				Ports: []corev1.ContainerPort{{ContainerPort: 8080, HostPort: 8080}},
			}},
		},
	}

	slim := slimPodForBench(pod)
	if len(slim.Spec.Volumes) != 1 || slim.Spec.Volumes[0].PersistentVolumeClaim == nil {
		t.Fatalf("volumes = %#v, want PVC volume preserved", slim.Spec.Volumes)
	}
	if got := slim.Spec.Containers[0].Ports; len(got) != 1 || got[0].HostPort != 8080 {
		t.Fatalf("container ports = %#v, want host port 8080 preserved", got)
	}
}

func TestSlimPodPreservesNativeSidecarResourceAccounting(t *testing.T) {
	restartPolicy := corev1.ContainerRestartPolicyAlways
	pod := &corev1.Pod{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "app",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("100m"),
			}},
		}},
		InitContainers: []corev1.Container{{
			Name:          "sidecar",
			RestartPolicy: &restartPolicy,
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("200m"),
			}},
		}},
	}}

	want := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{})
	slim := slimPodForBench(pod)
	if slim.Spec.InitContainers[0].RestartPolicy == nil || *slim.Spec.InitContainers[0].RestartPolicy != restartPolicy {
		t.Fatalf("init container restart policy = %v, want %q", slim.Spec.InitContainers[0].RestartPolicy, restartPolicy)
	}
	got := resourcehelper.PodRequests(slim, resourcehelper.PodResourcesOptions{})
	if got.Cpu().Cmp(*want.Cpu()) != 0 {
		t.Fatalf("pod CPU requests after slimming = %s, want %s", got.Cpu(), want.Cpu())
	}
	if got.Cpu().Cmp(resource.MustParse("300m")) != 0 {
		t.Fatalf("pod CPU requests = %s, want native sidecar accounting of 300m", got.Cpu())
	}
}

//nolint:gocyclo
func TestLoadAndBuildEnvPreservesStorageSchedulingState(t *testing.T) {
	const (
		nodeName     = "node-a"
		podNamespace = "default"
		podName      = "app"
		claimName    = "data"
		volumeName   = "pv-data"
		storageClass = "ebs"
		driverName   = "ebs.csi.aws.com"
	)
	ctx := options.ToContext(context.Background(), test.Options())
	dir := t.TempDir()

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				v1.NodePoolLabelKey:            "bench-pool",
				corev1.LabelInstanceTypeStable: "m5.large",
				corev1.LabelTopologyZone:       "test-zone-1",
				v1.CapacityTypeLabelKey:        "on-demand",
			},
		},
		Spec: corev1.NodeSpec{ProviderID: "aws:///us-west-2a/i-nodea"},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: podNamespace},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Volumes: []corev1.Volume{{
				Name: "data",
				VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: claimName,
				}},
			}},
			Containers: []corev1.Container{{
				Name:  "app",
				Ports: []corev1.ContainerPort{{ContainerPort: 8080, HostPort: 8080}},
			}},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        claimName,
			Namespace:   podNamespace,
			Annotations: map[string]string{volumehelpers.AnnBindCompleted: "yes"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:       volumeName,
			StorageClassName: lo.ToPtr(storageClass),
		},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volumeName},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: driverName}},
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      corev1.LabelTopologyZone,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"test-zone-1"},
				}},
			}}}},
		},
	}
	sc := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClass}, Provisioner: driverName}
	volumeLimit := int32(1)
	csiNode := &storagev1.CSINode{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName},
		Spec: storagev1.CSINodeSpec{Drivers: []storagev1.CSINodeDriver{{
			Name:        driverName,
			Allocatable: &storagev1.VolumeNodeResources{Count: &volumeLimit},
		}}},
	}
	writeYAML := func(name string, obj any) {
		t.Helper()
		data, err := yaml.Marshal(obj)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	writeYAML("nodes.yaml", node)
	writeYAML("pods.yaml", pod)
	writeYAML("persistentvolumeclaims.yaml", pvc)
	writeYAML("persistentvolumes.yaml", pv)
	writeYAML("storageclasses.yaml", sc)
	writeYAML("csinodes.yaml", csiNode)

	fixture, err := Load(dir)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(fixture.PersistentVolumeClaims) != 1 || len(fixture.PersistentVolumes) != 1 || len(fixture.StorageClasses) != 1 || len(fixture.CSINodes) != 1 {
		t.Fatalf("storage objects = PVC %d, PV %d, SC %d, CSINode %d; want one each", len(fixture.PersistentVolumeClaims), len(fixture.PersistentVolumes), len(fixture.StorageClasses), len(fixture.CSINodes))
	}

	env, err := fixture.BuildEnv(ctx, Options{})
	if err != nil {
		t.Fatalf("BuildEnv() error = %v", err)
	}
	for _, obj := range []client.Object{pvc, pv, sc, csiNode} {
		if err := env.Client.Get(ctx, client.ObjectKeyFromObject(obj), obj.DeepCopyObject().(client.Object)); err != nil {
			t.Fatalf("Get(%T/%s) error = %v", obj, obj.GetName(), err)
		}
	}
	stateNode := env.StateNodeForProviderID(node.Spec.ProviderID)
	if stateNode == nil {
		t.Fatal("expected state node")
	}
	if err := stateNode.HostPortUsage().Conflicts(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: podNamespace}}, scheduling.GetHostPorts(pod)); err == nil {
		t.Fatal("expected preserved host port to conflict")
	}
	if err := stateNode.VolumeUsage().ExceedsLimits(scheduling.Volumes{driverName: sets.New("default/another")}); err == nil {
		t.Fatal("expected preserved PVC volume usage to reach CSINode limit")
	}
	requirements, err := provisioningscheduling.NewVolumeTopology(env.Client).GetRequirements(ctx, pod)
	if err != nil {
		t.Fatalf("GetRequirements() error = %v", err)
	}
	if !requirements.Get(corev1.LabelTopologyZone).Has("test-zone-1") {
		t.Fatalf("volume topology requirements = %v, want test-zone-1", requirements)
	}
}
