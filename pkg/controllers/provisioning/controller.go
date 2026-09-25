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

package provisioning

import (
	"context"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	utilscontroller "sigs.k8s.io/karpenter/pkg/utils/controller"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
	"sigs.k8s.io/karpenter/pkg/utils/standby"
)

const (
	minReconciles = 10
	maxReconciles = 1000
)

// PodController for the resource
type PodController struct {
	kubeClient  client.Client
	provisioner *Provisioner
	cluster     *state.Cluster
}

// NewPodController constructs a controller instance
func NewPodController(kubeClient client.Client, provisioner *Provisioner, cluster *state.Cluster) *PodController {
	return &PodController{
		kubeClient:  kubeClient,
		provisioner: provisioner,
		cluster:     cluster,
	}
}

// Reconcile the resource
func (c *PodController) Name() string {
	return "provisioner.trigger.pod"
}

func (c *PodController) Reconcile(ctx context.Context, p *corev1.Pod) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name()) //nolint:ineffassign,staticcheck

	if !pod.IsProvisionable(p) {
		return reconcile.Result{}, nil
	}
	c.provisioner.Trigger(p.UID)
	// ACK the pending pod when first observed so that total time spent pending due to Karpenter is tracked.
	c.cluster.AckPods(p)
	// Continue to requeue until the pod is no longer provisionable. Pods may
	// not be scheduled as expected if new pods are created while nodes are
	// coming online. Even if a provisioning loop is successful, the pod may
	// require another provisioning loop to become schedulable.
	return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
}

func (c *PodController) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.Pod{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

// NodeController for the resource
type NodeController struct {
	kubeClient  client.Client
	provisioner *Provisioner
}

// NewNodeController constructs a controller instance
func NewNodeController(kubeClient client.Client, provisioner *Provisioner) *NodeController {
	return &NodeController{
		kubeClient:  kubeClient,
		provisioner: provisioner,
	}
}

// Reconcile the resource
func (c *NodeController) Name() string {
	return "provisioner.trigger.node"
}

func (c *NodeController) Reconcile(ctx context.Context, n *corev1.Node) (reconcile.Result, error) {
	//nolint:ineffassign
	ctx = injection.WithControllerName(ctx, c.Name()) //nolint:ineffassign,staticcheck

	shouldTrigger, err := c.shouldTriggerProvisioning(ctx, n)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !shouldTrigger {
		return reconcile.Result{}, nil
	}
	c.provisioner.Trigger(n.UID)
	// Continue to requeue until the node is no longer provisionable. Pods may
	// not be scheduled as expected if new pods are created while nodes are
	// coming online. Even if a provisioning loop is successful, the pod may
	// require another provisioning loop to become schedulable.
	return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
}

func (c *NodeController) shouldTriggerProvisioning(ctx context.Context, n *corev1.Node) (bool, error) {
	// If the disruption taint exists, pods are being rescheduled from this node.
	if lo.ContainsBy(n.Spec.Taints, func(taint corev1.Taint) bool {
		return taint.MatchTaint(&v1.DisruptedNoScheduleTaint)
	}) {
		return true, nil
	}
	if n.Annotations[standby.NodeClaimActivatingAnnotationKey] == "true" {
		return true, nil
	}
	if !standby.HasNodeTaint(n) {
		return false, nil
	}
	// Resolve the owning NodeClaim while the standby taint is still present.
	// This covers a restart after the claim marker is persisted but before its
	// activation marker is mirrored onto the Node.
	owner, ok := lo.Find(n.OwnerReferences, func(owner metav1.OwnerReference) bool {
		return owner.Kind == "NodeClaim" && owner.APIVersion == apis.Group+"/v1"
	})
	if !ok {
		return false, nil
	}
	nodeClaim := &v1.NodeClaim{}
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: owner.Name}, nodeClaim); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if owner.UID != nodeClaim.UID {
		return false, nil
	}
	return standby.IsNodeClaimActivating(nodeClaim), nil
}

func (c *NodeController) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&corev1.Node{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: utilscontroller.LinearScaleReconciles(utilscontroller.CPUCount(ctx), minReconciles, maxReconciles)}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
