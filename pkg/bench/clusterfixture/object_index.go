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
	"errors"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

var errNotIndexed = errors.New("object not indexed")

type objectIndex struct {
	allPods         []runtime.Object
	podsByNamespace map[string][]runtime.Object
	podsByNodeName  map[string][]runtime.Object
	nodesByName     map[string]runtime.Object
	daemonSets      []runtime.Object
	pdbs            []runtime.Object
	nodePools       []runtime.Object
	nodeClaims      []runtime.Object
	namespaces      []runtime.Object
}

func newObjectIndex(f *Fixture) *objectIndex {
	idx := &objectIndex{
		podsByNamespace: map[string][]runtime.Object{},
		podsByNodeName:  map[string][]runtime.Object{},
		nodesByName:     map[string]runtime.Object{},
	}

	for _, pod := range f.Pods {
		obj := runtimeObject(pod)
		idx.allPods = append(idx.allPods, obj)
		idx.podsByNamespace[pod.Namespace] = append(idx.podsByNamespace[pod.Namespace], obj)
		idx.podsByNodeName[pod.Spec.NodeName] = append(idx.podsByNodeName[pod.Spec.NodeName], obj)
	}
	for _, node := range f.Nodes {
		idx.nodesByName[node.Name] = runtimeObject(node)
	}
	for _, ds := range f.DaemonSets {
		idx.daemonSets = append(idx.daemonSets, runtimeObject(ds))
	}
	for _, pdb := range f.PDBs {
		idx.pdbs = append(idx.pdbs, runtimeObject(pdb))
	}
	for _, np := range f.NodePools {
		idx.nodePools = append(idx.nodePools, runtimeObject(np))
	}
	for _, nc := range f.NodeClaims {
		idx.nodeClaims = append(idx.nodeClaims, runtimeObject(nc))
	}
	for ns := range idx.podsByNamespace {
		idx.namespaces = append(idx.namespaces, runtimeObject(&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
		}))
	}
	return idx
}

func runtimeObject(obj client.Object) runtime.Object {
	return obj.DeepCopyObject()
}

func (idx *objectIndex) get(key client.ObjectKey, obj client.Object) error {
	switch dst := obj.(type) {
	case *corev1.Node:
		src, ok := idx.nodesByName[key.Name]
		if !ok {
			return apierrors.NewNotFound(corev1.Resource("nodes"), key.Name)
		}
		return assignObject(dst, src)
	case *corev1.Pod:
		for _, candidate := range idx.podsByNamespace[key.Namespace] {
			pod := candidate.(*corev1.Pod)
			if pod.Name == key.Name {
				return assignObject(dst, candidate)
			}
		}
		return apierrors.NewNotFound(corev1.Resource("pods"), key.Name)
	default:
		return errNotIndexed
	}
}

func (idx *objectIndex) list(list client.ObjectList, opts client.ListOptions) error {
	switch dst := list.(type) {
	case *corev1.PodList:
		return idx.listInto(dst, idx.podCandidates(opts), podFieldSet, podLabels, opts)
	case *corev1.NodeList:
		return idx.listInto(dst, idx.nodesByNameList(), nodeFieldSet, nodeLabels, opts)
	case *appsv1.DaemonSetList:
		return idx.listInto(dst, idx.daemonSets, daemonSetFieldSet, daemonSetLabels, opts)
	case *policyv1.PodDisruptionBudgetList:
		return idx.listInto(dst, idx.pdbs, pdbFieldSet, pdbLabels, opts)
	case *v1.NodePoolList:
		return idx.listInto(dst, idx.nodePools, nodePoolFieldSet, nodePoolLabels, opts)
	case *v1.NodeClaimList:
		return idx.listInto(dst, idx.nodeClaims, nodeClaimFieldSet, nodeClaimLabels, opts)
	case *corev1.NamespaceList:
		if opts.LabelSelector != nil && !opts.LabelSelector.Empty() {
			return errNotIndexed
		}
		return idx.listInto(dst, idx.namespaces, namespaceFieldSet, namespaceLabels, opts)
	default:
		return errNotIndexed
	}
}

func (idx *objectIndex) podCandidates(opts client.ListOptions) []runtime.Object {
	if opts.FieldSelector != nil {
		if nodeName, ok := opts.FieldSelector.RequiresExactMatch("spec.nodeName"); ok {
			return idx.podsByNodeName[nodeName]
		}
	}
	if opts.Namespace != "" {
		return idx.podsByNamespace[opts.Namespace]
	}
	return idx.allPods
}

func (idx *objectIndex) nodesByNameList() []runtime.Object {
	out := make([]runtime.Object, 0, len(idx.nodesByName))
	for _, node := range idx.nodesByName {
		out = append(out, node)
	}
	return out
}

func (idx *objectIndex) listInto(
	dst client.ObjectList,
	candidates []runtime.Object,
	fieldSet func(runtime.Object) fields.Set,
	labelSet func(runtime.Object) labels.Set,
	opts client.ListOptions,
) error {
	filtered, err := filterObjects(candidates, opts, fieldSet, labelSet)
	if err != nil {
		return err
	}
	return setObjectList(dst, filtered)
}

func filterObjects(
	candidates []runtime.Object,
	opts client.ListOptions,
	fieldSet func(runtime.Object) fields.Set,
	labelSet func(runtime.Object) labels.Set,
) ([]runtime.Object, error) {
	var filtered []runtime.Object
	for _, obj := range candidates {
		if opts.Namespace != "" {
			accessor, err := meta.Accessor(obj)
			if err != nil {
				return nil, err
			}
			if accessor.GetNamespace() != opts.Namespace {
				continue
			}
		}
		if opts.LabelSelector != nil && !opts.LabelSelector.Matches(labelSet(obj)) {
			continue
		}
		if opts.FieldSelector != nil && !opts.FieldSelector.Matches(fieldSet(obj)) {
			continue
		}
		filtered = append(filtered, obj)
	}
	return filtered, nil
}

func setObjectList(dst client.ObjectList, objs []runtime.Object) error {
	copied := make([]runtime.Object, len(objs))
	for i, obj := range objs {
		copied[i] = obj.DeepCopyObject()
	}
	return meta.SetList(dst, copied)
}

func podFieldSet(obj runtime.Object) fields.Set {
	pod := obj.(*corev1.Pod)
	return fields.Set{"spec.nodeName": pod.Spec.NodeName}
}

func podLabels(obj runtime.Object) labels.Set {
	return labels.Set(obj.(*corev1.Pod).Labels)
}

func nodeFieldSet(obj runtime.Object) fields.Set {
	node := obj.(*corev1.Node)
	return fields.Set{
		"spec.providerID": node.Spec.ProviderID,
	}
}

func nodeLabels(obj runtime.Object) labels.Set {
	return labels.Set(obj.(*corev1.Node).Labels)
}

func daemonSetFieldSet(runtime.Object) fields.Set { return fields.Set{} }

func daemonSetLabels(obj runtime.Object) labels.Set {
	return labels.Set(obj.(*appsv1.DaemonSet).Labels)
}

func pdbFieldSet(runtime.Object) fields.Set { return fields.Set{} }

func pdbLabels(obj runtime.Object) labels.Set {
	return labels.Set(obj.(*policyv1.PodDisruptionBudget).Labels)
}

func nodePoolFieldSet(obj runtime.Object) fields.Set {
	np := obj.(*v1.NodePool)
	ref := np.Spec.Template.Spec.NodeClassRef
	var group, kind, name string
	if ref != nil {
		group, kind, name = ref.Group, ref.Kind, ref.Name
	}
	return fields.Set{
		"spec.template.spec.nodeClassRef.group": group,
		"spec.template.spec.nodeClassRef.kind":  kind,
		"spec.template.spec.nodeClassRef.name":  name,
	}
}

func nodePoolLabels(obj runtime.Object) labels.Set {
	return labels.Set(obj.(*v1.NodePool).Labels)
}

func nodeClaimFieldSet(obj runtime.Object) fields.Set {
	nc := obj.(*v1.NodeClaim)
	ref := nc.Spec.NodeClassRef
	var group, kind, name string
	if ref != nil {
		group, kind, name = ref.Group, ref.Kind, ref.Name
	}
	return fields.Set{
		"status.providerID":       nc.Status.ProviderID,
		"spec.nodeClassRef.group": group,
		"spec.nodeClassRef.kind":  kind,
		"spec.nodeClassRef.name":  name,
	}
}

func nodeClaimLabels(obj runtime.Object) labels.Set {
	return labels.Set(obj.(*v1.NodeClaim).Labels)
}

func namespaceFieldSet(runtime.Object) fields.Set { return fields.Set{} }

func namespaceLabels(obj runtime.Object) labels.Set {
	return labels.Set(obj.(*corev1.Namespace).Labels)
}
