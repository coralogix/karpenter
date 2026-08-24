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
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type readCache struct {
	scheme *runtime.Scheme
	get    sync.Map
	list   sync.Map
}

func newReadCache(scheme *runtime.Scheme) *readCache {
	return &readCache{scheme: scheme}
}

func (c *readCache) interceptorFuncs() interceptor.Funcs {
	return interceptor.Funcs{
		Get:  c.getObject,
		List: c.listObjects,
	}
}

func (c *readCache) getObject(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	cacheKey, err := c.getCacheKey(obj, key)
	if err != nil {
		return cl.Get(ctx, key, obj, opts...)
	}
	if cached, ok := c.get.Load(cacheKey); ok {
		return assignObject(obj, cached.(runtime.Object))
	}
	if err := cl.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	c.get.Store(cacheKey, obj.DeepCopyObject())
	return nil
}

func (c *readCache) listObjects(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
	cacheKey, err := c.listCacheKey(list, opts...)
	if err != nil {
		return cl.List(ctx, list, opts...)
	}
	if cached, ok := c.list.Load(cacheKey); ok {
		return assignList(list, cached.(runtime.Object))
	}
	if err := cl.List(ctx, list, opts...); err != nil {
		return err
	}
	c.list.Store(cacheKey, list.DeepCopyObject())
	return nil
}

func (c *readCache) getCacheKey(obj client.Object, key client.ObjectKey) (string, error) {
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("get|%s|%s|%s", gvk.String(), key.Namespace, key.Name), nil
}

func (c *readCache) listCacheKey(list client.ObjectList, opts ...client.ListOption) (string, error) {
	gvk, err := apiutil.GVKForObject(list, c.scheme)
	if err != nil {
		return "", err
	}
	listOpts := client.ListOptions{}
	listOpts.ApplyOptions(opts)
	labelSelector := ""
	if listOpts.LabelSelector != nil {
		labelSelector = listOpts.LabelSelector.String()
	}
	fieldSelector := ""
	if listOpts.FieldSelector != nil {
		fieldSelector = listOpts.FieldSelector.String()
	}
	return fmt.Sprintf("list|%s|%s|%s|%s", gvk.String(), listOpts.Namespace, labelSelector, fieldSelector), nil
}

func assignObject(dst client.Object, src runtime.Object) error {
	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr || dstVal.IsNil() {
		return fmt.Errorf("destination must be a non-nil pointer")
	}
	srcCopy := src.DeepCopyObject()
	dstVal.Elem().Set(reflect.ValueOf(srcCopy).Elem())
	return nil
}

func assignList(dst client.ObjectList, src runtime.Object) error {
	objs, err := meta.ExtractList(src)
	if err != nil {
		return err
	}
	copied := make([]runtime.Object, len(objs))
	for i, obj := range objs {
		copied[i] = obj.DeepCopyObject()
	}
	return meta.SetList(dst, copied)
}
