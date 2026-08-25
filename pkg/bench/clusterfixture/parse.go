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
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"

	_ "sigs.k8s.io/karpenter/pkg/apis/v1"
	_ "sigs.k8s.io/karpenter/pkg/test/v1alpha1"
)

func decodeYAMLFile[T client.Object](path string) ([]T, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	return decodeYAMLDocuments[T](data)
}

//nolint:gocyclo
func decodeYAMLDocuments[T client.Object](data []byte) ([]T, error) {
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var out []T
	for {
		raw := runtime.RawExtension{}
		if err := decoder.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		if len(raw.Raw) == 0 {
			continue
		}
		docDecoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw.Raw), 4096)
		var meta metav1.TypeMeta
		if err := docDecoder.Decode(&meta); err != nil {
			return nil, err
		}
		if meta.Kind == "List" {
			listDecoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw.Raw), 4096)
			var list metav1.List
			if err := listDecoder.Decode(&list); err != nil {
				return nil, err
			}
			for _, item := range list.Items {
				obj, ok, err := decodeObject[T](item.Raw)
				if err != nil {
					return nil, err
				}
				if ok {
					out = append(out, obj)
				}
			}
			continue
		}
		obj, ok, err := decodeObject[T](raw.Raw)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, obj)
		}
	}
	return out, nil
}

func decodeObject[T client.Object](raw []byte) (T, bool, error) {
	var zero T
	obj := reflect.New(reflect.TypeOf(zero).Elem()).Interface().(T)
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	if err := decoder.Decode(obj); err != nil {
		return zero, false, err
	}
	if obj.GetName() == "" && obj.GetGenerateName() == "" {
		return zero, false, nil
	}
	sanitizeObject(obj)
	return obj, true, nil
}

func sanitizeObject(obj client.Object) {
	obj.SetManagedFields(nil)
	obj.SetResourceVersion("")
	obj.SetUID("")
	obj.SetGeneration(0)
}
