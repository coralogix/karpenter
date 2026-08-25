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

package cxtracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	attrK8sResource  = attribute.Key("k8s.resource")
	attrK8sNamespace = attribute.Key("k8s.namespace")
	attrK8sName      = attribute.Key("k8s.name")
)

// KubeList begins a span for a Kubernetes List call. Call the returned function with the List error when done.
func KubeList(ctx context.Context, resource, namespace string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	spanAttrs := []attribute.KeyValue{attrK8sResource.String(resource)}
	if namespace != "" {
		spanAttrs = append(spanAttrs, attrK8sNamespace.String(namespace))
	}
	spanAttrs = append(spanAttrs, attrs...)
	tracedCtx, end := Start(ctx, "karpenter.kube.List", spanAttrs...)
	return tracedCtx, func(err error) {
		recordSpanError(tracedCtx, end, err)
	}
}

// KubeGet begins a span for a Kubernetes Get call. Call the returned function with the Get error when done.
func KubeGet(ctx context.Context, resource, namespace, name string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	spanAttrs := []attribute.KeyValue{
		attrK8sResource.String(resource),
		attrK8sName.String(name),
	}
	if namespace != "" {
		spanAttrs = append(spanAttrs, attrK8sNamespace.String(namespace))
	}
	spanAttrs = append(spanAttrs, attrs...)
	tracedCtx, end := Start(ctx, "karpenter.kube.Get", spanAttrs...)
	return tracedCtx, func(err error) {
		recordSpanError(tracedCtx, end, err)
	}
}

func recordSpanError(ctx context.Context, end func(), err error) {
	if err != nil {
		span := trace.SpanFromContext(ctx)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	end()
}
