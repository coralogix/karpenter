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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Start begins a span and returns an updated context and an end function.
func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func()) {
	tracer := otel.Tracer(instrumentationName)
	ctx, span := tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	return ctx, func() { span.End() }
}

// Measure starts a span as a child of ctx and returns a context for work inside the phase.
// Pass the returned context only to callees during the phase. Keep using the original ctx
// when starting sibling phase spans.
func Measure(ctx context.Context, metricStop func(), name string, attrs ...attribute.KeyValue) (context.Context, func()) {
	ctx, endSpan := Start(ctx, name, attrs...)
	return ctx, func() {
		endSpan()
		if metricStop != nil {
			metricStop()
		}
	}
}
