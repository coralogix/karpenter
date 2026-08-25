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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func withTestTracer() *tracetest.SpanRecorder {
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	return sr
}

func TestKubeList(t *testing.T) {
	sr := withTestTracer()

	_, end := KubeList(context.Background(), "Pod", "default")
	end(nil)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "karpenter.kube.List", spans[0].Name())
	require.Equal(t, "Pod", spans[0].Attributes()[0].Value.AsString())
}

func TestKubeGetRecordsError(t *testing.T) {
	sr := withTestTracer()

	_, end := KubeGet(context.Background(), "Node", "", "node-1")
	end(errors.New("not found"))

	spans := sr.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, codes.Error, spans[0].Status().Code)
}

func TestKubeGet(t *testing.T) {
	sr := withTestTracer()

	_, end := KubeGet(context.Background(), "Node", "", "node-1")
	end(nil)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "karpenter.kube.Get", spans[0].Name())
}
