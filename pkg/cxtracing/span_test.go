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
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestWithoutSpanDetachesChildFromParent(t *testing.T) {
	sr := withTestTracer()

	ctx, endParent := Start(context.Background(), "parent")
	_, endChild := Start(WithoutSpan(ctx), "child")
	endChild()
	endParent()

	spans := sr.Ended()
	require.Len(t, spans, 2)

	var parent, child sdktrace.ReadOnlySpan
	for _, span := range spans {
		switch span.Name() {
		case "parent":
			parent = span
		case "child":
			child = span
		}
	}
	require.NotNil(t, parent)
	require.NotNil(t, child)
	require.False(t, child.Parent().IsValid())
}
