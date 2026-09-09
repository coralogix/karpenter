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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOtelEndpointConfigured(t *testing.T) {
	for _, key := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_AGENT_HOST",
	} {
		t.Setenv(key, "")
	}

	require.False(t, otelEndpointConfigured())

	t.Setenv("OTEL_EXPORTER_OTLP_AGENT_HOST", "collector")
	require.False(t, otelEndpointConfigured())

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector:4317")
	require.True(t, otelEndpointConfigured())
}
