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

package disruption

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"sigs.k8s.io/karpenter/pkg/cxtracing"
)

const disruptionLoopSpanName = "karpenter.disruption.loop"

// traceMethod runs one method in a child span of the loop span, named after the method's type, and records whether it
// started a command, did nothing, or failed.
func traceMethod(ctx context.Context, loopSpanName string, m Method, run func(context.Context, Method) (bool, error)) (bool, error) {
	typeName := fmt.Sprintf("%T", m)
	methodName := typeName[strings.LastIndex(typeName, ".")+1:]
	ctx, endSpan := cxtracing.Start(ctx, loopSpanName+"."+methodName)
	defer endSpan()
	success, err := run(ctx, m)
	span := trace.SpanFromContext(ctx)
	switch {
	case err != nil:
		span.SetAttributes(attribute.String("outcome", "failed"))
		span.SetStatus(codes.Error, err.Error())
	case success:
		span.SetAttributes(attribute.String("outcome", "command"))
	default:
		span.SetAttributes(attribute.String("outcome", "none"))
	}
	return success, err
}
