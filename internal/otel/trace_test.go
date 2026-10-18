package otel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

func TestInitTrace(t *testing.T) {
	t.Setenv("POD_NAME", "pod-test")
	tp, err := InitTrace("offline-scheduler", "127.0.0.1:4318")
	require.NoError(t, err)
	require.NotNil(t, tp)
	require.Same(t, tp, otel.GetTracerProvider())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, tp.Shutdown(ctx))
}

func TestInitTraceUsesDefaultEndpoint(t *testing.T) {
	tp, err := InitTrace("offline-scheduler", "")
	require.NoError(t, err)
	require.NotNil(t, tp)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, tp.Shutdown(ctx))
}
