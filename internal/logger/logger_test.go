package logger

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestInitLogger(t *testing.T) {
	InitLogger()
	require.NotNil(t, L)
	require.NotNil(t, S)

	L.Info("logger test")
	S.Infow("sugared logger test", "key", "value")
	Sync()

	InitLogger()
	require.NotNil(t, L)
	require.NotNil(t, S)
}

func TestInitLoggerWithLevel(t *testing.T) {
	require.NoError(t, InitLoggerWithLevel("debug"))
	require.NotNil(t, L)
	require.True(t, L.Core().Enabled(zapcore.DebugLevel))

	require.Error(t, InitLoggerWithLevel("unknown"))
}
