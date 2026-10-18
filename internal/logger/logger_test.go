package logger

import (
	"testing"

	"github.com/stretchr/testify/require"
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
