package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`namespace: test-ns
globalMaxConcurrency: 12
httpListenAddr: ":18080"
natsUrl: "nats://127.0.0.1:4222"
pebbleDataPath: "./data"
backupDir: "./backup"
leaseName: scheduler-leader
otelEndpoint: "collector:4318"
logLevel: debug
`)
	require.NoError(t, os.WriteFile(path, data, 0600))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.Equal(t, "test-ns", cfg.Namespace)
	require.Equal(t, 12, cfg.GlobalMaxConcurrency)
	require.Equal(t, ":18080", cfg.HttpListenAddr)
	require.Equal(t, "nats://127.0.0.1:4222", cfg.NatsUrl)
	require.Equal(t, "./data", cfg.PebbleDataPath)
	require.Equal(t, "./backup", cfg.BackupDir)
	require.Equal(t, "scheduler-leader", cfg.LeaseName)
	require.Equal(t, "collector:4318", cfg.OtelEndpoint)
	require.Equal(t, "debug", cfg.LogLevel)
}

func TestLoadConfigMissingFile(t *testing.T) {
	_, err := LoadConfig(filepath.Join(t.TempDir(), "missing.yaml"))
	require.Error(t, err)
}

func TestLoadConfigMalformedYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	require.NoError(t, os.WriteFile(path, []byte("namespace: [broken"), 0600))
	_, err := LoadConfig(path)
	require.Error(t, err)
}
