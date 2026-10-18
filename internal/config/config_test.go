package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	data := []byte(`namespace: test-ns
globalMaxConcurrency: 12
httpListenAddr: ":18080"
natsUrl: "nats://127.0.0.1:4222"
natsStreamMaxAgeSec: 0
pebbleDataPath: "./data"
backupDir: "./backup"
leaseName: scheduler-leader
otelEndpoint: "collector:4318"
logLevel: debug
workerImage: "registry.example.com/worker:v1"
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
	require.Equal(t, "registry.example.com/worker:v1", cfg.WorkerImage)
	require.Equal(t, "IfNotPresent", cfg.WorkerImagePullPolicy)
	require.Equal(t, 3600, cfg.BackupInterval)
	require.Equal(t, 5, cfg.NatsMaxDeliver)
	require.Equal(t, 24*3600, cfg.NatsStreamMaxAgeSec)
	require.Equal(t, 30*24*3600, cfg.TaskRetentionSec)
	require.Equal(t, 600, cfg.CleanupIntervalSec)
	require.True(t, cfg.ResourceCheckEnabled)
}

func TestLoadConfigAppliesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("workerImage: worker:v1\n"), 0600))

	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.Equal(t, "offline-job", cfg.Namespace)
	require.Equal(t, 50, cfg.GlobalMaxConcurrency)
	require.Equal(t, 5, cfg.HttpReadHeaderSec)
	require.Equal(t, int64(1<<20), cfg.HttpMaxBodyBytes)
	require.Equal(t, 64, cfg.PebbleCacheMB)
	require.Equal(t, 24, cfg.BackupRetain)
}

func TestLoadConfigEnvironmentOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("workerImage: worker:v1\n"), 0600))
	t.Setenv("SCHEDULER_GLOBALMAXCONCURRENCY", "7")

	cfg, err := LoadConfig(path)
	require.NoError(t, err)
	require.Equal(t, 7, cfg.GlobalMaxConcurrency)
}

func TestLoadConfigValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("workerImage: \"\"\n"), 0600))
	_, err := LoadConfig(path)
	require.Error(t, err)
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

func TestLoadConfigRejectsNegativeTaskRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("workerImage: worker:v1\ntaskRetentionSec: -1\n"), 0600))

	_, err := LoadConfig(path)
	require.Error(t, err)
}

func TestDurationHelpers(t *testing.T) {
	cfg := AppConfig{
		HttpReadHeaderSec: 1,
		HttpReadSec:       2,
		HttpWriteSec:      3,
		HttpIdleSec:       4,
		BackupInterval:    5,
		BackupTimeout:     6,
	}

	require.Equal(t, time.Second, cfg.HTTPReadHeaderTimeout())
	require.Equal(t, 2*time.Second, cfg.HTTPReadTimeout())
	require.Equal(t, 3*time.Second, cfg.HTTPWriteTimeout())
	require.Equal(t, 4*time.Second, cfg.HTTPIdleTimeout())
	require.Equal(t, 5*time.Second, cfg.BackupIntervalDuration())
	require.Equal(t, 6*time.Second, cfg.BackupTimeoutDuration())
}
