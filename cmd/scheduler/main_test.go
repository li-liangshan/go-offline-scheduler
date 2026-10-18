package main

import (
	"os"
	"path/filepath"
	"testing"

	"offline-scheduler/internal/config"

	"github.com/stretchr/testify/require"
)

func TestBuildKubeClientFromKubeconfig(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	path := filepath.Join(t.TempDir(), "config")
	data := []byte(`apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:6443
    insecure-skip-tls-verify: true
  name: test
contexts:
- context:
    cluster: test
    user: test
  name: test
current-context: test
users:
- name: test
  user:
    token: dummy
`)
	require.NoError(t, os.WriteFile(path, data, 0600))

	client, err := buildKubeClientFromKubeconfig(path)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.NotNil(t, client.CoreV1())
}

func TestBuildKubeClientFromKubeconfigMissingFile(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")

	client, err := buildKubeClientFromKubeconfig(filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
	require.Nil(t, client)
}

func TestBuildSchedulerOptions(t *testing.T) {
	cfg := &config.AppConfig{
		GlobalMaxConcurrency:   8,
		WorkerImage:            "worker:v1",
		WorkerImagePullPolicy:  "Always",
		WorkerCPURequest:       "250m",
		WorkerCPULimit:         "1",
		WorkerMemoryRequest:    "512Mi",
		WorkerMemoryLimit:      "2Gi",
		WorkerServiceAccount:   "worker-sa",
		DeleteFinishedJobs:     false,
		JobTTLAfterFinishedSec: 120,
		ResourceCheckEnabled:   true,
	}
	opts, err := buildSchedulerOptions(cfg)
	require.NoError(t, err)
	require.Equal(t, 8, opts.GlobalMaxConcurrency)
	require.Equal(t, "worker:v1", opts.WorkerImage)
	require.Equal(t, "Always", string(opts.ImagePullPolicy))
	require.Equal(t, "worker-sa", opts.ServiceAccountName)
	require.Equal(t, int32(120), opts.JobTTLSecondsAfterFinished)
	require.False(t, opts.DeleteFinishedJobs)
}

func TestBuildSchedulerOptionsRejectsInvalidLimits(t *testing.T) {
	cfg := &config.AppConfig{
		WorkerCPURequest:    "2",
		WorkerCPULimit:      "1",
		WorkerMemoryRequest: "1Gi",
		WorkerMemoryLimit:   "4Gi",
	}
	_, err := buildSchedulerOptions(cfg)
	require.Error(t, err)
}

func TestInstanceIdentity(t *testing.T) {
	t.Setenv("POD_NAME", "pod-test")
	require.Equal(t, "pod-test", instanceIdentity())
}
