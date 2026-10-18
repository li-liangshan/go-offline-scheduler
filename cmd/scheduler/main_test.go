package main

import (
	"os"
	"path/filepath"
	"testing"

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
