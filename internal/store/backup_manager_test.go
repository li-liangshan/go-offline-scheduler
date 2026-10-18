package store

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"offline-scheduler/internal/logger"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.InitLogger()
	os.Exit(m.Run())
}

func TestBackupManagerCreatesRestorableCheckpoint(t *testing.T) {
	root := t.TempDir()
	db, err := pebble.Open(filepath.Join(root, "db"), &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Set([]byte("key"), []byte("value"), pebble.Sync))

	backupDir := filepath.Join(root, "backups")
	manager, err := NewBackupManager(db, backupDir, 2, 5*time.Second)
	require.NoError(t, err)
	manager.DoBackup(context.Background())
	manager.Stop()
	manager.Stop() // Stop 必须可重复调用

	entries, err := os.ReadDir(backupDir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, entries[0].IsDir())

	restored, err := pebble.Open(filepath.Join(backupDir, entries[0].Name()), &pebble.Options{})
	require.NoError(t, err)
	defer restored.Close()

	value, closer, err := restored.Get([]byte("key"))
	require.NoError(t, err)
	defer closer.Close()
	require.Equal(t, "value", string(value))
}
