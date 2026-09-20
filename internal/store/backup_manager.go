package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"offline-scheduler/internal/logger"
)

type BackupManager struct {
	db       *pebble.DB
	backupDir string
	ticker   *time.Ticker
	wg       sync.WaitGroup
	stopCh   chan struct{}
}

func NewBackupManager(db *pebble.DB, backupDir string) (*BackupManager, error) {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, err
	}
	return &BackupManager{
		db: db,
		backupDir: backupDir,
		ticker: time.NewTicker(1 * time.Hour),
		stopCh: make(chan struct{}),
	}, nil
}

func (b *BackupManager) Start() {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		for {
			select {
			case <-b.stopCh:
				return
			case <-b.ticker.C:
				b.doBackup()
			}
		}
	}()
}

func (b *BackupManager) doBackup() {
	snap, err := b.db.NewSnapshot()
	if err != nil {
		logger.L.Error("pebble create snapshot fail", zap.Error(err))
		return
	}
	defer snap.Close()
	backupPath := filepath.Join(b.backupDir, fmt.Sprintf("backup-%d", time.Now().Unix()))
	err = snap.Save(backupPath)
	if err != nil {
		logger.L.Error("pebble snapshot save fail", zap.Error(err))
		return
	}
	logger.S.Infow("pebble backup done", "path", backupPath)
}

func (b *BackupManager) Stop() {
	close(b.stopCh)
	b.ticker.Stop()
	b.wg.Wait()
	logger.S.Info("backup manager stopped")
}

