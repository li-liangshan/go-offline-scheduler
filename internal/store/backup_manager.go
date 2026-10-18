package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"offline-scheduler/internal/logger"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

type BackupManager struct {
	db            *pebble.DB
	backupDir     string
	retention     int           // 保留多少份备份
	backupTimeout time.Duration // 单次备份超时
	ticker        *time.Ticker
	wg            sync.WaitGroup
	stopCh        chan struct{}
	stopOnce      sync.Once
	backupMu      sync.Mutex
}

func NewBackupManager(db *pebble.DB, backupDir string, retainCount int, timeout time.Duration) (*BackupManager, error) {
	return NewBackupManagerWithInterval(db, backupDir, retainCount, timeout, time.Hour)
}

func NewBackupManagerWithInterval(db *pebble.DB, backupDir string, retainCount int, timeout, interval time.Duration) (*BackupManager, error) {
	if db == nil {
		return nil, fmt.Errorf("pebble db cannot be nil")
	}
	if retainCount <= 0 {
		retainCount = 1
	}
	if timeout <= 0 {
		timeout = time.Minute
	}
	if interval <= 0 {
		interval = time.Hour
	}
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, err
	}
	return &BackupManager{
		db:            db,
		backupDir:     backupDir,
		retention:     retainCount,
		backupTimeout: timeout,
		ticker:        time.NewTicker(interval),
		stopCh:        make(chan struct{}),
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
	ctx, cancel := context.WithTimeout(context.Background(), b.backupTimeout)
	defer cancel()
	b.DoBackup(ctx)
}

func (b *BackupManager) Stop() {
	b.stopOnce.Do(func() {
		close(b.stopCh)
		b.ticker.Stop()
		b.wg.Wait()
		logger.S.Info("backup manager stopped")
	})
}

// DoBackup 执行一次在线快照备份（一致性热备）
func (b *BackupManager) DoBackup(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, b.backupTimeout)
	defer cancel()
	if err := b.checkpoint(ctx); err != nil {
		logger.L.Error("pebble checkpoint backup failed", zap.Error(err))
	}
}

// checkpoint 使用 Pebble 原生 Checkpoint 生成可恢复的一致性备份。
// 只复制 SST 文件而缺少 MANIFEST/CURRENT 的备份无法被 Pebble 打开，因此不能手工拼装备份目录。
func (b *BackupManager) checkpoint(ctx context.Context) error {
	b.backupMu.Lock()
	defer b.backupMu.Unlock()

	if err := os.MkdirAll(b.backupDir, 0700); err != nil {
		return fmt.Errorf("create backup root dir: %w", err)
	}

	ts := time.Now().UnixNano()
	tmpBackupPath := filepath.Join(b.backupDir, fmt.Sprintf("backup-%d.tmp", ts))
	finalBackupPath := filepath.Join(b.backupDir, fmt.Sprintf("backup-%d", ts))

	defer func() {
		_ = os.RemoveAll(tmpBackupPath)
	}()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.db.Checkpoint(tmpBackupPath); err != nil {
		return fmt.Errorf("create pebble checkpoint: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(tmpBackupPath, finalBackupPath); err != nil {
		return fmt.Errorf("rename backup: %w", err)
	}
	logger.S.Infow("pebble backup done", "path", finalBackupPath)

	if err := b.cleanOldBackups(); err != nil {
		logger.L.Warn("clean old backup warning", zap.Error(err))
	}
	return nil
}

// cleanOldBackups 按创建时间排序，删除超出保留数量的旧备份
func (b *BackupManager) cleanOldBackups() error {
	entries, err := os.ReadDir(b.backupDir)
	if err != nil {
		return err
	}

	var backupDirs []os.DirEntry
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		// 只匹配正式备份，忽略.tmp临时目录
		if len(name) > 7 && name[:7] == "backup-" && name[len(name)-4:] != ".tmp" {
			backupDirs = append(backupDirs, e)
		}
	}

	// 按修改时间升序，老的放前面
	type item struct {
		entry os.DirEntry
		modT  time.Time
	}
	var list []item
	for _, d := range backupDirs {
		fi, err := d.Info()
		if err != nil {
			continue
		}
		list = append(list, item{entry: d, modT: fi.ModTime()})
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].modT.Before(list[j].modT)
	})

	// 删除超出保留数量的
	delCount := len(list) - b.retention
	if delCount <= 0 {
		return nil
	}
	for i := 0; i < delCount; i++ {
		fullPath := filepath.Join(b.backupDir, list[i].entry.Name())
		logger.S.Infow("remove old pebble backup", "path", fullPath)
		if err := os.RemoveAll(fullPath); err != nil {
			logger.L.Warn("failed to delete old backup", zap.String("path", fullPath), zap.Error(err))
		}
	}
	return nil
}
