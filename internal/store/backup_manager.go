package store

import (
	"context"
	"fmt"
	"io"
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
}

func NewBackupManager(db *pebble.DB, backupDir string, retainCount int, timeout time.Duration) (*BackupManager, error) {
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return nil, err
	}
	return &BackupManager{
		db:            db,
		backupDir:     backupDir,
		retention:     retainCount,
		backupTimeout: timeout,
		ticker:        time.NewTicker(1 * time.Hour),
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
	snap := b.db.NewSnapshot()
	defer snap.Close()
	backupPath := filepath.Join(b.backupDir, fmt.Sprintf("backup-%d", time.Now().Unix()))
	err := snap.Save(backupPath)
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

// DoBackup 执行一次在线快照备份（一致性热备）
func (b *BackupManager) DoBackup(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, b.backupTimeout)
	defer cancel()

	if err := os.MkdirAll(b.backupDir, 0700); err != nil {
		logger.L.Error("create pebble backup root dir failed", zap.String("dir", b.backupDir), zap.Error(err))
		return
	}

	ts := time.Now().Unix()
	tmpBackupPath := filepath.Join(b.backupDir, fmt.Sprintf("backup-%d.tmp", ts))
	finalBackupPath := filepath.Join(b.backupDir, fmt.Sprintf("backup-%d", ts))

	// panic保护
	defer func() {
		if r := recover(); r != nil {
			logger.L.Error("pebble backup panic recovered", zap.Any("panic", r))
			_ = os.RemoveAll(tmpBackupPath)
		}
	}()

	// 空的KeyRange代表全库快照
	keyRanges := []pebble.KeyRange{{Start: nil, End: nil}}
	esSnap := b.db.NewEventuallyFileOnlySnapshot(keyRanges)
	defer func() {
		_ = esSnap.Close()
	}()

	// 等待：直到快照的sst文件都落盘，不再被memtable/wal依赖
	err := esSnap.WaitForFileOnlySnapshot(ctx, b.backupTimeout)
	if err != nil {
		logger.L.Error("WaitForFileOnlySnapshot timeout/fail", zap.Error(err))
		_ = os.RemoveAll(tmpBackupPath)
		return
	}

	// 创建临时备份目录
	if err := os.MkdirAll(tmpBackupPath, 0700); err != nil {
		logger.L.Error("create tmp backup dir fail", zap.Error(err))
		return
	}

	logger.S.Infow("start pebble file-only snapshot backup", "tmp", tmpBackupPath)

	// 遍历DB所有sst文件，复制到备份目录
	fileMetas, err := b.db.SSTables()
	if err != nil {
		logger.L.Error("get sstables meta failed", zap.Error(err))
		_ = os.RemoveAll(tmpBackupPath)
		return
	}
	for _, meta := range fileMetas {
		src := meta.FilePath
		dst := filepath.Join(tmpBackupPath, filepath.Base(src))
		if err := copyFile(src, dst); err != nil {
			logger.L.Error("copy sst file failed", zap.String("src", src), zap.String("dst", dst), zap.Error(err))
			_ = os.RemoveAll(tmpBackupPath)
			return
		}
	}

	// 原子重命名
	err = os.Rename(tmpBackupPath, finalBackupPath)
	if err != nil {
		logger.L.Error("rename backup to final failed", zap.Error(err))
		_ = os.RemoveAll(tmpBackupPath)
		return
	}
	logger.S.Infow("pebble backup done", "path", finalBackupPath)

	// 清理旧备份
	if err := b.cleanOldBackups(); err != nil {
		logger.L.Warn("clean old backup warning", zap.Error(err))
	}
}

// copyFile 流式复制文件，适合大sst，避免一次性读入内存
func copyFile(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	return err
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
