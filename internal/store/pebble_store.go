package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/model"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"
)

// 前缀区分不同KV
const (
	prefixTask  = "task:"
	prefixShard = "shard:"
)

var (
	// ErrTaskNotFound 任务不存在
	ErrTaskNotFound = errors.New("task not found")
	// ErrShardNotFound 分片不存在
	ErrShardNotFound = errors.New("shard not found")
	// ErrTaskExists 任务已存在
	ErrTaskExists = errors.New("task already exists")
)

type Store struct {
	db      *pebble.DB
	cache   *pebble.Cache
	writeMu sync.Mutex
}

func NewStore(path string) (*Store, error) {
	return NewStoreWithCacheMB(path, 64)
}

// NewStoreWithCacheMB 使用可配置的 block cache 打开 Pebble，便于按 Pod 内存规格调优。
func NewStoreWithCacheMB(path string, cacheMB int) (*Store, error) {
	if cacheMB <= 0 {
		cacheMB = 64
	}
	cache := pebble.NewCache(int64(cacheMB) << 20)
	opts := &pebble.Options{
		Cache:        cache,
		MemTableSize: 64 << 20,
		MaxConcurrentCompactions: func() int {
			return 2
		},
		BytesPerSync:    1 << 20,
		WALBytesPerSync: 1 << 20,
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		cache.Unref()
		return nil, err
	}
	return &Store{db: db, cache: cache}, nil
}

func (s *Store) DB() *pebble.DB {
	return s.db
}

func (s *Store) Close() error {
	err := s.db.Close()
	if s.cache != nil {
		s.cache.Unref()
		s.cache = nil
	}
	return err
}

// Healthy 用于 readiness 探针，确认底层存储仍可正常读取。
func (s *Store) Healthy() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("store is not initialized")
	}
	_, closer, err := s.db.Get([]byte("__health_check__"))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return closer.Close()
}

// SaveTask 保存任务
func (s *Store) SaveTask(t *model.OfflineTask) error {
	key := []byte(prefixTask + t.TaskID)
	val, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return s.db.Set(key, val, pebble.Sync)
}

// SaveTaskWithShards 原子地保存任务及其全部分片，避免任务已入库但分片缺失。
func (s *Store) SaveTaskWithShards(t *model.OfflineTask, shards []*model.TaskShard) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	taskVal, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := batch.Set([]byte(prefixTask+t.TaskID), taskVal, nil); err != nil {
		return err
	}
	for _, shard := range shards {
		shardVal, err := json.Marshal(shard)
		if err != nil {
			return err
		}
		if err := batch.Set([]byte(prefixShard+shard.ShardID), shardVal, nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

// CreateTaskWithShards 在同一进程内提供幂等创建语义，避免并发提交覆盖已有任务。
func (s *Store) CreateTaskWithShards(t *model.OfflineTask, shards []*model.TaskShard) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if _, err := s.GetTask(t.TaskID); err == nil {
		return fmt.Errorf("%w: %s", ErrTaskExists, t.TaskID)
	} else if !errors.Is(err, ErrTaskNotFound) {
		return err
	}
	return s.SaveTaskWithShards(t, shards)
}

// GetTask 获取任务
func (s *Store) GetTask(taskId string) (*model.OfflineTask, error) {
	key := []byte(prefixTask + taskId)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, fmt.Errorf("%w: %s", ErrTaskNotFound, taskId)
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var task model.OfflineTask
	err = json.Unmarshal(val, &task)
	if err != nil {
		return nil, err
	}
	return &task, nil
}

// SaveShard 保存分片
func (s *Store) SaveShard(shard *model.TaskShard) error {
	key := []byte(prefixShard + shard.ShardID)
	val, err := json.Marshal(shard)
	if err != nil {
		return err
	}
	return s.db.Set(key, val, pebble.Sync)
}

// GetShard 获取分片
func (s *Store) GetShard(shardId string) (*model.TaskShard, error) {
	key := []byte(prefixShard + shardId)
	val, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, fmt.Errorf("%w: %s", ErrShardNotFound, shardId)
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var shard model.TaskShard
	err = json.Unmarshal(val, &shard)
	if err != nil {
		return nil, err
	}
	return &shard, nil
}

// ListShardsByTaskID 根据taskId查询所有分片
func (s *Store) ListShardsByTaskID(taskId string) ([]*model.TaskShard, error) {
	var list []*model.TaskShard
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefixShard),
		UpperBound: []byte(prefixShard + "\xff"),
	})
	if err != nil {
		logger.L.Error("pebble list shards fail", zap.Error(err))
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var shard model.TaskShard
		err := json.Unmarshal(iter.Value(), &shard)
		if err != nil {
			continue
		}
		if shard.TaskID == taskId {
			list = append(list, &shard)
		}
	}
	return list, nil
}

// ListAllRunningShards 查询所有运行中的分片（超时巡检使用）
func (s *Store) ListAllRunningShards() ([]*model.TaskShard, error) {
	var list []*model.TaskShard
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefixShard),
		UpperBound: []byte(prefixShard + "\xff"),
	})
	if err != nil {
		logger.L.Error("pebble list all running shards fail", zap.Error(err))
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var shard model.TaskShard
		err := json.Unmarshal(iter.Value(), &shard)
		if err != nil {
			continue
		}
		if shard.Status == model.ShardStatusRunning {
			list = append(list, &shard)
		}
	}
	return list, nil
}

// ListTasks 返回全部任务，供 Leader 启动时恢复内存调度队列。
func (s *Store) ListTasks() ([]*model.OfflineTask, error) {
	var list []*model.OfflineTask
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefixTask),
		UpperBound: []byte(prefixTask + "\xff"),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var task model.OfflineTask
		if err := json.Unmarshal(iter.Value(), &task); err != nil {
			continue
		}
		list = append(list, &task)
	}
	return list, nil
}

// DeleteTerminalTasksBefore 删除 UpdatedAt 早于 cutoff 的终态任务及其全部分片。
// 任务和分片通过同一个 Batch 提交，避免清理过程中留下孤立分片。
func (s *Store) DeleteTerminalTasksBefore(cutoff time.Time) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	expired := make(map[string]struct{})
	taskIter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefixTask),
		UpperBound: []byte(prefixTask + "\xff"),
	})
	if err != nil {
		return 0, err
	}
	for taskIter.First(); taskIter.Valid(); taskIter.Next() {
		var task model.OfflineTask
		if err := json.Unmarshal(taskIter.Value(), &task); err != nil {
			continue
		}
		if isTerminalTaskStatus(task.Status) && !task.UpdatedAt.IsZero() && task.UpdatedAt.Before(cutoff) {
			expired[task.TaskID] = struct{}{}
		}
	}
	iterErr := taskIter.Error()
	_ = taskIter.Close()
	if iterErr != nil {
		return 0, iterErr
	}
	if len(expired) == 0 {
		return 0, nil
	}

	shardKeys := make([][]byte, 0)
	shardIter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefixShard),
		UpperBound: []byte(prefixShard + "\xff"),
	})
	if err != nil {
		return 0, err
	}
	for shardIter.First(); shardIter.Valid(); shardIter.Next() {
		var shard model.TaskShard
		if err := json.Unmarshal(shardIter.Value(), &shard); err != nil {
			continue
		}
		if _, ok := expired[shard.TaskID]; ok {
			key := append([]byte(nil), shardIter.Key()...)
			shardKeys = append(shardKeys, key)
		}
	}
	iterErr = shardIter.Error()
	_ = shardIter.Close()
	if iterErr != nil {
		return 0, iterErr
	}

	batch := s.db.NewBatch()
	defer batch.Close()
	for _, key := range shardKeys {
		if err := batch.Delete(key, nil); err != nil {
			return 0, err
		}
	}
	for taskID := range expired {
		if err := batch.Delete([]byte(prefixTask+taskID), nil); err != nil {
			return 0, err
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, err
	}
	return len(expired), nil
}

func isTerminalTaskStatus(status string) bool {
	switch status {
	case model.TaskStatusSuccess, model.TaskStatusFailed, model.TaskStatusCanceled:
		return true
	default:
		return false
	}
}
