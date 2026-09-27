package store

import (
	"encoding/json"
	"fmt"

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

type Store struct {
	db *pebble.DB
}

func NewStore(path string) (*Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

func (s *Store) DB() *pebble.DB {
	return s.db
}

func (s *Store) Close() error {
	return s.db.Close()
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

// GetTask 获取任务
func (s *Store) GetTask(taskId string) (*model.OfflineTask, error) {
	key := []byte(prefixTask + taskId)
	val, closer, err := s.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, fmt.Errorf("task not found")
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
	if err == pebble.ErrNotFound {
		return nil, fmt.Errorf("shard not found")
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
