package store

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"offline-scheduler/internal/model"

	"github.com/stretchr/testify/require"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := NewStore(filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	require.NotNil(t, st.DB())
	return st
}

func TestStoreTaskCRUD(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	task := &model.OfflineTask{
		TaskID:      "task-1",
		TotalShard:  2,
		Concurrency: 1,
		MaxRetry:    2,
		TimeoutSec:  60,
		Status:      model.TaskStatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	require.NoError(t, st.SaveTask(task))
	got, err := st.GetTask(task.TaskID)
	require.NoError(t, err)
	require.Equal(t, task, got)

	task.Status = model.TaskStatusSuccess
	task.UpdatedAt = now.Add(time.Second)
	require.NoError(t, st.SaveTask(task))
	got, err = st.GetTask(task.TaskID)
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusSuccess, got.Status)

	_, err = st.GetTask("missing")
	require.ErrorIs(t, err, ErrTaskNotFound)
}

func TestStoreShardCRUDAndQueries(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	shards := []*model.TaskShard{
		{ShardID: "task-1-shard-0", TaskID: "task-1", ShardIndex: 0, Status: model.ShardStatusRunning, CreatedAt: now, UpdatedAt: now},
		{ShardID: "task-1-shard-1", TaskID: "task-1", ShardIndex: 1, Status: model.ShardStatusPending, CreatedAt: now, UpdatedAt: now},
		{ShardID: "task-2-shard-0", TaskID: "task-2", ShardIndex: 0, Status: model.ShardStatusRunning, CreatedAt: now, UpdatedAt: now},
	}
	for _, shard := range shards {
		require.NoError(t, st.SaveShard(shard))
	}

	got, err := st.GetShard("task-1-shard-0")
	require.NoError(t, err)
	require.Equal(t, shards[0], got)

	_, err = st.GetShard("missing")
	require.ErrorIs(t, err, ErrShardNotFound)

	byTask, err := st.ListShardsByTaskID("task-1")
	require.NoError(t, err)
	require.Len(t, byTask, 2)
	for _, shard := range byTask {
		require.Equal(t, "task-1", shard.TaskID)
	}

	running, err := st.ListAllRunningShards()
	require.NoError(t, err)
	require.Len(t, running, 2)
	for _, shard := range running {
		require.Equal(t, model.ShardStatusRunning, shard.Status)
	}
}

func TestSaveTaskWithShards(t *testing.T) {
	st := newTestStore(t)
	task := &model.OfflineTask{TaskID: "atomic", TotalShard: 2, Status: model.TaskStatusPending}
	shards := []*model.TaskShard{
		{ShardID: "atomic-shard-0", TaskID: "atomic", ShardIndex: 0, Status: model.ShardStatusPending},
		{ShardID: "atomic-shard-1", TaskID: "atomic", ShardIndex: 1, Status: model.ShardStatusPending},
	}

	require.NoError(t, st.SaveTaskWithShards(task, shards))
	gotTask, err := st.GetTask("atomic")
	require.NoError(t, err)
	require.Equal(t, task, gotTask)

	gotShards, err := st.ListShardsByTaskID("atomic")
	require.NoError(t, err)
	require.Len(t, gotShards, 2)
}

func TestStoreNotFoundSentinels(t *testing.T) {
	st := newTestStore(t)
	_, err := st.GetTask("missing")
	require.True(t, errors.Is(err, ErrTaskNotFound))

	_, err = st.GetShard("missing")
	require.True(t, errors.Is(err, ErrShardNotFound))
}
