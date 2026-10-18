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

func TestCreateTaskWithShardsIsIdempotent(t *testing.T) {
	st := newTestStore(t)
	task := &model.OfflineTask{TaskID: "idempotent", TotalShard: 1, Status: model.TaskStatusPending}
	shards := []*model.TaskShard{{ShardID: "idempotent-shard-0", TaskID: "idempotent", Status: model.ShardStatusPending}}
	require.NoError(t, st.CreateTaskWithShards(task, shards))
	err := st.CreateTaskWithShards(task, shards)
	require.ErrorIs(t, err, ErrTaskExists)
}

func TestStoreNotFoundSentinels(t *testing.T) {
	st := newTestStore(t)
	_, err := st.GetTask("missing")
	require.True(t, errors.Is(err, ErrTaskNotFound))

	_, err = st.GetShard("missing")
	require.True(t, errors.Is(err, ErrShardNotFound))
}

func TestStoreListTasksAndHealth(t *testing.T) {
	st := newTestStore(t)
	require.NoError(t, st.Healthy())
	require.NoError(t, st.SaveTask(&model.OfflineTask{TaskID: "task-1", Status: model.TaskStatusPending}))
	require.NoError(t, st.SaveTask(&model.OfflineTask{TaskID: "task-2", Status: model.TaskStatusSuccess}))

	tasks, err := st.ListTasks()
	require.NoError(t, err)
	require.Len(t, tasks, 2)
	ids := map[string]bool{}
	for _, task := range tasks {
		ids[task.TaskID] = true
	}
	require.True(t, ids["task-1"])
	require.True(t, ids["task-2"])
}

func TestNewStoreWithCacheMB(t *testing.T) {
	st, err := NewStoreWithCacheMB(filepath.Join(t.TempDir(), "db"), 8)
	require.NoError(t, err)
	require.NotNil(t, st.DB())
	require.NoError(t, st.Healthy())
	require.NoError(t, st.Close())
}

func TestDeleteTerminalTasksBefore(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)
	recent := now.Add(-time.Hour)

	tasks := []*model.OfflineTask{
		{TaskID: "old-success", Status: model.TaskStatusSuccess, UpdatedAt: old},
		{TaskID: "old-canceled", Status: model.TaskStatusCanceled, UpdatedAt: old},
		{TaskID: "old-pending", Status: model.TaskStatusPending, UpdatedAt: old},
		{TaskID: "recent-success", Status: model.TaskStatusSuccess, UpdatedAt: recent},
	}
	for _, task := range tasks {
		shard := &model.TaskShard{ShardID: task.TaskID + "-shard-0", TaskID: task.TaskID, Status: model.ShardStatusSuccess, UpdatedAt: task.UpdatedAt}
		require.NoError(t, st.SaveTaskWithShards(task, []*model.TaskShard{shard}))
	}

	deleted, err := st.DeleteTerminalTasksBefore(now.Add(-24 * time.Hour))
	require.NoError(t, err)
	require.Equal(t, 2, deleted)

	for _, taskID := range []string{"old-success", "old-canceled"} {
		_, err := st.GetTask(taskID)
		require.ErrorIs(t, err, ErrTaskNotFound)
		_, err = st.GetShard(taskID + "-shard-0")
		require.ErrorIs(t, err, ErrShardNotFound)
	}
	for _, taskID := range []string{"old-pending", "recent-success"} {
		_, err := st.GetTask(taskID)
		require.NoError(t, err)
	}
}
