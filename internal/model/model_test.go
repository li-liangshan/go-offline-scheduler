package model

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStatusAndPriorityConstants(t *testing.T) {
	require.Equal(t, 1, PriorityLow)
	require.Equal(t, 5, PriorityNormal)
	require.Equal(t, 10, PriorityHigh)

	require.Equal(t, "pending", TaskStatusPending)
	require.Equal(t, "success", TaskStatusSuccess)
	require.Equal(t, "failed", TaskStatusFailed)
	require.Equal(t, "canceled", TaskStatusCanceled)

	require.Equal(t, "pending", ShardStatusPending)
	require.Equal(t, "running", ShardStatusRunning)
	require.Equal(t, "success", ShardStatusSuccess)
	require.Equal(t, "failed", ShardStatusFailed)
	require.Equal(t, "canceled", ShardStatusCanceled)
}

func TestSubmitTaskRequestJSONRoundTrip(t *testing.T) {
	req := SubmitTaskRequest{
		TaskID:      "task-1",
		TotalShard:  3,
		Concurrency: 2,
		MaxRetry:    1,
		TimeoutSec:  60,
		Params:      map[string]string{"key": "value"},
		Labels:      map[string]string{"env": "test"},
		Priority:    PriorityHigh,
	}

	data, err := json.Marshal(req)
	require.NoError(t, err)
	require.Contains(t, string(data), `"taskId":"task-1"`)

	var decoded SubmitTaskRequest
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, req, decoded)
}

func TestOfflineTaskJSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	task := OfflineTask{
		TaskID:      "task-2",
		TotalShard:  2,
		Concurrency: 1,
		MaxRetry:    2,
		TimeoutSec:  120,
		Params:      map[string]string{"a": "b"},
		Labels:      map[string]string{"team": "data"},
		Priority:    PriorityNormal,
		Status:      TaskStatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	data, err := json.Marshal(task)
	require.NoError(t, err)
	var decoded OfflineTask
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, task, decoded)
}

func TestTaskShardJSONRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	shard := TaskShard{
		ShardID:    "task-3-shard-0",
		TaskID:     "task-3",
		ShardIndex: 0,
		K8sJobName: "offline-task-3-shard-0-r0",
		Status:     ShardStatusRunning,
		RetryCount: 1,
		StartedAt:  now,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	data, err := json.Marshal(shard)
	require.NoError(t, err)
	var decoded TaskShard
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, shard, decoded)
}

func TestOfflineTaskAcceptsLegacyFieldNames(t *testing.T) {
	legacy := []byte(`{"TaskID":"legacy","TotalShard":1,"Status":"pending"}`)
	var task OfflineTask
	require.NoError(t, json.Unmarshal(legacy, &task))
	require.Equal(t, "legacy", task.TaskID)
	require.Equal(t, 1, task.TotalShard)
	require.Equal(t, TaskStatusPending, task.Status)
}
