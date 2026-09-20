package model

import "time"

// 任务优先级常量
const (
	PriorityLow    = 1
	PriorityNormal = 5
	PriorityHigh   = 10
)

// 任务状态
const (
	TaskStatusPending   = "pending"
	TaskStatusSuccess   = "success"
	TaskStatusFailed    = "failed"
	TaskStatusCanceled  = "canceled"
)

// 分片状态
const (
	ShardStatusPending = "pending"
	ShardStatusRunning = "running"
	ShardStatusSuccess = "success"
	ShardStatusFailed  = "failed"
)

type SubmitTaskRequest struct {
	TaskID      string            `json:"taskId"`
	TotalShard  int               `json:"totalShard"`
	Concurrency int               `json:"concurrency"`
	MaxRetry    int               `json:"maxRetry"`
	TimeoutSec  int64             `json:"timeoutSec"`
	Params      map[string]string `json:"params"`
	Labels      map[string]string `json:"labels"`
	Priority    int               `json:"priority"`
}

type OfflineTask struct {
	TaskID      string
	TotalShard  int
	Concurrency int
	MaxRetry    int
	TimeoutSec  int64
	Params      map[string]string
	Labels      map[string]string
	Priority    int
	Status      string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type TaskShard struct {
	ShardID    string
	TaskID     string
	ShardIndex int
	K8sJobName string
	Status     string
	RetryCount int
	StartedAt  time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

