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
	TaskStatusPending  = "pending"
	TaskStatusSuccess  = "success"
	TaskStatusFailed   = "failed"
	TaskStatusCanceled = "canceled"
)

// 分片状态
const (
	ShardStatusPending  = "pending"
	ShardStatusRunning  = "running"
	ShardStatusSuccess  = "success"
	ShardStatusFailed   = "failed"
	ShardStatusCanceled = "canceled"
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
	TaskID      string            `json:"taskId"`
	TotalShard  int               `json:"totalShard"`
	Concurrency int               `json:"concurrency"`
	MaxRetry    int               `json:"maxRetry"`
	TimeoutSec  int64             `json:"timeoutSec"`
	Params      map[string]string `json:"params,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Priority    int               `json:"priority"`
	Status      string            `json:"status"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
}

type TaskShard struct {
	ShardID    string    `json:"shardId"`
	TaskID     string    `json:"taskId"`
	ShardIndex int       `json:"shardIndex"`
	K8sJobName string    `json:"k8sJobName,omitempty"`
	Status     string    `json:"status"`
	RetryCount int       `json:"retryCount"`
	StartedAt  time.Time `json:"startedAt,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}
