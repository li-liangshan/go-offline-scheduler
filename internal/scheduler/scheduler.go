package scheduler

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/metric"
	"offline-scheduler/internal/model"
	"offline-scheduler/internal/resourcecheck"
	"offline-scheduler/internal/store"

	"go.uber.org/zap"
)

const (
	defaultPodCPU    = "2"
	defaultPodMemory = "4Gi"
	scanInterval     = 1 * time.Second
)

// PriorityTaskItem 优先级堆元素
type PriorityTaskItem struct {
	task *model.OfflineTask
}
type PriorityQueue []*PriorityTaskItem

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool {
	// 数字越大优先级越高
	return pq[i].task.Priority > pq[j].task.Priority
}
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}
func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*PriorityTaskItem)
	*pq = append(*pq, item)
}
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

type Scheduler struct {
	k8sClient   kubernetes.Interface
	store       *store.Store
	resChecker  *resourcecheck.ResourceChecker
	namespace   string
	jobInformer *JobInformer

	priorityQueue PriorityQueue
	queueMu       sync.Mutex

	maxConcurrency int
	runningShards  int
	runningMu      sync.Mutex

	ctx                  context.Context
	cancelFunc           context.CancelFunc
	wg                   sync.WaitGroup
	timeoutCheckerTicker *time.Ticker
}

func NewScheduler(k8sClient kubernetes.Interface, st *store.Store, rc *resourcecheck.ResourceChecker, ns string, globalMaxConcurrency int) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		k8sClient:      k8sClient,
		store:          st,
		resChecker:     rc,
		namespace:      ns,
		priorityQueue:  make(PriorityQueue, 0),
		maxConcurrency: globalMaxConcurrency,
		ctx:            ctx,
		cancelFunc:     cancel,
	}
}

// SetInformer 注入job informer实例
func (s *Scheduler) SetInformer(ji *JobInformer) {
	s.jobInformer = ji
}

// startShardTimeoutChecker 分片超时巡检后台协程
func (s *Scheduler) StartShardTimeoutChecker() {
	s.wg.Add(1)
	s.timeoutCheckerTicker = time.NewTicker(10 * time.Second)
	go func() {
		defer s.wg.Done()
		defer s.timeoutCheckerTicker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-s.timeoutCheckerTicker.C:
				s.checkShardTimeout()
			}
		}
	}()
}

func (s *Scheduler) checkShardTimeout() {
	// 列出所有running分片
	shards, err := s.store.ListAllRunningShards()
	if err != nil {
		logger.L.Error("list running shards failed", zap.Error(err))
		return
	}
	now := time.Now()
	for _, shard := range shards {
		task, err := s.store.GetTask(shard.TaskID)
		if err != nil {
			continue
		}
		// 分片运行时间超过任务超时时间
		if shard.StartedAt.IsZero() {
			continue
		}
		if now.Sub(shard.StartedAt) > time.Duration(task.TimeoutSec)*time.Second {
			logger.S.Warnw("shard timeout, kill job", "taskId", shard.TaskID, "shardId", shard.ShardID)
			// 标记分片失败，删除k8s job
			shard.Status = model.ShardStatusFailed
			shard.RetryCount++
			_ = s.store.SaveShard(shard)
			prop := metav1.DeletePropagationBackground
			_ = s.k8sClient.BatchV1().Jobs(s.namespace).Delete(context.Background(), shard.K8sJobName,
				metav1.DeleteOptions{PropagationPolicy: &prop})
			// 释放并发槽
			s.runningMu.Lock()
			s.runningShards--
			s.runningMu.Unlock()
		}
	}
}

// SubmitTask 外部http/nats入口提交任务
func (s *Scheduler) SubmitTask(req *model.SubmitTaskRequest) error {
	// 1. 构造任务模型
	now := time.Now()
	task := &model.OfflineTask{
		TaskID:      req.TaskID,
		TotalShard:  req.TotalShard,
		Concurrency: req.Concurrency,
		MaxRetry:    req.MaxRetry,
		TimeoutSec:  req.TimeoutSec,
		Params:      req.Params,
		Labels:      req.Labels,
		Priority:    req.Priority,
		Status:      model.TaskStatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	if task.Priority < model.PriorityLow || task.Priority > model.PriorityHigh {
		task.Priority = model.PriorityNormal
	}

	// 2. 资源预校验：单pod资源使用默认值
	cpuQ := resource.MustParse(defaultPodCPU)
	memQ := resource.MustParse(defaultPodMemory)
	ok, err := s.resChecker.Check(task, cpuQ, memQ)
	if err != nil || !ok {
		metric.TaskSubmitFailed.Inc()
		return fmt.Errorf("resource check reject: %w", err)
	}

	// 3.持久化任务元数据
	if err := s.store.SaveTask(task); err != nil {
		metric.TaskSubmitFailed.Inc()
		return err
	}

	// 4.推入优先级调度堆
	s.queueMu.Lock()
	heap.Push(&s.priorityQueue, &PriorityTaskItem{task: task})
	s.queueMu.Unlock()

	metric.TaskSubmitOk.Inc()
	logger.S.Infow("task accepted", "taskId", task.TaskID, "priority", task.Priority, "totalShard", task.TotalShard)
	return nil
}

// Run 调度主循环，阻塞运行
func (s *Scheduler) Run() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(scanInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				logger.S.Info("scheduler main loop receive shutdown signal, exit")
				return
			case <-ticker.C:
				s.scheduleLoopOnce()
			}
		}
	}()
	logger.S.Info("scheduler run started, global max concurrency:", s.maxConcurrency)
}

// scheduleLoopOnce 单次调度逻辑：取高优任务、分配分片、创建k8s job
func (s *Scheduler) scheduleLoopOnce() {
	s.runningMu.Lock()
	running := s.runningShards
	s.runningMu.Unlock()
	if running >= s.maxConcurrency {
		// 全局并发已满，跳过本轮调度
		return
	}

	s.queueMu.Lock()
	if s.priorityQueue.Len() == 0 {
		s.queueMu.Unlock()
		return
	}
	// 弹出最高优先级任务
	item := heap.Pop(&s.priorityQueue).(*PriorityTaskItem)
	task := item.task
	s.queueMu.Unlock()

	// 读取最新db状态，防止内存队列与存储状态不一致
	dbTask, err := s.store.GetTask(task.TaskID)
	if err != nil {
		logger.S.Warnw("task not found in store, drop from queue", "taskId", task.TaskID)
		return
	}
	task = dbTask

	// 任务已经结束，不再处理
	switch task.Status {
	case model.TaskStatusSuccess, model.TaskStatusFailed, model.TaskStatusCanceled:
		return
	}

	// 获取该任务下所有分片
	shards, err := s.store.ListShardsByTaskID(task.TaskID)
	if err != nil {
		logger.L.Error("list shards failed", zap.String("taskId", task.TaskID), zap.Error(err))
		// 重新塞回队列，下一轮重试
		s.queueMu.Lock()
		heap.Push(&s.priorityQueue, &PriorityTaskItem{task: task})
		s.queueMu.Unlock()
		return
	}

	// 统计该任务待运行分片
	var pendingShards []*model.TaskShard
	for _, shard := range shards {
		if shard.Status == model.ShardStatusPending || shard.Status == model.ShardStatusFailed {
			if shard.RetryCount < task.MaxRetry {
				pendingShards = append(pendingShards, shard)
			}
		}
	}
	if len(pendingShards) == 0 {
		// 该任务没有待跑分片，更新任务状态
		s.tryFinalizeTask(task)
		return
	}

	// 本轮最多启动多少分片：全局剩余槽位 和 任务自身并发取小
	s.runningMu.Lock()
	globalFree := s.maxConcurrency - s.runningShards
	s.runningMu.Unlock()
	canStart := min(globalFree, task.Concurrency, len(pendingShards))

	for i := 0; i < canStart; i++ {
		shard := pendingShards[i]
		s.launchShard(task, shard)
	}

	// 还有剩余未启动分片，把任务重新放回优先级队列，下一轮继续调度
	if canStart < len(pendingShards) {
		s.queueMu.Lock()
		heap.Push(&s.priorityQueue, &PriorityTaskItem{task: task})
		s.queueMu.Unlock()
	}
}

// launchShard 创建K8s Job，启动分片
func (s *Scheduler) launchShard(task *model.OfflineTask, shard *model.TaskShard) {
	defer func() {
		if r := recover(); r != nil {
			logger.L.Error("launchShard panic recover",
				zap.String("taskId", task.TaskID),
				zap.String("shardId", shard.ShardID),
				zap.Any("panic", r))
			metric.ShardLaunchPanic.Inc()
		}
	}()

	jobName := fmt.Sprintf("offline-%s-shard-%d", task.TaskID, shard.ShardIndex)
	shard.K8sJobName = jobName
	shard.Status = model.ShardStatusRunning
	shard.StartedAt = time.Now()

	// 更新分片状态
	_ = s.store.SaveShard(shard)

	job := s.buildK8sJob(task, shard)
	_, err := s.k8sClient.BatchV1().Jobs(s.namespace).Create(context.Background(), job, metav1.CreateOptions{})
	if err != nil {
		metric.JobCreateErr.Inc()
		logger.L.Error("create k8s job fail",
			zap.String("taskId", task.TaskID),
			zap.String("shardId", shard.ShardID),
			zap.Error(err))
		// 创建失败，标记分片失败，增加重试计数
		shard.Status = model.ShardStatusFailed
		shard.RetryCount++
		_ = s.store.SaveShard(shard)
		return
	}

	metric.JobCreateOk.Inc()
	s.runningMu.Lock()
	s.runningShards++
	s.runningMu.Unlock()
	logger.S.Infow("shard job created", "taskId", task.TaskID, "shardId", shard.ShardID, "jobName", jobName)
}

// buildK8sJob 构造job对象，根据业务自行修改镜像、命令、环境变量
func (s *Scheduler) buildK8sJob(task *model.OfflineTask, shard *model.TaskShard) *batchv1.Job {
	backoffLimit := int32(0)
	activeDeadlineSeconds := int64(task.TimeoutSec)

	envList := make([]corev1.EnvVar, 0)
	envList = append(envList, corev1.EnvVar{Name: "TASK_ID", Value: task.TaskID})
	envList = append(envList, corev1.EnvVar{Name: "SHARD_ID", Value: shard.ShardID})
	envList = append(envList, corev1.EnvVar{Name: "SHARD_INDEX", Value: fmt.Sprintf("%d", shard.ShardIndex)})
	for k, v := range task.Params {
		envList = append(envList, corev1.EnvVar{Name: k, Value: v})
	}

	cpu := resource.MustParse(defaultPodCPU)
	mem := resource.MustParse(defaultPodMemory)

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      shard.K8sJobName,
			Namespace: s.namespace,
			Labels: map[string]string{
				"task-id":  task.TaskID,
				"shard-id": shard.ShardID,
				"app":      "offline-worker",
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:          &backoffLimit,
			ActiveDeadlineSeconds: &activeDeadlineSeconds,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"task-id":  task.TaskID,
						"shard-id": shard.ShardID,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "worker",
							Image: "your-worker-image:latest",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    cpu,
									corev1.ResourceMemory: mem,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    cpu,
									corev1.ResourceMemory: mem,
								},
							},
							Env: envList,
						},
					},
				},
			},
		},
	}
}

// OnJobStatusUpdate informer回调，Job状态变更回调入口
func (s *Scheduler) OnJobStatusUpdate(job *batchv1.Job) {
	taskId := job.Labels["task-id"]
	shardId := job.Labels["shard-id"]
	if taskId == "" || shardId == "" {
		return
	}

	shard, err := s.store.GetShard(shardId)
	if err != nil {
		return
	}

	oldStatus := shard.Status
	switch {
	case job.Status.Succeeded > 0:
		shard.Status = model.ShardStatusSuccess
	case job.Status.Failed > 0:
		shard.Status = model.ShardStatusFailed
		shard.RetryCount++
	default:
		return
	}

	if oldStatus == shard.Status {
		return
	}

	// 分片状态发生变更
	if err := s.store.SaveShard(shard); err != nil {
		logger.L.Error("save shard status fail", zap.Error(err))
	}

	// 分片结束，释放并发槽
	s.runningMu.Lock()
	s.runningShards--
	s.runningMu.Unlock()

	// 删除完成job，避免资源堆积
	prop := metav1.DeletePropagationBackground
	_ = s.k8sClient.BatchV1().Jobs(s.namespace).Delete(context.Background(), job.Name, metav1.DeleteOptions{
		PropagationPolicy: &prop,
	})

	// 判断整个任务是否可以结束
	task, err := s.store.GetTask(taskId)
	if err == nil {
		s.tryFinalizeTask(task)
	}
}

// tryFinalizeTask 判断所有分片完成，更新任务总状态
func (s *Scheduler) tryFinalizeTask(task *model.OfflineTask) {
	shards, err := s.store.ListShardsByTaskID(task.TaskID)
	if err != nil {
		return
	}
	allDone := true
	hasFailed := false
	for _, sd := range shards {
		if sd.Status != model.ShardStatusSuccess && sd.Status != model.ShardStatusFailed {
			allDone = false
		}
		if sd.Status == model.ShardStatusFailed {
			hasFailed = true
		}
	}
	if !allDone {
		return
	}
	if hasFailed {
		task.Status = model.TaskStatusFailed
		metric.TaskTotalFail.Inc()
	} else {
		task.Status = model.TaskStatusSuccess
		metric.TaskTotalSuccess.Inc()
	}
	task.UpdatedAt = time.Now()
	_ = s.store.SaveTask(task)
	logger.S.Infow("task final done", "taskId", task.TaskID, "status", task.Status)
}

// CancelTask 外部取消任务接口
func (s *Scheduler) CancelTask(taskId string) error {
	task, err := s.store.GetTask(taskId)
	if err != nil {
		return fmt.Errorf("task not found")
	}
	if task.Status == model.TaskStatusCanceled ||
		task.Status == model.TaskStatusSuccess ||
		task.Status == model.TaskStatusFailed {
		return fmt.Errorf("task already finished")
	}

	// 更新任务状态为取消
	task.Status = model.TaskStatusCanceled
	task.UpdatedAt = time.Now()
	if err := s.store.SaveTask(task); err != nil {
		return err
	}

	// 删除该任务下所有正在运行的job
	shards, err := s.store.ListShardsByTaskID(taskId)
	if err != nil {
		return err
	}
	prop := metav1.DeletePropagationBackground
	for _, shard := range shards {
		if shard.K8sJobName != "" {
			_ = s.k8sClient.BatchV1().Jobs(s.namespace).Delete(context.Background(), shard.K8sJobName,
				metav1.DeleteOptions{PropagationPolicy: &prop})
		}
	}
	metric.TaskCancel.Inc()
	logger.S.Infow("task canceled", "taskId", taskId)
	return nil
}

// Stop 优雅关闭调度器
func (s *Scheduler) Stop() {
	logger.S.Info("scheduler stopping ...")
	s.cancelFunc()
	s.wg.Wait()
	logger.S.Info("scheduler stopped complete")
}

func min(a, b, c int) int {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	return m
}
