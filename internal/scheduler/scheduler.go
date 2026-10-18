package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"

	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/metric"
	"offline-scheduler/internal/model"
	"offline-scheduler/internal/resourcecheck"
	"offline-scheduler/internal/store"

	"go.uber.org/zap"
)

const (
	defaultPodCPU        = "2"
	defaultPodMemory     = "4Gi"
	defaultTaskTimeout   = 3600
	scanInterval         = 1 * time.Second
	timeoutCheckInterval = 10 * time.Second
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
	old[n-1] = nil
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
	queuedTasks   map[string]struct{}

	maxConcurrency int
	runningShards  int
	runningMu      sync.Mutex
	shardMu        sync.Mutex

	runMu      sync.Mutex
	cancelFunc context.CancelFunc
	running    bool
	wg         sync.WaitGroup
}

func NewScheduler(k8sClient kubernetes.Interface, st *store.Store, rc *resourcecheck.ResourceChecker, ns string, globalMaxConcurrency int) *Scheduler {
	if globalMaxConcurrency <= 0 {
		globalMaxConcurrency = 1
	}
	runningShards := 0
	if st != nil {
		if shards, err := st.ListAllRunningShards(); err == nil {
			runningShards = len(shards)
		}
	}
	return &Scheduler{
		k8sClient:      k8sClient,
		store:          st,
		resChecker:     rc,
		namespace:      ns,
		priorityQueue:  make(PriorityQueue, 0),
		queuedTasks:    make(map[string]struct{}),
		maxConcurrency: globalMaxConcurrency,
		runningShards:  runningShards,
	}
}

// SetInformer 注入job informer实例
func (s *Scheduler) SetInformer(ji *JobInformer) {
	s.jobInformer = ji
}

// Run 启动调度主循环和超时巡检。只有 leader 调用；重复调用是安全的。
func (s *Scheduler) Run() {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if s.running {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelFunc = cancel
	s.running = true

	s.wg.Add(2)
	go s.scheduleLoop(ctx)
	go s.timeoutLoop(ctx)
	logger.S.Infow("scheduler run started", "globalMaxConcurrency", s.maxConcurrency)
}

// Stop 停止调度循环并等待正在执行的调度动作结束。重复调用是安全的。
func (s *Scheduler) Stop() {
	s.runMu.Lock()
	defer s.runMu.Unlock()
	if !s.running {
		return
	}
	logger.S.Info("scheduler stopping ...")
	s.cancelFunc()
	s.wg.Wait()
	s.running = false
	logger.S.Info("scheduler stopped complete")
}

func (s *Scheduler) scheduleLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.S.Info("scheduler main loop receive shutdown signal, exit")
			return
		case <-ticker.C:
			s.scheduleLoopOnce()
		}
	}
}

func (s *Scheduler) timeoutLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(timeoutCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkShardTimeout()
		}
	}
}

func (s *Scheduler) enqueueTask(task *model.OfflineTask) {
	if task == nil || isTaskTerminal(task.Status) {
		return
	}
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if _, ok := s.queuedTasks[task.TaskID]; ok {
		return
	}
	heap.Push(&s.priorityQueue, &PriorityTaskItem{task: task})
	s.queuedTasks[task.TaskID] = struct{}{}
}

func (s *Scheduler) popTask() *model.OfflineTask {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if s.priorityQueue.Len() == 0 {
		return nil
	}
	item := heap.Pop(&s.priorityQueue).(*PriorityTaskItem)
	delete(s.queuedTasks, item.task.TaskID)
	return item.task
}

func (s *Scheduler) globalFree() int {
	s.runningMu.Lock()
	defer s.runningMu.Unlock()
	return s.maxConcurrency - s.runningShards
}

func (s *Scheduler) addRunningSlot() {
	s.runningMu.Lock()
	s.runningShards++
	s.runningMu.Unlock()
}

func (s *Scheduler) releaseRunningSlot() {
	s.runningMu.Lock()
	if s.runningShards > 0 {
		s.runningShards--
	}
	s.runningMu.Unlock()
}

// SubmitTask 外部http/nats入口提交任务
func (s *Scheduler) SubmitTask(req *model.SubmitTaskRequest) error {
	if req == nil {
		return fmt.Errorf("request cannot be nil")
	}
	if s.store == nil || s.resChecker == nil {
		return fmt.Errorf("scheduler is not initialized")
	}

	taskID := strings.TrimSpace(req.TaskID)
	if taskID == "" {
		return fmt.Errorf("taskId is required")
	}
	if errs := validation.IsValidLabelValue(taskID); len(errs) > 0 {
		return fmt.Errorf("invalid taskId: %s", strings.Join(errs, ", "))
	}
	if req.TotalShard <= 0 {
		return fmt.Errorf("totalShard must be greater than 0")
	}
	for key := range req.Params {
		if errs := validation.IsEnvVarName(key); len(errs) > 0 {
			return fmt.Errorf("invalid param name %q: %s", key, strings.Join(errs, ", "))
		}
	}

	concurrency := req.Concurrency
	if concurrency <= 0 {
		concurrency = req.TotalShard
	}
	if concurrency > req.TotalShard {
		concurrency = req.TotalShard
	}
	maxRetry := req.MaxRetry
	if maxRetry < 0 {
		maxRetry = 0
	}
	timeoutSec := req.TimeoutSec
	if timeoutSec <= 0 {
		timeoutSec = defaultTaskTimeout
	}
	priority := req.Priority
	if priority < model.PriorityLow || priority > model.PriorityHigh {
		priority = model.PriorityNormal
	}

	if _, err := s.store.GetTask(taskID); err == nil {
		return fmt.Errorf("task %s already exists", taskID)
	} else if !errors.Is(err, store.ErrTaskNotFound) {
		return err
	}

	now := time.Now()
	task := &model.OfflineTask{
		TaskID:      taskID,
		TotalShard:  req.TotalShard,
		Concurrency: concurrency,
		MaxRetry:    maxRetry,
		TimeoutSec:  timeoutSec,
		Params:      req.Params,
		Labels:      req.Labels,
		Priority:    priority,
		Status:      model.TaskStatusPending,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	cpuQ := resource.MustParse(defaultPodCPU)
	memQ := resource.MustParse(defaultPodMemory)
	ok, err := s.resChecker.Check(task, cpuQ, memQ)
	if err != nil {
		metric.TaskSubmitFailed.Inc()
		return fmt.Errorf("resource check failed: %w", err)
	}
	if !ok {
		metric.TaskSubmitFailed.Inc()
		return fmt.Errorf("cluster resource insufficient")
	}

	shards := make([]*model.TaskShard, 0, task.TotalShard)
	for i := 0; i < task.TotalShard; i++ {
		shards = append(shards, &model.TaskShard{
			ShardID:    buildShardID(task.TaskID, i),
			TaskID:     task.TaskID,
			ShardIndex: i,
			Status:     model.ShardStatusPending,
			CreatedAt:  now,
			UpdatedAt:  now,
		})
	}
	if err := s.store.SaveTaskWithShards(task, shards); err != nil {
		metric.TaskSubmitFailed.Inc()
		return err
	}

	s.enqueueTask(task)
	metric.TaskSubmitOk.Inc()
	logger.S.Infow("task accepted", "taskId", task.TaskID, "priority", task.Priority, "totalShard", task.TotalShard)
	return nil
}

// scheduleLoopOnce 单次调度逻辑：取高优任务、分配分片、创建k8s job
func (s *Scheduler) scheduleLoopOnce() {
	if s.globalFree() <= 0 {
		return
	}

	task := s.popTask()
	if task == nil {
		return
	}

	dbTask, err := s.store.GetTask(task.TaskID)
	if err != nil {
		logger.S.Warnw("task not found in store, drop from queue", "taskId", task.TaskID, "error", err)
		return
	}
	task = dbTask
	if isTaskTerminal(task.Status) {
		return
	}

	shards, err := s.store.ListShardsByTaskID(task.TaskID)
	if err != nil {
		logger.L.Error("list shards failed", zap.String("taskId", task.TaskID), zap.Error(err))
		s.enqueueTask(task)
		return
	}

	var pendingShards []*model.TaskShard
	runningForTask := 0
	for _, shard := range shards {
		switch shard.Status {
		case model.ShardStatusRunning:
			runningForTask++
		case model.ShardStatusPending, model.ShardStatusFailed:
			if shard.RetryCount <= task.MaxRetry {
				pendingShards = append(pendingShards, shard)
			}
		}
	}
	if len(pendingShards) == 0 {
		if runningForTask == 0 {
			s.tryFinalizeTask(task)
		}
		return
	}

	// 任务自身并发上限：已在运行的分片也占用该任务的并发额度。
	availableForTask := task.Concurrency - runningForTask
	if availableForTask <= 0 {
		return
	}

	globalFree := s.globalFree()
	if globalFree <= 0 {
		s.enqueueTask(task)
		return
	}

	launchable := min(availableForTask, len(pendingShards))
	canStart := min(globalFree, launchable)
	failedLaunch := false
	for i := 0; i < canStart; i++ {
		if err := s.launchShard(task, pendingShards[i]); err != nil {
			failedLaunch = true
		}
	}

	// 全局槽位不足时下一轮继续；本次有创建失败也需要依靠下一轮重试。
	if canStart < launchable || failedLaunch {
		s.enqueueTask(task)
	}
}

// launchShard 创建K8s Job，启动分片
func (s *Scheduler) launchShard(task *model.OfflineTask, shard *model.TaskShard) (err error) {
	slotAdded := false
	defer func() {
		if r := recover(); r != nil {
			if slotAdded {
				s.releaseRunningSlot()
			}
			err = fmt.Errorf("launch shard panic: %v", r)
			logger.L.Error("launchShard panic recover",
				zap.String("taskId", task.TaskID),
				zap.String("shardId", shard.ShardID),
				zap.Any("panic", r))
			metric.ShardLaunchPanic.Inc()
		}
	}()

	s.shardMu.Lock()
	latest, getErr := s.store.GetShard(shard.ShardID)
	if getErr != nil {
		s.shardMu.Unlock()
		return getErr
	}
	if latest.Status != model.ShardStatusPending && latest.Status != model.ShardStatusFailed {
		s.shardMu.Unlock()
		return nil
	}
	latest.K8sJobName = buildJobName(task.TaskID, latest.ShardIndex, latest.RetryCount)
	latest.Status = model.ShardStatusRunning
	latest.StartedAt = time.Now()
	latest.UpdatedAt = latest.StartedAt
	if saveErr := s.store.SaveShard(latest); saveErr != nil {
		s.shardMu.Unlock()
		return saveErr
	}
	s.addRunningSlot()
	slotAdded = true
	*shard = *latest
	s.shardMu.Unlock()

	job := s.buildK8sJob(task, shard)
	_, createErr := s.k8sClient.BatchV1().Jobs(s.namespace).Create(context.Background(), job, metav1.CreateOptions{})
	if createErr != nil {
		if apierrors.IsAlreadyExists(createErr) {
			logger.S.Warnw("shard job already exists, keep running state",
				"taskId", task.TaskID, "shardId", shard.ShardID, "jobName", shard.K8sJobName)
			return nil
		}
		metric.JobCreateErr.Inc()
		logger.L.Error("create k8s job fail",
			zap.String("taskId", task.TaskID),
			zap.String("shardId", shard.ShardID),
			zap.Error(createErr))

		s.shardMu.Lock()
		latest, getErr = s.store.GetShard(shard.ShardID)
		if getErr == nil && latest.Status == model.ShardStatusRunning && latest.K8sJobName == shard.K8sJobName {
			latest.Status = model.ShardStatusFailed
			latest.RetryCount++
			latest.UpdatedAt = time.Now()
			_ = s.store.SaveShard(latest)
			*shard = *latest
		}
		if slotAdded {
			s.releaseRunningSlot()
			slotAdded = false
		}
		s.shardMu.Unlock()
		return createErr
	}

	metric.JobCreateOk.Inc()
	// 创建期间任务可能已被取消，及时清理刚创建的 Job。
	if current, getErr := s.store.GetTask(task.TaskID); getErr == nil && current.Status == model.TaskStatusCanceled {
		prop := metav1.DeletePropagationBackground
		_ = s.k8sClient.BatchV1().Jobs(s.namespace).Delete(context.Background(), shard.K8sJobName,
			metav1.DeleteOptions{PropagationPolicy: &prop})
		logger.S.Infow("task canceled during job creation, job deleted", "taskId", task.TaskID, "jobName", shard.K8sJobName)
		return nil
	}
	logger.S.Infow("shard job created", "taskId", task.TaskID, "shardId", shard.ShardID, "jobName", shard.K8sJobName)
	return nil
}

// buildK8sJob 构造job对象，根据业务自行修改镜像、命令、环境变量
func (s *Scheduler) buildK8sJob(task *model.OfflineTask, shard *model.TaskShard) *batchv1.Job {
	backoffLimit := int32(0)
	activeDeadlineSeconds := task.TimeoutSec

	envList := make([]corev1.EnvVar, 0, len(task.Params)+3)
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
	if job == nil {
		return
	}
	taskID := job.Labels["task-id"]
	shardID := job.Labels["shard-id"]
	if taskID == "" || shardID == "" {
		return
	}

	shard, applied := s.applyJobStatus(job)
	if !applied {
		return
	}

	prop := metav1.DeletePropagationBackground
	if err := s.k8sClient.BatchV1().Jobs(s.namespace).Delete(context.Background(), job.Name, metav1.DeleteOptions{
		PropagationPolicy: &prop,
	}); err != nil && !apierrors.IsNotFound(err) {
		logger.L.Warn("delete finished job failed", zap.String("job", job.Name), zap.Error(err))
	}

	task, err := s.store.GetTask(taskID)
	if err != nil || isTaskTerminal(task.Status) {
		return
	}
	if shard.Status == model.ShardStatusFailed && shard.RetryCount <= task.MaxRetry {
		s.enqueueTask(task)
		return
	}
	s.tryFinalizeTask(task)
}

// applyJobStatus 串行化分片终态更新，避免 informer 与超时巡检重复释放并发槽。
func (s *Scheduler) applyJobStatus(job *batchv1.Job) (*model.TaskShard, bool) {
	s.shardMu.Lock()
	defer s.shardMu.Unlock()

	shard, err := s.store.GetShard(job.Labels["shard-id"])
	if err != nil {
		return nil, false
	}
	// 忽略旧 Job 的迟到事件，避免把已重新启动的分片误判为失败。
	if shard.K8sJobName != "" && job.Name != shard.K8sJobName {
		return nil, false
	}
	if shard.Status == model.ShardStatusSuccess ||
		shard.Status == model.ShardStatusFailed ||
		shard.Status == model.ShardStatusCanceled {
		return nil, false
	}

	wasRunning := shard.Status == model.ShardStatusRunning
	switch {
	case job.Status.Succeeded > 0:
		shard.Status = model.ShardStatusSuccess
	case job.Status.Failed > 0:
		shard.Status = model.ShardStatusFailed
		shard.RetryCount++
	default:
		return nil, false
	}
	shard.UpdatedAt = time.Now()
	if err := s.store.SaveShard(shard); err != nil {
		logger.L.Error("save shard status fail", zap.Error(err))
		return nil, false
	}
	if wasRunning {
		s.releaseRunningSlot()
	}
	return shard, true
}

// tryFinalizeTask 判断所有分片完成，更新任务总状态
func (s *Scheduler) tryFinalizeTask(task *model.OfflineTask) {
	if task == nil {
		return
	}
	s.shardMu.Lock()
	defer s.shardMu.Unlock()
	latestTask, err := s.store.GetTask(task.TaskID)
	if err != nil || isTaskTerminal(latestTask.Status) {
		return
	}
	task = latestTask
	shards, err := s.store.ListShardsByTaskID(task.TaskID)
	if err != nil || len(shards) == 0 {
		return
	}

	hasFailed := false
	hasRunning := false
	hasSchedulable := false
	for _, shard := range shards {
		switch shard.Status {
		case model.ShardStatusRunning:
			hasRunning = true
		case model.ShardStatusPending:
			hasSchedulable = true
		case model.ShardStatusFailed:
			if shard.RetryCount <= task.MaxRetry {
				hasSchedulable = true
				continue
			}
			hasFailed = true
		case model.ShardStatusSuccess:
		case model.ShardStatusCanceled:
			return
		default:
			return
		}
	}
	if hasSchedulable {
		s.enqueueTask(task)
		return
	}
	if hasRunning {
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
	if err := s.store.SaveTask(task); err != nil {
		logger.L.Error("save task final status fail", zap.Error(err))
		return
	}
	logger.S.Infow("task final done", "taskId", task.TaskID, "status", task.Status)
}

// checkShardTimeout 分片超时巡检后台任务
func (s *Scheduler) checkShardTimeout() {
	shards, err := s.store.ListAllRunningShards()
	if err != nil {
		logger.L.Error("list running shards failed", zap.Error(err))
		return
	}
	now := time.Now()
	for _, listedShard := range shards {
		task, err := s.store.GetTask(listedShard.TaskID)
		if err != nil || isTaskTerminal(task.Status) {
			continue
		}
		timeoutSec := task.TimeoutSec
		if timeoutSec <= 0 {
			timeoutSec = defaultTaskTimeout
		}

		s.shardMu.Lock()
		shard, err := s.store.GetShard(listedShard.ShardID)
		if err != nil || shard.Status != model.ShardStatusRunning || shard.StartedAt.IsZero() {
			s.shardMu.Unlock()
			continue
		}
		if now.Sub(shard.StartedAt) <= time.Duration(timeoutSec)*time.Second {
			s.shardMu.Unlock()
			continue
		}

		logger.S.Warnw("shard timeout, kill job", "taskId", shard.TaskID, "shardId", shard.ShardID)
		shard.Status = model.ShardStatusFailed
		shard.RetryCount++
		shard.UpdatedAt = now
		if err := s.store.SaveShard(shard); err != nil {
			logger.L.Error("save timed out shard fail", zap.Error(err))
			s.shardMu.Unlock()
			continue
		}
		s.releaseRunningSlot()
		s.shardMu.Unlock()

		prop := metav1.DeletePropagationBackground
		_ = s.k8sClient.BatchV1().Jobs(s.namespace).Delete(context.Background(), shard.K8sJobName,
			metav1.DeleteOptions{PropagationPolicy: &prop})
		s.enqueueTask(task)
	}
}

// CancelTask 外部取消任务接口
func (s *Scheduler) CancelTask(taskID string) error {
	jobNames, err := s.cancelTaskState(taskID)
	if err != nil {
		return err
	}

	prop := metav1.DeletePropagationBackground
	for _, jobName := range jobNames {
		if err := s.k8sClient.BatchV1().Jobs(s.namespace).Delete(context.Background(), jobName,
			metav1.DeleteOptions{PropagationPolicy: &prop}); err != nil && !apierrors.IsNotFound(err) {
			logger.L.Warn("delete canceled job failed", zap.String("job", jobName), zap.Error(err))
		}
	}
	metric.TaskCancel.Inc()
	logger.S.Infow("task canceled", "taskId", taskID)
	return nil
}

func (s *Scheduler) cancelTaskState(taskID string) ([]string, error) {
	s.shardMu.Lock()
	defer s.shardMu.Unlock()

	task, err := s.store.GetTask(taskID)
	if err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			return nil, fmt.Errorf("task not found")
		}
		return nil, err
	}
	if isTaskTerminal(task.Status) {
		return nil, fmt.Errorf("task already finished")
	}

	shards, err := s.store.ListShardsByTaskID(taskID)
	if err != nil {
		return nil, err
	}

	task.Status = model.TaskStatusCanceled
	task.UpdatedAt = time.Now()
	if err := s.store.SaveTask(task); err != nil {
		return nil, err
	}

	jobNames := make([]string, 0, len(shards))
	for _, listedShard := range shards {
		shard, err := s.store.GetShard(listedShard.ShardID)
		if err != nil {
			continue
		}
		if shard.Status == model.ShardStatusSuccess || shard.Status == model.ShardStatusCanceled {
			continue
		}
		wasRunning := shard.Status == model.ShardStatusRunning
		shard.Status = model.ShardStatusCanceled
		shard.UpdatedAt = task.UpdatedAt
		_ = s.store.SaveShard(shard)
		if wasRunning {
			s.releaseRunningSlot()
		}
		if shard.K8sJobName != "" {
			jobNames = append(jobNames, shard.K8sJobName)
		}
	}
	return jobNames, nil
}

func isTaskTerminal(status string) bool {
	switch status {
	case model.TaskStatusSuccess, model.TaskStatusFailed, model.TaskStatusCanceled:
		return true
	default:
		return false
	}
}

func buildJobName(taskID string, shardIndex, retry int) string {
	base := fmt.Sprintf("offline-%s-shard-%d-r%d", sanitizeName(taskID), shardIndex, retry)
	if len(base) <= 63 {
		return base
	}
	return fmt.Sprintf("offline-%x-shard-%d-r%d", taskIDHash(taskID), shardIndex, retry)
}

func buildShardID(taskID string, shardIndex int) string {
	id := fmt.Sprintf("%s-shard-%d", taskID, shardIndex)
	if len(id) <= 63 {
		return id
	}
	return fmt.Sprintf("%x-shard-%d", taskIDHash(taskID), shardIndex)
}

func sanitizeName(value string) string {
	var b strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(value) {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	name := strings.Trim(b.String(), "-")
	if name == "" {
		return "task"
	}
	return name
}

func taskIDHash(value string) [6]byte {
	var h [6]byte
	for i := 0; i < len(value); i++ {
		h[i%len(h)] ^= value[i]
		h[i%len(h)] = h[i%len(h)]*31 + byte(i)
	}
	return h
}
