package scheduler

import (
	"context"
	"sync"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"offline-scheduler/internal/logger"
)

type JobInformer struct {
	k8sClient kubernetes.Interface
	namespace string

	informer cache.SharedIndexInformer
	// 回调，指向Scheduler的OnJobStatusUpdate
	updateCb func(job *batchv1.Job)

	ctx        context.Context
	cancelFunc context.CancelFunc
	wg         sync.WaitGroup

	// 本地缓存 jobName -> job，避免重复api调用
	localCache sync.Map
}

// NewJobInformer 构造Job informer，只监听本namespace，筛选标签 app=offline‑worker
func NewJobInformer(client kubernetes.Interface, ns string, callback func(job *batchv1.Job)) *JobInformer {
	ctx, cancel := context.WithCancel(context.Background())

	// field selector不支持label，使用label selector在ListWatch过滤
	lw := cache.NewListWatchFromClient(
		client.BatchV1().RESTClient(),
		"jobs",
		ns,
		fields.Everything(),
	)

	informer := cache.NewSharedIndexInformer(
		lw,
		&batchv1.Job{},
		30*time.Second, // resync周期，每30s全量重同步一次
		cache.Indexers{},
	)

	ji := &JobInformer{
		k8sClient:  client,
		namespace:  ns,
		informer:   informer,
		updateCb:   callback,
		ctx:        ctx,
		cancelFunc: cancel,
	}

	// 注册事件处理函数
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ji.onAdd,
		UpdateFunc: ji.onUpdate,
		DeleteFunc: ji.onDelete,
	})
	return ji
}

func (ji *JobInformer) onAdd(obj interface{}) {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return
	}
	ji.localCache.Store(job.Name, job)
	ji.dispatch(job)
}

func (ji *JobInformer) onUpdate(oldObj, newObj interface{}) {
	oldJob := oldObj.(*batchv1.Job)
	newJob := newObj.(*batchv1.Job)

	// resourceVersion没变直接跳过
	if oldJob.ResourceVersion == newJob.ResourceVersion {
		return
	}
	ji.localCache.Store(newJob.Name, newJob)
	ji.dispatch(newJob)
}

func (ji *JobInformer) onDelete(obj interface{}) {
	job, ok := obj.(*batchv1.Job)
	if !ok {
		return
	}
	ji.localCache.Delete(job.Name)
}

// dispatch 过滤非本业务的Job，再执行上层回调
func (ji *JobInformer) dispatch(job *batchv1.Job) {
	// 只处理我们调度器创建的job：必须带标签 app=offline‑worker
	if job.Labels == nil || job.Labels["app"] != "offline-worker" {
		return
	}
	// 必须携带task‑id、shard‑id标签
	taskId := job.Labels["task-id"]
	shardId := job.Labels["shard-id"]
	if taskId == "" || shardId == "" {
		return
	}

	ji.updateCb(job)
}

// Run 启动informer，阻塞
func (ji *JobInformer) Run() {
	ji.wg.Add(1)
	go func() {
		defer ji.wg.Done()
		logger.S.Info("job informer starting")

		// 启动informer
		ji.informer.Run(ji.ctx.Done())

		// 等待cache同步完成
		if !cache.WaitForCacheSync(ji.ctx.Done(), ji.informer.HasSynced) {
			logger.L.Error("job informer cache sync failed")
			return
		}
		logger.S.Info("job informer cache synced ok")

		<-ji.ctx.Done()
		logger.S.Info("job informer receive shutdown signal")
	}()
}

// Stop 优雅关闭informer
func (ji *JobInformer) Stop() {
	ji.cancelFunc()
	ji.wg.Wait()
	logger.S.Info("job informer stopped")
}

// HasSynced 判断本地缓存是否同步完成，启动阶段等待用
func (ji *JobInformer) HasSynced() bool {
	return ji.informer.HasSynced()
}

// GetCachedJob 从内存缓存获取job，无需访问apiserver
func (ji *JobInformer) GetCachedJob(jobName string) (*batchv1.Job, bool) {
	val, ok := ji.localCache.Load(jobName)
	if !ok {
		return nil, false
	}
	return val.(*batchv1.Job), true
}
