package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/model"
	"offline-scheduler/internal/resourcecheck"
	"offline-scheduler/internal/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.InitLogger()
	os.Exit(m.Run())
}

func TestPriorityQueue(t *testing.T) {
	// 测试堆优先级，高优任务优先弹出
	pq := make(PriorityQueue, 0)
	item1 := &PriorityTaskItem{task: &model.OfflineTask{TaskID: "t1", Priority: 1}}
	item2 := &PriorityTaskItem{task: &model.OfflineTask{TaskID: "t2", Priority: 10}}
	heap.Push(&pq, item1)
	heap.Push(&pq, item2)
	top := heap.Pop(&pq).(*PriorityTaskItem)
	assert.Equal(t, "t2", top.task.TaskID)
}

func newTestScheduler(t *testing.T) (*Scheduler, *store.Store, *fake.Clientset) {
	t.Helper()
	st, err := store.NewStore(filepath.Join(t.TempDir(), "pebble"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("16"),
			corev1.ResourceMemory: resource.MustParse("64Gi"),
		}},
	}
	client := fake.NewSimpleClientset(node)
	resChecker := resourcecheck.NewResourceChecker(client, "default")
	sched := NewScheduler(client, st, resChecker, "default", 10)
	return sched, st, client
}

func listJobs(t *testing.T, client *fake.Clientset) []batchv1.Job {
	t.Helper()
	list, err := client.BatchV1().Jobs("default").List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	return list.Items
}

func runningShard(t *testing.T, st *store.Store, taskID string) *model.TaskShard {
	t.Helper()
	shards, err := st.ListShardsByTaskID(taskID)
	require.NoError(t, err)
	for _, shard := range shards {
		if shard.Status == model.ShardStatusRunning {
			return shard
		}
	}
	t.Fatalf("no running shard for task %s", taskID)
	return nil
}

func finishShard(t *testing.T, sched *Scheduler, client *fake.Clientset, shard *model.TaskShard, success bool) {
	t.Helper()
	job, err := client.BatchV1().Jobs("default").Get(context.Background(), shard.K8sJobName, metav1.GetOptions{})
	require.NoError(t, err)
	if success {
		job.Status.Succeeded = 1
	} else {
		job.Status.Failed = 1
	}
	sched.OnJobStatusUpdate(job)
}

func TestSubmitTaskCreatesShardsAndRespectsConcurrency(t *testing.T) {
	sched, st, client := newTestScheduler(t)

	err := sched.SubmitTask(&model.SubmitTaskRequest{
		TaskID:      "demo",
		TotalShard:  3,
		Concurrency: 2,
		MaxRetry:    1,
		TimeoutSec:  60,
	})
	require.NoError(t, err)

	task, err := st.GetTask("demo")
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusPending, task.Status)

	shards, err := st.ListShardsByTaskID("demo")
	require.NoError(t, err)
	require.Len(t, shards, 3)
	for _, shard := range shards {
		assert.Equal(t, model.ShardStatusPending, shard.Status)
	}

	sched.scheduleLoopOnce()
	assert.Len(t, listJobs(t, client), 2)

	sched.scheduleLoopOnce()
	assert.Len(t, listJobs(t, client), 2, "per-task concurrency must be enforced")

	finishShard(t, sched, client, runningShard(t, st, "demo"), true)
	sched.scheduleLoopOnce()
	assert.Len(t, listJobs(t, client), 2, "one finished shard should be replaced")

	for {
		shards, err = st.ListShardsByTaskID("demo")
		require.NoError(t, err)
		var running *model.TaskShard
		for _, shard := range shards {
			if shard.Status == model.ShardStatusRunning {
				running = shard
				break
			}
		}
		if running == nil {
			break
		}
		finishShard(t, sched, client, running, true)
	}

	task, err = st.GetTask("demo")
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusSuccess, task.Status)
	assert.Empty(t, listJobs(t, client))
}

func TestFailedShardRetriesWithinLimit(t *testing.T) {
	sched, st, client := newTestScheduler(t)

	err := sched.SubmitTask(&model.SubmitTaskRequest{
		TaskID:      "retry-demo",
		TotalShard:  1,
		Concurrency: 1,
		MaxRetry:    1,
		TimeoutSec:  60,
	})
	require.NoError(t, err)

	sched.scheduleLoopOnce()
	first := runningShard(t, st, "retry-demo")
	finishShard(t, sched, client, first, false)

	shard, err := st.GetShard(first.ShardID)
	require.NoError(t, err)
	assert.Equal(t, model.ShardStatusFailed, shard.Status)
	assert.Equal(t, 1, shard.RetryCount)

	sched.scheduleLoopOnce()
	second := runningShard(t, st, "retry-demo")
	assert.NotEqual(t, first.K8sJobName, second.K8sJobName)
	finishShard(t, sched, client, second, false)

	shard, err = st.GetShard(first.ShardID)
	require.NoError(t, err)
	assert.Equal(t, 2, shard.RetryCount)

	task, err := st.GetTask("retry-demo")
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusFailed, task.Status)
}

func TestCancelTaskReleasesRunningSlot(t *testing.T) {
	sched, st, client := newTestScheduler(t)

	err := sched.SubmitTask(&model.SubmitTaskRequest{
		TaskID:      "cancel-demo",
		TotalShard:  2,
		Concurrency: 1,
		TimeoutSec:  60,
	})
	require.NoError(t, err)
	sched.scheduleLoopOnce()
	assert.Equal(t, 9, sched.globalFree())

	err = sched.CancelTask("cancel-demo")
	require.NoError(t, err)

	task, err := st.GetTask("cancel-demo")
	require.NoError(t, err)
	assert.Equal(t, model.TaskStatusCanceled, task.Status)
	assert.Empty(t, listJobs(t, client))
	assert.Equal(t, 10, sched.globalFree(), "running slot should be released")
}

func TestSubmitTaskValidation(t *testing.T) {
	sched, _, _ := newTestScheduler(t)
	cases := []struct {
		name string
		req  *model.SubmitTaskRequest
	}{
		{name: "nil request", req: nil},
		{name: "empty task id", req: &model.SubmitTaskRequest{TotalShard: 1}},
		{name: "invalid label value", req: &model.SubmitTaskRequest{TaskID: "bad task id", TotalShard: 1}},
		{name: "zero shards", req: &model.SubmitTaskRequest{TaskID: "task", TotalShard: 0}},
		{name: "invalid param name", req: &model.SubmitTaskRequest{TaskID: "task", TotalShard: 1, Params: map[string]string{"bad key": "v"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, sched.SubmitTask(tc.req))
		})
	}
}

func TestSubmitTaskRejectsDuplicate(t *testing.T) {
	sched, _, _ := newTestScheduler(t)
	req := &model.SubmitTaskRequest{TaskID: "duplicate", TotalShard: 1, TimeoutSec: 60}
	require.NoError(t, sched.SubmitTask(req))
	require.Error(t, sched.SubmitTask(req))
}

func TestSubmitTaskResourceCheckFailure(t *testing.T) {
	st, err := store.NewStore(filepath.Join(t.TempDir(), "pebble"))
	require.NoError(t, err)
	defer st.Close()

	client := fake.NewSimpleClientset()
	sched := NewScheduler(client, st, resourcecheck.NewResourceChecker(client, "default"), "default", 10)
	err = sched.SubmitTask(&model.SubmitTaskRequest{TaskID: "no-resource", TotalShard: 1, TimeoutSec: 60})
	require.Error(t, err)
	_, err = st.GetTask("no-resource")
	require.ErrorIs(t, err, store.ErrTaskNotFound)
}

func TestCheckShardTimeout(t *testing.T) {
	sched, st, client := newTestScheduler(t)
	require.NoError(t, sched.SubmitTask(&model.SubmitTaskRequest{
		TaskID:      "timeout-task",
		TotalShard:  1,
		Concurrency: 1,
		TimeoutSec:  1,
	}))
	sched.scheduleLoopOnce()

	shard := runningShard(t, st, "timeout-task")
	shard.StartedAt = time.Now().Add(-time.Hour)
	require.NoError(t, st.SaveShard(shard))

	sched.checkShardTimeout()
	shard, err := st.GetShard(shard.ShardID)
	require.NoError(t, err)
	require.Equal(t, model.ShardStatusFailed, shard.Status)
	require.Equal(t, 1, shard.RetryCount)
	require.Equal(t, 10, sched.globalFree())
	require.Empty(t, listJobs(t, client))

	sched.scheduleLoopOnce()
	task, err := st.GetTask("timeout-task")
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusFailed, task.Status)
}

func TestLaunchShardCreateFailure(t *testing.T) {
	sched, st, client := newTestScheduler(t)
	client.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("create job failed")
	})
	require.NoError(t, sched.SubmitTask(&model.SubmitTaskRequest{
		TaskID:      "create-fail",
		TotalShard:  1,
		Concurrency: 1,
		TimeoutSec:  60,
	}))

	sched.scheduleLoopOnce()
	shards, err := st.ListShardsByTaskID("create-fail")
	require.NoError(t, err)
	require.Len(t, shards, 1)
	require.Equal(t, model.ShardStatusFailed, shards[0].Status)
	require.Equal(t, 1, shards[0].RetryCount)
	require.Equal(t, 10, sched.globalFree())

	sched.scheduleLoopOnce()
	task, err := st.GetTask("create-fail")
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusFailed, task.Status)
}

func TestRunAndStopAreIdempotent(t *testing.T) {
	sched, _, _ := newTestScheduler(t)
	require.NotPanics(t, func() {
		sched.Run()
		sched.Run()
		sched.Stop()
		sched.Stop()
	})
}

func TestCancelTaskErrors(t *testing.T) {
	sched, _, _ := newTestScheduler(t)
	require.Error(t, sched.CancelTask("missing"))

	require.NoError(t, sched.SubmitTask(&model.SubmitTaskRequest{
		TaskID:      "cancel-twice",
		TotalShard:  1,
		Concurrency: 1,
		TimeoutSec:  60,
	}))
	require.NoError(t, sched.CancelTask("cancel-twice"))
	require.Error(t, sched.CancelTask("cancel-twice"))
}

func TestFinalizeDoesNotOverwriteCanceledTask(t *testing.T) {
	sched, st, _ := newTestScheduler(t)
	require.NoError(t, sched.SubmitTask(&model.SubmitTaskRequest{
		TaskID:      "stale-task",
		TotalShard:  1,
		Concurrency: 1,
		TimeoutSec:  60,
	}))
	stale, err := st.GetTask("stale-task")
	require.NoError(t, err)
	require.NoError(t, sched.CancelTask("stale-task"))

	sched.tryFinalizeTask(stale)
	task, err := st.GetTask("stale-task")
	require.NoError(t, err)
	require.Equal(t, model.TaskStatusCanceled, task.Status)
}
