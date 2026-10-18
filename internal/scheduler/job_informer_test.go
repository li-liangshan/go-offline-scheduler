package scheduler

import (
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/stretchr/testify/require"
)

func managedJob(name, taskID, shardID string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			ResourceVersion: "1",
			Labels: map[string]string{
				"app":      "offline-worker",
				"task-id":  taskID,
				"shard-id": shardID,
			},
		},
	}
}

func TestIsManagedJob(t *testing.T) {
	require.False(t, isManagedJob(nil))
	require.False(t, isManagedJob(&batchv1.Job{}))
	require.False(t, isManagedJob(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "other"}}}))

	job := managedJob("job-1", "task-1", "shard-1")
	require.True(t, isManagedJob(job))

	delete(job.Labels, "task-id")
	require.False(t, isManagedJob(job))
}

func TestJobInformerDispatch(t *testing.T) {
	var got []*batchv1.Job
	ji := &JobInformer{updateCb: func(job *batchv1.Job) {
		got = append(got, job)
	}}

	ji.dispatch(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "other"}})
	require.Empty(t, got)

	ji.dispatch(managedJob("job-1", "task-1", "shard-1"))
	require.Len(t, got, 1)
	require.Equal(t, "job-1", got[0].Name)
}

func TestJobInformerCacheLifecycle(t *testing.T) {
	var callbacks int
	ji := &JobInformer{updateCb: func(job *batchv1.Job) { callbacks++ }}

	job := managedJob("job-1", "task-1", "shard-1")
	ji.onAdd(job)
	cached, ok := ji.GetCachedJob("job-1")
	require.True(t, ok)
	require.Equal(t, job, cached)
	require.Equal(t, 1, callbacks)

	ji.onUpdate(job, job)
	require.Equal(t, 1, callbacks, "same resourceVersion should be ignored")

	updated := job.DeepCopy()
	updated.ResourceVersion = "2"
	ji.onUpdate(job, updated)
	require.Equal(t, 2, callbacks)

	ji.onDelete(job)
	_, ok = ji.GetCachedJob("job-1")
	require.False(t, ok)
}

func TestJobInformerDeleteTombstone(t *testing.T) {
	ji := &JobInformer{}
	job := managedJob("job-1", "task-1", "shard-1")
	ji.localCache.Store(job.Name, job)

	ji.onDelete(cache.DeletedFinalStateUnknown{Key: job.Name, Obj: job})
	_, ok := ji.GetCachedJob(job.Name)
	require.False(t, ok)
}

func TestNewJobInformerAndStop(t *testing.T) {
	ji := NewJobInformer(fake.NewSimpleClientset(), "default", func(job *batchv1.Job) {})
	require.NotNil(t, ji)
	require.False(t, ji.HasSynced())
	require.NotPanics(t, ji.Stop)
}
