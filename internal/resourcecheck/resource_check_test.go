package resourcecheck

import (
	"errors"
	"testing"

	"offline-scheduler/internal/model"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/stretchr/testify/require"
)

func testNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("8"),
			corev1.ResourceMemory: resource.MustParse("32Gi"),
		}},
	}
}

func TestCheckSucceedsWhenConcurrencyFits(t *testing.T) {
	client := fake.NewSimpleClientset(testNode())
	checker := NewResourceChecker(client, "default")
	task := &model.OfflineTask{TaskID: "task", TotalShard: 100, Concurrency: 2}

	ok, err := checker.Check(task, resource.MustParse("2"), resource.MustParse("4Gi"))
	require.NoError(t, err)
	require.True(t, ok)
}

func TestCheckRejectsInsufficientResources(t *testing.T) {
	client := fake.NewSimpleClientset(testNode())
	checker := NewResourceChecker(client, "default")
	task := &model.OfflineTask{TaskID: "task", TotalShard: 10, Concurrency: 8}

	ok, err := checker.Check(task, resource.MustParse("2"), resource.MustParse("4Gi"))
	require.Error(t, err)
	require.False(t, ok)
}

func TestCheckUsesTotalShardWhenConcurrencyUnset(t *testing.T) {
	client := fake.NewSimpleClientset(testNode())
	checker := NewResourceChecker(client, "default")
	task := &model.OfflineTask{TaskID: "task", TotalShard: 10}

	ok, err := checker.Check(task, resource.MustParse("2"), resource.MustParse("4Gi"))
	require.Error(t, err)
	require.False(t, ok)
}

func TestCheckRejectsInvalidTask(t *testing.T) {
	checker := NewResourceChecker(fake.NewSimpleClientset(), "default")
	ok, err := checker.Check(nil, resource.MustParse("1"), resource.MustParse("1Gi"))
	require.Error(t, err)
	require.False(t, ok)

	ok, err = checker.Check(&model.OfflineTask{TotalShard: 0}, resource.MustParse("1"), resource.MustParse("1Gi"))
	require.Error(t, err)
	require.False(t, ok)
}

func TestCheckReturnsClientError(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "nodes", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("list nodes failed")
	})
	checker := NewResourceChecker(client, "default")
	task := &model.OfflineTask{TaskID: "task", TotalShard: 1, Concurrency: 1}

	ok, err := checker.Check(task, resource.MustParse("1"), resource.MustParse("1Gi"))
	require.Error(t, err)
	require.False(t, ok)
}
