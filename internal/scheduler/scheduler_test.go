package scheduler

import (
	"container/heap"
	"testing"

	"offline-scheduler/internal/model"

	"github.com/stretchr/testify/assert"
)

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
