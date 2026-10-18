package resourcecheck

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"offline-scheduler/internal/model"
)

type ResourceChecker struct {
	client    kubernetes.Interface
	namespace string
}

func NewResourceChecker(client kubernetes.Interface, ns string) *ResourceChecker {
	return &ResourceChecker{
		client:    client,
		namespace: ns,
	}
}

// Check 简单资源预校验：查询集群可分配资源，这里为简易实现，生产可增强
func (r *ResourceChecker) Check(task *model.OfflineTask, cpu, mem resource.Quantity) (bool, error) {
	if task == nil || task.TotalShard <= 0 {
		return false, fmt.Errorf("invalid task")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	nodes, err := r.client.CoreV1().Nodes().List(ctx, v1.ListOptions{})
	if err != nil {
		return false, err
	}
	var totalAllocCPU, totalAllocMem resource.Quantity
	for _, node := range nodes.Items {
		totalAllocCPU.Add(node.Status.Allocatable[corev1.ResourceCPU])
		totalAllocMem.Add(node.Status.Allocatable[corev1.ResourceMemory])
	}

	pods, err := r.client.CoreV1().Pods(r.namespace).List(ctx, v1.ListOptions{})
	if err != nil {
		return false, err
	}
	var usedCPU, usedMem resource.Quantity
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Labels["app"] != "offline-worker" {
			continue
		}
		for _, container := range pod.Spec.Containers {
			usedCPU.Add(container.Resources.Requests[corev1.ResourceCPU])
			usedMem.Add(container.Resources.Requests[corev1.ResourceMemory])
		}
	}
	totalAllocCPU.Sub(usedCPU)
	totalAllocMem.Sub(usedMem)

	// 调度器按 concurrency 控制同时运行的分片数，因此只需要保证当前并发批次能放下。
	parallel := task.Concurrency
	if parallel <= 0 || parallel > task.TotalShard {
		parallel = task.TotalShard
	}
	needCPU := cpu.MilliValue() * int64(parallel)
	needMem := mem.Value() * int64(parallel)
	if needCPU > totalAllocCPU.MilliValue() || needMem > totalAllocMem.Value() {
		return false, fmt.Errorf("cluster resource insufficient")
	}
	return true, nil
}
