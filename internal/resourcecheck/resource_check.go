package resourcecheck

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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
	nodes, err := r.client.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		return false, err
	}
	var totalAllocCPU, totalAllocMem resource.Quantity
	for _, node := range nodes.Items {
		totalAllocCPU.Add(node.Status.Allocatable[corev1.ResourceCPU])
		totalAllocMem.Add(node.Status.Allocatable[corev1.ResourceMemory])
	}
	needCPU := cpu.MilliValue() * int64(task.TotalShard)
	needMem := mem.Value() * int64(task.TotalShard)
	if needCPU > totalAllocCPU.MilliValue() || needMem > totalAllocMem.Value() {
		return false, fmt.Errorf("cluster resource insufficient")
	}
	return true, nil
}

