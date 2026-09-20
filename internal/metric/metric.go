package metric

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	TaskSubmitOk = promauto.NewCounter(prometheus.CounterOpts{
		Name: "offline_task_submit_ok_total",
		Help: "submit task success",
	})
	TaskSubmitFailed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "offline_task_submit_fail_total",
		Help: "submit task failed",
	})
	TaskSubmitHttp = promauto.NewCounter(prometheus.CounterOpts{
		Name: "offline_task_http_submit_total",
		Help: "http receive submit request",
	})
	TaskTotalSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "offline_task_success_total",
		Help: "task final success count",
	})
	TaskTotalFail = promauto.NewCounter(prometheus.CounterOpts{
		Name: "offline_task_failed_total",
		Help: "task final failed count",
	})
	TaskCancel = promauto.NewCounter(prometheus.CounterOpts{
		Name: "offline_task_cancel_total",
		Help: "task canceled count",
	})
	JobCreateOk = promauto.NewCounter(prometheus.CounterOpts{
		Name: "offline_job_create_ok_total",
		Help: "k8s job create ok",
	})
	JobCreateErr = promauto.NewCounter(prometheus.CounterOpts{
		Name: "offline_job_create_err_total",
		Help: "k8s job create error",
	})
	ShardLaunchPanic = promauto.NewCounter(prometheus.CounterOpts{
		Name: "offline_shard_launch_panic_total",
		Help: "panic when launch shard",
	})
)

