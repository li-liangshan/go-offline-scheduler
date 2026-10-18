package metric

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestCountersCanBeIncremented(t *testing.T) {
	counters := map[string]prometheus.Counter{
		"task_submit_ok":     TaskSubmitOk,
		"task_submit_failed": TaskSubmitFailed,
		"task_submit_http":   TaskSubmitHttp,
		"task_total_success": TaskTotalSuccess,
		"task_total_fail":    TaskTotalFail,
		"task_cancel":        TaskCancel,
		"job_create_ok":      JobCreateOk,
		"job_create_err":     JobCreateErr,
		"shard_launch_panic": ShardLaunchPanic,
	}

	for name, counter := range counters {
		t.Run(name, func(t *testing.T) {
			before := testutil.ToFloat64(counter)
			counter.Inc()
			require.Equal(t, before+1, testutil.ToFloat64(counter))
		})
	}
}
