package leader

import (
	"os"
	"testing"
	"time"

	"offline-scheduler/internal/logger"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.InitLogger()
	os.Exit(m.Run())
}

func TestNewLeaderElectorUsesConfig(t *testing.T) {
	elector, err := NewLeaderElector(fake.NewSimpleClientset(), Config{
		Namespace:     "offline-job",
		LeaseName:     "scheduler-leader",
		Identity:      "pod-1",
		LeaseDuration: 20 * time.Second,
		RenewDeadline: 15 * time.Second,
		RetryPeriod:   3 * time.Second,
	}, nil, nil)
	require.NoError(t, err)
	require.NotNil(t, elector.elector)

	lock, ok := elector.lock.(*resourcelock.LeaseLock)
	require.True(t, ok)
	require.Equal(t, "offline-job", lock.LeaseMeta.Namespace)
	require.Equal(t, "scheduler-leader", lock.LeaseMeta.Name)
	require.Equal(t, "pod-1", lock.LockConfig.Identity)
	require.False(t, elector.IsLeader())
}

func TestNewLeaderElectorRejectsInvalidConfig(t *testing.T) {
	_, err := NewLeaderElector(nil, Config{Namespace: "ns", LeaseName: "lease"}, nil, nil)
	require.Error(t, err)

	_, err = NewLeaderElector(fake.NewSimpleClientset(), Config{LeaseName: "lease"}, nil, nil)
	require.Error(t, err)

	_, err = NewLeaderElector(fake.NewSimpleClientset(), Config{Namespace: "ns"}, nil, nil)
	require.Error(t, err)
}

func TestNewLeaderElectorAppliesDefaults(t *testing.T) {
	elector, err := NewLeaderElector(fake.NewSimpleClientset(), Config{
		Namespace: "ns",
		LeaseName: "lease",
	}, nil, nil)
	require.NoError(t, err)
	require.NotEmpty(t, elector.lock.Identity())
	require.NotNil(t, elector.elector)
}

func TestLeaderElectorRunAndStopAreIdempotent(t *testing.T) {
	elector, err := NewLeaderElector(fake.NewSimpleClientset(), Config{
		Namespace:     "ns",
		LeaseName:     "lease",
		Identity:      "pod-1",
		LeaseDuration: 100 * time.Millisecond,
		RenewDeadline: 50 * time.Millisecond,
		RetryPeriod:   10 * time.Millisecond,
	}, nil, nil)
	require.NoError(t, err)

	elector.Run()
	elector.Stop()
	elector.Stop()
	require.False(t, elector.IsLeader())
}
