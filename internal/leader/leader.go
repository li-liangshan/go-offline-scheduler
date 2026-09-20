package leader

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"offline-scheduler/internal/logger"
)

type LeaderElector struct {
	lock           resourcelock.Interface
	elector        *leaderelection.LeaderElector
	isLeader       bool
	leaderMu       sync.RWMutex
	onBecomeLeader func()
	onLostLeader   func()
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

func NewLeaderElector(client kubernetes.Interface, ns, leaseName string, becomeLeader, loseLeader func()) (*LeaderElector, error) {
	ctx, cancel := context.WithCancel(context.Background())
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: ns,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: "offline-scheduler-" + fmt.Sprintf("%d", time.Now().UnixNano()),
		},
	}

	leCfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.S.Info("become leader")
				leader.leaderMu.Lock()
				leader.isLeader = true
				leader.leaderMu.Unlock()
				if becomeLeader != nil {
					becomeLeader()
				}
			},
			OnStoppedLeading: func() {
				logger.S.Warn("lost leader")
				leader.leaderMu.Lock()
				leader.isLeader = false
				leader.leaderMu.Unlock()
				if loseLeader != nil {
					loseLeader()
				}
			},
			OnNewLeader: func(id string) {
				logger.S.Infow("new leader elected", "leaderId", id)
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(leCfg)
	if err != nil {
		cancel()
		return nil, err
	}

	leader := &LeaderElector{
		lock:           lock,
		elector:        elector,
		onBecomeLeader: becomeLeader,
		onLostLeader:   loseLeader,
		ctx:            ctx,
		cancel:         cancel,
	}
	return leader, nil
}

func (l *LeaderElector) Run() {
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.elector.Run(l.ctx)
	}()
}

func (l *LeaderElector) IsLeader() bool {
	l.leaderMu.RLock()
	defer l.leaderMu.RUnlock()
	return l.isLeader
}

func (l *LeaderElector) Stop() {
	l.cancel()
	l.wg.Wait()
	logger.S.Info("leader elector stopped")
}

