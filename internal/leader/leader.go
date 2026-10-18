package leader

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"offline-scheduler/internal/logger"
)

// LeaderElector k8s lease 基于租约的选主器
// 多实例部署，只有leader节点执行调度逻辑，follower只监听不创建Job
type LeaderElector struct {
	lock           resourcelock.Interface
	elector        *leaderelection.LeaderElector
	isLeader       bool
	leaderMu       sync.RWMutex
	onBecomeLeader func(ctx context.Context) // 成为主节点回调
	onLostLeader   func()                    // 丢失主节点回调
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

// Config 选主配置，外部传入，方便配置化管理
type Config struct {
	Namespace     string
	LeaseName     string
	Identity      string // 唯一标识，推荐podName
	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration
}

func NewLeaderElector(client kubernetes.Interface, cfg Config, becomeLeader func(context.Context), loseLeader func()) (*LeaderElector, error) {
	if client == nil {
		return nil, fmt.Errorf("kubernetes client cannot be nil")
	}
	if cfg.Namespace == "" || cfg.LeaseName == "" {
		return nil, fmt.Errorf("namespace and leaseName must not empty")
	}
	// 默认值兜底
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 15 * time.Second
	}
	if cfg.RenewDeadline <= 0 {
		cfg.RenewDeadline = 10 * time.Second
	}
	if cfg.RetryPeriod <= 0 {
		cfg.RetryPeriod = 2 * time.Second
	}
	if cfg.Identity == "" {
		// fallback，生产环境优先传入pod name
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			cfg.Identity = hostname
		} else {
			cfg.Identity = fmt.Sprintf("offline-scheduler-%d", time.Now().UnixNano())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      cfg.LeaseName,
			Namespace: cfg.Namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: cfg.Identity,
		},
	}

	leader := &LeaderElector{
		lock:           lock,
		onBecomeLeader: becomeLeader,
		onLostLeader:   loseLeader,
		ctx:            ctx,
		cancel:         cancel,
	}

	leCfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   cfg.LeaseDuration,
		RenewDeadline:   cfg.RenewDeadline,
		RetryPeriod:     cfg.RetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.S.Info("become leader")
				leader.leaderMu.Lock()
				leader.isLeader = true
				leader.leaderMu.Unlock()
				if becomeLeader != nil {
					becomeLeader(ctx)
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

	leader.elector = elector
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
