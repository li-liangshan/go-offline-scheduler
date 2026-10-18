package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	"offline-scheduler/internal/api"
	"offline-scheduler/internal/config"
	"offline-scheduler/internal/leader"
	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/model"
	"offline-scheduler/internal/otel"
	"offline-scheduler/internal/queue"
	"offline-scheduler/internal/resourcecheck"
	"offline-scheduler/internal/scheduler"
	"offline-scheduler/internal/store"

	corev1 "k8s.io/api/core/v1"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "./configs/config.yaml", "path to config file")
	kubeconfig := flag.String("kubeconfig", defaultKubeconfigPath(), "path to kubeconfig file")
	flag.Parse()

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		panic(err)
	}

	if err := logger.InitLoggerWithLevel(cfg.LogLevel); err != nil {
		panic(err)
	}
	defer logger.Sync()
	logger.S.Infow("offline-scheduler starting", "config", *configPath)

	tp, err := otel.InitTrace("offline-scheduler", cfg.OtelEndpoint)
	if err != nil {
		logger.S.Warn("otel trace init failed", zap.Error(err))
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tp.Shutdown(ctx)
		}()
	}

	kubeClient, err := buildKubeClientFromKubeconfig(*kubeconfig)
	if err != nil {
		logger.L.Fatal("build k8s client failed", zap.Error(err))
	}

	st, err := store.NewStoreWithCacheMB(cfg.PebbleDataPath, cfg.PebbleCacheMB)
	if err != nil {
		logger.L.Fatal("pebble open failed", zap.Error(err))
	}
	defer st.Close()

	backupMgr, err := store.NewBackupManagerWithInterval(
		st.DB(), cfg.BackupDir, cfg.BackupRetain, cfg.BackupTimeoutDuration(), cfg.BackupIntervalDuration())
	if err != nil {
		logger.L.Fatal("backup manager create failed", zap.Error(err))
	}
	backupMgr.Start()
	defer backupMgr.Stop()

	schedulerOpts, err := buildSchedulerOptions(cfg)
	if err != nil {
		logger.L.Fatal("invalid scheduler config", zap.Error(err))
	}
	resChecker := resourcecheck.NewResourceChecker(kubeClient, cfg.Namespace)
	sched := scheduler.NewSchedulerWithOptions(kubeClient, st, resChecker, cfg.Namespace, schedulerOpts)

	jobInformer := scheduler.NewJobInformer(kubeClient, cfg.Namespace, sched.OnJobStatusUpdate)
	sched.SetInformer(jobInformer)
	jobInformer.Run()
	cacheSyncDeadline := time.After(30 * time.Second)
	for !jobInformer.HasSynced() {
		select {
		case <-cacheSyncDeadline:
			logger.L.Fatal("job informer cache sync timeout")
		case <-time.After(100 * time.Millisecond):
		}
	}

	natsQueue, err := queue.NewNatsQueueWithOptions(cfg.NatsUrl, queue.Options{
		Name:         "offline-scheduler",
		MaxDeliver:   cfg.NatsMaxDeliver,
		StreamMaxAge: time.Duration(cfg.NatsStreamMaxAgeSec) * time.Second,
	})
	if err != nil {
		logger.L.Fatal("nats connect failed", zap.Error(err))
	}
	defer natsQueue.Close()

	leaderCfg := leader.Config{
		Namespace: cfg.Namespace,
		LeaseName: cfg.LeaseName,
		Identity:  instanceIdentity(),
	}
	leaderElector, err := leader.NewLeaderElector(kubeClient, leaderCfg,
		func(ctx context.Context) {
			subscribeWithRetry(ctx, natsQueue, func(req *model.SubmitTaskRequest) error {
				submitErr := sched.SubmitTask(req)
				if errors.Is(submitErr, scheduler.ErrTaskExists) {
					logger.S.Infow("duplicate task message acknowledged", "taskId", req.TaskID)
					return nil
				}
				return submitErr
			})
			sched.Run()
		},
		func() {
			natsQueue.StopSubscribe()
			sched.Stop()
		})
	if err != nil {
		logger.L.Fatal("leader elector create failed", zap.Error(err))
	}
	leaderElector.Run()

	httpServer := api.NewHttpServerWithOptions(sched, st, api.Options{
		MaxBodyBytes: cfg.HttpMaxBodyBytes,
		AuthToken:    cfg.ApiAuthToken,
		Submitter:    natsQueue,
		AsyncSubmit:  true,
		ReadyFunc: func() error {
			if !leaderElector.IsLeader() {
				return fmt.Errorf("instance is not leader")
			}
			return nil
		},
	})
	mux := http.NewServeMux()
	httpServer.RegisterRoute(mux)
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:              cfg.HttpListenAddr,
		Handler:           httpServer.AuthMiddleware(mux),
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout(),
		ReadTimeout:       cfg.HTTPReadTimeout(),
		WriteTimeout:      cfg.HTTPWriteTimeout(),
		IdleTimeout:       cfg.HTTPIdleTimeout(),
		MaxHeaderBytes:    1 << 20,
	}
	go func() {
		logger.S.Infow("http server listening", "addr", cfg.HttpListenAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Fatal("http listen error", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.S.Info("shutdown signal received")

	ctxShutdown, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := server.Shutdown(ctxShutdown); err != nil {
		logger.L.Warn("http server shutdown failed", zap.Error(err))
	}

	leaderElector.Stop()
	sched.Stop()
	jobInformer.Stop()
	natsQueue.Close()
	backupMgr.Stop()
	logger.S.Info("offline scheduler exit gracefully")
}

func defaultKubeconfigPath() string {
	if home := homedir.HomeDir(); home != "" {
		return home + "/.kube/config"
	}
	return ""
}

func buildSchedulerOptions(cfg *config.AppConfig) (scheduler.Options, error) {
	cpuRequest, err := resource.ParseQuantity(cfg.WorkerCPURequest)
	if err != nil {
		return scheduler.Options{}, fmt.Errorf("workerCpuRequest: %w", err)
	}
	cpuLimit, err := resource.ParseQuantity(cfg.WorkerCPULimit)
	if err != nil {
		return scheduler.Options{}, fmt.Errorf("workerCpuLimit: %w", err)
	}
	memoryRequest, err := resource.ParseQuantity(cfg.WorkerMemoryRequest)
	if err != nil {
		return scheduler.Options{}, fmt.Errorf("workerMemoryRequest: %w", err)
	}
	memoryLimit, err := resource.ParseQuantity(cfg.WorkerMemoryLimit)
	if err != nil {
		return scheduler.Options{}, fmt.Errorf("workerMemoryLimit: %w", err)
	}
	if cpuLimit.Cmp(cpuRequest) < 0 {
		return scheduler.Options{}, fmt.Errorf("workerCpuLimit must be >= workerCpuRequest")
	}
	if memoryLimit.Cmp(memoryRequest) < 0 {
		return scheduler.Options{}, fmt.Errorf("workerMemoryLimit must be >= workerMemoryRequest")
	}
	if cfg.JobTTLAfterFinishedSec > 1<<31-1 {
		return scheduler.Options{}, fmt.Errorf("jobTtlSecondsAfterFinished overflows int32")
	}

	return scheduler.Options{
		GlobalMaxConcurrency: cfg.GlobalMaxConcurrency,
		WorkerImage:          cfg.WorkerImage,
		ImagePullPolicy:      corev1.PullPolicy(cfg.WorkerImagePullPolicy),
		WorkerResources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    cpuRequest,
				corev1.ResourceMemory: memoryRequest,
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    cpuLimit,
				corev1.ResourceMemory: memoryLimit,
			},
		},
		ServiceAccountName:         cfg.WorkerServiceAccount,
		DeleteFinishedJobs:         cfg.DeleteFinishedJobs,
		JobTTLSecondsAfterFinished: int32(cfg.JobTTLAfterFinishedSec),
		CleanupInterval:            time.Duration(cfg.CleanupIntervalSec) * time.Second,
		TaskRetention:              time.Duration(cfg.TaskRetentionSec) * time.Second,
		ResourceCheckEnabled:       cfg.ResourceCheckEnabled,
	}, nil
}

func instanceIdentity() string {
	if identity := os.Getenv("POD_NAME"); identity != "" {
		return identity
	}
	identity, _ := os.Hostname()
	return identity
}

func subscribeWithRetry(ctx context.Context, q *queue.NatsQueue, handler func(*model.SubmitTaskRequest) error) {
	delay := time.Second
	for {
		err := q.Subscribe(ctx, handler)
		if err == nil {
			return
		}
		logger.L.Error("nats subscribe failed, retrying", zap.Duration("delay", delay), zap.Error(err))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < 30*time.Second {
			delay *= 2
		}
	}
}

func buildKubeClientFromKubeconfig(kubeconfig string) (*kubernetes.Clientset, error) {
	kubeCfg, err := rest.InClusterConfig()
	if err != nil {
		kubeCfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}
	return kubernetes.NewForConfig(kubeCfg)
}
