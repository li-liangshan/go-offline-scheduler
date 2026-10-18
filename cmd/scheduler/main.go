package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flag"

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

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func buildKubeClient() (*kubernetes.Clientset, error) {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", home+"/.kube/config", "absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	return buildKubeClientFromKubeconfig(*kubeconfig)
}

func buildKubeClientFromKubeconfig(kubeconfig string) (*kubernetes.Clientset, error) {
	// 集群内优先InClusterConfig
	config, err := rest.InClusterConfig()
	if err != nil {
		// 本地开发读取kubeconfig
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, err
		}
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return clientset, nil
}

func main() {
	// 1.加载配置
	cfg, err := config.LoadConfig("./configs/config.yaml")
	if err != nil {
		panic(err)
	}

	// 2.日志初始化
	logger.InitLogger()
	defer logger.Sync()
	logger.S.Info("offline-scheduler starting")

	// 3.链路追踪
	tp, err := otel.InitTrace("offline-scheduler", cfg.OtelEndpoint)
	if err != nil {
		logger.S.Warn("otel trace init failed", zap.Error(err))
	} else {
		defer func() {
			_ = tp.Shutdown(context.Background())
		}()
	}

	// 4.k8s client
	kubeClient, err := buildKubeClient()
	if err != nil {
		logger.L.Fatal("build k8s client failed", zap.Error(err))
	}

	// 5.存储
	st, err := store.NewStore(cfg.PebbleDataPath)
	if err != nil {
		logger.L.Fatal("pebble open failed", zap.Error(err))
	}
	defer st.Close()

	backupMgr, err := store.NewBackupManager(st.DB(), cfg.BackupDir, 100, 100*time.Second)
	if err != nil {
		logger.L.Fatal("backup manager create failed", zap.Error(err))
	}
	backupMgr.Start()
	defer backupMgr.Stop()

	// 6.资源校验器
	resChecker := resourcecheck.NewResourceChecker(kubeClient, cfg.Namespace)

	// 7.调度器
	sched := scheduler.NewScheduler(kubeClient, st, resChecker, cfg.Namespace, cfg.GlobalMaxConcurrency)

	// 8.Job informer
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

	// 9.Leader选举：只有leader启动调度Run()
	identity := os.Getenv("POD_NAME")
	if identity == "" {
		identity, _ = os.Hostname()
	}
	leaderCfg := leader.Config{
		Namespace: cfg.Namespace,
		LeaseName: cfg.LeaseName,
		Identity:  identity,
	}
	leaderElector, err := leader.NewLeaderElector(kubeClient, leaderCfg,
		func(ctx context.Context) {
			// 当选leader，启动调度循环
			sched.Run()
		},
		func() {
			// 丢失leader，停止调度循环
			sched.Stop()
		})
	if err != nil {
		logger.L.Fatal("leader elector create failed", zap.Error(err))
	}
	leaderElector.Run()

	// 10.NATS队列
	natsQueue, err := queue.NewNatsQueue(cfg.NatsUrl)
	if err != nil {
		logger.L.Fatal("nats connect failed", zap.Error(err))
	}
	defer natsQueue.Close()
	go func() {
		err := natsQueue.Subscribe(context.Background(), func(req *model.SubmitTaskRequest) error {
			return sched.SubmitTask(req)
		})
		if err != nil {
			logger.L.Fatal("nats subscribe err", zap.Error(err))
		}
	}()

	// 11.Http API
	httpServer := api.NewHttpServer(sched, st)
	mux := http.NewServeMux()
	httpServer.RegisterRoute(mux)
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{Addr: cfg.HttpListenAddr, Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Fatal("http listen error", zap.Error(err))
		}
	}()

	// 优雅关闭信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.S.Info("shutdown signal received")

	ctxShutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctxShutdown)

	leaderElector.Stop()
	sched.Stop()
	jobInformer.Stop()
	backupMgr.Stop()
	logger.S.Info("offline scheduler exit gracefully")
}
