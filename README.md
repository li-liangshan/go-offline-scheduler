# offline-scheduler
> 基于 Golang + Kubernetes Job + NATS JetStream + PebbleDB 的**大规模离线任务分发调度器**
> 支持任务分片、优先级调度、限流并发控制、任务超时、失败重试、死信队列、Leader选举、Prometheus监控、优雅退出

## ✨ 特性清单
- ✅ **任务分片调度**：一个大任务拆成多个Shard分片，分布式并行执行
- ✅ **Kubernetes Job**：真正利用K8s集群资源，每个分片启动独立Job执行业务worker
- ✅ **NATS JetStream**：任务消息队列，支持死信队列DLQ、消息持久化、消费重试
- ✅ **PebbleDB本地存储**：任务、分片元数据持久化，不需要额外部署MySQL/Redis
- ✅ **优先级调度**：高优先级任务优先抢占调度资源
- ✅ **全局并发控制**：限制集群同时运行分片数量，防止集群打爆
- ✅ **资源校验**：调度前校验K8s集群资源，资源不足暂缓调度
- ✅ **Leader选举**：多实例部署保证高可用，同一时间只有一个主调度器工作
- ✅ **Job Informer**：实时监听K8s Job状态，自动同步分片状态
- ✅ **超时巡检**：定时扫描运行中超时任务，自动标记失败+重试
- ✅ **优雅关闭**：收到SIGINT/SIGTERM信号，停止新调度、等待正在调度任务处理完再退出
- ✅ **Prometheus指标**：任务数量、分片状态、调度耗时、失败计数
- ✅ **OpenTelemetry链路追踪**：可对接Jaeger等追踪系统
- ✅ **HTTP REST API**：任务提交、任务取消、任务查询接口
- ✅ **任务重试**：分片失败自动重试，支持最大重试次数配置

## 📦 项目目录
```plaintext
offline-scheduler/
├── go.mod
├── go.sum
├── README.md                 # 本文档
├── cmd/scheduler/main.go    # 程序入口
├── configs/
│   ├── config.yaml           # 主配置文件
│   └── rbac.yaml             # K8s RBAC权限配置
└── internal/
    ├── api/
    │   └── http_api.go       # HTTP接口实现
    ├── config/
    │   └── config.go         # 配置解析
    ├── leader/
    │   └── leader.go          # Leader选举实现
    ├── logger/
    │   └── logger.go          # 日志模块
    ├── metric/
    │   └── metric.go          # Prometheus指标
    ├── model/
    │   └── model.go           # Task、Shard数据模型
    ├── otel/
    │   └── trace.go           # OTEL链路追踪
    ├── queue/
    │   └── nats_queue.go      # NATS JetStream队列封装
    ├── resourcecheck/
    │   └── resource_check.go  # K8s集群资源校验
    ├── scheduler/
    │   ├── scheduler.go       # 核心调度逻辑
    │   ├── job_informer.go    # K8s Job事件监听
    │   └── scheduler_test.go  # 调度单元测试
    └── store/
        ├── pebble_store.go    # PebbleDB持久化存储
        └── backup_manager.go  # 存储备份管理

