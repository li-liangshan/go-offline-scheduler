# offline-scheduler

基于 Go + Kubernetes Job + NATS JetStream + PebbleDB 的离线任务分片调度器。

## 核心能力

- 任务分片与优先级调度
- 全局并发和单任务并发控制
- 分片超时、失败重试、任务取消
- Kubernetes Job 动态创建与状态同步
- NATS JetStream 持久化提交、消费重试和 DLQ
- Leader 选举，仅 Leader 消费任务并调度
- Leader-only readiness，避免流量落到状态不完整的实例
- PebbleDB 本地持久化与一致性 Checkpoint 热备
- 终态任务保留策略与定时清理，避免 Pebble 数据无限增长
- Prometheus 指标、OpenTelemetry 链路追踪
- HTTP 健康检查、就绪检查、请求体限制和可选 Bearer Token 鉴权

## 目录

```text
Dockerfile                     多阶段生产镜像
.dockerignore                  镜像构建忽略规则
cmd/scheduler/main.go          程序入口
configs/config.yaml            本地运行配置
configs/rbac.yaml              Kubernetes RBAC
configs/deployment.yaml        Kubernetes 部署示例
docs/FIX_REPORT.md             缺陷修复报告
docs/PRODUCTION_READINESS.md   生产就绪说明与已知限制
internal/api                   HTTP API
internal/config                配置加载与校验
internal/leader                Leader 选举
internal/logger                日志
internal/metric                Prometheus 指标
internal/model                 数据模型
internal/otel                  OpenTelemetry
internal/queue                 NATS JetStream
internal/resourcecheck         Kubernetes 资源校验
internal/scheduler             调度、恢复、Job Informer
internal/store                 PebbleDB 与备份
```

## 构建与测试

```bash
go build ./...
go test ./...
go vet ./...
go test -race ./...
```

构建容器镜像：

```bash
docker build -t offline-scheduler:latest .
```

## 配置

默认读取 `./configs/config.yaml`，也可以指定：

```bash
./offline-scheduler -config=/etc/offline-scheduler/config.yaml -kubeconfig=$HOME/.kube/config
```

所有配置均支持环境变量覆盖，前缀为 `SCHEDULER_`：

```bash
export SCHEDULER_WORKERIMAGE=registry.example.com/offline-worker:v1
export SCHEDULER_GLOBALMAXCONCURRENCY=100
export SCHEDULER_APIAUTHTOKEN=change-me
```

关键生产配置：

| 配置 | 说明 |
| --- | --- |
| `workerImage` | Worker 镜像，生产必须修改 |
| `workerCpuRequest/workerCpuLimit` | Worker CPU 请求与限制 |
| `workerMemoryRequest/workerMemoryLimit` | Worker 内存请求与限制 |
| `globalMaxConcurrency` | 集群级同时运行分片数 |
| `jobTtlSecondsAfterFinished` | 完成 Job 保留时间，便于查日志 |
| `deleteFinishedJobs` | 完成后立即删除 Job，默认关闭 |
| `resourceCheckEnabled` | 提交时校验节点可分配资源 |
| `apiAuthToken` | 非空时启用 `Authorization: Bearer <token>` |
| `natsMaxDeliver/natsStreamMaxAgeSec` | 消息最大投递次数与 JetStream 保留时长 |
| `backupIntervalSec/backupRetainCount` | Pebble 备份周期与保留数量 |
| `taskRetentionSec` | 终态任务保留秒数，默认 30 天，设为 `0` 关闭清理 |
| `cleanupIntervalSec` | 过期任务清理巡检周期，默认 10 分钟 |
| `logLevel` | `debug`、`info`、`warn`、`error` |

配置加载后会执行必填项、资源规格和枚举值校验，校验失败时进程直接退出，避免带病运行。
生产环境不要把 `apiAuthToken` 明文写入 ConfigMap，应通过 Secret 注入
`SCHEDULER_APIAUTHTOKEN` 环境变量。

## API

### 提交任务

```bash
curl -X POST http://127.0.0.1:8080/api/v1/task/submit \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer change-me" \
  -d '{
    "taskId": "demo-task-001",
    "totalShard": 10,
    "concurrency": 3,
    "maxRetry": 2,
    "timeoutSec": 3600,
    "priority": 5,
    "params": {"key": "value"}
  }'
```

生产模式通过 NATS 异步接收，成功返回 `202 Accepted`。任务最终由 Leader 校验资源并持久化。

### 查询、取消

```bash
curl "http://127.0.0.1:8080/api/v1/task?taskId=demo-task-001"
curl "http://127.0.0.1:8080/api/v1/task/shards?taskId=demo-task-001"
curl -X POST http://127.0.0.1:8080/api/v1/task/cancel \
  -H "Content-Type: application/json" \
  -d '{"taskId":"demo-task-001"}'
```

### 运维端点

- `/healthz`、`/health`：存活检查
- `/readyz`：就绪检查，非 Leader 或存储不可用时返回 503
- `/metrics`：Prometheus 指标

## 部署

```bash
kubectl apply -f configs/rbac.yaml
kubectl apply -f configs/deployment.yaml
```

部署前需要：

1. 修改 `configs/deployment.yaml` 中的 `workerImage`。
2. 提前部署启用 JetStream 的 NATS，并将 `natsUrl` 指向可达地址。
3. 配置可访问的 OTel Collector；不需要追踪时可留空或指向本地 collector。
4. 如需鉴权，通过 Secret 注入 `SCHEDULER_APIAUTHTOKEN`。
5. 确认 PVC StorageClass 支持 `ReadWriteOnce` 和所需容量。
6. 将镜像推送到集群可访问的仓库并替换 Deployment 中的镜像地址。

容器的根文件系统为只读，Pebble 数据、备份目录和临时目录分别挂载到
`/data`、`/backup`、`/tmp`。调度器收到 `SIGTERM` 后会先停止接收请求，
再释放 Leader、停止调度循环、关闭 Informer/NATS 并完成备份管理器退出。

## 高可用说明

当前状态存储为 PebbleDB。Pebble 同一时间只允许一个进程打开同一数据目录，因此：

- 默认部署为单副本。
- HTTP 提交先写入 NATS JetStream，单个调度实例故障时已接收消息不会丢失。
- 任务与分片状态仍保存在本地 Pebble，实例重建后依赖 PVC 恢复。
- 如果要支持多副本无损故障切换，需要将任务和分片状态迁移到共享存储，例如 PostgreSQL、etcd 或 NATS KV。
- 在完成共享存储改造前，不建议直接扩大 `replicas`。

更完整的生产检查项、验证命令和剩余限制见
[docs/PRODUCTION_READINESS.md](docs/PRODUCTION_READINESS.md)。
