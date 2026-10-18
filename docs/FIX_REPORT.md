# offline-scheduler 修复报告

- 修复日期：2026-10-18
- 项目：offline-scheduler
- 修复目标：恢复项目可编译、可启动、可调度状态，并修正任务生命周期中的并发与状态一致性问题。

## 一、问题概览

本次修复前，项目存在以下关键问题：

1. 存储备份模块使用不存在的 Pebble API，导致 `go build ./...` 直接失败。
2. 提交任务时只保存任务元数据，没有创建任何分片，调度器实际上永远没有分片可执行。
3. Leader 选举配置未正确传入，启动时必然返回 `namespace and leaseName must not empty`。
4. 任务级并发限制未生效，已运行分片没有计入该任务的并发额度。
5. 分片失败、超时、取消后的并发槽位释放不完整，可能导致槽位泄漏或重复释放。
6. 失败重试判断错误，`maxRetry` 语义不一致，任务可能提前结束或永不重试。
7. 任务最终状态可能被已取消任务覆盖，取消后仍可能残留运行中的 Job。
8. Job Informer 的缓存同步顺序错误，删除事件未处理 tombstone。
9. 退出流程中备份管理器被重复 `Stop`，可能触发 `close of closed channel`。
10. 节点资源校验所需权限使用了 namespaced `Role`，无法授权集群级 `nodes` 资源。

## 二、修复内容

### 1. 存储与备份

- 使用 Pebble 原生 `Checkpoint` 替换不存在的 `Snapshot.Save`：
  - `internal/store/backup_manager.go`
- 移除对 `SSTableInfo.FilePath` 的错误使用，避免只复制 SST 文件而生成不可恢复的备份。
- 备份目录先写入 `.tmp`，完成后原子重命名。
- 为备份流程增加互斥锁，避免定时备份与手动备份并发冲突。
- `Stop` 增加 `sync.Once`，保证重复调用安全。
- 增加备份恢复测试：打开备份数据库并验证键值可读取。

### 2. 任务提交与持久化

- `SubmitTask` 增加参数校验：
  - `taskId` 必填且符合 Kubernetes Label Value 规则。
  - `totalShard` 必须大于 0。
  - `concurrency` 缺省时自动取 `totalShard`，且不超过分片总数。
  - `maxRetry` 负数归零。
  - `timeoutSec` 缺省为 3600 秒。
  - 参数名必须是合法的环境变量名。
- 提交任务时同步创建全部分片。
- 新增 `SaveTaskWithShards`，使用 Pebble Batch 原子保存任务与分片，避免任务已入库但分片缺失。
- 重复提交相同 `taskId` 会被拒绝。

相关文件：

- `internal/scheduler/scheduler.go`
- `internal/store/pebble_store.go`

### 3. 调度与并发控制

- 调度时统计任务当前正在运行的分片数，并将 `task.Concurrency` 作为单任务并发上限。
- 全局并发槽位与任务并发额度同时生效：`canStart = min(globalFree, taskAvailable, pendingShards)`。
- 分片启动前先占用全局槽位，避免 Job 创建成功但 Informer 回调先于槽位计数导致计数泄漏。
- 任务创建失败时回滚分片状态并释放槽位。
- 使用任务入队集合去重，避免同一任务在优先级队列中重复堆积。
- Job 名称增加重试序号，避免失败重试时与残留 Job 名称冲突。
- 对过长或包含非法字符的任务 ID 生成合法、长度受限的 Job 名称和分片 ID。

### 4. 分片状态与生命周期

- 分片状态更新统一通过 `shardMu` 串行化，避免 Informer 与超时巡检同时处理同一分片。
- 任务完成、失败、超时、取消时均正确释放全局并发槽位。
- 超时巡检只处理仍处于 `running` 的最新分片状态，避免使用过期数据。
- 分片完成后删除对应 Kubernetes Job，避免资源堆积。
- 忽略旧 Job 的迟到事件，避免重试后的新分片被旧事件误标为失败。

### 5. 重试语义

- `maxRetry` 表示首次执行失败后的最大重试次数。
- 分片失败后 `RetryCount` 递增，仅当 `RetryCount <= MaxRetry` 时重新调度。
- 达到重试上限后任务最终标记为 `failed`。
- 已取消任务不会被后续回调或最终状态计算覆盖。

### 6. 任务取消

- 取消任务时先更新任务状态，再逐个更新分片状态。
- 正在运行的分片会释放并发槽位并删除对应 Job。
- 已成功或已取消的分片不会重复处理。
- Job 删除操作在分片状态锁释放后执行，避免长时间持锁。
- 增加“Job 创建期间任务被取消”的清理逻辑，防止取消后仍残留新 Job。

### 7. Leader 选举与生命周期

- 修复 `NewLeaderElector` 参数签名，统一通过 `leader.Config` 传入命名空间、租约名和身份。
- 使用 `POD_NAME` 或主机名作为默认选举身份。
- 选举配置中的 `LeaseDuration`、`RenewDeadline`、`RetryPeriod` 现在真正生效。
- 调度主循环和超时巡检仅在节点成为 Leader 后启动。
- `Scheduler.Run` 与 `Scheduler.Stop` 支持重复调用，并可在 Leader 变更后重新启动。

相关文件：

- `internal/leader/leader.go`
- `cmd/scheduler/main.go`
- `internal/scheduler/scheduler.go`

### 8. Job Informer

- 修正 Informer 启动方式，避免 `WaitForCacheSync` 在 `informer.Run` 退出后才执行。
- 启动阶段等待缓存同步增加 30 秒超时，避免无限阻塞。
- 处理 `DeletedFinalStateUnknown` tombstone。
- 仅缓存本调度器管理的 Job。
- 增加回调空指针保护。

### 9. 启动与优雅退出

- OTel 初始化失败时不再调用空指针 `Shutdown`。
- OTel 端点使用配置项 `otelEndpoint`。
- 备份管理器 `Stop` 可重复调用，修复退出时重复关闭 channel 的 panic 风险。
- Leader、调度器、Informer、备份管理器按顺序停止。

### 10. HTTP API 与 RBAC

- 新增任务查询接口：`GET /api/v1/task?taskId=...`
- 新增分片查询接口：`GET /api/v1/task/shards?taskId=...`
- 更新 `test.sh` 增加查询示例。
- 为集群级 `nodes` 资源读取增加 `ClusterRole` 与 `ClusterRoleBinding`。

相关文件：

- `internal/api/http_api.go`
- `configs/rbac.yaml`
- `test.sh`

## 三、测试与验证

已执行并通过以下命令：

```bash
go build ./...
go test ./...
go vet ./...
go test -race ./internal/scheduler ./internal/store ./internal/leader
```

新增测试覆盖：

1. 任务提交后正确创建分片。
2. 单任务并发上限生效。
3. 分片完成释放槽位后继续调度剩余分片。
4. 分片失败后按 `maxRetry` 重试，达到上限后任务失败。
5. 取消任务后释放运行槽位并删除 Job。
6. Pebble Checkpoint 备份可恢复。
7. Leader 选举配置可正常构造。

### 测试文件覆盖

已为全部 Go 包补齐测试文件：

```text
cmd/scheduler/main_test.go
internal/api/http_api_test.go
internal/config/config_test.go
internal/leader/leader_test.go
internal/logger/logger_test.go
internal/metric/metric_test.go
internal/model/model_test.go
internal/otel/trace_test.go
internal/queue/nats_queue_test.go
internal/resourcecheck/resource_check_test.go
internal/scheduler/job_informer_test.go
internal/scheduler/scheduler_test.go
internal/store/backup_manager_test.go
internal/store/pebble_store_test.go
```

覆盖率结果：

| 包 | 覆盖率 |
| --- | --- |
| `cmd/scheduler` | 12.4% |
| `internal/api` | 86.6% |
| `internal/config` | 87.0% |
| `internal/leader` | 78.0% |
| `internal/logger` | 92.3% |
| `internal/metric` | 无语句 |
| `internal/model` | 无语句 |
| `internal/otel` | 81.8% |
| `internal/queue` | 63.2% |
| `internal/resourcecheck` | 94.6% |
| `internal/scheduler` | 80.7% |
| `internal/store` | 75.3% |

`cmd/scheduler` 的覆盖率较低，因为 `main()` 包含完整的 K8s、NATS、HTTP 启动流程，不适合在单元测试中直接执行；其 kubeconfig 构造逻辑已单独测试。

## 四、变更文件

```text
Dockerfile
.dockerignore
cmd/scheduler/main.go
configs/rbac.yaml
configs/deployment.yaml
configs/config.yaml
docs/FIX_REPORT.md
docs/PRODUCTION_READINESS.md
internal/api/http_api.go
internal/api/http_api_test.go
internal/config/config_test.go
internal/leader/leader.go
internal/leader/leader_test.go
internal/logger/logger_test.go
internal/metric/metric_test.go
internal/model/model.go
internal/model/model_test.go
internal/otel/trace.go
internal/otel/trace_test.go
internal/queue/nats_queue.go
internal/queue/nats_queue_test.go
internal/resourcecheck/resource_check.go
internal/resourcecheck/resource_check_test.go
internal/scheduler/job_informer.go
internal/scheduler/job_informer_test.go
internal/scheduler/scheduler.go
internal/scheduler/scheduler_test.go
internal/store/backup_manager.go
internal/store/backup_manager_test.go
internal/store/pebble_store.go
internal/store/pebble_store_test.go
cmd/scheduler/main_test.go
test.sh
```

## 五、生产级改造补充

在完成上述缺陷修复后，项目又进行了一轮面向生产运行的改造：

- 配置系统支持默认值、环境变量覆盖和启动校验。
- HTTP 提交改为先写入 NATS JetStream，增加鉴权、请求体限制、健康检查和就绪检查。
- JetStream 使用文件存储，支持配置最大投递次数和消息保留时长，DLQ 发布失败不丢消息。
- Leader 启动时恢复未完成任务，并回收 Kubernetes 中已不存在的孤儿分片。
- 任务与分片的创建、取消使用批量原子写入，避免部分写入导致状态不一致。
- 增加终态任务保留策略和定时清理，防止 Pebble 数据无限增长。
- 增加 Dockerfile、Kubernetes Deployment、探针、安全上下文和 PVC 示例。
- 全量执行 `go build`、`go test`、`go vet`、`go test -race` 并通过。

详细的生产检查清单、验证结果和已知限制见
[PRODUCTION_READINESS.md](PRODUCTION_READINESS.md)。

## 六、剩余架构限制

当前任务与分片状态仍保存在实例本地 PebbleDB：

- Pebble 同一时间只允许一个进程打开同一数据目录，因此默认部署保持单副本。
- HTTP 提交已通过 NATS 持久化，实例故障时已接收消息不会丢失。
- 任务元数据仍依赖 PVC 恢复；PVC 丢失时需要从 Pebble Checkpoint 备份恢复。
- follower 不持有完整本地状态，不应直接服务查询和取消请求。

要彻底支持多副本无损故障切换，需要将任务和分片状态迁移到共享存储，
例如 PostgreSQL、etcd 或 NATS KV，并增加对应的索引和事务语义。
在完成共享存储改造前，不建议直接扩大 `replicas`。
