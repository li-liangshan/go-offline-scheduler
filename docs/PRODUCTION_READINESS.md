# offline-scheduler 生产就绪说明

- 更新日期：2026-10-18
- 适用范围：`offline-scheduler` 单 Leader 部署模型
- 结论：代码已具备单实例生产部署条件；多副本无损故障切换仍需共享状态存储改造。

## 一、本轮生产化改造

### 配置与启动

- 支持配置文件、`-config`、`-kubeconfig` 和 `SCHEDULER_` 环境变量覆盖。
- 增加必填项、并发数、资源规格、日志级别、镜像拉取策略和 Job TTL 校验。
- HTTP 服务设置请求头、读、写、空闲超时和请求体大小上限。
- 仅在成为 Leader 后订阅 NATS 并启动调度循环。
- 支持 `SIGTERM` 优雅退出，按 HTTP、Leader、调度器、Informer、NATS、备份顺序关闭。

### 可靠性

- JetStream 使用文件存储；已存在的 Stream 会合并 subject 并补齐保留策略。
- 消息按 `MaxDeliver` 重投，超过上限转入 DLQ；DLQ 发布失败时保留原消息并继续重试。
- NATS 连接启用自动重连、连接名、连接超时和重连抖动。
- HTTP 异步提交先写 JetStream，成功返回 `202 Accepted`，避免请求直接依赖 Leader 内存状态。
- 相同 `taskId` 重复消息会被幂等确认，不会重复创建任务。
- 任务与分片通过 Pebble Batch 原子写入；取消任务也使用原子批量写入。
- Leader 启动时恢复未完成任务，并核对运行中分片对应的 Kubernetes Job，回收孤儿分片槽位。
- 调度全局并发与任务级并发同时生效，Job 创建失败会回滚分片状态并释放槽位。
- 分片超时、失败重试、取消、完成均会更新状态并释放对应并发槽位。

### 数据生命周期

- Pebble 使用可配置 block cache、WAL 同步和 Pebble 原生 Checkpoint 热备。
- 备份先写临时目录再原子重命名，按保留数量清理旧备份。
- 终态任务默认保留 30 天，由 Leader 定期清理任务及其全部分片，避免数据无限增长。
- 可通过 `taskRetentionSec` 调整保留期，设置为 `0` 关闭自动清理。

### API 与安全

- 提供 `/healthz`、`/readyz`、`/metrics` 和任务提交、查询、取消接口。
- 非 Leader 或存储异常时 `/readyz` 返回 `503`。
- 可选 Bearer Token 鉴权，使用常量时间比较。
- 请求 JSON 禁止未知字段、限制请求体大小并拒绝尾随 JSON。
- 参数错误返回 400，重复或已结束任务返回 409，任务不存在返回 404。
- 内部错误只向客户端返回通用错误信息，详细原因写入服务端日志。

### 部署资产

- `Dockerfile`：多阶段构建，CGO 关闭，运行于 distroless nonroot 镜像。
- `configs/deployment.yaml`：Namespace、ConfigMap、Deployment、PVC、Service、探针、安全上下文。
- `configs/rbac.yaml`：Job、Pod、Lease 命名空间权限和 Node 集群级只读权限。

## 二、上线前检查清单

1. 将 `workerImage` 替换为真实 Worker 镜像，并确认镜像拉取策略正确。
2. 部署启用 JetStream 的 NATS；生产环境建议使用 3 节点以上 NATS 集群。
3. 为 NATS、OTel Collector 和 Kubernetes API 配置网络策略与可达地址。
4. 通过 Secret 注入 `SCHEDULER_APIAUTHTOKEN`，不要写入 ConfigMap。
5. 为 Pebble 数据、备份目录准备支持 `ReadWriteOnce` 的持久卷。
6. 按 Worker 规格设置 `globalMaxConcurrency`，避免超过节点可分配资源。
7. 确认 `offline-scheduler-sa` 已绑定 Job、Pod、Lease 和 Node 读取权限。
8. 根据业务查询需求设置 `taskRetentionSec`，避免清理仍在使用的历史任务。
9. 配置 Prometheus 抓取 `/metrics`，并为 `/readyz` 配置告警。
10. 确认 Deployment 保持 `replicas: 1`，直到完成共享状态存储改造。

## 三、验证结果

以下命令已在仓库根目录执行并通过：

```bash
gofmt -l ./cmd ./internal
GOCACHE=/tmp/offline-scheduler-gocache go build ./...
GOCACHE=/tmp/offline-scheduler-gocache go test ./...
GOCACHE=/tmp/offline-scheduler-gocache go vet ./...
GOCACHE=/tmp/offline-scheduler-gocache go test -race ./...
git diff --check
```

测试覆盖全部 Go 包，包括配置、HTTP API、Leader 选举、NATS、资源校验、调度器、Informer、Pebble 存储和备份。

## 四、已知限制

1. **本地状态存储**：Pebble 只允许单进程打开同一数据目录，当前部署固定为单副本。
2. **故障恢复依赖 PVC**：调度器 Pod 重建后从 PVC 恢复任务状态；PVC 丢失时只能依赖备份。
3. **全量扫描查询**：`ListShardsByTaskID` 和过期任务清理会遍历 Pebble 键空间；任务量达到百万级前应迁移到带索引的共享存储。
4. **Leader 切换存在短暂不可用**：新 Leader 需要完成存储恢复和 Job 状态核对后，`/readyz` 才返回正常。
5. **HTTP 提交与查询路由**：当前只部署一个实例；多副本时 follower 不持有完整本地状态，不应直接服务查询和取消请求。
6. **OpenTelemetry 业务埋点有限**：当前完成 TracerProvider 初始化，核心业务链路尚未全部创建 span，可结合 `otel.Tracer` 继续补充。

## 五、后续演进建议

1. 将任务、分片和调度游标迁移到 PostgreSQL、etcd 或 NATS KV，实现多副本共享状态。
2. 为任务 ID、状态和更新时间建立二级索引，替代 Pebble 全量扫描。
3. 引入 Worker Job 事件审计和状态机校验，增加终态不可回退的存储层约束。
4. 增加端到端集成测试，覆盖 NATS、Kubernetes fake/kind 集群和 Leader 切换。
5. 补充 Grafana 面板与告警规则，覆盖积压、失败率、槽位利用率和备份失败。
