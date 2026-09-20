# 提交任务
curl -X POST http://127.0.0.1:8080/api/v1/task/submit \
-H "Content-Type:application/json" \
-d '{
    "taskId":"demo-task-001",
    "totalShard":10,
    "concurrency":3,
    "maxRetry":2,
    "timeoutSec":3600,
    "priority":5,
    "params":{"key":"value"}
}'

# 取消任务
curl -X POST http://127.0.0.1:8080/api/v1/task/cancel \
-H "Content-Type:application/json" \
-d '{"taskId":"demo-task-001"}'

