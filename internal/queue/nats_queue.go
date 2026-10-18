package queue

import (
	"context"
	"encoding/json"
	"time"

	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/model"

	"github.com/nats-io/nats.go"
)

type NatsQueue struct {
	nc     *nats.Conn
	js     nats.JetStreamContext
	sub    *nats.Subscription
	dlqSub *nats.Subscription
}

const (
	streamName = "OFFLINE_TASK"
	subject    = "offline.task.submit"
	dlqSubject = "offline.task.dlq"
)

func NewNatsQueue(url string) (*NatsQueue, error) {
	nc, err := nats.Connect(url, nats.MaxReconnects(-1))
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}

	// 创建主stream
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject, dlqSubject},
		MaxAge:   24 * time.Hour,
	})
	if err != nil {
		logger.S.Warnf("stream create warning: %v", err)
	}

	return &NatsQueue{nc: nc, js: js}, nil
}

// Publish 提交任务到nats
func (n *NatsQueue) Publish(req *model.SubmitTaskRequest) error {
	b, _ := json.Marshal(req)
	_, err := n.js.Publish(subject, b)
	return err
}

// Subscribe 主消费 + DLQ死信处理
func (n *NatsQueue) Subscribe(ctx context.Context, handler func(*model.SubmitTaskRequest) error) error {
	sub, err := n.js.QueueSubscribe(subject, "offline-worker", func(msg *nats.Msg) {
		var req model.SubmitTaskRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Nak()
			return
		}
		err := handler(&req)
		if err != nil {
			// 失败，重试3次，超过转入DLQ
			meta, metaErr := msg.Metadata()
			if metaErr == nil && meta.NumDelivered >= 3 {
				// 死信
				_, _ = n.js.Publish(dlqSubject, msg.Data)
				_ = msg.Ack()
				logger.S.Warnw("task move to DLQ", "taskId", req.TaskID)
				return
			}
			_ = msg.NakWithDelay(2 * time.Second)
			return
		}
		_ = msg.Ack()
	}, nats.Durable("offline-consumer"), nats.ManualAck())
	if err != nil {
		return err
	}
	n.sub = sub

	// DLQ消费器，可手动重放死信任务
	dlqSub, err := n.js.Subscribe(dlqSubject, func(msg *nats.Msg) {
		var req model.SubmitTaskRequest
		_ = json.Unmarshal(msg.Data, &req)
		logger.S.Errorw("DLQ task received", "taskId", req.TaskID)
		// 可在这里增加重入调度器逻辑
		_ = msg.Ack()
	}, nats.Durable("offline-dlq-consumer"), nats.ManualAck())
	if err != nil {
		return err
	}
	n.dlqSub = dlqSub
	return nil
}

func (n *NatsQueue) Close() {
	if n.sub != nil {
		_ = n.sub.Unsubscribe()
	}
	if n.dlqSub != nil {
		_ = n.dlqSub.Unsubscribe()
	}
	n.nc.Close()
}
