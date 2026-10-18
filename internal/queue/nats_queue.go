package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/model"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type NatsQueue struct {
	nc        *nats.Conn
	js        nats.JetStreamContext
	sub       *nats.Subscription
	dlqSub    *nats.Subscription
	options   Options
	subMu     sync.Mutex
	closeOnce sync.Once
}

type Options struct {
	Name           string
	ConnectTimeout time.Duration
	MaxDeliver     int
	AckWait        time.Duration
	MaxAckPending  int
	StreamMaxAge   time.Duration
}

const (
	streamName = "OFFLINE_TASK"
	subject    = "offline.task.submit"
	dlqSubject = "offline.task.dlq"
)

func NewNatsQueue(url string) (*NatsQueue, error) {
	return NewNatsQueueWithOptions(url, Options{})
}

func NewNatsQueueWithOptions(url string, opts Options) (*NatsQueue, error) {
	if opts.Name == "" {
		opts.Name = "offline-scheduler"
	}
	if opts.ConnectTimeout <= 0 {
		opts.ConnectTimeout = 10 * time.Second
	}
	if opts.MaxDeliver <= 0 {
		opts.MaxDeliver = 5
	}
	if opts.AckWait <= 0 {
		opts.AckWait = 30 * time.Second
	}
	if opts.MaxAckPending <= 0 {
		opts.MaxAckPending = 1024
	}
	if opts.StreamMaxAge <= 0 {
		opts.StreamMaxAge = 24 * time.Hour
	}
	nc, err := nats.Connect(url,
		nats.Name(opts.Name),
		nats.Timeout(opts.ConnectTimeout),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 2*time.Second),
	)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, err
	}

	// 创建主stream；已存在时合并 subjects 并更新配置。
	streamCfg := &nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject, dlqSubject},
		MaxAge:   opts.StreamMaxAge,
		Storage:  nats.FileStorage,
	}
	_, err = js.AddStream(streamCfg)
	if err != nil {
		info, infoErr := js.StreamInfo(streamName)
		if infoErr != nil {
			nc.Close()
			return nil, fmt.Errorf("ensure stream %s: %w", streamName, err)
		}
		merged := info.Config
		merged.Subjects = mergeSubjects(merged.Subjects, streamCfg.Subjects)
		if merged.MaxAge < streamCfg.MaxAge {
			merged.MaxAge = streamCfg.MaxAge
		}
		if _, updateErr := js.UpdateStream(&merged); updateErr != nil {
			nc.Close()
			return nil, fmt.Errorf("update stream %s: %w", streamName, updateErr)
		}
	}

	return &NatsQueue{nc: nc, js: js, options: opts}, nil
}

// Publish 提交任务到nats
func (n *NatsQueue) Publish(req *model.SubmitTaskRequest) error {
	if req == nil {
		return fmt.Errorf("submit request cannot be nil")
	}
	b, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal submit request: %w", err)
	}
	_, err = n.js.Publish(subject, b)
	return err
}

// SubmitTask 让 NatsQueue 实现异步提交接口：HTTP 只负责把任务可靠写入 JetStream。
func (n *NatsQueue) SubmitTask(req *model.SubmitTaskRequest) error {
	return n.Publish(req)
}

// Subscribe 主消费 + DLQ死信处理
func (n *NatsQueue) Subscribe(ctx context.Context, handler func(*model.SubmitTaskRequest) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.subMu.Lock()
	defer n.subMu.Unlock()
	if n.sub != nil {
		return nil
	}
	maxDeliver := n.options.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = 5
	}
	sub, err := n.js.QueueSubscribe(subject, "offline-worker", func(msg *nats.Msg) {
		var req model.SubmitTaskRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			_ = msg.Nak()
			return
		}
		err := handler(&req)
		if err != nil {
			// 失败后由 JetStream 按 MaxDeliver 重投，达到上限后转入 DLQ。
			meta, metaErr := msg.Metadata()
			if metaErr == nil && int(meta.NumDelivered) >= maxDeliver {
				if _, pubErr := n.js.Publish(dlqSubject, msg.Data); pubErr != nil {
					logger.L.Error("publish task to DLQ failed", zap.String("taskId", req.TaskID), zap.Error(pubErr))
					_ = msg.NakWithDelay(2 * time.Second)
					return
				}
				_ = msg.Ack()
				logger.S.Warnw("task move to DLQ", "taskId", req.TaskID)
				return
			}
			if metaErr != nil {
				logger.L.Warn("read message metadata failed", zap.String("taskId", req.TaskID), zap.Error(metaErr))
			}
			_ = msg.NakWithDelay(2 * time.Second)
			return
		}
		_ = msg.Ack()
	}, nats.Durable("offline-consumer"), nats.ManualAck(),
		nats.MaxDeliver(maxDeliver), nats.AckWait(n.options.AckWait), nats.MaxAckPending(n.options.MaxAckPending))
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
	}, nats.Durable("offline-dlq-consumer"), nats.ManualAck(), nats.MaxDeliver(maxDeliver))
	if err != nil {
		_ = sub.Unsubscribe()
		n.sub = nil
		return err
	}
	n.dlqSub = dlqSub
	return nil
}

func (n *NatsQueue) Close() {
	n.closeOnce.Do(func() {
		n.StopSubscribe()
		n.nc.Close()
	})
}

// StopSubscribe 只停止消费，不关闭 NATS 连接，便于 Leader 切换后重新订阅。
func (n *NatsQueue) StopSubscribe() {
	n.subMu.Lock()
	defer n.subMu.Unlock()
	if n.sub != nil {
		_ = n.sub.Unsubscribe()
		n.sub = nil
	}
	if n.dlqSub != nil {
		_ = n.dlqSub.Unsubscribe()
		n.dlqSub = nil
	}
}

func mergeSubjects(existing, required []string) []string {
	seen := make(map[string]struct{}, len(existing)+len(required))
	result := make([]string, 0, len(existing)+len(required))
	for _, subject := range append(existing, required...) {
		if subject == "" {
			continue
		}
		if _, ok := seen[subject]; ok {
			continue
		}
		seen[subject] = struct{}{}
		result = append(result, subject)
	}
	return result
}
