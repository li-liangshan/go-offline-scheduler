package queue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/model"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

type fakeJetStream struct {
	nats.JetStreamContext

	publishSubject string
	publishData    []byte
	publishErr     error

	queueSubject string
	queueName    string
	queueHandler nats.MsgHandler
	queueErr     error

	dlqSubject string
	dlqHandler nats.MsgHandler
	dlqErr     error
}

func (f *fakeJetStream) Publish(subject string, data []byte, _ ...nats.PubOpt) (*nats.PubAck, error) {
	f.publishSubject = subject
	f.publishData = append([]byte(nil), data...)
	return &nats.PubAck{}, f.publishErr
}

func (f *fakeJetStream) QueueSubscribe(subject, queue string, cb nats.MsgHandler, _ ...nats.SubOpt) (*nats.Subscription, error) {
	f.queueSubject = subject
	f.queueName = queue
	f.queueHandler = cb
	if f.queueErr != nil {
		return nil, f.queueErr
	}
	return &nats.Subscription{}, nil
}

func (f *fakeJetStream) Subscribe(subject string, cb nats.MsgHandler, _ ...nats.SubOpt) (*nats.Subscription, error) {
	f.dlqSubject = subject
	f.dlqHandler = cb
	if f.dlqErr != nil {
		return nil, f.dlqErr
	}
	return &nats.Subscription{}, nil
}

func TestPublish(t *testing.T) {
	js := &fakeJetStream{}
	q := &NatsQueue{js: js}
	req := &model.SubmitTaskRequest{TaskID: "task-1", TotalShard: 1}

	require.NoError(t, q.Publish(req))
	require.Equal(t, subject, js.publishSubject)
	var got model.SubmitTaskRequest
	require.NoError(t, json.Unmarshal(js.publishData, &got))
	require.Equal(t, req, &got)
}

func TestPublishReturnsError(t *testing.T) {
	js := &fakeJetStream{publishErr: errors.New("publish failed")}
	q := &NatsQueue{js: js}

	require.Error(t, q.Publish(&model.SubmitTaskRequest{TaskID: "task-1"}))
}

func TestSubscribeRegistersHandlers(t *testing.T) {
	logger.InitLogger()
	js := &fakeJetStream{}
	q := &NatsQueue{js: js}
	handler := func(req *model.SubmitTaskRequest) error {
		require.Equal(t, "task-1", req.TaskID)
		return nil
	}

	require.NoError(t, q.Subscribe(context.Background(), handler))
	require.Equal(t, subject, js.queueSubject)
	require.Equal(t, "offline-worker", js.queueName)
	require.NotNil(t, js.queueHandler)
	require.Equal(t, dlqSubject, js.dlqSubject)
	require.NotNil(t, js.dlqHandler)

	data, err := json.Marshal(&model.SubmitTaskRequest{TaskID: "task-1"})
	require.NoError(t, err)
	js.queueHandler(&nats.Msg{Data: data})
}

func TestSubscribeInvalidJSONDoesNotCallHandler(t *testing.T) {
	logger.InitLogger()
	js := &fakeJetStream{}
	q := &NatsQueue{js: js}
	called := false
	handler := func(req *model.SubmitTaskRequest) error {
		called = true
		return nil
	}

	require.NoError(t, q.Subscribe(context.Background(), handler))
	js.queueHandler(&nats.Msg{Data: []byte("not-json")})
	require.False(t, called)
}

func TestSubscribeReturnsQueueSubscribeError(t *testing.T) {
	js := &fakeJetStream{queueErr: errors.New("queue subscribe failed")}
	q := &NatsQueue{js: js}

	require.Error(t, q.Subscribe(context.Background(), func(req *model.SubmitTaskRequest) error { return nil }))
}

func TestSubscribeHandlerErrorNaksMessage(t *testing.T) {
	logger.InitLogger()
	js := &fakeJetStream{}
	q := &NatsQueue{js: js}
	require.NoError(t, q.Subscribe(context.Background(), func(req *model.SubmitTaskRequest) error {
		return errors.New("handler failed")
	}))

	data, err := json.Marshal(&model.SubmitTaskRequest{TaskID: "task-1"})
	require.NoError(t, err)
	require.NotPanics(t, func() {
		js.queueHandler(&nats.Msg{Data: data})
	})
}

func TestSubscribeReturnsDLQSubscribeError(t *testing.T) {
	js := &fakeJetStream{dlqErr: errors.New("dlq subscribe failed")}
	q := &NatsQueue{js: js}

	require.Error(t, q.Subscribe(context.Background(), func(req *model.SubmitTaskRequest) error { return nil }))
}

func TestCloseWithNoSubscriptions(t *testing.T) {
	q := &NatsQueue{}
	require.NotPanics(t, q.Close)
}

func TestNewNatsQueueConnectionError(t *testing.T) {
	q, err := NewNatsQueue("nats://127.0.0.1:0")
	require.Error(t, err)
	require.Nil(t, q)
}
