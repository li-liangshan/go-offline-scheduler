package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/model"
	"offline-scheduler/internal/resourcecheck"
	"offline-scheduler/internal/scheduler"
	"offline-scheduler/internal/store"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	logger.InitLogger()
	os.Exit(m.Run())
}

func newTestMux(t *testing.T) (*http.ServeMux, *store.Store) {
	return newTestMuxWithOptions(t, Options{})
}

func newTestMuxWithOptions(t *testing.T, opts Options) (*http.ServeMux, *store.Store) {
	t.Helper()
	st, err := store.NewStore(filepath.Join(t.TempDir(), "pebble"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("16"),
			corev1.ResourceMemory: resource.MustParse("64Gi"),
		}},
	}
	client := fake.NewSimpleClientset(node)
	resChecker := resourcecheck.NewResourceChecker(client, "default")
	sched := scheduler.NewScheduler(client, st, resChecker, "default", 10)
	server := NewHttpServerWithOptions(sched, st, opts)
	mux := http.NewServeMux()
	server.RegisterRoute(mux)
	return mux, st
}

func request(mux *http.ServeMux, method, target string, body []byte) *httptest.ResponseRecorder {
	var req *http.Request
	if body == nil {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, req)
	return recorder
}

func TestHealthHandler(t *testing.T) {
	mux, _ := newTestMux(t)
	rec := request(mux, http.MethodGet, "/health", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status":"ok"}`, rec.Body.String())
}

func TestSubmitAndQueryTask(t *testing.T) {
	mux, _ := newTestMux(t)
	payload, err := json.Marshal(model.SubmitTaskRequest{
		TaskID:      "api-task",
		TotalShard:  2,
		Concurrency: 1,
		TimeoutSec:  60,
	})
	require.NoError(t, err)

	rec := request(mux, http.MethodPost, "/api/v1/task/submit", payload)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"code":200,"msg":"success"}`, rec.Body.String())

	rec = request(mux, http.MethodGet, "/api/v1/task?taskId=api-task", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var taskResp struct {
		Code int               `json:"code"`
		Data model.OfflineTask `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &taskResp))
	require.Equal(t, 200, taskResp.Code)
	require.Equal(t, "api-task", taskResp.Data.TaskID)
	require.Equal(t, model.TaskStatusPending, taskResp.Data.Status)

	rec = request(mux, http.MethodGet, "/api/v1/task/shards?taskId=api-task", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var shardsResp struct {
		Code int                `json:"code"`
		Data []*model.TaskShard `json:"data"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &shardsResp))
	require.Equal(t, 200, shardsResp.Code)
	require.Len(t, shardsResp.Data, 2)
}

func TestCancelTaskHandler(t *testing.T) {
	mux, _ := newTestMux(t)
	payload, err := json.Marshal(model.SubmitTaskRequest{
		TaskID:      "cancel-api",
		TotalShard:  1,
		Concurrency: 1,
		TimeoutSec:  60,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, request(mux, http.MethodPost, "/api/v1/task/submit", payload).Code)

	cancelBody := []byte(`{"taskId":"cancel-api"}`)
	rec := request(mux, http.MethodPost, "/api/v1/task/cancel", cancelBody)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"code":200,"msg":"success"}`, rec.Body.String())

	rec = request(mux, http.MethodPost, "/api/v1/task/cancel", cancelBody)
	require.Equal(t, http.StatusConflict, rec.Code)
}

func TestReadyHandler(t *testing.T) {
	mux, _ := newTestMux(t)
	rec := request(mux, http.MethodGet, "/readyz", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.JSONEq(t, `{"status":"ready"}`, rec.Body.String())
}

func TestReadyHandlerChecksReadinessFunc(t *testing.T) {
	mux, _ := newTestMuxWithOptions(t, Options{ReadyFunc: func() error { return errors.New("not leader") }})
	rec := request(mux, http.MethodGet, "/readyz", nil)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestDuplicateSubmitReturnsConflict(t *testing.T) {
	mux, _ := newTestMux(t)
	payload, err := json.Marshal(model.SubmitTaskRequest{
		TaskID:      "duplicate-api",
		TotalShard:  1,
		Concurrency: 1,
		TimeoutSec:  60,
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, request(mux, http.MethodPost, "/api/v1/task/submit", payload).Code)
	require.Equal(t, http.StatusConflict, request(mux, http.MethodPost, "/api/v1/task/submit", payload).Code)
}

func TestSubmitValidationReturnsBadRequest(t *testing.T) {
	mux, _ := newTestMux(t)
	rec := request(mux, http.MethodPost, "/api/v1/task/submit", []byte(`{"totalShard":1}`))
	require.Equal(t, http.StatusBadRequest, rec.Code)

	rec = request(mux, http.MethodPost, "/api/v1/task/submit", []byte(`{"taskId":"x","totalShard":1,"unknown":true}`))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSubmitRejectsTrailingJSON(t *testing.T) {
	mux, _ := newTestMux(t)
	body := []byte(`{"taskId":"trailing","totalShard":1,"concurrency":1,"timeoutSec":60}{"taskId":"second"}`)

	rec := request(mux, http.MethodPost, "/api/v1/task/submit", body)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAuthMiddleware(t *testing.T) {
	mux, _ := newTestMuxWithOptions(t, Options{AuthToken: "secret"})
	server := &HttpServer{authToken: "secret"}
	handler := server.AuthMiddleware(mux)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/task?taskId=x", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/task?taskId=missing", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestBadRequests(t *testing.T) {
	mux, _ := newTestMux(t)

	rec := request(mux, http.MethodPost, "/api/v1/task/submit", []byte("{"))
	require.Equal(t, http.StatusBadRequest, rec.Code)

	rec = request(mux, http.MethodPost, "/api/v1/task/cancel", []byte("{"))
	require.Equal(t, http.StatusBadRequest, rec.Code)

	rec = request(mux, http.MethodGet, "/api/v1/task", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)

	rec = request(mux, http.MethodGet, "/api/v1/task/shards", nil)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetTaskNotFound(t *testing.T) {
	mux, _ := newTestMux(t)
	rec := request(mux, http.MethodGet, "/api/v1/task?taskId=missing", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.JSONEq(t, `{"code":404,"msg":"task not found"}`, rec.Body.String())
}

func TestListShardsNotFound(t *testing.T) {
	mux, _ := newTestMux(t)
	rec := request(mux, http.MethodGet, "/api/v1/task/shards?taskId=missing", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.JSONEq(t, `{"code":404,"msg":"task not found"}`, rec.Body.String())
}

func TestSubmitRejectsWhenResourcesInsufficient(t *testing.T) {
	st, err := store.NewStore(filepath.Join(t.TempDir(), "pebble"))
	require.NoError(t, err)
	defer st.Close()

	client := fake.NewSimpleClientset()
	resChecker := resourcecheck.NewResourceChecker(client, "default")
	sched := scheduler.NewScheduler(client, st, resChecker, "default", 10)
	server := NewHttpServer(sched, st)
	mux := http.NewServeMux()
	server.RegisterRoute(mux)

	payload, err := json.Marshal(model.SubmitTaskRequest{
		TaskID:      "no-resource",
		TotalShard:  1,
		Concurrency: 1,
		TimeoutSec:  60,
	})
	require.NoError(t, err)
	rec := request(mux, http.MethodPost, "/api/v1/task/submit", payload)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
}

type fakeSubmitter struct {
	req *model.SubmitTaskRequest
	err error
}

func (f *fakeSubmitter) SubmitTask(req *model.SubmitTaskRequest) error {
	f.req = req
	return f.err
}

func TestAsyncSubmitPublishesNormalizedRequest(t *testing.T) {
	submitter := &fakeSubmitter{}
	mux, _ := newTestMuxWithOptions(t, Options{Submitter: submitter, AsyncSubmit: true})
	rec := request(mux, http.MethodPost, "/api/v1/task/submit", []byte(`{"taskId":"async","totalShard":2}`))
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.NotNil(t, submitter.req)
	require.Equal(t, "async", submitter.req.TaskID)
	require.Equal(t, 2, submitter.req.Concurrency)
	require.Equal(t, int64(3600), submitter.req.TimeoutSec)
}

func TestAsyncSubmitReturnsServiceUnavailable(t *testing.T) {
	submitter := &fakeSubmitter{err: errors.New("nats unavailable")}
	mux, _ := newTestMuxWithOptions(t, Options{Submitter: submitter, AsyncSubmit: true})
	rec := request(mux, http.MethodPost, "/api/v1/task/submit", []byte(`{"taskId":"async","totalShard":1}`))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestAsyncSubmitValidatesBeforePublish(t *testing.T) {
	submitter := &fakeSubmitter{}
	mux, _ := newTestMuxWithOptions(t, Options{Submitter: submitter, AsyncSubmit: true})
	rec := request(mux, http.MethodPost, "/api/v1/task/submit", []byte(`{"taskId":"bad task","totalShard":1}`))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Nil(t, submitter.req)
}
