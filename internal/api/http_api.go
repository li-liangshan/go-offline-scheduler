package api

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"offline-scheduler/internal/logger"
	"offline-scheduler/internal/metric"
	"offline-scheduler/internal/model"
	"offline-scheduler/internal/scheduler"
	"offline-scheduler/internal/store"

	"go.uber.org/zap"
)

type Options struct {
	MaxBodyBytes int64
	AuthToken    string
	Submitter    TaskSubmitter
	AsyncSubmit  bool
	ReadyFunc    func() error
}

type TaskSubmitter interface {
	SubmitTask(*model.SubmitTaskRequest) error
}

type HttpServer struct {
	sched        *scheduler.Scheduler
	store        *store.Store
	maxBodyBytes int64
	authToken    string
	submitter    TaskSubmitter
	asyncSubmit  bool
	readyFunc    func() error
}

func NewHttpServer(sched *scheduler.Scheduler, st *store.Store) *HttpServer {
	return NewHttpServerWithOptions(sched, st, Options{})
}

func NewHttpServerWithOptions(sched *scheduler.Scheduler, st *store.Store, opts Options) *HttpServer {
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = 1 << 20
	}
	return &HttpServer{
		sched:        sched,
		store:        st,
		maxBodyBytes: opts.MaxBodyBytes,
		authToken:    opts.AuthToken,
		submitter:    opts.Submitter,
		asyncSubmit:  opts.AsyncSubmit,
		readyFunc:    opts.ReadyFunc,
	}
}

func (h *HttpServer) RegisterRoute(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/task/submit", h.SubmitTaskHandler)
	mux.HandleFunc("POST /api/v1/task/cancel", h.CancelTaskHandler)
	mux.HandleFunc("GET /api/v1/task", h.GetTaskHandler)
	mux.HandleFunc("GET /api/v1/task/shards", h.ListShardsHandler)
	mux.HandleFunc("GET /health", h.HealthHandler)
	mux.HandleFunc("GET /healthz", h.HealthHandler)
	mux.HandleFunc("GET /readyz", h.ReadyHandler)
}

// AuthMiddleware 只保护 API 路径，健康检查和 metrics 保持可被探针/监控访问。
func (h *HttpServer) AuthMiddleware(next http.Handler) http.Handler {
	if h.authToken == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}
		expected := "Bearer " + h.authToken
		actual := r.Header.Get("Authorization")
		if subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSON(w, http.StatusUnauthorized, map[string]any{"code": 401, "msg": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *HttpServer) HealthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (h *HttpServer) ReadyHandler(w http.ResponseWriter, r *http.Request) {
	if h.readyFunc != nil {
		if err := h.readyFunc(); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not ready", "error": err.Error()})
			return
		}
	}
	if err := h.store.Healthy(); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not ready", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func (h *HttpServer) SubmitTaskHandler(w http.ResponseWriter, r *http.Request) {
	metric.TaskSubmitHttp.Inc()
	var req model.SubmitTaskRequest
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "msg": "bad request"})
		return
	}
	if h.submitter != nil {
		normalized, err := scheduler.NormalizeSubmitRequest(&req)
		if err != nil {
			writeSchedulerError(w, err)
			return
		}
		if err := h.submitter.SubmitTask(normalized); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "msg": err.Error()})
			return
		}
		if h.asyncSubmit {
			writeJSON(w, http.StatusAccepted, map[string]any{"code": 202, "msg": "accepted"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"code": 200, "msg": "success"})
		return
	}
	if err := h.sched.SubmitTask(&req); err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": 200, "msg": "success"})
}

func (h *HttpServer) CancelTaskHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TaskID string `json:"taskId"`
	}
	if err := h.decodeJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "msg": "bad request"})
		return
	}
	if err := h.sched.CancelTask(req.TaskID); err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": 200, "msg": "success"})
}

func (h *HttpServer) GetTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("taskId")
	if taskID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "msg": "taskId is required"})
		return
	}
	task, err := h.store.GetTask(taskID)
	if err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "msg": "task not found"})
			return
		}
		logger.L.Error("get task failed", zap.String("taskId", taskID), zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{"code": 500, "msg": "internal server error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": 200, "data": task})
}

func (h *HttpServer) ListShardsHandler(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("taskId")
	if taskID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "msg": "taskId is required"})
		return
	}
	if _, err := h.store.GetTask(taskID); err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "msg": "task not found"})
			return
		}
		logger.L.Error("get task before listing shards failed", zap.String("taskId", taskID), zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{"code": 500, "msg": "internal server error"})
		return
	}
	shards, err := h.store.ListShardsByTaskID(taskID)
	if err != nil {
		logger.L.Error("list shards failed", zap.String("taskId", taskID), zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{"code": 500, "msg": "internal server error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"code": 200, "data": shards})
}

func (h *HttpServer) decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	// 拒绝对象后继续追加 JSON 的请求，避免同一请求被解释出多个语义。
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON value")
	}
	return nil
}

func writeSchedulerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, scheduler.ErrInvalidRequest):
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "msg": err.Error()})
	case errors.Is(err, scheduler.ErrTaskExists), errors.Is(err, scheduler.ErrTaskFinished):
		writeJSON(w, http.StatusConflict, map[string]any{"code": 409, "msg": err.Error()})
	case errors.Is(err, store.ErrTaskNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{"code": 404, "msg": "task not found"})
	default:
		logger.L.Error("request failed", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]any{"code": 500, "msg": "internal server error"})
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
