package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"offline-scheduler/internal/model"
	"offline-scheduler/internal/scheduler"
	"offline-scheduler/internal/store"
)

type HttpServer struct {
	sched *scheduler.Scheduler
	store *store.Store
}

func NewHttpServer(sched *scheduler.Scheduler, st *store.Store) *HttpServer {
	return &HttpServer{sched: sched, store: st}
}

func (h *HttpServer) RegisterRoute(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/task/submit", h.SubmitTaskHandler)
	mux.HandleFunc("POST /api/v1/task/cancel", h.CancelTaskHandler)
	mux.HandleFunc("GET /api/v1/task", h.GetTaskHandler)
	mux.HandleFunc("GET /api/v1/task/shards", h.ListShardsHandler)
	mux.HandleFunc("GET /health", h.HealthHandler)
}

func (h *HttpServer) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *HttpServer) SubmitTaskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req model.SubmitTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]any{"code": 400, "msg": "bad request"})
		return
	}
	err := h.sched.SubmitTask(&req)
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]any{"code": 500, "msg": err.Error()})
		return
	}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]any{"code": 200, "msg": "success"})
}

func (h *HttpServer) CancelTaskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var req struct {
		TaskID string `json:"taskId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]any{"code": 400, "msg": "bad request"})
		return
	}
	err := h.sched.CancelTask(req.TaskID)
	if err != nil {
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]any{"code": 500, "msg": err.Error()})
		return
	}
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]any{"code": 200, "msg": "success"})
}

func (h *HttpServer) GetTaskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	taskID := r.URL.Query().Get("taskId")
	if taskID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 400, "msg": "taskId is required"})
		return
	}
	task, err := h.store.GetTask(taskID)
	if err != nil {
		if errors.Is(err, store.ErrTaskNotFound) {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 404, "msg": "task not found"})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 500, "msg": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": task})
}

func (h *HttpServer) ListShardsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	taskID := r.URL.Query().Get("taskId")
	if taskID == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 400, "msg": "taskId is required"})
		return
	}
	shards, err := h.store.ListShardsByTaskID(taskID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 500, "msg": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"code": 200, "data": shards})
}
