package api

import (
	"encoding/json"
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
	mux.HandleFunc("GET /health", h.HealthHandler)
}

func (h *HttpServer) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *HttpServer) SubmitTaskHandler(w http.ResponseWriter, r *http.Request) {
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
