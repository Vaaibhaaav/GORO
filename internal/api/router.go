package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/Vaaibhaaav/GORO/internal/queue"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type JobServer struct {
	Queue queue.Queue
}

type JobRequest struct {
	Type     string         `json:"type"`
	Priority queue.Priority `json:"priority"`
	Payload  map[string]any `json:"payload"`
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func NewJobServer(q queue.Queue) *JobServer {
	return &JobServer{
		Queue: q,
	}
}

func (s *JobServer) SubmitJobs(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1048576)

	var req JobRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(&req); err != nil {
		s.respondWithError(w, http.StatusBadRequest, "Invalid JSON payload")
		return
	}

	if req.Type == "" {
		s.respondWithError(w, http.StatusUnprocessableEntity, "Job type is required")
		return
	}

	if req.Priority == "" {
		req.Priority = queue.PriorityDefault
	}

	newJob := &queue.Job{
		ID:         uuid.New().String(),
		Type:       req.Type,
		Priority:   req.Priority,
		Payload:    req.Payload,
		Status:     queue.StatusPending,
		MaxRetries: 3,
		CreatedAt:  time.Now(),
	}

	ctx := r.Context()
	if err := s.Queue.Push(ctx, newJob); err != nil {
		s.respondWithError(w, http.StatusInternalServerError, "Failed to enqueue job")
		return
	}

	s.respondWithJSON(w, http.StatusAccepted, map[string]string{
		"job_id": newJob.ID,
		"status": string(newJob.Status),
	})
}

func (s *JobServer) GetJob(w http.ResponseWriter, r *http.Request) {
	jobId := chi.URLParam(r, "id")
	if jobId == "" {
		s.respondWithError(w, http.StatusBadRequest, "Job ID is required")
		return
	}
	ctx := r.Context()
	job, err := s.Queue.GetJob(ctx, jobId)
	if err != nil {
		s.respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}
	if job == nil {
		s.respondWithError(w, http.StatusNotFound, "Job not found")
		return
	}

	s.respondWithJSON(w, http.StatusOK, job)
}

func (s *JobServer) GetQueueStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats, err := s.Queue.Stats(ctx)
	if err != nil {
		log.Printf("[API ERROR] Error while fetching queue stats: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}
	s.respondWithJSON(w, http.StatusOK, stats)
}

func (s *JobServer) GetDLQ(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dlq, err := s.Queue.GetDLQ(ctx)

	if err != nil {
		log.Printf("[API ERROR] Error while fetching dead letter queue: %v", err)
		s.respondWithError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	s.respondWithJSON(w, http.StatusOK, dlq)
}

func (s *JobServer) RequeueDLQJob(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	if jobID == "" {
		s.respondWithError(w, http.StatusBadRequest, "Job ID is required")
		return
	}

	ctx := r.Context()

	job, err := s.Queue.GetJob(ctx, jobID)
	if err != nil {
		s.respondWithError(w, http.StatusInternalServerError, "Database error")
		return
	}
	if job == nil {
		s.respondWithError(w, http.StatusNotFound, "Job not found")
		return
	}

	if job.Status != queue.StatusFailed {
		s.respondWithError(w, http.StatusBadRequest, "Only failed jobs can be requeued")
		return
	}

	job.Status = queue.StatusPending
	job.Retries = 0
	job.Error = ""
	job.UpdatedAt = time.Now()

	if err := s.Queue.RemoveFromDLQ(ctx, job); err != nil {
		s.respondWithError(w, http.StatusInternalServerError, "Failed to remove job from DLQ")
		return
	}

	if err := s.Queue.Push(ctx, job); err != nil {
		s.respondWithError(w, http.StatusInternalServerError, "Failed to requeue job")
		return
	}

	s.respondWithJSON(w, http.StatusOK, map[string]string{
		"message": "Job successfully requeued",
		"job_id":  job.ID,
	})
}

func (s *JobServer) RetryDLQJob(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "id")
	fmt.Printf("The id is %s",jobID)
	if jobID == "" {
		s.respondWithError(w, http.StatusBadRequest, "Job ID is required")
		return
	}

	ctx := r.Context()

	job, err := s.Queue.GetJob(ctx, jobID)
	if err != nil || job == nil {
		s.respondWithError(w, http.StatusNotFound, "Job not found")
		return
	}

	if err := s.Queue.RetryJob(ctx, job); err != nil {
		s.respondWithError(w, http.StatusInternalServerError, "Failed to perform atomic retry")
		return
	}

	s.respondWithJSON(w, http.StatusOK, map[string]string{
		"message": "Job moved from DLQ to Critical queue",
		"job_id":  job.ID,
	})
}

func (s *JobServer) StreamStats(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Websocket upgrade error: %v", err)
		return
	}
	defer conn.Close()

	log.Println("Dashboard client connected via WebSocket")

	for {
		stats, err := s.Queue.Stats(r.Context())
		if err != nil {
			break
		}

		if err := conn.WriteJSON(stats); err != nil {
			break
		}

		time.Sleep(2 * time.Second)
	}
}

func (s *JobServer) respondWithError(w http.ResponseWriter, code int, message string) {
	s.respondWithJSON(w, code, map[string]string{"error": message})
}

func (s *JobServer) respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}
