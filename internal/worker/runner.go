package worker

import (
	"context"
	"fmt"
	"log"
	"time" // Added time import

	"github.com/Vaaibhaaav/GORO/internal/metrics"
	"github.com/Vaaibhaaav/GORO/internal/queue"
	"github.com/Vaaibhaaav/GORO/internal/retry"
)

type Runner struct {
	q         queue.Queue
	processor queue.JobProcessor
}

func NewRunner(q queue.Queue, p queue.JobProcessor) *Runner {
	return &Runner{
		q:         q,
		processor: p,
	}
}

func (r *Runner) Execute(ctx context.Context, job *queue.Job) {
	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec() 
	start := time.Now()
	workerCount := metrics.GetActiveWorkerCount()
	r.q.SetActiveWorkerCount(ctx, int64(workerCount))

	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("CRITICAL: Panic in job %s: %v", job.ID, rec)
			job.Error = fmt.Sprintf("Panic: %v", rec)
			_ = r.q.MoveToDLQ(context.Background(), job)

			metrics.JobsProcessed.WithLabelValues(job.Type).Inc()
		}
	}()

	job.Status = queue.StatusProcessing
	if err := r.q.UpdateStatus(ctx, job); err != nil {
		log.Printf("Failed to set processing status for job %s: %v", job.ID, err)
		return
	}

	err := r.processor.Process(ctx, job)

	if err != nil {
		r.handleFailure(ctx, job, &r.q, err)
	} else {
		r.handleSuccess(ctx, job)
	}

	duration := time.Since(start).Seconds()
	metrics.JobsProcessed.WithLabelValues(job.Type).Inc()
	metrics.JobsDuration.WithLabelValues(job.Type).Observe(duration)
	workerCount = metrics.GetActiveWorkerCount()
	r.q.SetActiveWorkerCount(ctx, int64(workerCount))
}

func (r *Runner) handleSuccess(ctx context.Context, job *queue.Job) {
	job.Status = queue.StatusDone
	if err := r.q.UpdateStatus(ctx, job); err != nil {
		log.Printf("Final status update failed for job %s: %v", job.ID, err)
	}
}

func (r *Runner) handleFailure(ctx context.Context, job *queue.Job, q *queue.Queue, err error) {
	retry.Handle(ctx, job, *q, err)
}
