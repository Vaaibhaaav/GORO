package retry

import (
	"context"
	"math"
	"time"

	"github.com/Vaaibhaaav/GORO/internal/metrics"
	"github.com/Vaaibhaaav/GORO/internal/queue"
)

func Handle(ctx context.Context, job *queue.Job, q queue.Queue, err error) {
	job.Error = err.Error()
	job.Retries++
	if job.Retries >= job.MaxRetries {
		job.Status = queue.StatusDLQ
		q.MoveToDLQ(ctx, job)
		metrics.DLQCounter.Inc()
		q.UpdateStatus(ctx, job)
		return
	}

	delay := time.Duration(math.Pow(2, float64(job.Retries))) * time.Second
	job.Status = queue.StatusPending
	time.AfterFunc(delay, func() {
		q.Push(ctx, job)
	})
}
