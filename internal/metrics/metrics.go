package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

var (
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goro_jobs_enqueued_total",
		Help: "Total jobs pushed to the queue.",
	}, []string{"priority", "type"})
	JobsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goro_jobs_processed_total",
		Help: "Total jobs processed by goro",
	}, []string{"type"})
	JobsDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goro_jobs_duration_seconds",
		Help:    "Time taken to process each job",
		Buckets: prometheus.DefBuckets,
	}, []string{"type"})
	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goro_active_workers",
		Help: "Number of workers currently processing jobs",
	})
	DLQCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "goro_dlq_total",
		Help: "Total jobs moved to dead letter queue",
	})
)

func GetActiveWorkerCount() float64 {
	var m io_prometheus_client.Metric
	if err := ActiveWorkers.Write(&m); err != nil {
		return 0
	}
	return m.GetGauge().GetValue()
}
