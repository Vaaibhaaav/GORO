package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Vaaibhaaav/GORO/internal/metrics"
	"github.com/redis/go-redis/v9"
)

type Queue interface {
	Push(ctx context.Context, job *Job) error
	Pop(ctx context.Context) (*Job, error)
	GetJob(ctx context.Context, id string) (*Job, error)
	UpdateStatus(ctx context.Context, job *Job) error
	MoveToDLQ(ctx context.Context, job *Job) error
	GetDLQ(ctx context.Context) ([]*Job, error)
	RemoveFromDLQ(ctx context.Context, job *Job) error
	Stats(ctx context.Context) (*QueueStats, error)
	RetryJob(ctx context.Context, job *Job) error
	SetActiveWorkerCount(ctx context.Context, number int64) error
}

type QueueStats struct {
	Critical      int64   `json:"critical"`
	High          int64   `json:"high"`
	Default       int64   `json:"default"`
	Low           int64   `json:"low"`
	DLQ           int64   `json:"dlq"`
	ActiveWorkers float64 `json:"active_workers"`
	RecentJobs    []Job   `json:"recent_jobs"`
}

type RedisQueue struct {
	client       *redis.Client
	priorityKeys []string
}

func NewRedisQueue() (*RedisQueue, error) {
	REDIS_URL := os.Getenv("REDIS_URL")
	if REDIS_URL == "" {
		panic("REDIS_URL is not set")
	}

	opts, err := redis.ParseURL(REDIS_URL)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse REDIS_URL: %v", err))
	}

	poolSizeStr := os.Getenv("REDIS_POOL_SIZE")
	poolSize, _ := strconv.Atoi(poolSizeStr)
	if poolSize <= 0 {
		poolSize = 20
	}
	opts.PoolSize = poolSize
	opts.MinIdleConns = 5
	opts.ConnMaxIdleTime = 5 * time.Minute

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		panic(fmt.Sprintf("Failed to connect to Redis: %v", err))
	}

	keys := make([]string, len(priorityOrder))
	for i, p := range priorityOrder {
		keys[i] = "queue:" + string(p)
	}

	return &RedisQueue{
		client:       client,
		priorityKeys: keys,
	}, nil
}

func (r *RedisQueue) Pop(ctx context.Context) (*Job, error) {
	result, err := r.client.BLPop(ctx, 0, r.priorityKeys...).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	jobID := result[1]
	return r.GetJob(ctx, jobID)
}

func (r *RedisQueue) Push(ctx context.Context, job *Job) error {
	if job.ID == "" {
		return fmt.Errorf("job id is required")
	}

	job.Status = StatusPending
	job.CreatedAt = time.Now()
	job.UpdatedAt = time.Now()

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	pipe := r.client.TxPipeline()
	// Store full data in a keyed string
	pipe.Set(ctx, "job:"+job.ID, data, 7*24*time.Hour)
	// Push only ID to the queue
	pipe.RPush(ctx, "queue:"+string(job.Priority), job.ID)
	// Push ID to history for the dashboard
	pipe.LPush(ctx, "goro:history", job.ID)
	pipe.LTrim(ctx, "goro:history", 0, 49)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis pipe exec: %w", err)
	}

	metrics.JobsEnqueued.WithLabelValues(string(job.Priority), job.Type).Inc()
	return nil
}

func (r *RedisQueue) GetJob(ctx context.Context, id string) (*Job, error) {
	data, err := r.client.Get(ctx, "job:"+id).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var job Job
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (r *RedisQueue) UpdateStatus(ctx context.Context, job *Job) error {
	job.UpdatedAt = time.Now()
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	err = r.client.SetXX(ctx, "job:"+job.ID, data, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to update job status: %w", err)
	}
	return nil
}

func (r *RedisQueue) MoveToDLQ(ctx context.Context, job *Job) error {
	job.Status = StatusFailed
	job.UpdatedAt = time.Now()
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal error: %w", err)
	}

	pipe := r.client.TxPipeline()
	pipe.Set(ctx, "job:"+job.ID, data, 0)
	pipe.RPush(ctx, "queue:dlq", job.ID) // Store ID in DLQ

	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisQueue) GetDLQ(ctx context.Context) ([]*Job, error) {
	ids, err := r.client.LRange(ctx, "queue:dlq", 0, -1).Result()
	if err != nil {
		return nil, err
	}

	jobs := make([]*Job, 0)
	if len(ids) == 0 {
		return jobs, nil
	}

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = "job:" + id
	}

	blobs, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	for _, b := range blobs {
		if b == nil {
			continue
		}
		var j Job
		json.Unmarshal([]byte(b.(string)), &j)
		jobs = append(jobs, &j)
	}
	return jobs, nil
}

func (r *RedisQueue) RemoveFromDLQ(ctx context.Context, job *Job) error {
	return r.client.LRem(ctx, "queue:dlq", 0, job.ID).Err()
}
func (r *RedisQueue) SetActiveWorkerCount(ctx context.Context, number int64) error {
	return r.client.Set(ctx, "queue:stats:active_workers", number, 30*time.Second).Err()
}

func (r *RedisQueue) RetryJob(ctx context.Context, job *Job) error {
	err := r.client.LRem(ctx, "queue:dlq", 0, job.ID).Err()
	if err != nil {
		return err
	}
	return r.client.LPush(ctx, "queue:critical", job.ID).Err()
}

func (r *RedisQueue) Stats(ctx context.Context) (*QueueStats, error) {
	stats := &QueueStats{
		RecentJobs: make([]Job, 0),
	}

	pipe := r.client.Pipeline()
	critCmd := pipe.LLen(ctx, "queue:critical")
	highCmd := pipe.LLen(ctx, "queue:high")
	defCmd := pipe.LLen(ctx, "queue:default")
	lowCmd := pipe.LLen(ctx, "queue:low")
	dlqCmd := pipe.LLen(ctx, "queue:dlq")
	historyCmd := pipe.LRange(ctx, "goro:history", 0, 9)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("pipeline error: %w", err)
	}

	stats.Critical = critCmd.Val()
	stats.High = highCmd.Val()
	stats.Default = defCmd.Val()
	stats.Low = lowCmd.Val()
	stats.DLQ = dlqCmd.Val()

	jobIDs := historyCmd.Val()
	if len(jobIDs) > 0 {
		keys := make([]string, len(jobIDs))
		for i, id := range jobIDs {
			keys[i] = "job:" + id
		}

		jobBlobs, err := r.client.MGet(ctx, keys...).Result()
		if err == nil {
			for _, blob := range jobBlobs {
				if blob == nil {
					continue
				}
				var j Job
				if err := json.Unmarshal([]byte(blob.(string)), &j); err == nil {
					stats.RecentJobs = append(stats.RecentJobs, j)
				}
			}
		}
	}

	val, err := r.client.Get(ctx, "queue:stats:active_workers").Result()
	if err == nil {
		count, _ := strconv.ParseFloat(val, 64)
		stats.ActiveWorkers = count
	} else {
		stats.ActiveWorkers = 0 
	}
	return stats, nil
}
