package queue

import "time"

type Priority string

const (
	PriorityLow      Priority = "low"
	PriorityDefault  Priority = "default"
	PriorityHigh     Priority = "high"
	PriorityCritical Priority = "critical"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusProcessing Status = "processing"
	StatusDone       Status = "done"
	StatusFailed     Status = "failed"
	StatusDLQ        Status = "dlq"
)

type Job struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Payload    map[string]any `json:"payload"`
	Priority   Priority       `json:"priority"`
	Status     Status         `json:"status"`
	Retries    int            `json:"retries"`
	MaxRetries int            `json:"max_retries"`
	Error      string         `json:"error,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

var priorityOrder = []Priority{
	PriorityCritical,
	PriorityHigh,
	PriorityDefault,
	PriorityLow}
