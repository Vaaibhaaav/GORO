package handlers

import (
	"context"

	"github.com/Vaaibhaaav/GORO/internal/queue"
)

type HandlerFunc func(ctx context.Context, job *queue.Job) error

var registry = map[string]HandlerFunc{}

func Register(jobType string, fn HandlerFunc) {
	registry[jobType] = fn
}

func GetJob(jobType string) (HandlerFunc, bool) {
	fn, ok := registry[jobType]
	return fn, ok
}
