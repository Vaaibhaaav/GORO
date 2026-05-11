package queue

import (
	"context"
)

type JobProcessor interface {
	Process(ctx context.Context, job *Job) error
}
