package worker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Vaaibhaaav/GORO/internal/queue"
)

type WorkerPool struct {
	workers   int
	queue     queue.Queue
	processor queue.JobProcessor
	wg        sync.WaitGroup
}

func NewWorkerPool(workers int, q queue.Queue, p queue.JobProcessor) *WorkerPool {
	return &WorkerPool{
		workers:   workers,
		queue:     q,
		processor: p,
	}
}

func (p *WorkerPool) Start(ctx context.Context) {
	r := NewRunner(p.queue, p.processor)

	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.RunWorker(ctx, r)
	}
}

func (p *WorkerPool) RunWorker(ctx context.Context, r *Runner) {
	defer p.wg.Done()
	fmt.Println("--- MILESTONE 1: Goroutine Started ---")
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fmt.Println("--- MILESTONE 2: Attempting Pop ---")
			job, err := p.queue.Pop(ctx)
			if err != nil {
				fmt.Printf("DEBUG: Pop Error: %v\n", err)
				if ctx.Err() != nil {
					return
				}
				log.Printf("Queue Pop error: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}

			if job != nil {
				fmt.Printf("DEBUG: Job Found! Calling Execute for ID: %s\n", job.ID) // ADD THIS
				r.Execute(ctx, job)
			} else {
				fmt.Println("DEBUG: Pop returned nil job") // ADD THIS
			}
		}
	}
}

func (p *WorkerPool) Stop() {
	p.wg.Wait()
}
