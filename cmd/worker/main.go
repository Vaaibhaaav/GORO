package main

import (
	"context"
	// "errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Vaaibhaaav/GORO/internal/queue"
	"github.com/Vaaibhaaav/GORO/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MyEmailTask struct {
	APIKey string
}

func (m *MyEmailTask) Process(ctx context.Context, job *queue.Job) error {

	fmt.Printf("[Worker] Processing Job %s (Type: %s) using API Key: %s\n", job.ID, job.Type, m.APIKey)
	time.Sleep(2 * time.Second)
	// return errors.New("boom")
	return nil
}

func main() {
	workerCountStr := os.Getenv("WORKER_COUNT")
	workerCount, err := strconv.Atoi(workerCountStr)
	if err != nil || workerCount <= 0 {
		log.Printf("Invalid worker count, defaulting to 5")
		workerCount = 5
	}

	port := ":" + os.Getenv("METRICS_PORT")
	if port == "" {
		port = ":8081" // Fallback
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		fmt.Printf("Server started running at 8081")
		log.Fatal(http.ListenAndServe(port, nil))
	}()

	q, err := queue.NewRedisQueue()
	if err != nil {
		log.Fatalf("Failed to initialize the queue: %v", err)
	}

	processor := &MyEmailTask{APIKey: "string123"}

	ctx, cancel := context.WithCancel(context.Background())

	pool := worker.NewWorkerPool(workerCount, q, processor)
	pool.Start(ctx)
	fmt.Printf("GORO Engine: %d Workers Started\n", workerCount)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	<-quit

	fmt.Println("\nShutdown signal received. Stopping Pool...")

	cancel()
	waitChan := make(chan struct{})
	go func() {
		pool.Stop()
		close(waitChan)
	}()

	select {
	case <-waitChan:
		fmt.Println("Shutdown Complete: All workers finished cleanly.")
	case <-time.After(30 * time.Second):
		fmt.Println("Shutdown Timeout: Forcing exit.")
	}
}
