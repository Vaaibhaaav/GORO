package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

type JobReq struct {
	Type     string         `json:"type"`
	Priority string         `json:"priority"`
	Payload  map[string]any `json:"payload"`
}

func main() {
	apiUrl := "http://localhost:8080/jobs"
	var wg sync.WaitGroup

	// Priorities to test
	tests := []struct {
		priority string
		count    int
	}{
		{"low", 5},
		{"default", 5},
		{"high", 5},
		{"critical", 5},
	}

	fmt.Println("🚀 Starting GORO Priority Stress Test...")

	for _, t := range tests {
		for i := 0; i < t.count; i++ {
			wg.Add(1)
			go func(p string, id int) {
				defer wg.Done()

				reqBody := JobReq{
					Type:     "test-task",
					Priority: p,
					Payload:  map[string]any{"test_id": id, "sleep": 2},
				}
				
				body, _ := json.Marshal(reqBody)
				resp, err := http.Post(apiUrl, "application/json", bytes.NewBuffer(body))
				
				if err != nil {
					fmt.Printf("❌ Failed to push %s: %v\n", p, err)
					return
				}
				defer resp.Body.Close()
			}(t.priority, i)
		}
	}

	wg.Wait()
	fmt.Println("\n✅ All 20 jobs enqueued. Watch your Dashboard now!")
}