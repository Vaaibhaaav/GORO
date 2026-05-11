package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/Vaaibhaaav/GORO/internal/api"
	"github.com/Vaaibhaaav/GORO/internal/queue"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func StructuredLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Str("remote_ip", r.RemoteAddr).
			Dur("latency", time.Since(start)).
			Msg("HTTP Request")
	})
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr}) // Makes logs readable in terminal

	q, err := queue.NewRedisQueue()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to connect to Redis")
	}

	jobServer := api.NewJobServer(q)
	r := chi.NewRouter()

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: true,
		MaxAge:           300,
	}))
	r.Use(StructuredLogger)

	r.Post("/jobs", jobServer.SubmitJobs)
	r.Get("/jobs/{id}", jobServer.GetJob)
	r.Get("/stats", jobServer.GetQueueStats)
	r.Get("/dlq", jobServer.GetDLQ)
	r.Post("/dlq/{id}/requeue", jobServer.RequeueDLQJob)
	r.Post("/jobs/{id}/retry", jobServer.RetryDLQJob)
	r.Get("/ws/stats", jobServer.StreamStats)
	r.Get("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./static/index.html")
	})
	r.Handle("/metrics", promhttp.Handler())

	port := ":" + os.Getenv("PORT")
	if port == "" {
		port = ":8080" // Fallback
	}
	fmt.Printf("GORO API is running on %s\n", port)

	server := &http.Server{
		Addr:         port,
		Handler:      r,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	if err := server.ListenAndServe(); err != nil {
		log.Fatal().Err(err).Msg("HTTP server failed")
	}
}
