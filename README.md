# ⚙️ GORO — Distributed Task Queue in Go

![Go Version](https://img.shields.io/badge/Go-1.22+-00ADD8?style=flat&logo=go)
![Redis](https://img.shields.io/badge/Redis-7.0+-DC382D?style=flat&logo=redis)
![Docker](https://img.shields.io/badge/Docker-Ready-2496ED?style=flat&logo=docker)
![License](https://img.shields.io/badge/License-MIT-green.svg)

**GORO** is a production-grade, horizontally scalable Distributed Task Queue built in Go. It allows applications to offload background work (like sending emails, image processing, and webhooks) to a pool of concurrent workers. 

Unlike simple FIFO lists, GORO is built for reliability and observability, featuring priority scheduling, smart retry logic, a Dead Letter Queue, and a real-time web dashboard.

---

## ✨ Key Features

* **🚦 4-Level Priority Scheduling:** Jobs are processed based on priority (`critical`, `high`, `default`, `low`). *Note: Low-priority jobs are guaranteed to run within a set timeframe via a starvation check.*
* **🛡️ Resilient Retry & DLQ:** Features exponential backoff for failed jobs before eventually moving them to an inspectable Dead Letter Queue (DLQ).
* **📊 Prometheus Observability:** Exposes a `/metrics` endpoint tracking job throughput, latency, and queue depth for full system visibility.
* **⚡ Real-Time Dashboard:** A live, dependency-free (Vanilla JS + HTML) WebSocket dashboard served directly by the Go server to monitor worker heartbeats and job events.

---

## 🏗️ Architecture

The system is split into two independently runnable binaries (API Server and Worker Pool) that communicate strictly through Redis, enabling seamless horizontal scaling.

```text
 ┌──────────────┐       HTTP POST /jobs        ┌─────────────────────┐
 │  Client App  │ ─────────────────────────►   │   API Server (Go)   │
 └──────────────┘                              │  • Validate payload │
                                               │  • Assign priority  │
                                               │  • Push to Redis    │
                                               └──────────┬──────────┘
                                                          │ ZADD / LPUSH
                                                          ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │                              Redis                                │
 │  queue:critical  │  queue:high  │  queue:default  │  queue:low    │
 │                       dlq (Dead Letter Queue)                     │
 └──────────────────────────────┬────────────────────────────────────┘
                                │ BRPOP (blocking pop)
                         ┌──────▼───────┐
                         │ Worker Pool  │
                         │ goroutine xN │
                         └──────┬───────┘
                                │
             success ◄──────────┴──────────► fail → retry → DLQ
