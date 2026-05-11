FROM golang:1.26.2-alpine AS builder
WORKDIR /app

RUN apk add --no-cache gcc musl-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -ldflags="-s -w" -o goro-server ./cmd/server/main.go
RUN go build -ldflags="-s -w" -o goro-worker ./cmd/worker/main.go

FROM alpine:3.19
WORKDIR /root/

COPY --from=builder /app/goro-server .
COPY --from=builder /app/goro-worker .

COPY --from=builder /app/static ./static

EXPOSE 8080 8081