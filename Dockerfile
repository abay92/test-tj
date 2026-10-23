# syntax=docker/dockerfile:1

# ============================================================
# Stage 1: Build binary Go
# ============================================================
FROM golang:1.27-alpine AS builder

WORKDIR /src
ARG TARGETARCH

# Layer cache: dependency di-download dulu sebelum copy source
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# lib/pq & paho mqtt pure-Go => CGO tidak diperlukan
RUN CGO_ENABLED=0 GOOS=linux GOARCH="${TARGETARCH:-$(go env GOARCH)}" \
    go build -trimpath -ldflags="-s -w" -o /out/server .

# ============================================================
# Stage 2: Runtime (image kecil, non-root)
# ============================================================
FROM alpine:3.22

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S app \
    && adduser -S -G app -h /app app

ENV TZ=Asia/Jakarta \
    APP_PORT=8080 \
    GIN_MODE=release

WORKDIR /app
COPY --from=builder /out/server /app/server

USER app
EXPOSE 8080

# Endpoint /health memeriksa PostgreSQL, RabbitMQ, dan MQTT
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD wget -q -T 9 -O - "http://127.0.0.1:${APP_PORT}/health" > /dev/null || exit 1

ENTRYPOINT ["/app/server"]
