# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o konsen ./cmd/main.go

# Runtime stage
FROM alpine:3.21

RUN apk add --no-cache ca-certificates curl && \
    addgroup -S konsen && adduser -S konsen -G konsen

COPY --from=builder /build/konsen /usr/local/bin/konsen

USER konsen

EXPOSE 10001 20001

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD curl -sf http://localhost:20001/health || exit 1

ENTRYPOINT ["konsen"]
