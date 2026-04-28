# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder

RUN apk add --no-cache ca-certificates git

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
	-trimpath \
	-ldflags="-w -s" \
	-o /out/cfg-distributor ./cmd/distributor

FROM alpine:3.22.2

RUN apk add --no-cache ca-certificates wget

WORKDIR /app

COPY --from=builder /out/cfg-distributor ./cfg-distributor
COPY config.yaml ./config.yaml

RUN addgroup -g 1000 appuser && \
	adduser -D -u 1000 -G appuser appuser && \
	chown -R appuser:appuser /app

USER appuser
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
	CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["./cfg-distributor"]
CMD ["config.yaml"]
