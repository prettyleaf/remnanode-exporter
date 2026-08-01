BIN     := remnanode-exporter
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build test lint fmt e2e dashboards up down logs clean

build:
	CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o bin/$(BIN) ./cmd/$(BIN)

test:
	go test ./...

fmt:
	gofmt -w .

lint:
	go vet ./...

# Runs the schema, pipeline and every dashboard query against a real server.
# Start one first, e.g.:
#   docker run --rm -d -p 9000:9000 --name ch clickhouse/clickhouse-server:24.12-alpine
e2e:
	CLICKHOUSE_TEST_ADDR=$${CLICKHOUSE_TEST_ADDR:-127.0.0.1:9000} go test -v ./internal/e2e/...

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f exporter

clean:
	rm -rf bin
