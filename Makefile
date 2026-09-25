.PHONY: all build test test-race test-integration lint clean run-api run-worker sqlc-generate sqlc-verify db-up db-down migrate-up migrate-down tidy verify

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
BINARY_DIR=bin
API_BINARY=$(BINARY_DIR)/api
WORKER_BINARY=$(BINARY_DIR)/worker
MIGRATE_BINARY=$(BINARY_DIR)/migrate

# Local databases from docker-compose.yml. Override by exporting DATABASE_URL / TEST_DATABASE_URL / TEST_REDIS_URL.
DATABASE_URL ?= postgres://postgres:postgrespassword@localhost:5433/crossborder_dev?sslmode=disable
TEST_DATABASE_URL ?= postgres://postgres:postgrespassword@localhost:5433/crossborder_test?sslmode=disable
TEST_REDIS_URL ?= redis://localhost:6379/15
export DATABASE_URL

all: test build

build:
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) -o $(API_BINARY) ./cmd/api
	$(GOBUILD) -o $(WORKER_BINARY) ./cmd/worker
	$(GOBUILD) -o $(MIGRATE_BINARY) ./cmd/migrate

test:
	$(GOTEST) -v ./...

test-race:
	$(GOTEST) -v -race ./...

test-integration:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" TEST_REDIS_URL="$(TEST_REDIS_URL)" $(GOTEST) -v -race -count=1 -p 1 ./... # -p 1: packages share one test DB

# Every mandatory gate; starts a throwaway PostgreSQL when TEST_DATABASE_URL is unset.
verify:
	./scripts/verify.sh

db-up:
	docker compose up -d

db-down:
	docker compose down

migrate-up:
	$(GOCMD) run ./cmd/migrate up

migrate-down:
	$(GOCMD) run ./cmd/migrate down

lint:
	golangci-lint run ./...

clean:
	@rm -rf $(BINARY_DIR) coverage.out coverage.html api.exe worker.exe migrate.exe

run-api:
	$(GOCMD) run ./cmd/api

run-worker:
	$(GOCMD) run ./cmd/worker

tidy:
	$(GOMOD) tidy

sqlc-generate:
	sqlc generate

sqlc-verify:
	sqlc compile
