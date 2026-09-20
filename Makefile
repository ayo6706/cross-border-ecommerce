.PHONY: all build test test-race lint clean run-api run-worker

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOMOD=$(GOCMD) mod
BINARY_DIR=bin
API_BINARY=$(BINARY_DIR)/api
WORKER_BINARY=$(BINARY_DIR)/worker

all: test build

build:
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) -o $(API_BINARY) ./cmd/api
	$(GOBUILD) -o $(WORKER_BINARY) ./cmd/worker

test:
	$(GOTEST) -v ./...

test-race:
	$(GOTEST) -v -race ./...

lint:
	golangci-lint run ./...

clean:
	@rm -rf $(BINARY_DIR) coverage.out coverage.html

run-api:
	$(GOCMD) run ./cmd/api

run-worker:
	$(GOCMD) run ./cmd/worker

tidy:
	$(GOMOD) tidy
