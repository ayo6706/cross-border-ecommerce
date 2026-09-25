# Cross-Border Commerce Backend

A high-throughput cross-border catalog ingestion, compliance, and landed-cost calculation platform.

## Architecture

This service is structured using strict **Hexagonal / Clean Architecture (Ports & Adapters)**. All dependencies strictly flow inward toward the pure domain layer:

```text
Adapters (cmd/, internal/adapters/)
       ↓
Application Layer (internal/application/)
       ↓
Pure Domain Models & Ports (internal/domain/)
       ↑
Infrastructure Adapters (internal/infrastructure/)
```

### Layer Responsibilities

- **`cmd/`**: Entrypoints for executables (`cmd/api` REST API, `cmd/worker` background worker, `cmd/ingest` ingestion CLI, `cmd/migrate` schema migrations).
- **`internal/domain/`**: Pure Go domain models, value objects, domain errors, and repository interfaces. Has **zero** external dependencies on HTTP routers, SQL drivers, or messaging brokers.
  - `product/`: Product canonical entities and lifecycle status.
  - `source/`: Supplier and catalogue ingestion source definitions and configurations.
  - `ingestion/`: Ingestion runs, raw records, checkpoints, error budgets, and the source adapter port.
- **`internal/application/`**: Use case orchestrators coordinating domain operations and calling domain repository ports.
- **`internal/infrastructure/`**: Secondary / Driven adapters implementing domain and application ports (`postgres` repositories and the transaction runner).
- **`internal/adapters/`**: Primary / Driving adapters (`httpapi` router, middleware, health endpoints; `sources` REST and feed adapters).

## Tooling & Commands

### Prerequisites

- Go (version from `go.mod`)
- Docker (for the local PostgreSQL 16 from `docker-compose.yml`)

### Configuration

`DATABASE_URL` is required for all database-backed services.
`REDIS_URL` is required by the background worker (`cmd/worker`) for publishing outbox events to Redis Streams.

Outbox Relay Tuning Variables (with defaults):
- `OUTBOX_BATCH_SIZE`: Batch size for outbox claim query (default: `100`, range 1–1000).
- `OUTBOX_POLL_INTERVAL`: Polling interval when backlog is empty (default: `500ms`).
- `OUTBOX_LEASE`: Claim lease duration (default: `30s`).
- `OUTBOX_BASE_BACKOFF`: Base retry backoff duration (default: `1s`).
- `OUTBOX_MAX_BACKOFF`: Maximum retry backoff duration (default: `5m`).
- `OUTBOX_MAX_ATTEMPTS`: Max retry attempts before marking an event as `FAILED` (default: `10`).

Stream Broker Tuning Variables (with defaults):
- `STREAM_RETENTION`: Time-based retention cutoff for stream trimming (default: `168h` / 7 days).

Stream Consumer & Worker Pool Tuning Variables (with defaults):
- `STREAM_CONSUMER_BLOCK`: XREADGROUP block timeout duration (default: `2s`).
- `STREAM_CONSUMER_BATCH`: Maximum number of messages read per batch from Redis Stream (default: `10`, max 1000).
- `STREAM_CLAIM_MIN_IDLE`: Minimum idle duration before reclaiming pending messages with XAUTOCLAIM (default: `30s`, must be > `STREAM_HANDLER_TIMEOUT`).
- `STREAM_CLAIM_INTERVAL`: Periodic interval between XAUTOCLAIM sweeps (default: `10s`).
- `STREAM_HANDLER_TIMEOUT`: Maximum execution time allowed per message handler (default: `5s`).
- `WORKER_CONCURRENCY`: Fixed number of concurrent worker goroutines in the pool (default: `10`, must be <= 80% of `DB_MAX_CONNS`).
- `WORKER_QUEUE_SIZE`: Buffer capacity of the worker task queue (default: `10`).
- `WORKER_DRAIN_TIMEOUT`: Graceful shutdown drain timeout before cancelling in-flight tasks (default: `10s`).

Idempotency Tuning Variables (with defaults):
- `IDEMPOTENCY_LEASE_TTL`: Execution lease TTL for in-flight stream handlers (default: `30s`, must be strictly > `STREAM_HANDLER_TIMEOUT`).

Source credentials are never stored in source config. API sources reference
secrets instead, e.g. `"auth_kind": "bearer", "auth_ref": "env:SUPPLIER_TOKEN"`,
and the value is read from the environment when the adapter is built.

### Common Commands

```bash
# Start local PostgreSQL (dev + test databases)
make db-up

# Apply migrations to the dev database
make migrate-up

# Unit tests (integration tests skip without TEST_DATABASE_URL)
make test

# Unit + PostgreSQL integration tests with the race detector (same as CI)
make test-integration

# Build executables into bin/
make build

# Start API server / background worker
make run-api
make run-worker
```
