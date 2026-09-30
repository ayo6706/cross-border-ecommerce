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

- **`cmd/`**: Entrypoints for executables (`cmd/api` REST API, `cmd/worker` background worker, `cmd/ingest` ingestion CLI, `cmd/regload` regulatory dataset CLI, `cmd/migrate` schema migrations).
- **`internal/domain/`**: Pure Go domain models, value objects, domain errors, and repository interfaces. Has **zero** external dependencies on HTTP routers, SQL drivers, or messaging brokers.
  - `product/`: Product canonical entities and lifecycle status.
  - `source/`: Supplier and catalogue ingestion source definitions and configurations.
  - `ingestion/`: Ingestion runs, raw records, checkpoints, error budgets, and the source adapter port.
  - `regulatory/`: Regulatory dataset versions (review, activation, supersession), coverage and refresh SLAs.
- **`internal/application/`**: Use case orchestrators coordinating domain operations and calling domain repository ports.
- **`internal/infrastructure/`**: Secondary / Driven adapters implementing domain and application ports (`postgres` repositories and the transaction runner).
- **`internal/adapters/`**: Primary / Driving adapters (`httpapi` router, middleware, health, DLQ replay and catalogue endpoints; `sources` REST and feed adapters).

## Tooling & Commands

### Prerequisites

- Go (version from `go.mod`)
- Docker (for the local PostgreSQL 16 from `docker-compose.yml`)

### Configuration

`DATABASE_URL` is required for all database-backed services.
`REDIS_URL` is required by the background worker (`cmd/worker`) for publishing outbox events to Redis Streams.
Every variable is read once by `internal/platform/config`; a malformed or out-of-range value fails
startup (all bad variables are reported together), it is never replaced by the default.

Database Pool Variables (with defaults), used by every binary (`api`, `worker`, `ingest`, `migrate`):
- `DB_MAX_CONNS`: Pool size per process (default: `25`). Each process opens its own pool, so the sum
  over all running processes must stay below PostgreSQL `max_connections` (100 by default) minus
  its reserved slots. The worker runs two loops (run processing, outbox relay); the API holds one
  connection per in-flight query. These defaults are not yet sized against measured load.
- `DB_MIN_CONNS`: Idle connections kept open (default: `5`, must be <= `DB_MAX_CONNS`).
- `DB_MAX_CONN_IDLE_TIME`: Idle connection lifetime (default: `15m`).
- `DB_MAX_CONN_LIFETIME`: Connection lifetime (default: `1h`).
- `DB_CONNECT_TIMEOUT`: Bound on every connection attempt and the startup ping (default: `5s`).

Logging Variables (with defaults):
- `LOG_LEVEL`: `debug`, `info`, `warn` or `error` (default: `info`); any other value fails startup.
- `LOG_FORMAT`: `json` or `text` (default: `json`).
- `LOG_ADD_SOURCE`: Add the source file and line to each record (default: `false`).

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
- `STREAM_CLAIM_MIN_IDLE`: Minimum idle duration before reclaiming pending messages with XAUTOCLAIM (default: `60s`, must exceed the worst-case retry window: `STREAM_RETRY_MAX_ATTEMPTS` handler timeouts plus the backoffs between them).
- `STREAM_CLAIM_INTERVAL`: Periodic interval between XAUTOCLAIM sweeps (default: `10s`).
- `STREAM_HANDLER_TIMEOUT`: Maximum execution time allowed per message handler (default: `5s`).
- `WORKER_CONCURRENCY`: Fixed number of concurrent worker goroutines in the pool (default: `10`). Used by the stream consumer, which no binary starts yet (ENG-025).
- `WORKER_QUEUE_SIZE`: Buffer capacity of the worker task queue (default: `10`).
- `WORKER_DRAIN_TIMEOUT`: Graceful shutdown drain timeout before cancelling in-flight tasks (default: `10s`).

Run Processing Variables (with defaults), used by the worker loop and `cmd/ingest`:
- `RUN_PROCESSING_POLL_INTERVAL`: How often the worker claims the next finished run (default: `2s`).
- `RUN_PROCESSING_LEASE`: Claim lease, renewed per page (default: `30s`); default of `ingest process --lease`.
- `RUN_PROCESSING_BATCH_SIZE`: Raw records per page (default: `500`); default of `ingest process --batch-size`.
- `INGESTION_ERROR_BUDGET_MAX_RATE`: Share of rows a run may fail to normalize before the run is failed (default: `0.05`, must be > 0 and < 1).
- `INGESTION_ERROR_BUDGET_MIN_ROWS`: Rows seen before the budget is enforced (default: `100`, must be > 0). An explicit value outside these bounds fails startup; it is never clamped.

Idempotency Tuning Variables (with defaults):
- `IDEMPOTENCY_LEASE_TTL`: Execution lease TTL for in-flight stream handlers (default: `30s`, must be strictly > `STREAM_HANDLER_TIMEOUT`).

Regulatory Refresh SLAs (required, no defaults), used only by `cmd/regload`: the maximum age of a
dataset's fetched data before coverage reports `STALE_REGULATORY_DATA` (ADR 0013). A missing,
malformed or non-positive value fails startup. Suggested values:
- `REGULATORY_SLA_SANCTIONS`: `24h`
- `REGULATORY_SLA_TARIFF`, `REGULATORY_SLA_EXPORT_CONTROL`: `168h`
- `REGULATORY_SLA_IMPORT_RESTRICTION`, `REGULATORY_SLA_PERMIT`, `REGULATORY_SLA_PREFERENTIAL_AGREEMENT`: `720h`

Source credentials are never stored in source config. API sources reference
secrets instead, e.g. `"auth_kind": "bearer", "auth_ref": "env:SUPPLIER_TOKEN"`,
and the value is read from the environment when the adapter is built.

### Common Commands

```bash
# Start local PostgreSQL and Redis (dev + test databases)
make db-up

# Apply migrations to the dev database
make migrate-up

# golangci-lint (version pinned in .golangci-lint-version; includes funlen/gocognit/dupl)
make lint

# Unit tests (integration tests skip without TEST_DATABASE_URL)
make test

# Unit + PostgreSQL & Redis live integration tests with the race detector
make test-integration

# Every gate, the same script CI runs: gofmt, go vet, staticcheck (go.mod tool), golangci-lint,
# sqlc diff, tests against live PostgreSQL + Redis; any skipped test fails. VERIFY_RACE=1 adds -race.
make verify

# Query-plan suite (separate CI job): seeds ~100k-200k rows per large table into TEST_DATABASE_URL,
# which it TRUNCATES, and fails when a statement the worker, relay or API sends reads a large table
# without an index. Timings are logged, not asserted (ADR 0010).
make perf

# Process one finished run now (lease/batch default to RUN_PROCESSING_*). migrate reads only
# DATABASE_URL (or -database-url) and DB_*, logs plain text (LOG_* do not apply), and has no
# deadline: the deploy job owns the timeout
go run ./cmd/ingest process --run <run-id> [--from-start] [--lease 45s] [--batch-size 250]
go run ./cmd/migrate [-database-url URL] up | down [steps] | version

# Regulatory datasets (ADR 0013): load a curated CSV as a LOADED version, sign it off (the reviewer
# must not be the loader), activate it (supersedes the previous version of the same source), and
# check coverage; not covered or stale prints the HOLD reason and exits non-zero
go run ./cmd/regload load-curated --file ng.csv --jurisdiction NG --category IMPORT_RESTRICTION \
  --source ng_prohibition_list --version 2026-09 --fetched-at 2026-09-29T08:00:00Z \
  --licence "Public sector information" --attribution "Nigeria Customs Service" --loaded-by alice
go run ./cmd/regload review --dataset <id> --reviewer bob --note "checked against the gazette"
go run ./cmd/regload activate --dataset <id>
go run ./cmd/regload reject --dataset <id> --reason "partial upstream file"
go run ./cmd/regload coverage --jurisdiction NG --category IMPORT_RESTRICTION [--at 2026-07-01T00:00:00Z]

# Build executables into bin/
make build

# Start API server / background worker
make run-api
make run-worker
```

### Catalogue API

| Route | Response |
|---|---|
| `GET /v1/products?limit=&cursor=` | `{"items": [...], "next_cursor": "..." \| null}`, newest first (`created_at DESC, id DESC`) |
| `GET /v1/products/{id}` | one product; `404` if unknown |

`limit` defaults to 50 and must be 1..500. `next_cursor` is opaque: pass it back unchanged as
`cursor` to get the next page; it is `null` on the last page. A limit out of range or a cursor
that was not issued by the API is `400`. Pages are keyset seeks, so page depth does not slow them
down (ADR 0010).

```bash
curl 'http://localhost:8080/v1/products?limit=100'
curl "http://localhost:8080/v1/products?limit=100&cursor=$NEXT_CURSOR"
```
