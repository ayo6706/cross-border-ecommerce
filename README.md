# Cross-Border Commerce Backend

Backend services for cross-border e-commerce: supplier catalogue ingestion, change detection, event
publication, and versioned regulatory data (tariffs, restrictions, permits, sanctions) for customs
compliance. Written in Go on PostgreSQL 16 and Redis 7.

## Features

- **Catalogue ingestion** from supplier REST APIs and CSV/NDJSON feeds, with resumable checkpoints,
  raw-payload provenance and a per-run error budget.
- **Change detection**: products are normalized and fingerprinted, so downstream work runs only for
  records that actually changed. Prices and inventory are tracked separately from product identity.
- **Reliable events**: state changes and their events commit in one transaction (transactional
  outbox); a relay publishes them to Redis Streams with retries, backoff and a replayable dead-letter queue.
- **Regulatory datasets**: every load is an immutable, effective-dated version that is reviewed by a
  second person before activation, superseded by later versions, and checked against a refresh SLA.
- **Tariff resolution**: the duty in force for an HS code, origin and destination on a transaction
  date, with HS-prefix fallback, origin-specific rates and stacked additional duties. Missing,
  ambiguous or unsupported data is reported as a HOLD reason, never as a zero rate.
- **Catalogue API** with keyset pagination.

## Architecture

The code follows a hexagonal (ports and adapters) layout; dependencies point inward to the domain.

```text
cmd/, internal/adapters/        entrypoints, HTTP handlers, supplier adapters
        ↓
internal/application/           use cases and ports
        ↓
internal/domain/                entities, value objects, domain errors (no I/O dependencies)
        ↑
internal/infrastructure/        PostgreSQL (pgx + sqlc) and Redis implementations of the ports
```

| Path | Contents |
|---|---|
| `cmd/api` | HTTP API |
| `cmd/worker` | Background worker: processes finished ingestion runs and relays the outbox |
| `cmd/ingest` | Ingestion CLI |
| `cmd/regload` | Regulatory dataset CLI |
| `cmd/migrate` | Schema migrations |
| `internal/domain` | `product`, `source`, `ingestion`, `regulatory` |
| `internal/platform` | Configuration, logging, secrets, backoff, worker pool |
| `migrations` | Numbered SQL migrations (up and down) |
| `tests/performance` | Query-plan suite (build tag `perf`) |

Money, duty and tax values use `github.com/shopspring/decimal`, never floating point. Regulatory
dates are calendar dates, and lookups use the UTC date of the transaction.

## Getting Started

### Prerequisites

- Go (version in `go.mod`)
- Docker, for local PostgreSQL and Redis
- `golangci-lint` (version in `.golangci-lint-version`) and `sqlc`, for development

### Run locally

```bash
make db-up          # PostgreSQL on localhost:5433 (crossborder_dev, crossborder_test), Redis on localhost:6379
make migrate-up     # apply migrations to crossborder_dev
make run-api        # http://localhost:8080
```

In another terminal:

```bash
export REDIS_URL=redis://localhost:6379/0
make run-worker
```

The Makefile sets `DATABASE_URL` for the local dev database; override it for any other database.

## Configuration

All configuration comes from environment variables and is validated at startup. Every invalid
variable is reported together, and an invalid value is never replaced by its default.

### Required

| Variable | Used by |
|---|---|
| `DATABASE_URL` | All binaries |
| `REDIS_URL` | `worker` |
| `REGULATORY_SLA_<CATEGORY>` | `regload` (see [Regulatory data](#regulatory-data)) |

### Optional

| Variable | Default | Notes |
|---|---|---|
| `PORT` | `8080` | API listen port |
| `SERVER_READ_TIMEOUT` / `SERVER_WRITE_TIMEOUT` | `10s` | |
| `SERVER_IDLE_TIMEOUT` | `60s` | |
| `SERVER_SHUTDOWN_TIMEOUT` | `15s` | |
| `DB_MAX_CONNS` | `25` | Per process. The sum across processes must stay below PostgreSQL's `max_connections` |
| `DB_MIN_CONNS` | `5` | Must be ≤ `DB_MAX_CONNS` |
| `DB_MAX_CONN_IDLE_TIME` | `15m` | |
| `DB_MAX_CONN_LIFETIME` | `1h` | |
| `DB_CONNECT_TIMEOUT` | `5s` | Connection attempts and the startup ping |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | `json` | `json` or `text` |
| `LOG_ADD_SOURCE` | `false` | Add file and line to each record |
| `OUTBOX_BATCH_SIZE` | `100` | 1–1000 |
| `OUTBOX_POLL_INTERVAL` | `500ms` | Poll interval when the outbox is empty |
| `OUTBOX_LEASE` | `30s` | Claim lease |
| `OUTBOX_BASE_BACKOFF` / `OUTBOX_MAX_BACKOFF` | `1s` / `5m` | Retry backoff |
| `OUTBOX_MAX_ATTEMPTS` | `10` | Attempts before an event is marked `FAILED` |
| `STREAM_RETENTION` | `168h` | Stream trimming cutoff |
| `STREAM_CONSUMER_BLOCK` | `2s` | `XREADGROUP` block timeout |
| `STREAM_CONSUMER_BATCH` | `10` | Messages per read, max 1000 |
| `STREAM_CLAIM_MIN_IDLE` | `60s` | Idle time before `XAUTOCLAIM`; must exceed the worst-case retry window |
| `STREAM_CLAIM_INTERVAL` | `10s` | Interval between reclaim sweeps |
| `STREAM_HANDLER_TIMEOUT` | `5s` | Per-message handler timeout |
| `STREAM_RETRY_MAX_ATTEMPTS` | `5` | Handler attempts before dead-lettering |
| `STREAM_RETRY_BASE_BACKOFF` / `STREAM_RETRY_MAX_BACKOFF` | `200ms` / `2s` | |
| `WORKER_CONCURRENCY` | `10` | Stream consumer pool size |
| `WORKER_QUEUE_SIZE` | `10` | |
| `WORKER_DRAIN_TIMEOUT` | `10s` | Graceful shutdown drain |
| `RUN_PROCESSING_POLL_INTERVAL` | `2s` | How often the worker claims a finished run |
| `RUN_PROCESSING_LEASE` | `30s` | Renewed per page |
| `RUN_PROCESSING_BATCH_SIZE` | `500` | Raw records per page |
| `INGESTION_ERROR_BUDGET_MAX_RATE` | `0.05` | Share of rows that may fail normalization; > 0 and < 1 |
| `INGESTION_ERROR_BUDGET_MIN_ROWS` | `100` | Rows seen before the budget applies |
| `IDEMPOTENCY_LEASE_TTL` | `30s` | Must be greater than `STREAM_HANDLER_TIMEOUT` |

The stream consumer settings (`STREAM_CONSUMER_*`, `STREAM_CLAIM_*`, `STREAM_HANDLER_TIMEOUT`,
`STREAM_RETRY_*`, `WORKER_*`, `IDEMPOTENCY_LEASE_TTL`) are validated at startup, but no binary
starts the consumer yet.

`cmd/migrate` reads only `DATABASE_URL` (or `-database-url`) and the `DB_*` variables.

Supplier credentials are never stored in source configuration. A source references a secret, for
example `"auth_kind": "bearer", "auth_ref": "env:SUPPLIER_TOKEN"`, and the value is read from the
environment when the adapter is built.

## Usage

### HTTP API

| Method and path | Description |
|---|---|
| `GET /health/live` | Liveness |
| `GET /health/ready` | Readiness (database reachable) |
| `GET /v1/products?limit=&cursor=` | Products, newest first: `{"items": [...], "next_cursor": "..." \| null}` |
| `GET /v1/products/{id}` | One product; `404` if unknown |
| `POST /v1/dlq/{id}/replay` | Replay a dead-lettered message |

`limit` defaults to 50 and must be between 1 and 500. `next_cursor` is opaque: pass it back
unchanged as `cursor`. It is `null` on the last page. An out-of-range limit or an unknown cursor
returns `400`.

```bash
curl 'http://localhost:8080/v1/products?limit=100'
curl "http://localhost:8080/v1/products?limit=100&cursor=$NEXT_CURSOR"
```

### Ingestion

```bash
# Process one finished run now (lease and batch size default to RUN_PROCESSING_*)
go run ./cmd/ingest process --run <run-id> [--from-start] [--lease 45s] [--batch-size 250]
```

### Regulatory data

Each load is stored as a new dataset version. Curated datasets must be reviewed by someone other
than the loader before activation, and activating a version supersedes the previous version from
the same source.

```bash
go run ./cmd/regload load-curated --file ng.csv --jurisdiction NG --category IMPORT_RESTRICTION \
  --source ng_prohibition_list --version 2026-09 --fetched-at 2026-09-29T08:00:00Z \
  --licence "Public sector information" --attribution "Nigeria Customs Service" --loaded-by alice
go run ./cmd/regload review --dataset <id> --reviewer bob --note "checked against the gazette"
go run ./cmd/regload activate --dataset <id>
go run ./cmd/regload reject --dataset <id> --reason "partial upstream file"
go run ./cmd/regload coverage --jurisdiction NG --category IMPORT_RESTRICTION [--at 2026-07-01T00:00:00Z]
```

`coverage` prints the datasets in force. When none is in force, or one is older than its refresh
SLA, it prints the HOLD reason (`NO_REGULATORY_COVERAGE` or `STALE_REGULATORY_DATA`) and exits
non-zero.

Refresh SLAs are required, one per category, with no defaults:

| Variable | Suggested value |
|---|---|
| `REGULATORY_SLA_SANCTIONS` | `24h` |
| `REGULATORY_SLA_TARIFF` | `168h` |
| `REGULATORY_SLA_EXPORT_CONTROL` | `168h` |
| `REGULATORY_SLA_IMPORT_RESTRICTION` | `720h` |
| `REGULATORY_SLA_PERMIT` | `720h` |
| `REGULATORY_SLA_PREFERENTIAL_AGREEMENT` | `720h` |

### Migrations

```bash
go run ./cmd/migrate [-database-url URL] up | down [steps] | version
```

## Development

```bash
make build              # binaries in bin/
make lint               # golangci-lint
make sqlc-generate      # regenerate internal/infrastructure/postgres/generated
make test               # unit tests; live tests skip without TEST_DATABASE_URL / TEST_REDIS_URL
make test-integration   # unit and live PostgreSQL/Redis tests with -race
make verify             # every CI gate (see below)
make perf               # query-plan suite
```

`make verify` runs `scripts/verify.sh`, the same script CI runs: gofmt, go vet, staticcheck,
golangci-lint, a sqlc diff, and the full test suite against live PostgreSQL and Redis. It starts
throwaway containers when `TEST_DATABASE_URL` and `TEST_REDIS_URL` are unset (ports
`VERIFY_PG_PORT`, default `55432`, and `VERIFY_REDIS_PORT`, default `56379`). Any skipped test fails
the run. Set `VERIFY_RACE=1` to add `-race`, which needs CGO.

`make perf` seeds 100,000–200,000 rows per large table into `TEST_DATABASE_URL` and **truncates it
first**, so point it at a throwaway database. It fails when a statement sent by the worker, relay,
API or tariff resolver reads a large table without an index. Timings are logged, not asserted.
