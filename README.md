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

- **`cmd/`**: Entrypoints for executables (`cmd/api` for REST API, `cmd/worker` for asynchronous queue/stream workers).
- **`internal/domain/`**: Pure Go domain models, value objects, domain errors, and repository interfaces. Has **zero** external dependencies on HTTP routers, SQL drivers, or messaging brokers.
  - `product/`: Product canonical entities, lifecycle status, decoupled price/inventory entities.
  - `source/`: Supplier and catalogue ingestion source definitions and configurations.
  - `compliance/`: HS codes, effective-dated tariff rules, sanctions, and compliance determinations.
- **`internal/application/`**: Use case orchestrators coordinating domain operations and calling domain repository ports.
- **`internal/infrastructure/`**: Secondary / Driven adapters implementing domain repository ports (`postgres`, `outbox`, etc.).
- **`internal/adapters/`**: Primary / Driving adapters (e.g. `http` router, middleware, health endpoints).

## Tooling & Commands

### Prerequisites

- Go 1.23+
- PostgreSQL 16+ (with `uuid-ossp` extension)
- Redis 7+ (Streams)

### Common Commands

```bash
# Run automated tests
make test

# Run tests with race detection
make test-race

# Build executables into bin/
make build

# Run linting
make lint

# Start API server
make run-api

# Start background worker
make run-worker
```
