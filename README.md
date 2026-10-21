# PostgreSQL CDC and Kafka POC

A local proof of concept for capturing PostgreSQL row changes with Debezium, streaming them through Kafka, and applying them to a target PostgreSQL database.

```text
PostgreSQL source -> Debezium source connector -> Kafka -> JDBC sink connector -> PostgreSQL target
```

The project demonstrates insert, update, and delete replication, dynamic table capture, connector recovery, event inspection, and basic operational monitoring. It is a local learning and interview-demo environment, not a production deployment.

## Prerequisites

- Docker Engine with Docker Compose v2
- GNU Make
- Python 3.12 or compatible
- Git

## Quick Start

1. Create your local environment file and set local-only passwords:

   ```bash
   cp .env.example .env
   ```

2. Start the stack, install the Python dependencies, apply the source timestamp migration, and register the connectors:

   ```bash
   make up
   ```

3. Check service and connector status:

   ```bash
   make status
   ```

`make up` waits for services and registers or updates the source and sink connectors. The first startup may take a few minutes. The default source and target databases are available on `localhost:5433` and `localhost:5434`; Kafka Connect's REST API is on `http://localhost:8083`.

## Watch and Generate Events

Start the visual event feed in one terminal after the stack is running:

```bash
make event-ui
```

Open [http://localhost:8001](http://localhost:8001). The feed is a host-side Python process, not a Compose service. It shows recent Kafka events, filters by source table and operation, and displays each record's key, offset, and before/after images. Leave its terminal running; stop it with `Ctrl+C`.

Generate committed source database changes from another terminal:

```bash
make generate-events
```

By default, this generates 12 rows in `customers`, with a one-second pause between changes. Each row produces an insert and update, then a delete. Customize the table, count, and pause:

```bash
make generate-events TABLE=customers EVENT_COUNT=3 EVENT_INTERVAL=2
```

Generated rows are deleted by default. For details on type support, table requirements, and retaining rows with `--keep`, see [the CDC process guide](docs/cdc-process.md#generate-source-events).

## Useful Commands

| Command | Purpose |
| --- | --- |
| `make help` | List available Make targets |
| `make up` | Start services and register connectors |
| `make status` | Show service and connector status |
| `make event-ui` | Start the visual event feed |
| `make generate-events TABLE=customers EVENT_COUNT=3` | Generate source-side changes |
| `make trace TABLE=customers` | Print raw Kafka records |
| `make observability` | Start Prometheus and Grafana |
| `make test` | Run the CDC end-to-end tests |
| `make benchmark` | Measure a local replication batch |
| `make down` | Stop Compose services and retain persisted data |
| `make clean` | Remove Compose services and local persisted data |

The event feed process is separate from Compose and must be stopped separately. `make down` retains database and Kafka data; `make clean` deletes it.

## Repository Map

- `connect/`: Debezium source and JDBC sink connector configurations
- `source/`: Source database initialization, timestamp triggers, and table template
- `scripts/`: Connector setup, event generator, visual feed server, metrics exporter, and benchmark
- `web/`: Event feed page
- `monitoring/`: Prometheus rules and Grafana provisioning/dashboard
- `tests/`: CDC end-to-end and event-generator tests
- `docs/`: Process guide, benchmark notes, interview preparation, and architecture decision records

## Configuration and Data

Copy `.env.example` to `.env` and use local-only credentials. `.env` is ignored by Git. Do not expose the development services publicly or use the example credentials outside a local demo.

PostgreSQL source/target data and Kafka data persist in local directories across `docker compose down`. `make clean` removes those directories and their data.

## Scope and Limitations

This POC uses a single-node local Kafka broker and a local PostgreSQL sink. The sink can create tables for captured topics, but that does not mean arbitrary source DDL is replicated. The event generator creates demo values from PostgreSQL column metadata; it cannot infer application-specific foreign-key relationships or arbitrary check constraints. Review complex table constraints before generating rows.

For full setup, operations, CDC semantics, troubleshooting, and testing, see [docs/cdc-process.md](docs/cdc-process.md). Architecture decisions are documented in [docs/adr/README.md](docs/adr/README.md).
