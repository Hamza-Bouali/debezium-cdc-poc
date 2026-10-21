# CDC POC Process

## Purpose and Scope

This POC demonstrates PostgreSQL change data capture through Kafka:

```text
PostgreSQL source -> Debezium PostgreSQL source connector -> Kafka
                                                        -> Debezium JDBC sink connector -> PostgreSQL target
```

It verifies row-level `INSERT`, `UPDATE`, and `DELETE` replication, plus automatic creation of target tables for newly captured source tables. It is a local development demonstration, not a production deployment. The Kafka broker is single-node, credentials are demonstration values, and the JDBC sink's basic schema evolution does not replicate arbitrary source DDL.

## Prerequisites

- Docker Engine and Docker Compose
- GNU Make
- Python 3.12 or a compatible version
- `curl`
- A local `.env` file created from `.env.example`

## Start and Operate with Make

Create the ignored local environment file and set local-only passwords before starting the stack:

```bash
cp .env.example .env
```

Then use Make to install the Python dependencies, validate Compose, wait for healthy services, and create or update both connectors:

```bash
make up
make status
```

`make down` stops the containers while retaining local data. `make clean` permanently removes the containers and local persisted data directories; use it only when that data can be discarded. Run `make help` to see all targets.

## Start the Stack

The `make up` target starts the services with `docker compose up --wait`. For manual startup:

```bash
docker compose up -d
```

The stack includes PostgreSQL source and target databases, Kafka, Kafka Connect, and pgAdmin. Database data and Kafka state are stored in local bind-mounted directories ignored by Git. The first startup may take a little time while containers initialize.

`make up` also applies `source/updated_at.sql` to the source database. It sets an insert default and a `BEFORE UPDATE` trigger on each public table present at migration time that has an `updated_at` column. The migration is safe to rerun. If you later add another table with this column, run `make migrate-source` to install the trigger for it.

Check service state and wait until Connect responds:

```bash
docker compose ps
curl --retry 30 --retry-connrefused --retry-delay 2 --fail http://localhost:8083/connectors
```

## Register Connectors

`make up` registers both connectors idempotently: it creates missing connectors and updates existing ones from the checked-in configuration files. To register them manually instead:

```bash
curl -i -X POST \
  -H 'Content-Type: application/json' \
  --data @connect/source-connector.json \
  http://localhost:8083/connectors

curl -i -X POST \
  -H 'Content-Type: application/json' \
  --data @connect/sink-connector.json \
  http://localhost:8083/connectors
```

A successful registration returns HTTP 201. If a connector is already registered, Kafka Connect returns HTTP 409; inspect its configuration or update it with `PUT /connectors/{name}/config` instead of repeatedly posting it.

Check connector and task status:

```bash
curl --fail http://localhost:8083/connectors/postgres-source/status
curl --fail http://localhost:8083/connectors/sink-connector/status
```

Both connector state and each task state should be `RUNNING`.

## Run the End-to-End Tests

Create and activate a virtual environment, then install dependencies and run the tests:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
pytest -s -v tests/test_cdc_e2e.py
```

The tests use Docker containers and the local Connect REST API. They verify replication of row changes, dynamic source-table creation, and recovery after Kafka Connect restarts while a source change is made. The dynamic-table test creates a uniquely named table, waits for its target copy, exercises changes, and removes the test tables afterward.

Run only the recovery test with `make test-recovery`.

## Observe the Data Path

Debezium names source topics using the configured topic prefix, schema, and table, for example `cdc.public.customers`. The sink subscribes to `cdc.public.*`, removes that prefix with `RegexRouter`, and writes each topic to a correspondingly named table in the target database.

List topics and inspect connector logs with:

```bash
docker exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker logs --follow --timestamps kafka-connect
```

To inspect up to ten raw Debezium records, including timestamp, partition, offset, and key:

```bash
make trace TABLE=customers
```

The output value is the Debezium change event. Its `payload.op` identifies the operation (`c` create/insert, `u` update, `d` delete, or `r` snapshot read); `payload.before` and `payload.after` show row state when present. The Kafka key identifies the row, and the record metadata identifies its topic, partition, offset, and timestamp. The connector configurations do not enable a heartbeat or transaction metadata stream, so this trace shows row changes, not complete transaction-level lineage.

### Generate Source Events

The event generator makes real, committed changes in the source PostgreSQL database. It inspects the selected table's columns and PostgreSQL types, discovers its primary key, and then generates an insert followed by an update and (by default) a delete for each row. These changes flow through Debezium and Kafka like application writes; they are not synthetic records written directly to Kafka.

Start the complete stack first so the source table is captured by the source connector:

```bash
make up
```

Generate events for the default `customers` table:

```bash
make generate-events
```

The Make target defaults to 12 rows and a one-second pause between changes. Choose a table, row count, and interval with Make variables:

```bash
make generate-events TABLE=customers EVENT_COUNT=3 EVENT_INTERVAL=2
```

The script can also be run directly. Its standalone defaults are three rows and a one-second interval. `--keep` leaves generated rows in the source and target instead of emitting the final delete:

```bash
python3 scripts/generate_cdc_events.py --table customers --count 2 --interval 1 --keep
```

Supported generated values include text and character strings, integer and numeric types, booleans, UUIDs, JSON/JSONB, dates, times, timestamps, intervals, bytea, arrays, network address types, XML, and PostgreSQL enums. Identity, generated, and defaulted columns are omitted from inserts. Nullable columns with unsupported types are set to `NULL`; an unsupported required type stops generation before any rows are inserted. Each generated row uses the table's primary key to target its update and delete. A primary key and at least one writable non-key column are required.

This is a demo-data generator, not a general-purpose schema faker. It does not infer foreign-key relationships or arbitrary `CHECK` constraints, and unique values in application-specific columns may need custom handling. Review a table's constraints before generating data; use a disposable local database for tables with complex business rules. Generated demo strings include a random run ID; rows are deleted by default.

### Visual Event Feed

The browser feed is served by `scripts/event_feed_server.py` on the host; it is not a Docker Compose service. It reads the latest records from Kafka through the local Docker CLI. Start Kafka (or the full stack) and then run:

```bash
make event-ui
```

Open [http://localhost:8001](http://localhost:8001). The Make target stays in the foreground while serving the page, so leave that terminal running. Stop the UI with `Ctrl+C`; this does not stop the Compose services. Set another port if needed:

```bash
make event-ui EVENT_UI_PORT=8002
```

The page provides a source-table selector, operation filter, five-second auto-refresh, and a newest-first timeline. Select an event to inspect its key, Kafka partition and offset, before/after row images, and full Debezium record. Create/update/delete map to Debezium operations `c`, `u`, and `d`; snapshot reads use `r`; a Kafka null-value record following a delete is shown as a tombstone. The current page's table selector lists `customers` and `orders`; add an option in `web/event-feed.html` when introducing another table. An API request for a table must match its `cdc.public.<table>` Kafka topic.

For a complete demo, use separate terminals after `make up`:

```bash
make event-ui
```

```bash
make generate-events TABLE=customers EVENT_COUNT=3 EVENT_INTERVAL=2
```

The feed should show the generated changes as they arrive. `make down` stops the Compose stack; the foreground event-feed process should be stopped separately with `Ctrl+C`.

## Measure a Batch

Run a local insert-and-convergence measurement:

```bash
make benchmark
make benchmark BENCH_ROWS=10000
```

The script reports end-to-end batch latency and effective rows per second, then removes the generated source and target rows. Record the row count, host resources, Docker versions, and environment when comparing runs. This is a simple local POC measurement, not a controlled benchmark or a production capacity claim. CI checks correctness but does not gate on benchmark timing.

## Continuous Integration

GitHub Actions validates the Compose model and runs the end-to-end suite on pushes and pull requests. It uses the `.env.example` local-demo values and reports Compose logs if the job fails. The workflow needs a Linux runner with Docker Compose available.

The Connect worker sets `CONNECT_CONSUMER_METADATA_MAX_AGE_MS=5000` so the sink consumer refreshes topic metadata frequently enough to discover tables created after the sink starts. This setting was verified by the dynamic-table integration test.

## Stop the Stack

Stop containers while retaining local database and Kafka state:

```bash
docker compose down
```

The ignored bind-mounted data directories remain on disk and will be reused next time. Removing those directories permanently deletes local POC data; do so deliberately and only when it is no longer needed.

## Configuration and Security Notes

Connector credentials and pgAdmin credentials are loaded from the ignored `.env` file. The values in `.env.example` are local-demo placeholders, not production secrets. Replace them for local use and never expose these unauthenticated development services publicly. For a shared or deployed environment, use a secret manager, restrict network access, and design availability, monitoring, recovery, retention, and throughput requirements before relying on the pipeline.

Architecture decisions and their rationale are recorded in [the ADR directory](adr/README.md).
