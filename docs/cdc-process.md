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
