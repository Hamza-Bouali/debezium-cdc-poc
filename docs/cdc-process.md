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
- Python 3.12 or a compatible version
- `curl`
- The Python dependencies in `requirements.txt`

## Start the Stack

From the repository root, start the services:

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

Register the source and sink connectors from their checked-in configuration files:

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

The tests use Docker containers and the local Connect REST API. They verify replication of row changes and dynamic source-table creation. The dynamic-table test creates a uniquely named table, waits for its target copy, exercises changes, and removes the test tables afterward.

## Observe the Data Path

Debezium names source topics using the configured topic prefix, schema, and table, for example `cdc.public.customers`. The sink subscribes to `cdc.public.*`, removes that prefix with `RegexRouter`, and writes each topic to a correspondingly named table in the target database.

List topics and inspect connector logs with:

```bash
docker exec kafka /kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker logs --follow --timestamps kafka-connect
```

The Connect worker sets `CONNECT_CONSUMER_METADATA_MAX_AGE_MS=5000` so the sink consumer refreshes topic metadata frequently enough to discover tables created after the sink starts. This setting was verified by the dynamic-table integration test.

## Stop the Stack

Stop containers while retaining local database and Kafka state:

```bash
docker compose down
```

The ignored bind-mounted data directories remain on disk and will be reused next time. Removing those directories permanently deletes local POC data; do so deliberately and only when it is no longer needed.

## Configuration and Security Notes

The checked-in connector JSON and Compose file use local demonstration credentials. Do not reuse these credentials or expose the services publicly. For a shared or deployed environment, move secrets out of source control, restrict network access, and design availability, monitoring, recovery, retention, and throughput requirements before relying on the pipeline.

Architecture decisions and their rationale are recorded in [the ADR directory](adr/README.md).
