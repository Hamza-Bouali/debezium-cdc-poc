# ADR 0001: PostgreSQL CDC Through Debezium and Kafka

- Status: Accepted
- Date: 2026-10-18

## Context

The POC needs to capture changes from a PostgreSQL source and apply them to a separate PostgreSQL target. The pipeline should demonstrate row-level inserts, updates, deletes, and capture of tables added after connector startup, while exposing Kafka as the event-streaming layer between source and target.

## Decision

Use the Debezium PostgreSQL source connector with PostgreSQL logical decoding (`pgoutput`) to publish change events to Kafka. Use the Debezium JDBC sink connector to consume matching topics and upsert/delete records in the target PostgreSQL database.

Topics follow the `cdc.<schema>.<table>` pattern. The sink subscribes to `cdc.public.*` and applies `RegexRouter` to map each table topic to a target table name. Enable basic sink schema evolution so a target table can be created from the incoming record schema.

## Alternatives Considered

- Direct application-level dual writes: not selected because it couples the application write path to replication and does not demonstrate Kafka streaming.
- A custom Kafka consumer and database writer: not selected because the JDBC sink already provides the required POC behavior with less custom code.
- A different event transport or target database: not selected because PostgreSQL on both ends keeps this POC focused on CDC and Kafka behavior.

## Consequences

- The pipeline is asynchronous and Kafka provides a durable intermediate event stream.
- The source must use logical replication, and the Debezium publication must include the tables to be captured.
- The JDBC sink's basic schema evolution creates/adjusts tables from record schemas; it does not replicate arbitrary source DDL such as table drops or all schema-management operations.
- The current Compose topology is a local POC with one Kafka broker and demonstration credentials. It does not establish production availability, security, recovery, or throughput characteristics.
- Connector offsets and local database/Kafka state are persisted in the development environment, so reruns may reuse existing state.
