# ADR 0002: Refresh Topic Metadata for Dynamic Tables

- Status: Accepted
- Date: 2026-10-18

## Context

The sink subscribes to topics using the `topics.regex` setting `cdc\\.public\\..*`. During the dynamic-table integration test, Debezium created a new table topic after the sink task had started. The source published a valid change event and the sink task remained `RUNNING`, but the sink did not discover the new topic within the test timeout. A delayed metadata refresh was visible in the Connect consumer logs. The existing-topic CDC test continued to pass.

## Decision

Set `CONNECT_CONSUMER_METADATA_MAX_AGE_MS=5000` on the Kafka Connect worker so its Kafka consumer refreshes topic metadata every five seconds. Keep regex-based subscription to support tables created after connector startup.

## Alternatives Considered

- Increase the integration-test timeout: not selected as the primary fix because it would hide slow topic discovery rather than improve it.
- Pre-create or manually subscribe to a topic for every table: not selected because it defeats the dynamic-table behavior this POC is intended to demonstrate.
- Restart the sink whenever a source table is added: not selected because it adds an operational step to a dynamic workflow.

## Consequences

- Newly created topics are discovered promptly enough for the dynamic-table CDC test; the focused test passed after applying the setting, and the complete CDC test file passed.
- The Connect worker performs more frequent metadata refreshes than the default. This is a small operational trade-off for this single-node POC; production tuning should consider cluster size and topic count.
- This setting controls consumer metadata refresh behavior in Kafka Connect. It does not change Debezium capture or PostgreSQL publication behavior.
