# Architecture Decision Records

This directory stores Architecture Decision Records (ADRs): concise, versioned notes about significant technical choices and their consequences.

## Writing an ADR

- Add a file named `NNNN-short-title.md`, using the next available number.
- Copy the structure from [template.md](template.md).
- Record one decision per ADR. Capture the context and constraints, the chosen option, alternatives considered, and consequences, including trade-offs.
- Use `Proposed` while a decision is under discussion, `Accepted` once adopted, and `Superseded` or `Deprecated` when it no longer applies. Link a superseding ADR when relevant.
- Keep accepted ADRs as historical records. When a decision changes, add a new ADR instead of rewriting the old rationale.
- Link to the ADR from relevant process or design documentation when useful.

## Index

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-postgres-debezium-kafka-postgres.md) | PostgreSQL CDC through Debezium and Kafka | Accepted |
| [0002](0002-refresh-topic-metadata-for-dynamic-tables.md) | Refresh topic metadata for dynamic tables | Accepted |
