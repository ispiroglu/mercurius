# ADR Index

> Her ADR bir port'tur — `PORTS.md` ile birlikte okunur. Template: ADR-000-template.md

| ADR | Port | Durum | Özet |
|-----|------|-------|------|
| ADR-001 | WAL | `proposed` | topic offset'a fsync'li append/replay |
| ADR-002 | Consensus | `proposed` | term/leader/quorum — Raft vs Paxos vs Zab |
| ADR-003 | Replicator | `proposed` | log distribution — global vs per-topic vs partitioned vs quorum |
| ADR-004 | StretchGateway | `proposed` | inter-DC ship/ingest/failover — active-passive vs active-active |
| ADR-005 | Codec | `proposed` | Event serialization — protobuf vs flatbuffers vs gob breaking fields |
| ADR-006 | Transport | `proposed` | RPC framing — gRPC stream vs bidi vs QUIC vs raw TCP vs WebSocket |
| ADR-007 | TopicCatalog | `proposed` | topic lifecycle & partition assignment — single vs hash vs range vs dynamic |
| ADR-008 | DeliveryGuarantee | `proposed` | guarantee level & dedup — at-most vs at-least vs exactly-once |
| ADR-009 | ConsumerGroup | `proposed` | group membership & assignment — fan-out vs group vs exclusive vs push/pull |
| ADR-010 | ConsumeTracker | `proposed` | ack/nack/dlq/trace — memory vs WAL vs Postgres vs SQLite vs DLQ topic |
| ADR-011 | Authenticator | `proposed` | authn/authz/TLS — mTLS vs JWT vs ACL vs SPIFFE |
| ADR-012 | ConfigSource | `proposed` | config & deploy — Helm StatefulSet vs Kustomize vs Operator vs systemd |
| ADR-013 | Metrics | `proposed` | metrics/traces/logs emission — OTEL vs Prom only vs Datadog |
| ADR-014 | TestCluster | `proposed` | test harness — testcontainers vs kind vs toxiproxy vs rapid vs buf |

---

- ADR-001 — WAL — `proposed` — topic offset'a fsync'li append/replay
- ADR-002 — Consensus — `proposed` — term/leader/quorum konsensus algoritması
- ADR-003 — Replicator — `proposed` — log replication topology
- ADR-004 — StretchGateway — `proposed` — multi-DC gateway ship/ingest/promote/demote
- ADR-005 — Codec — `proposed` — Event Marshal/Unmarshal ve breaking schema (offset/partition/producer_id/dc_id)
- ADR-006 — Transport — `proposed` — Publish/Subscribe/Retry RPC framing
- ADR-007 — TopicCatalog — `proposed` — Create/Get/List/PartitionFor topic catalog
- ADR-008 — DeliveryGuarantee — `proposed` — Guarantee() + Deduplicator Seen/Mark
- ADR-009 — ConsumerGroup — `proposed` — Join/Leave/Assignment/Next group semantics
- ADR-010 — ConsumeTracker — `proposed` — Ack/Nack/DLQ/Trace consume tracking
- ADR-011 — Authenticator — `proposed` — Authenticate/Authorize + TLSProvider
- ADR-012 — ConfigSource — `proposed` — Load/Watch config source ve Helm chart layout
- ADR-013 — Metrics — `proposed` — IncCounter/ObserveHistogram/SetGauge + Tracer Start
- ADR-014 — TestCluster — `proposed` — Leader/Kill/Partition/CorruptWAL test harness (test-only)
