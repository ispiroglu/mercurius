# Mercurius v2 Dokümantasyonu

> Sıfırdan rewrite. Hazır kütüphane yok. Kararlar ADR ile loglanır.

| # | Doküman | Durum | Açıklama |
|---|---------|-------|----------|
| 01 | [Konsept Haritası](01-concepts-overview.md) | ✅ hazır | Ne nerede lazım — harita |
| 02 | [Persistence](02-persistence.md) | ⏳ yazılıyor | WAL, LSM, segment, modern alternatifler |
| 03 | [Consensus & Replication](03-consensus-replication.md) | ⏳ yazılıyor | Raft/Paxos/Zab, quorum, from-scratch maliyet |
| 04 | [Stretch Multi-DC](04-stretch-multi-dc.md) | ⏳ yazılıyor | Active-passive/active-active, gateway |
| 05 | [Operability](05-operability.md) | ⏳ yazılıyor | Helm, StatefulSet, observability |
| 06 | [Delivery Guarantees](06-delivery-guarantees.md) | ⏳ yazılıyor | Exactly-once, DLQ, consumer group, tracking |
| 07 | [Testing Strategy](07-testing-strategy.md) | ⏳ sonra | İnanılmaz testsuite |
| ADR | [ADR Template](ADR-000-template.md) | ✅ hazır | Karar şablonu |
| PORTS | [PORTS — Hexagonal Map](PORTS.md) | ✅ hazır | Her ADR bir port — traceable map |

## Karar Durumu

| ADR | Port | Konu | İlgili Doküman | Durum |
|-----|------|------|----------------|-------|
| [ADR-001](ADR-001-persistence-wal.md) | WAL | Persistence — WAL | 02-persistence.md | `proposed` |
| [ADR-002](ADR-002-consensus-algorithm.md) | Consensus | Consensus algoritması | 03-consensus-replication.md | `proposed` |
| [ADR-003](ADR-003-replication-topology.md) | Replicator | Replication topology | 03-consensus-replication.md | `proposed` |
| [ADR-004](ADR-004-stretch-multi-dc.md) | StretchGateway | Stretch — Multi-DC gateway | 04-stretch-multi-dc.md | `proposed` |
| [ADR-005](ADR-005-wire-protocol.md) | Codec | Wire protocol — Codec | 03-consensus-replication.md | `proposed` |
| [ADR-006](ADR-006-transport-rpc.md) | Transport | Transport — RPC | 03-consensus-replication.md | `proposed` |
| [ADR-007](ADR-007-topic-partitioning.md) | TopicCatalog | Topic & Partitioning | 02-persistence.md | `proposed` |
| [ADR-008](ADR-008-delivery-semantics.md) | DeliveryGuarantee | Delivery semantics | 06-delivery-guarantees.md | `proposed` |
| [ADR-009](ADR-009-consumer-model.md) | ConsumerGroup | Consumer model | 06-delivery-guarantees.md | `proposed` |
| [ADR-010](ADR-010-retry-dlq-tracking.md) | ConsumeTracker | Retry / DLQ / Tracking | 06-delivery-guarantees.md | `proposed` |
| [ADR-011](ADR-011-security.md) | Authenticator | Security — Auth & TLS | 05-operability.md | `proposed` |
| [ADR-012](ADR-012-operability.md) | ConfigSource | Operability — Config & Deploy | 05-operability.md | `proposed` |
| [ADR-013](ADR-013-observability.md) | Metrics | Observability | 05-operability.md | `proposed` |
| [ADR-014](ADR-014-testing-strategy.md) | TestCluster | Testing strategy | 07-testing-strategy.md | `proposed` |

Hepsi `proposed` — sen araştırmadan sonra `accepted` yapacaksın.

## Nasıl Okunur?

1. `01` ile başla — harita
2. `02`→`06` her biri bağımsız, istediğin sıradan oku
3. Her dokümanın sonundaki **Karar Bekliyor** tablosunu doldur
4. Her ADR bir port'tur — önce `PORTS.md` oku, sonra ADR'de Port/Alternatifler/Adapter tablosunu oku.

## Kod Silme Planı

Dokümanlar `accepted` olduktan sonra:

```bash
git checkout v2
git rm -r internal/ pkg/ proto/  # v1 kod tamamen silinir
mkdir -p internal/{wal,raft,stretch,gateway,store,consume}
# from-scratch iskelet kurulur
```

Şu an kod **silinmedi** — dokümanlar bitene kadar v1 referans olarak duruyor.
