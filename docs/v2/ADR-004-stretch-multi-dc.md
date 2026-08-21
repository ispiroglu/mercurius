# ADR-004: Stretch — Multi-DC Gateway

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/04-stretch-multi-dc.md

## Bağlam

v1 tek DC. RD-04 multi-DC stretch için gateway, async ship/ingest, failover (promote/demote) ve lag takibi gerekiyor. Active-passive mi active-active mi? Vector clock gerekli mi? Intra-DC quorum ile inter-DC replikasyon ayrımı nasıl?

## Port

- **Port Interface:** `internal/port/stretch.go: StretchGateway` — one interface, one file location (future `internal/port/stretch.go`)
- **Signature:** `type StretchGateway interface { Ship(dc string, batch []Event) error; Ingest(batch []Event) error; Promote() error; Demote() error; Lag(dc string) time.Duration }`
- **Scope:**
  - Owns: inter-DC ship/ingest, failover promote/demote, lag monitoring
  - NOT owns: intra-DC quorum/consensus (ADR-002 `Consensus` port), WAL (ADR-001 `WAL` port)
- **Adapters:** Active-passive async tailer, Active-active vector clock, Quorum per-DC+async, Log shipping vs Raft ship

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| Active-passive async tailer | Basit, tek writer, conflict yok | Passive DC stale, failover'da lag | 3-4 hafta | Basit DR, tek aktif DC yeterliyse | async-tailer |
| Active-active vector clock | Her DC yazabilir, düşük latency | Conflict resolution karmaşık, vector clock overhead | 6-8 hafta | Her DC'de yazma, düşük latency isteniyorsa | active-active |
| Quorum per-DC+async | Her DC içinde quorum, DC'ler arası async | DC sayısı arttıkça quorum hesabı karmaşık | 4-5 hafta | Her DC güçlü consistency + async stretch isteniyorsa | per-dc-quorum |
| Log shipping vs Raft ship | Raft log'u direkt ship — tutarlı, basit | Raft coupling yüksek, WAL formatına bağımlı | 3 hafta | Raft zaten seçildiyse log'u reuse etmek isteniyorsa | raft-log-ship |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- Kafka MirrorMaker 2
- CockroachDB multi-region
- Dynamo global tables
