# ADR-003: Replication Topology

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/03-consensus-replication.md

## Bağlam

Consensus algoritması seçildikten sonra log nasıl dağıtılacak? Tek global log mu, topic başına grup mu, partition'lı mı? Quorum parametreleri (W/R/N) ne olmalı? Topology kararı latency, throughput ve availability'yi doğrudan etkiler.

## Port

- **Port Interface:** `internal/port/replication.go: Replicator` — one interface, one file location (future `internal/port/replication.go`)
- **Signature:** `type Replicator interface { Replicate(topic string, e Event) error; Committed(topic string) Offset; Topology() string }`
- **Scope:**
  - Owns: log distribution strategy, committed offset tracking, topology descriptor
  - NOT owns: consensus algorithm term/quorum (ADR-002 `Consensus` port), WAL fsync (ADR-001 `WAL` port)
- **Adapters:** Single global log, Per-topic group, Partitioned hash, Quorum W2R2N3

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| Single global log | En basit, total order garantisi | Tek log bottleneck, topic izolasyonu yok | 1-2 hafta | Az topic, basitlik öncelikliyse | global-log |
| Per-topic group | Topic izolasyonu, bağımsız quorum | Çok topic'te çok grup overhead'i | 2-3 hafta | Topic başına farklı quorum/retention isteniyorsa | per-topic |
| Partitioned hash | Paralel yazma, yüksek throughput, Kafka benzeri | Ordering sadece partition içinde, rebalancing karmaşık | 3-4 hafta | Yüksek throughput, key-based ordering isteniyorsa | partitioned |
| Quorum W2R2N3 | Tunable consistency (W+R>N), esnek | Tuning karmaşık, latency/availability tradeoff | 2 hafta | Tunable quorum ve farklı W/R profilleri isteniyorsa | quorum-tunable |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- Kafka partition design
- Dynamo quorum (W+R>N)
- `internal/broker/topic.go:17` — mevcut single partition model
