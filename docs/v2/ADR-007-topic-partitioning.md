# ADR-007: Topic & Partitioning — TopicCatalog

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/02-persistence.md

## Bağlam

v1 `internal/broker/topic.go:17` `Topic{SubscriberRepository, workerPool nil}` tek partition, static topic. Partition assignment, topic lifecycle (create/delete), key-based routing yok. Hangi partitioning stratejisi? Hash mi range mi dynamic mi?

## Port

- **Port Interface:** `internal/port/topic.go: TopicCatalog` — one interface, one file location (future `internal/port/topic.go`)
- **Signature:** `type TopicCatalog interface { Create(topic string) error; Get(topic string) (Topic, error); List() []string; PartitionFor(topic string, key string) uint32 }`
- **Scope:**
  - Owns: topic lifecycle, partition assignment, key→partition mapping
  - NOT owns: storage/WAL (ADR-001 `WAL` port), replication (ADR-003), delivery (ADR-008)
- **Adapters:** Single partition (v1 topic.go:17), Hash partition, Range partition, Dynamic auto-split

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| Single partition (v1 topic.go:17) | En basit, ordering garantisi | Throughput bottleneck, tek partition | 0 hafta (mevcut) | Az throughput, total order şartsa | single |
| Hash partition | Paralelism, Kafka benzeri, key affinity | Rebalancing, hot partition riski | 2 hafta | Key-based ordering + paralelism isteniyorsa | hash |
| Range partition | Range scan verimli, sıralı key'ler için iyi | Range boundary yönetimi karmaşık | 2-3 hafta | Sıralı key range query'leri isteniyorsa | range |
| Dynamic auto-split | Otomatik scale, load-based split/merge | Çok karmaşık, split sırasında ordering riski | 4-6 hafta | Otomatik scale ve load balancing şartsa | dynamic |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- Kafka partitioning
- `internal/broker/topic.go:17-24` — v1 Topic yapısı
