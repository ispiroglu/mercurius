# ADR-009: Consumer Model

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/06-delivery-guarantees.md

## Bağlam

v1 `internal/broker/stream_pool.go:9` `StreamPool` per `Name` fan-out only, `subscriber.go:17` `subscriberBulkEventCount=1` batching yok, group yok, push/pull ayrımı yok. Hangi consumer modeli? Fan-out+group (Kafka) mi exclusive mi? Push vs pull? BulkEvent batch tuning?

## Port

- **Port Interface:** `internal/port/consumer.go: ConsumerGroup` — one interface, one file location (future `internal/port/consumer.go`)
- **Signature:** `type ConsumerGroup interface { Join(group, subscriberID string) error; Leave(group, subscriberID string) error; Assignment(group, topic string) []Partition; Next(topic string, group string) (subscriberID string) }`
- **Scope:**
  - Owns: group membership, partition assignment, next-consumer selection, batch linger
  - NOT owns: delivery guarantee/dedup (ADR-008 `DeliveryGuarantee` port), ack/nack/dlq (ADR-010)
- **Adapters:** Fan-out only (v1 StreamPool stream_pool.go:9 per Name), Fan-out+group (Kafka), Exclusive, Push vs Pull vs Hybrid, BulkEvent batch 1 vs 100+10ms linger

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| Fan-out only (v1 StreamPool stream_pool.go:9 per Name) | Basit, her subscriber her mesajı alır | Group yok, scale yok, her consumer full copy | 0 hafta (mevcut) | Broadcast/fan-out yeterliyse | fan-out |
| Fan-out+group (Kafka) | Group içinde load-share, fan-out gruplar arası | Rebalancing karmaşık, coordinator gerekir | 3-4 hafta | Kafka benzeri group semantics isteniyorsa | group |
| Exclusive | Tek consumer, ordering garantisi | Paralelism yok, failover gerekir | 1-2 hafta | Strict ordering, tek consumer şartsa | exclusive |
| Push vs Pull vs Hybrid | Push düşük latency; Pull backpressure; Hybrid esnek | Push backpressure zor; Pull polling overhead | 2-3 hafta | Backpressure ve latency tradeoff'una göre | push-pull |
| BulkEvent batch 1 vs 100+10ms linger | Batch 100+linger throughput'u katlar | Batch latency ekler, linger tuning gerekir | 1 hafta | Throughput öncelikliyse batch+linger | batching |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- Kafka consumer group protocol
- `internal/broker/stream_pool.go:9` — v1 fan-out
- `internal/broker/subscriber.go:17` — BulkEvent batch=1
