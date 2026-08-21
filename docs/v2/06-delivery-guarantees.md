# 06 — Delivery Guarantees & Consumption Tracking

> **Soru:** Mesaj en az bir kez mi, tam bir kez mi gider? Kim ne zaman consume etti, nerede loglanır? Poison message ne olur?
> **Karar yok** — ADR-004'te seçilecek.

## Kavramlar

| Kavram | Nedir | Mercurius'ta Ne Demek |
|--------|-------|----------------------|
| **At-most-once** | Mesaj 0 veya 1 kez gider, kayıp kabul | v1 `topic.go:29` `select/default` drop = at-most-once (kötü) |
| **At-least-once** | En az 1 kez gider, duplicate olabilir | Retry ile bu — idempotent consumer gerek |
| **Exactly-once** | Tam 1 kez, ne kayıp ne duplicate | En zor — idempotency + transaction gerek |
| **Idempotency** | Aynı mesajı 2 kez işlesen de sonuç aynı | `event.id` deduplication (WAL'da seen set) |
| **Ordering** | Per-topic FIFO vs global vs per-partition | Kafka per-partition FIFO, Mercurius per-topic FIFO hedef |
| **Backpressure** | Consumer yavaşsa producer'ı yavaşlat | v1'de yok — unbuffered chan bloklar ama propagate etmez |
| **Flow Control** | gRPC stream window, consumer `Ack` hızına göre | `StreamPool.Ch` bounded olmalı |
| **DLQ (Dead Letter Queue)** | N kez retry sonrası poison message'ı park et | `retryCount=5` sonrası DLQ topic'e yaz |
| **Consumer Group** | Bir topic'i N consumer paylaşır, her mesaj 1 consumer'a | v1'de `SubscriberName` = pool key, ama group coordinator yok |
| **Ack/Nack** | Consumer `Ack` (başarılı) vs `Nack` (retry) | `RetryRequest` = Nack, `ACK` = Ack |

## Subscription Modelleri

| Model | Nasıl | Artı | Eksi | Ne Zaman |
|-------|-------|------|------|----------|
| **Fan-out (v1)** | Her subscriber her mesajı alır | Basit, pub/sub | Her mesaj N kopya, scale zor | Broadcast (chat, notification) |
| **Consumer Group (Kafka)** | Topic'i group paylaşır, her mesaj 1 consumer'a, offset per group | Scale (partition), load balancing | Coordinator gerek, rebalance karmaşık | Queue (work distribution) |
| **Exclusive** | Bir topic'e 1 subscriber | Ordering garanti | Scale yok | FIFO queue |
| **Shared (Pulsar)** | Round-robin | Basit | Ordering yok | Load balancing |
| **Push (v1)** | Server stream push (`HandleBulkEvent`) | Düşük latency | Backpressure zor | Gerçek zamanlı |
| **Pull** | Client `Poll(offset, max)` | Backpressure doğal (client çeker) | Latency yüksek, polling | Batch processing |

**Mercurius v1:** Fan-out + push (`StreamPool` per `SubscriberName`). Consumer group yok.

**v2 hedef:** Hem fan-out hem consumer group — `SubscribeRequest`'e `group_id` ekle:

```proto
message SubscribeRequest {
  string subscriberID = 1;
  string subscriberName = 2;
  string topic = 3;
  google.protobuf.Timestamp createdAt = 4;
  string group_id = 5;        // NEW: empty = fan-out, set = group
  uint64 from_offset = 6;     // NEW: 0 = latest, N = replay
}
```

## Consumption Tracking — "Kim Ne Zaman Consume Etti"

v1'de yok. `retry.go:64` `eventRetryCount` memory map, restart'ta gider.

v2'de:

```sql
-- Postgres (veya SQLite) — consume_log
CREATE TABLE consume_log (
  event_id      TEXT NOT NULL,
  topic         TEXT NOT NULL,
  offset        BIGINT NOT NULL,
  subscriber_id TEXT NOT NULL,
  subscriber_name TEXT NOT NULL,
  group_id      TEXT,
  dc            TEXT NOT NULL,
  node          TEXT NOT NULL,
  status        TEXT NOT NULL, -- acked | nacked | retry | dlq | expired
  latency_ms    INT,
  attempt       INT NOT NULL DEFAULT 1,
  consumed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (topic, offset, subscriber_id)
);
CREATE INDEX ON consume_log (event_id);
CREATE INDEX ON consume_log (topic, consumed_at);
```

*   Her `Ack` / `Nack` / `DLQ` -> async batch insert (100 row batch, 100ms flush)
*   API: `GET /v2/topics/:topic/events/:id/trace` -> `SELECT * FROM consume_log WHERE event_id = $1`
*   Retention: 30 gün, sonra S3'e archive

Alternatif: WAL'a da yaz (consume log = WAL entry type `CONSUME`), ama query için SQL daha iyi. **Hybrid:** WAL (durability) + async Postgres (query).

## Retry & DLQ Akışı

```
Publisher -> WAL -> fanout -> Subscriber.HandleBulkEvent -> client fn(event)
                                                      |
                                          success -> Ack -> consume_log(acked)
                                          failure -> Nack(RetryRequest) -> retry queue
                                                                      |
                                                              attempt < 5 -> backoff (100ms * 2^attempt) -> requeue (EventChannel)
                                                              attempt >=5 -> DLQ topic (`__dlq.<topic>`) + consume_log(dlq)
```

v1'de iki ayrı retry: `broker.Retry` (sync.Map, limit 4) + `RetryHandler.HandleRetryQueue` (map[string]int, limit 5, `-1/-2` sentinel bug). v2'de tek mekanizma: `RetryHandler` WAL-backed.

## Backpressure & Flow Control

v1'de `StreamPool.Ch` unbuffered, `EventChannel` cap 1 — 1 yavaş consumer `PublishEvent`'teki blocking send (`topic.go:37`) ile tüm publish'i bloklar.

v2'de:

*   `StreamPool.Ch` bounded (örn 1000), dolunca `Publish` **bloklamaz**, WAL'da kalır, consumer `lag` metric artar
*   gRPC stream `Send` backpressure'ı otomatik (TCP window), ama `HandleBulkEvent` içinde `select { case EventChannel <- e: default: drop }` yerine `lag` ile ölç
*   `BulkEvent` batching: `subscriberBulkEventCount=1` yerine `100` + `10ms` linger (Kafka gibi)

## Modern Örnekler

| Sistem | Nasıl Yapıyor | Not |
|--------|---------------|-----|
| **Kafka Consumer Group** | Coordinator (group leader) + partition assignment + offset commit (`__consumer_offsets`) | En olgun, rebalance protocol karmaşık |
| **Pulsar Cursor** | Per-subscription cursor (offset), `ack` ile ilerler, `redeliver` | Cursor = offset pointer, basit |
| **RabbitMQ Ack** | `basic.ack` / `nack` + `requeue`, DLX (dead letter exchange) | AMQP |
| **NATS Deliver Policy** | `all`, `last`, `new`, `by_start_sequence` | JetStream `Consumer` config |
| **SQS Visibility Timeout** | Mesaj alınınca invisible, ack yoksa tekrar görünür | Basit, ama polling |

## From-Scratch Maliyet

```
internal/consume/
  log.go          // consume_log WAL entry + Postgres batch writer
  retry.go        // single retry handler, backoff, DLQ
  group.go        // consumer group coordinator (assignment, rebalance)
  dlq.go          // __dlq topic handling
```

*   Retry + DLQ + tracking: **2 hafta**
*   Consumer group coordinator: **+2-3 hafta** (rebalance en zor)

## Araştırma Görevleri

- [ ] Kafka `__consumer_offsets` nasıl çalışır — offset commit
- [ ] Pulsar `ManagedCursor` kaynak oku
- [ ] SQS visibility timeout paper
- [ ] gRPC backpressure: `Send` ne zaman bloklar, `Context` cancel nasıl propagate

## Karar Bekliyor — ADR-004

| Soru | Seçenekler |
|------|------------|
| Guarantee | At-least-once vs exactly-once (idempotency) |
| Subscription | Fan-out only vs fan-out + consumer group |
| Push vs Pull | Push (stream) vs Pull (poll) vs hybrid |
| Retry limit | 5 vs configurable vs exponential backoff |
| DLQ | Ayrı topic vs ayrı table |
| Tracking storage | Postgres vs SQLite vs WAL only |
| Ordering | Per-topic FIFO vs per-partition vs global |

> Hepsi `proposed`.
