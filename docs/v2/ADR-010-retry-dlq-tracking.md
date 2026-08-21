# ADR-010: Retry / DLQ / Tracking — ConsumeTracker

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/06-delivery-guarantees.md

## Bağlam

v1 `internal/broker/broker.go:17` `retriedEvents sync.Map` + `retry.go:64` iki ayrı mekanizma, `-1/-2` sentinel, `map[string]int` race, DLQ yok, trace yok. Ack/nack/dlq/trace nasıl persist edilecek? Memory map mi WAL-backed mi Postgres mi?

## Port

- **Port Interface:** `internal/port/consume.go: ConsumeTracker` — one interface, one file location (future `internal/port/consume.go`)
- **Signature:** `type ConsumeTracker interface { Ack(e Event, sub Subscriber) error; Nack(e Event, sub Subscriber) error; DLQ(e Event) error; Trace(eventID string) ([]ConsumeRecord, error) }`
- **Scope:**
  - Owns: ack/nack/dlq/trace lifecycle, consume record persistence
  - NOT owns: retry backoff/scheduling (ayrı), delivery guarantee (ADR-008), transport (ADR-006)
- **Adapters:** Memory map (v1 broker.go:17 retriedEvents), WAL-backed retry, Postgres consume_log, SQLite WAL, DLQ topic __dlq.<topic>

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| Memory map (v1 broker.go:17 retriedEvents) | Basit, hızlı, sıfır bağımlılık | Restart'ta kayıp, race, DLQ yok | 0 hafta (mevcut) | Prototip, durability gerekmezse | in-memory |
| WAL-backed retry | Durable, WAL reuse, replay mümkün | WAL coupling, GC gerekir | 2-3 hafta | WAL zaten varsa reuse isteniyorsa | wal-backed |
| Postgres consume_log | Durable, query edilebilir, trace kolay | Harici bağımlılık, latency | 1-2 hafta | Trace/query öncelikli, Postgres zaten varsa | rdbms |
| SQLite WAL | Embedded, durable, SQL query | Single-writer, scale sınırlı | 1-2 hafta | Embedded durability, SQL trace isteniyorsa | embedded-sql |
| DLQ topic __dlq.<topic> | Kafka benzeri, topic olarak DLQ, reprocess kolay | Topic sayısı artar, retention yönetimi gerekir | 2 hafta | DLQ'yu topic olarak yönetmek isteniyorsa | topic-dlq |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- Kafka DLQ pattern
- `internal/broker/broker.go:17` — retriedEvents
- `internal/broker/retry.go:64` — retry mekanizması
