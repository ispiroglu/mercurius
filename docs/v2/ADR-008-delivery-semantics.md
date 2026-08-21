# ADR-008: Delivery Semantics

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/06-delivery-guarantees.md

## Bağlam

v1 `topic.go:29` `select/default` drop = at-most-once, `broker.go:63` retry yarım. Hangi guarantee? At-least-once mı exactly-once mı? Idempotency ve deduplication nasıl? LWW vs vector clock?

## Port

- **Port Interface:** `internal/port/delivery.go: DeliveryGuarantee + Deduplicator` — one interface, one file location (future `internal/port/delivery.go`)
- **Signature:** `type DeliveryGuarantee interface { Guarantee() string }` + `type Deduplicator interface { Seen(id string) bool; Mark(id string) }`
- **Scope:**
  - Owns: guarantee level (at-most/at-least/exactly-once), idempotency, deduplication, LWW/vector clock kararı
  - NOT owns: consumer assignment (ADR-009 `ConsumerGroup` port), ack/nack/dlq tracking (ADR-010 `ConsumeTracker` port)
- **Adapters:** At-most-once (v1 drop topic.go:29), At-least-once, Exactly-once, LWW vs vector clock

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| At-most-once (v1 drop topic.go:29) | En basit, duplicate yok, hızlı | Mesaj kaybı kabul edilir | 0 hafta (mevcut) | Kayıp tolere edilebilirse (metrics, logs) | best-effort |
| At-least-once | Kayıp yok, retry ile garantili | Duplicate mümkün, dedup gerekir | 1-2 hafta | Kayıp kabul edilemez, duplicate tolere edilirse | retry-based |
| Exactly-once | Kayıp ve duplicate yok, en güçlü garanti | Çok karmaşık — idempotency + transaction gerekir | 4-6 hafta | Finansal/ödeme gibi duplicate'in pahalı olduğu durumlar | idempotent |
| LWW vs vector clock | LWW basit; VC conflict'i doğru çözer | LWW data loss riski; VC overhead ve karmaşıklık | 2-3 hafta | Conflict resolution stratejisi seçimi (stretch ile birlikte) | conflict-resolution |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- Kafka delivery semantics
- Exactly-once via idempotency + transactions
- `internal/broker/topic.go:29` — v1 drop
