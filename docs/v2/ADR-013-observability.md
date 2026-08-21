# ADR-013: Observability

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/05-operability.md

## Bağlam

v1 `cmd/mercurius.go:40` `promhttp` ile sadece Prometheus endpoint'i var — tracing yok, structured logging zayıf. Hangi observability stack? OTEL+Tempo+Prom+Grafana+Loki mi sadece Prometheus mu Datadog mu?

## Port

- **Port Interface:** `internal/port/metrics.go: Metrics + Tracer` — one interface, one file location (future `internal/port/metrics.go`)
- **Signature:** `type Metrics interface { IncCounter(name string, labels ...string); ObserveHistogram(name string, v float64, labels ...string); SetGauge(name string, v float64, labels ...string) }` + `type Tracer interface { Start(ctx context.Context, name string) (context.Context, Span) }`
- **Scope:**
  - Owns: metrics/traces/logs emission, instrumentation API
  - NOT owns: collection/storage/visualization (Prometheus/Grafana/Tempo), config (ADR-012 `ConfigSource` port)
- **Adapters:** OTEL+Tempo+Prom+Grafana+Loki, Prom only, Datadog, No obs (v1 promhttp mercurius.go:40)

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| OTEL+Tempo+Prom+Grafana+Loki | Full stack, vendor-agnostic, tracing+metrics+logs birleşik | Kurulum ve operasyon karmaşık, çok bileşen | 2-3 hafta | Full observability, vendor lock-in istenmiyorsa | otel-full |
| Prom only | Basit, v1'e yakın, metrics yeterli | Tracing ve log aggregation yok | 0.5 hafta | Sadece metrics yeterliyse | prom-only |
| Datadog | Managed, tek pane, kolay | Vendor lock-in, maliyetli, OTEL değil | 1 hafta | Managed ve tek vendor isteniyorsa | vendor-managed |
| No obs (v1 promhttp mercurius.go:40) | Sıfır maliyet | Kör uçuş — debug ve alert imkansız | 0 hafta | Dev/prototip only | none |

### Metrics Table

| Metric | Type | Labels | Açıklama |
|--------|------|--------|----------|
| `mercurius_publish_total` | Counter | `topic`, `partition` | Toplam publish sayısı |
| `mercurius_publish_latency_seconds` | Histogram | `topic` | Publish fsync+replicate latency |
| `mercurius_consume_total` | Counter | `topic`, `group`, `status(ack/nack/dlq)` | Consume sonuçları |
| `mercurius_wal_sync_duration_seconds` | Histogram | `topic` | WAL fsync süresi |
| `mercurius_replication_lag_seconds` | Gauge | `topic`, `follower` | Replication lag |
| `mercurius_stretch_lag_seconds` | Gauge | `dc` | Inter-DC lag (ADR-004) |
| `mercurius_consumer_lag` | Gauge | `topic`, `group`, `partition` | Consumer lag |
| `mercurius_dlq_total` | Counter | `topic` | DLQ'ya düşen mesaj sayısı |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- OpenTelemetry Go SDK
- Prometheus client_golang
- `cmd/mercurius.go:40` — v1 promhttp
