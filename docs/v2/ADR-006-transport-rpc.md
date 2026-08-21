# ADR-006: Transport — RPC

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/03-consensus-replication.md

## Bağlam

v1 `proto/Event.proto:33-36` `service Mercurius { rpc Publish; rpc Subscribe returns (stream BulkEvent); rpc Retry; }` gRPC server-stream'e hard-wired. Keepalive, flow-control, retry ve framing soyutlanmamış (`pkg/client/grpc_dial.go:8` insecure). Hangi transport?

## Port

- **Port Interface:** `internal/port/transport.go: Transport` — one interface, one file location (future `internal/port/transport.go`)
- **Signature:** `type Transport interface { Publish(ctx context.Context, e Event) error; Subscribe(ctx context.Context, req SubscribeRequest) (<-chan Event, error); Retry(ctx context.Context, req RetryRequest) error }`
- **Scope:**
  - Owns: RPC framing, keepalive, flow-control, streaming lifecycle
  - NOT owns: business logic (broker), serialization (ADR-005 `Codec` port), auth (ADR-011)
- **Adapters:** gRPC server-stream (Event.proto:34-36), gRPC bidi, QUIC, raw TCP length-prefix, WebSocket

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| gRPC server-stream (Event.proto:34-36) | Mevcut v1 uyumlu, streaming hazır, ekosistem geniş | HTTP/2 overhead, bidi değil | 0 hafta (mevcut) | v1 uyumluluğu ve basitlik isteniyorsa | grpc-stream |
| gRPC bidi | Çift yönlü, flow-control daha iyi, tek connection | Karmaşıklık artar, client/server senkronizasyonu | 1-2 hafta | Çift yönlü ack/nack gerekirse | grpc-bidi |
| QUIC | Düşük latency, migration, 0-RTT, modern | Ekosistem genç, UDP firewall sorunları | 3-4 hafta | Mobil/edge, connection migration şartsa | quic |
| raw TCP length-prefix | Sıfır bağımlılık, tam kontrol, from-scratch | Her şeyi kendin yaz (framing, keepalive, TLS) | 3-4 hafta, ~800 LOC | Sıfır bağımlılık ve tam kontrol (v2 felsefesi) | raw-tcp |
| WebSocket | Browser uyumlu, firewall-friendly, basit | Overhead, binary framing zayıf | 1-2 hafta | Browser client şartsa | websocket |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- `proto/Event.proto:33-36` — mevcut service tanımı
- `pkg/client/grpc_dial.go:8` — insecure dial
- gRPC streaming docs
