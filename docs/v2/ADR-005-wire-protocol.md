# ADR-005: Wire Protocol — Codec

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/03-consensus-replication.md

## Bağlam

Mevcut `proto/Event.proto:1-12` sadece `id, topic, body, createdAt, expiresAt` taşıyor; `offset`, `partition`, `producer_id`, `dc_id` yok — breaking change gerekli. `pkg/serialize/serialize.go:5` `gob` codec'i var ama soyutlanmamış. Hangi serialization? protobuf mu flatbuffers mı? Breaking schema nasıl yönetilir?

## Port

- **Port Interface:** `internal/port/codec.go: Codec` — one interface, one file location (future `internal/port/codec.go`)
- **Signature:** `type Codec interface { Marshal(Event) ([]byte, error); Unmarshal([]byte, *Event) error; Name() string }`
- **Scope:**
  - Owns: Event serialization/deserialization, schema versioning, field set (offset/partition/producer_id/dc_id breaking ekleri)
  - NOT owns: transport/RPC framing (ADR-006 `Transport` port), WAL storage (ADR-001)
- **Adapters:** protobuf (current Event.proto:1-31), flatbuffers, capnproto, gob (pkg/serialize/serialize.go:5), JSON

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| protobuf (current Event.proto:1-31) | Geniş adoption, breaking için `reserved`, codegen var | Binary, schema evolution dikkat ister; breaking fields eklenmeli | 0 hafta (mevcut) | gRPC ile uyum, codegen isteniyorsa | schema-binary |
| flatbuffers | Zero-copy, çok hızlı deserialization | Schema karmaşık, Go desteği protobuf'tan zayıf | 1 hafta | Ultra düşük latency, zero-copy şartsa | zero-copy |
| capnproto | Zero-copy, capability model, hızlı | Ekosistem küçük, Go desteği sınırlı | 1 hafta | Cap'n Proto modeli isteniyorsa | zero-copy |
| gob (pkg/serialize/serialize.go:5) | Go native, basit, v1'de var | Go'ya özgü, dil-bağımsız değil, versioning zayıf | 0 hafta | Sadece Go client'lar, hızlı prototip | go-native |
| JSON | İnsan okunur, debug kolay, evrensel | Verbose, yavaş, binary değil | 0 hafta | Debug edilebilirlik ve evrensellik öncelikliyse | text |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- `proto/Event.proto:1-31` — mevcut schema
- `pkg/serialize/serialize.go:5` — mevcut gob codec
- protobuf breaking: `reserved` fields
