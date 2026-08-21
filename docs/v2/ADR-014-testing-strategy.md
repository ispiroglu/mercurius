# ADR-014: Testing Strategy — TestCluster

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/07-testing-strategy.md

## Bağlam

v1'de test harness yok — chaos, partition, WAL corruption testi imkansız. Hangi test stratejisi? testcontainers-go mu kind+helm mi toxiproxy+pumba mı property-based mi contract (buf breaking) mi?

## Port

- **Port Interface:** `internal/port/testkit.go: TestCluster` — one interface, one file location (future `internal/port/testkit.go`) — test-only port
- **Signature:** `type TestCluster interface { Leader() Node; Kill(Node); Partition(a,b []Node); CorruptWAL(Node, offset Offset) }`
- **Scope:**
  - Owns: test harness — leader/kill/partition/corruption fault injection, cluster lifecycle
  - NOT owns: production code (hiçbir production port'u değil — sadece test)
- **Adapters:** testcontainers-go, kind+helm, toxiproxy+pumba, rapid property, buf breaking

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| testcontainers-go | Gerçek container'larla integration test, Go native | Docker gerekir, yavaş | 1-2 hafta | Integration test için gerçek servisler isteniyorsa | container-integration |
| kind+helm | Gerçek K8s, Helm chart'ı ile e2e | Kurulum ağır, CI'de yavaş | 2-3 hafta | K8s e2e ve Helm chart testi isteniyorsa | k8s-e2e |
| toxiproxy+pumba | Network partition, latency, chaos injection | Ek setup, sadece network chaos | 1-2 hafta | Chaos/partition testi isteniyorsa | chaos |
| rapid property | Property-based, edge case bulur, deterministik | Test yazımı zor, shrink karmaşık | 1 hafta | Invariant ve edge case coverage isteniyorsa | property-based |
| buf breaking | Proto breaking change detection, contract test | Sadece proto, diğer contract'ları kapsamaz | 0.5 hafta | Proto schema contract'ı korunmak isteniyorsa | contract |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- testcontainers-go docs
- kind + Helm test
- toxiproxy, pumba
- rapid (Go property-based)
- buf breaking change detection
