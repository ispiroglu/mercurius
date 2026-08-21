# Mercurius v2 Dokümantasyonu

> Sıfırdan rewrite. Hazır kütüphane yok. Kararlar ADR ile loglanır.

| # | Doküman | Durum | Açıklama |
|---|---------|-------|----------|
| 01 | [Konsept Haritası](01-concepts-overview.md) | ✅ hazır | Ne nerede lazım — harita |
| 02 | [Persistence](02-persistence.md) | ⏳ yazılıyor | WAL, LSM, segment, modern alternatifler |
| 03 | [Consensus & Replication](03-consensus-replication.md) | ⏳ yazılıyor | Raft/Paxos/Zab, quorum, from-scratch maliyet |
| 04 | [Stretch Multi-DC](04-stretch-multi-dc.md) | ⏳ yazılıyor | Active-passive/active-active, gateway |
| 05 | [Operability](05-operability.md) | ⏳ yazılıyor | Helm, StatefulSet, observability |
| 06 | [Delivery Guarantees](06-delivery-guarantees.md) | ⏳ yazılıyor | Exactly-once, DLQ, consumer group, tracking |
| 07 | [Testing Strategy](07-testing-strategy.md) | ⏳ sonra | İnanılmaz testsuite |
| ADR | [ADR Template](ADR-000-template.md) | ✅ hazır | Karar şablonu |

## Karar Durumu

| ADR | Konu | Durum |
|-----|------|-------|
| ADR-001 | Persistence engine | `proposed` — araştırma bekleniyor |
| ADR-002 | Consensus algoritması | `proposed` |
| ADR-003 | Stretch pattern | `proposed` |
| ADR-004 | Subscription model | `proposed` |
| ADR-005 | Observability stack | `proposed` |

Hepsi `proposed` — sen araştırmadan sonra `accepted` yapacaksın.

## Nasıl Okunur?

1. `01` ile başla — harita
2. `02`→`06` her biri bağımsız, istediğin sıradan oku
3. Her dokümanın sonundaki **Karar Bekliyor** tablosunu doldur
4. `ADR-*.md` oluştur ve `README` tablosunu güncelle

## Kod Silme Planı

Dokümanlar `accepted` olduktan sonra:

```bash
git checkout v2
git rm -r internal/ pkg/ proto/  # v1 kod tamamen silinir
mkdir -p internal/{wal,raft,stretch,gateway,store,consume}
# from-scratch iskelet kurulur
```

Şu an kod **silinmedi** — dokümanlar bitene kadar v1 referans olarak duruyor.
