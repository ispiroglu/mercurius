# Mercurius v2 — Konsept Haritası

> **Felsefe:** Hazır kütüphane yok. Her şey from-scratch. Bu doküman *ne lazım, nerede lazım, alternatifler neler* sorusunu cevaplar — karar vermez.
> Kararlar `docs/v2/ADR-*.md` içinde loglanacak.

## Mevcut Durum (v1) — Neden Sıfırdan?

`internal/broker/*` analizi:

| Bileşen | Durum | Sorun |
|---------|-------|-------|
| `Broker` (`broker.go:13`) | `sync.Map` + embed `TopicRepository` | Persistence 0, replication 0, lifecycle 0 |
| `Topic.PublishEvent` (`topic.go:26`) | `poolCount==0` ise `select/default` drop | Mesaj kaybı sessiz |
| `Topic.workerPool` (`topic.go:23`) | `nil` | `pond` import edilmiş ama kullanılmıyor |
| `StreamPool.Ch` (`stream_pool.go:19`) | `make(chan Event)` unbuffered | 1 yavaş consumer tüm publish'i bloklar |
| `Subscriber.EventChannel` (`subscriber.go:34)` | `cap 1` | Backpressure yok |
| `BulkEvent` (`subscriber.go:17)` | `subscriberBulkEventCount=1` | Batching yok, her mesaj 1 gRPC frame |
| `Retry` (`broker.go:63` + `retry.go:64)` | İki ayrı mekanizma, `-1/-2` sentinel, `map[string]int` race | Yarım, test edilemez |
| `SubscriberRepository` | Global + per-topic iki instance, key=`SubscriberName` | Aynı name farklı topic'te collision |

**Sonuç:** v1 bir PoC. v2'de **hiçbir kod taşınmayacak** — sadece konseptler referans.

## Kavram Haritası — Ne Nerede Lazım?

```
Publish Path:  Client.Publish -> gRPC -> Gateway -> WAL fsync -> Raft replicate -> ACK
                                          |                         |
                                          v                         v
                                       Pebble/Badger          Quorum commit
                                                    \
Subscribe Path: Client.Subscribe -> gRPC stream -> Replay (WAL) -> Live tail -> Ack/Nack -> ConsumeLog -> DLQ
                                                             \
Stretch:        DC-1 WAL tailer -> Gateway -> gRPC -> DC-2 ingest -> kendi WAL'ına yaz
```

| Katman | Kavramlar | Doküman | Port |
|--------|-----------|---------|------|
| **Persistence** | WAL, LSM, SSTable, MemTable, fsync, segment, compaction, snapshot, recovery | `02-persistence.md` | WAL (ADR-001) |
| **Consensus** | Raft/Paxos, quorum, leader election, log replication, membership | `03-consensus-replication.md` | Consensus (ADR-002) / Replicator (ADR-003) |
| **Stretch** | Active-passive, async replication, conflict, vector clock, gateway | `04-stretch-multi-dc.md` | StretchGateway (ADR-004) |
| **Operability** | Helm, StatefulSet, PVC, PDB, operator, config, TLS, upgrade | `05-operability.md` | ConfigSource (ADR-012) / Metrics (ADR-013) |
| **Delivery** | At-least-once, exactly-once, idempotency, ordering, DLQ, consumer group, backpressure | `06-delivery-guarantees.md` | DeliveryGuarantee (ADR-008) / ConsumerGroup (ADR-009) / ConsumeTracker (ADR-010) |
| **Testing** | Unit, integration (testcontainers), contract (buf), e2e (kind), bench (ghz/k6), chaos (toxiproxy) | `07-testing-strategy.md` (sonra) | TestCluster (ADR-014) |
| **Security** | mTLS, ACL, JWT | `05-operability.md` | Authenticator (ADR-011) |
| **Wire/Transport** | protobuf, gRPC | `03-consensus-replication.md` | Codec (ADR-005) / Transport (ADR-006) |

## From-Scratch İlkesi — Ne Demek?

Her alternatif için iki kolon olacak:

*   **Hazır kullanırsan:** ne kazanırsın, ne kaybedersin
*   **Kendin yazarsan:** ne kadar kod, ne kadar risk, ne öğrenirsin

Örnek: Pebble kullanmak 2 hafta kazandırır ama LSM'i öğrenemezsin. Kendi WAL'ını yazmak 3-4 hafta alır ama segment formatını, CRC'yi, recovery'yi **gerçekten** anlarsın. v2'de ikinciyi seçiyoruz — dokümanlar bu maliyeti dürüstçe yazacak.

## Karar Logu — ADR

Her karar `docs/v2/ADR-00X-*.md` olarak loglanır. Template:

```md
# ADR-00X: Başlık
- Durum: proposed | accepted | rejected | superseded
- Tarih:
- Bağlam: neden karar gerekiyor
- Alternatifler: tablo
- Karar: ne seçildi
- Sonuçlar: ne kazanıldı/ne kaybedildi
```

Şu an **hiçbir ADR accepted değil** — hepsi `proposed` olarak kalacak, sen araştırmadan sonra dolduracaksın.

## Sonraki Adımlar

1. `02`–`06` dokümanları subagent'lar dolduruyor (paralel)
2. Sen oku, not al, ADR'leri `proposed` olarak işaretle
3. Onay sonrası `v2` branch'inde tüm kod silinip `internal/wal/`, `internal/raft/`, `internal/stretch/` iskeleti from-scratch kurulacak
