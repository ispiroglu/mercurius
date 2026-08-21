# 04 — Stretch Multi-DC

> **Soru:** 2 DC var, aralarında 80ms latency — mesaj nasıl replike olur? Conflict olursa ne olur?
> **Karar yok** — ADR-003'te seçilecek.

## Kavramlar

| Kavram | Nedir |
|--------|-------|
| **Stretch Cluster** | Tek cluster'ın node'ları farklı DC'lere yayılmış |
| **WAN Replication** | DC'ler arası async/sync log shipping |
| **RPO / RTO** | Recovery Point Objective (ne kadar veri kaybı kabul), Recovery Time Objective (ne kadar sürede geri gelir) |
| **Active-Passive** | Bir DC primary (yazılır), diğeri standby (sadece replike, failover'da active olur) |
| **Active-Active** | Her DC yazabilir, conflict resolution gerekir |
| **Gateway / MirrorMaker** | DC'ler arası replikasyonu yapan ayrı process |
| **Vector Clock / Dotted Version Vector** | Hangi yazma önce, conflict var mı tespit için |
| **LWW (Last-Write-Wins)** | En son timestamp kazanır — basit ama veri kaybı riski |
| **CRDT** | Merge'i deterministik data type (counter, OR-Set) |

## Pattern'ler

### A) Active-Passive Async (Öneri Değil, En Basit)

```
DC-1 (Primary, 3 node Raft)
  WAL -> Stretch Gateway (tailer)
              |
              | gRPC stream (compressed, batched, 1s window)
              v
DC-2 (Standby, 3 node Raft, read-only)
  ingest -> kendi WAL'ına yaz -> follower'lara replike
```

*   Artı: Basit, conflict yok (tek writer), CAP'te AP, latency düşük (async)
*   Eksi: Failover manuel veya otomatik ama RPO > 0 (async olduğu için son 1s veri kaybı olabilir), DC-2'ye yazma yok
*   Ne zaman: 90% use case için yeterli. Bankacılık değilse bu model.

Failover:

```
DC-1 öldü -> health check (gateway) -> promote DC-2 to primary -> DNS/anycast switch -> publisher'lar DC-2'ye
DC-1 geri geldi -> catch-up (DC-2 WAL'ını DC-1'e ship) -> demote to standby
```

### B) Active-Active Multi-Master

```
DC-1 Raft  <--> Gateway mesh (bidirectional) <--> DC-2 Raft
  ^                                                    ^
  | publish topic/A                                  | publish topic/A  -> CONFLICT?
```

*   Artı: Her DC yazabilir, latency düşük (local write)
*   Eksi: Aynı `topic+offset` veya `event.id` conflict, resolution gerek (LWW vs vector clock vs CRDT). Implementasyon **çok** karmaşık.
*   Ne zaman: Gerçek multi-region write gerekirse (örn global chat). Yoksa overkill.

### C) Quorum Per-DC + Async Inter-DC

Her DC kendi quorum'unda commit eder (W=2/3), sonra async inter-DC ship. Okuma local quorum'dan. En dengeli ama iki kat replication kodu.

### D) Log Shipping vs CDC

| Yöntem | Nasıl | Artı/Eksi |
|--------|-------|-----------|
| **WAL tailer** | Gateway WAL dosyasını tail eder, batch ship | Basit, WAL formatını bilmek yeter |
| **CDC (Change Data Capture)** | DB'den change feed (Debezium gibi) | DB'ye bağımlı, Mercurius'ta DB zaten WAL değil |
| **Raft log ship** | Raft commit'i ship | Strong consistency inter-DC, ama WAN Raft yavaş |

Mercurius için **WAL tailer** en doğal — zaten WAL var.

## Modern Örnekler — Nasıl Yapıyorlar?

| Sistem | Stretch Nasıl | Mercurius İçin Not |
|--------|---------------|-------------------|
| **Kafka MirrorMaker 2** | Source cluster tail -> target cluster produce (consumer group offset de ship) | Gateway = MirrorMaker klonu. En yakın ilham. |
| **Pulsar Geo-Replication** | Per-namespace async replication, broker-to-broker | Pulsar'ın `replicationClusters` config'i |
| **CockroachDB Multi-Region** | Per-range Raft + leaseholder, `REGIONAL` table | Çok karmaşık, ama `REGIONAL BY ROW` fikri alınabilir |
| **Spanner TrueTime** | Atomic clock + 2PC, external consistency | Google'a özel, TrueTime olmadan yapılamaz — ilham değil |
| **NATS Supercluster** | Gateway mesh, interest-based routing, no persistence inter-DC | NATS `gateway` config'i basit, Mercurius gateway'e ilham |
| **Redpanda Shadow Index** | S3'e tiered storage, DC'ler S3'ten catch-up | S3 tiering sonra eklenebilir |

## Mercurius Mapping

| Soru | Tasarım Notu |
|------|--------------|
| `event.id` global mi DC-local mi? | Global `uuid` + `dc_id` prefix (`dc1-uuid`) — DC-local collision engel |
| Aynı topic'e iki DC'de publish | Active-passive'de sorun yok (sadece primary yazar). Active-active'de conflict — vector clock gerek |
| Offset global mi per-DC mi? | Per-DC offset, inter-DC ship sonrası target DC kendi offset'ini verir (offset remapping) |
| Gateway nerede durur? | Ayrı Deployment (2 replika), WAL'ı tail eder, `mTLS` ile diğer DC gateway'e bağlanır |
| WAN backpressure | Gateway'de bounded queue (10k event), dolunca `Publish` yavaşlar (backpressure propagate) veya DC-local WAL'da kalır |
| Compression | Inter-DC batch `zstd` — WAN pahalı |

## From-Scratch Maliyet

```
internal/stretch/
  gateway.go      // WAL tailer + batch + ship
  ingest.go       // remote DC'den gelen batch'i WAL'a yaz
  failover.go     // health check + promote/demote
  offset_map.go   // source offset -> target offset mapping
```

Maliyet: **3 hafta** active-passive için, active-active **+3 hafta** conflict resolution.

## Araştırma Görevleri

- [ ] Kafka MirrorMaker 2 design doc oku (KIP-382)
- [ ] NATS supercluster gateway `gateway.go` kaynak oku
- [ ] Vector clock paper: "Why Vector Clocks Are Easy" (Bailis)
- [ ] CockroachDB `multi-region` docs: `REGIONAL` vs `GLOBAL`

## Karar Bekliyor — ADR-003

| Soru | Seçenekler |
|------|------------|
| Pattern | Active-passive vs active-active vs per-DC quorum |
| Ship yöntemi | WAL tailer vs Raft log ship |
| Conflict resolution | LWW vs vector clock vs CRDT vs "yasak" (single writer) |
| Failover | Manuel vs otomatik (health check) |
| Offset stratejisi | Global vs per-DC remapping |

> Hepsi `proposed`.
