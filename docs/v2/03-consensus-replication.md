# 03 — Consensus & Replication

> **Soru:** 3 node var, 1'i öldü — mesaj kaybolmadan nasıl devam eder? Hangi node leader?
> **Karar yok** — ADR-002'de seçilecek.

## Neden Lazım?

v1'de `Broker` tek instance, `sync.Map` memory'de. Node ölürse topic'ler gider. `docs/Project-Scope-and-Procedure.md:7` multi-broker vaad ediyor ama kodda yok.

v2'de hedef: 3-5 node, 1 leader, quorum commit.

```
Publisher -> Leader (WAL append) -> replicate to followers -> quorum (2/3) -> ACK
Follower öldü -> leader devam (quorum var)
Leader öldü -> election -> yeni leader -> WAL replay -> devam
```

## Kavramlar

| Kavram | Nedir |
|--------|-------|
| **Linearizability** | Her read/write sanki tek node'da sırayla olmuş gibi görünür |
| **Quorum** | N node'dan W'sine yaz, R'sinden oku — W+R > N ise overlap garanti |
| **Leader Election** | Node'lar oy verir, çoğunluğu alan leader. Term/epoch ile fencing |
| **Log Replication** | Leader log'u follower'a stream eder, follower ACK, leader commit index ilerletir |
| **Commit vs Applied** | Commit = quorum'a yazıldı, Applied = state machine'e uygulandı (topic'e fanout) |
| **Membership** | Node ekle/çıkar (joint consensus). Helm scale ile entegrasyon |
| **Split-Brain** | Network partition'da iki taraf da kendini leader sanır — quorum engeller |
| **Fencing Token** | Eski leader'ın yazmasını engelle (term id ile) |
| **Snapshot** | Log çok büyüdü, state'i snapshot'la, eski log'u sil |

## Algoritma Alternatifleri

| Algoritma | Fikir | Karmaşıklık | Artı | Eksi | Ne Zaman |
|-----------|-------|-------------|------|------|----------|
| **Raft** | Leader + log replication, herkes anlar | Orta | Dokümantasyon bol, debug kolay, leader var | Leader bottleneck, yazma tek node | **Genel default** |
| **Multi-Paxos** | Paxos'u log'a genişletme | Yüksek | Raft'tan teorik farkı az, daha esnek | Anlaması zor, implementasyon hatası kolay | Akademik |
| **Zab** (Zookeeper) | Leader + atomic broadcast | Orta-yüksek | Zookeeper kanıtlı | Raft'tan farkı az, ayrı öğrenme | ZK klonu yapacaksan |
| **Viewstamped Replication (VR)** | Raft'a çok benzer, 2012 paper | Orta | Raft ile neredeyse aynı | Ekosistem küçük | VR paper okumak için |
| **EPaxos / Mencius** | Leaderless, her node yazabilir | Çok yüksek | Leader bottleneck yok, WAN iyi | Conflict resolution karmaşık, implementasyon canavar | Multi-DC active-active gerekirse |
| **Chain Replication** | Head -> ... -> Tail, chain | Düşük | Basit, tail read güçlü | Head bottleneck, chain kırılırsa durur | Basit replication, tail scale |
| **Gossip / SWIM / memberlist** | Her node herkese yayar, eventual consistency | Düşük-orta | Partition tolerant, basit | Linearizability yok, conflict | Discovery + failure detection için (consensus yerine değil) |
| **CRDT** | Conflict-free replicated data type | Orta | Merge otomatik, AP | Sadece CRDT uygun data (counter, set) | Topic listesi gibi metadata için uygun, log için değil |

**Mercurius için:** Log replication gerekiyor — o yüzden **Raft ailesi** en doğal. Gossip sadece *discovery* (kim yaşıyor) için yan katman.

## Go Implementasyon Alternatifleri

| Implementasyon | Durum | Artı | Eksi | From-Scratch Maliyet |
|----------------|-------|------|------|----------------------|
| **hashicorp/raft** | Olgun, Consul/Nomad kullanıyor | Dokümantasyon, `raft-boltdb` storage, `memberlist` entegrasyon | Log storage'ı BoltDB, WAL ile entegrasyon elle | — |
| **etcd/raft** | etcd'nin core'u | En test edilmiş, `raftpb` clean | Sadece algorithm, network/storage sen yaz, öğrenme eğrisi dik | — |
| **dragonboat** | Çoklu Raft group, yüksek perf | Multi-group (per-topic Raft), Go native, batch | Daha az doküman, karmaşık | — |
| **From-scratch Raft** | Kendi implementasyonun | Öğrenme max, dependency 0, paper'ı gerçekten anlarsın | **6-8 hafta**, correctness zor, Jepsen tarzı test gerek | ~3000 LOC, en zor kısım election + log matching + snapshot |

**From-scratch Raft yazmak ne demek?**

```
internal/raft/
  state.go        // Follower/Candidate/Leader enum, term, votedFor
  log.go          // LogEntry{term, index, command(Event)}
  rpc.go          // RequestVote, AppendEntries (gRPC)
  election.go     // timeout (150-300ms random), vote, stepDown
  replication.go  // leader: nextIndex/matchIndex per follower, pipeline
  commit.go       // quorum commit -> applyCh -> Broker.Apply()
  snapshot.go     // Snapshot{lastIndex, lastTerm, state: Topics}
  storage.go      // persistent: currentTerm, votedFor, log (WAL ile aynı dosya olabilir)
```

Maliyet: **6-8 hafta**, en kritik testler: `kill leader mid-replicate`, `partition 2-1`, `old leader try write`.

## Replication Pattern'leri (Raft üstüne)

| Pattern | Nasıl | Ne Zaman |
|---------|-------|----------|
| **Single Global Log** | Tüm topic'ler tek Raft log | Basit, ama bir topic'in yükü hepsini etkiler |
| **Per-Topic Raft Group** (öneri değil, not) | Her topic ayrı Raft (dragonboat multi-group) | İzolasyon iyi, ama 1000 topic = 1000 group, overhead yüksek |
| **Partitioned Log** | Topic hash -> partition -> Raft group | Kafka gibi, scale iyi, complexity orta |
| **Quorum Write (W=2,R=2,N=3)** | Leader yaz, 1 follower ACK bekle, commit | Latency vs durability trade-off |

**Başlangıç için:** Single global log + single Raft group (3 node). Per-topic'e sonra geçersin.

## Mercurius Mapping

| Soru | Cevap (tasarım) |
|------|-----------------|
| Publisher nereye yazar? | Gateway -> leader'a forward (follower'a gelirse `NotLeader` + leader hint döner) |
| Commit ne zaman ACK? | Quorum ACK sonrası. `Publish` gRPC `ACK` ancak commit sonrası döner — latency artar ama durability garanti |
| Subscribe nereden okur? | Her node kendi WAL'ından okuyabilir (read scaling), ama `from_offset` için leader'ın commit index'i referans |
| Node ekle/çıkar | Helm `replicas: 3 -> 5` -> Raft `AddVoter` RPC |

## Araştırma Görevleri

- [ ] Raft paper: "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout) — oku, 20 sayfa
- [ ] `hashicorp/raft` `raft.go` + `log.go` kaynak oku (log replication kısmı)
- [ ] Jepsen Raft testleri: nasıl partition test ediliyor
- [ ] `memberlist` SWIM paper: failure detection nasıl

## Karar Bekliyor — ADR-002

| Soru | Seçenekler |
|------|------------|
| Algoritma | Raft vs VR vs Chain vs Gossip-only |
| Implementasyon | hashicorp/raft vs etcd/raft vs from-scratch |
| Log granularitesi | Single global vs per-topic vs partitioned |
| Commit ACK stratejisi | Quorum sonrası ACK vs async (riskli) |
| Storage | WAL ile Raft log aynı dosya mı ayrı mı |

> Hepsi `proposed`.
