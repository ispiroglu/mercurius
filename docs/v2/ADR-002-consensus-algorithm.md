# ADR-002: Consensus Algoritması

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/03-consensus-replication.md

## Bağlam

v1 single-node — quorum, leader election, log replication yok. Multi-broker için consensus şart (`docs/Project-Scope-and-Procedure.md:7` replikasyon vaadi). Hangi algoritma? Raft mı Paxos varyantları mı? From-scratch Raft maliyeti ne kadar?

## Port

- **Port Interface:** `internal/port/consensus.go: Consensus` — one interface, one file location (future `internal/port/consensus.go`)
- **Signature:** `type Consensus interface { Propose(cmd []byte) (Index uint64, err error); ReadIndex() (Index uint64, err error); AddVoter(id string, addr string) error; RemoveVoter(id string) error; Leader() (id string, isLeader bool) }`
- **Scope:**
  - Owns: term, leader election, quorum, membership (AddVoter/RemoveVoter)
  - NOT owns: storage/WAL (ADR-001 `WAL` port), log distribution topology (ADR-003 `Replicator` port)
- **Adapters:** Raft, Multi-Paxos, Zab, VR, EPaxos, from-scratch Raft

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| Raft | Anlaşılması en kolay, geniş ekosistem (etcd/raft, hashicorp/raft), leader-based basit | Leader bottleneck, tek leader yazma | 6-8 hafta, ~3000 LOC (from-scratch) / 0 hafta (kütüphane) | Varsayılan — öğrenmesi ve debug'ı en kolay | leader-based |
| Multi-Paxos | Leader'lı Paxos, Raft'a benzer ama daha esnek | Anlaması zor, implementasyon hataya açık | 6-8 hafta, ~3000 LOC | Paxos ailesini öğrenmek isteniyorsa | leader-based |
| Zab (ZooKeeper) | Raft'a çok benzer, ZooKeeper kanıtlı | Ekosistem dar, Go implementasyonu az | 5-7 hafta | ZK uyumluluğu gerekirse | leader-based |
| VR (Viewstamped Replication) | Basit, reconfiguration iyi | Ekosistem küçük, dokümantasyon az | 5-7 hafta | VR paper'ını takip etmek isteniyorsa | leader-based |
| EPaxos | Leaderless, yüksek concurrency, düşük latency | Çok karmaşık, implementasyon zor, conflict resolution | 10-12 hafta, ~5000 LOC | Leaderless ve yüksek write concurrency şartsa | leaderless |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- Raft paper: Ongaro & Ousterhout 2014
- Paxos Made Simple — Lamport
- EPaxos: Moraru et al. 2013
- Zab: Junqueira et al.
