# 02 — Persistence

> **Soru:** Mesaj diske ne zaman, nasıl, ne formatta yazılır? Crash olursa nasıl geri gelir?
> **Karar yok** — sadece harita. ADR-001'de seçilecek.

## Neden Lazım? (v1'de Yok)

v1'de `Broker.Publish` (`broker.go:29`) direkt `StreamPool.Ch`'ye (unbuffered chan) yazar. Process ölürse **her şey gider**. `retriedEvents sync.Map` de memory'de. Restart = veri kaybı.

v2'de `Publish` şu sırayla olmalı:

```
Client.Publish -> Gateway -> WAL append + fsync -> MemTable -> ACK
                                |-> background: MemTable -> SSTable (flush) -> compaction
```

Crash sonrası: `WAL replay` ile state geri gelir. Bu olmadan replication'ın da anlamı yok — follower'a neyi replike edeceksin?

## Kavramlar

| Kavram | Nedir | Neden Lazım |
|--------|-------|-------------|
| **WAL (Write-Ahead Log)** | Her yazma önce append-only dosyaya yazılır, sonra memory'e | Crash recovery. `fsync` olmadan WAL = yalan |
| **fsync / fdatasync** | Kernel buffer'ı diske zorla | Power loss'ta veri garantisi. Batch `group commit` ile amortize edilir |
| **Segment** | WAL sabit boyutta parçalara bölünür (örn 64MB) | Rotation, truncation, GC. Tek dev dosya yönetilemez |
| **CRC32/XXHash** | Her record sonuna checksum | Torn write / corruption tespiti. Recovery'de corrupt tail truncate edilir |
| **MemTable** | Memory'de sorted map (skiplist/B-tree) | WAL'dan sonra hızlı okuma/yazma |
| **SSTable** | MemTable dolunca diske sorted string table olarak flush | Sequential read, binary search |
| **LSM-tree** | MemTable + SSTable + compaction | Write-heavy workload (broker) için ideal |
| **B-Tree** | In-place update tree | Read-heavy için iyi, write'da random I/O |
| **Compaction** | SSTable'ları merge edip garbage toplama | Space amplification kontrolü |
| **Snapshot + Log Truncation** | Raft log'u snapshot'la kesme | Sonsuz WAL büyümesini engelleme |
| **O_DIRECT / io_uring** | Kernel page cache bypass | Tail latency kontrolü (ileri seviye) |

## Mercurius Mapping

| Nokta | Ne Olur |
|-------|---------|
| `Publish(topic, body)` | `WAL.append({topic, offset, body, crc}) -> fsync (batch) -> index update -> fanout` |
| `Subscribe(topic, from_offset)` | `WAL replay (historical) + live tail (channel)` |
| `expiresAt` (proto'da ölü alan) | TTL GC: compaction sırasında `now > expiresAt` ise drop |
| `offset` (v1'de yok, eklenecek) | Per-topic monotonik `uint64`. `Event` proto'ya ekle |
| `partition` (opsiyonel) | Topic içi sharding. Başta 1 partition, sonra eklenir |

Proto değişimi (breaking):

```proto
message Event {
  string id = 1;
  string topic = 2;
  bytes body = 3;
  google.protobuf.Timestamp createdAt = 4;
  uint32 expiresAt = 5;
  uint64 offset = 6;      // NEW: per-topic sequence
  uint32 partition = 7;   // NEW: default 0
  string producer_id = 8; // NEW: idempotency için
}
```

## Modern Alternatifler

| Alternatif | Tip | Artı | Eksi | Go Uyumu | Ne Zaman |
|------------|-----|------|------|----------|----------|
| **Pebble** | LSM (CockroachDB) | Pure Go, WAL+LSM tek paket, `fsync` batch, pebbledb.org dokümantasyon iyi | API büyük, compaction tuning gerek | ✅ native | Öneri olmayan, ama en dengeli |
| **BadgerDB** | LSM (Dgraph) | Hızlı write, value log separation | GC sorunlu, space amp yüksek, Dgraph ekibi bakıyor ama tartışmalı | ✅ native | Prototip hızlı lazım ise |
| **RocksDB** | LSM (Facebook) | En olgun, en hızlı, tuning çok | CGO, cross-compile eziyet, Go wrapper `grocksdb` CGO | ⚠️ CGO | Max perf, CGO kabul ise |
| **LevelDB** | LSM (Google) | Basit, küçük | Eski, tek thread compaction, Pebble superset'i | ✅ `syndtr/goleveldb` | Öğrenme için basit |
| **SQLite WAL mode** | B-Tree + WAL | SQL, `consume_log` için ideal, tek dosya | Write throughput düşük, WAL file + shm | ✅ `mattn/go-sqlite3` CGO | Sadece consume tracking için |
| **Postgres** | B-Tree + WAL | Zaten consume_log için lazım, SQL zengin | Broker WAL için yavaş, vacuum, connection overhead | ✅ `pgx` | WAL değil, index/tracking için |
| **Turso / libSQL** | SQLite fork | Edge, replication built-in | Çok yeni, broker WAL için kanıtlanmamış | ⚠️ | Edge case |
| **Redpanda / Kafka segment log** | Segment log | Broker'a en yakın tasarım, sıfır LSM | Kendi implementasyonun = Redpanda'yı klonlamak | — | İlham alınacak tasarım |
| **NATS JetStream FileStore** | File per subject | NATS'ın Go file store'u, basit | JetStream'e özgü, generic değil | ✅ NATS code | İlham |
| **FoundationDB** | Ordered KV + log | Dağıtık, test edilmiş | Operasyonel ağır, Go binding zayıf | ⚠️ | Overkill |
| **From-scratch WAL** | File + CRC + fsync | Öğrenme max, dependency 0, format senin | 3-4 hafta, fsync, truncation, corruption handling zor | ✅ | **Senin hedefin** |

## From-Scratch WAL — Ne Yazacaksın?

Eğer hazır LSM kullanmazsan (hedefin bu), yazman gereken:

```
internal/wal/
  segment.go      // file layout: [magic 4B][record_len 4B][crc 4B][payload ...]
  wal.go          // Append(event) -> segment, Fsync(), Rotate() on 64MB
  reader.go       // Replay: open segments in order, verify CRC, truncate corrupt tail
  index.go        // offset -> (segment_id, position) map, memory + snapshot
  gc.go           // expiresAt + retention (örn 7 gün) -> segment delete
  batch.go        // group commit: 1ms window içinde gelen N publish tek fsync
```

Maliyet tahmini: **3-4 hafta**, ~1500-2000 LOC, en zor kısım `crash recovery + torn write` testi (chaos test: `kill -9` ortasında yazarken).

**Önerilen yol (karar değil, not):** Önce from-scratch `WAL` + `memory index` yap, LSM'i sonra ekle. Başta `WAL + in-memory map[offset]Event` yeterli — compaction olmadan da çalışır, sadece restart'ta replay yavaş.

## Araştırma Görevleri (Sen Yap)

- [ ] Pebble `wal` paketi kaynak oku: `cockroachdb/pebble` `record/log_writer.go`
- [ ] Kafka log segment format: `RecordBatch` + CRC + offset
- [ ] `man 2 fsync` + `open(O_SYNC)` farkı, `fdatasync` ne zaman yeter
- [ ] Badger `value log GC` neden eleştiriliyor — issue'ları oku

## Karar Bekliyor — ADR-001

| Soru | Seçenekler |
|------|------------|
| WAL formatı | Kendi binary format vs Pebble WAL reuse vs Kafka RecordBatch klonu |
| Index | Memory map vs Pebble LSM vs Badger |
| Consume log storage | Aynı WAL vs Postgres vs SQLite |
| Fsync stratejisi | Her Publish fsync vs group commit (1ms/100 mesaj) vs async (riskli) |

> Şu an hepsi `proposed`. Araştır, `ADR-001` oluştur.
