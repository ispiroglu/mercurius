# ADR-001: Persistence — WAL

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/02-persistence.md

## Bağlam

v1'de `Broker{TopicRepository, retriedEvents sync.Map}` tamamen in-memory — restart = veri kaybı. `internal/broker/topic.go:29` `select/default` drop sessiz kayıp. WAL olmadan durability, replay ve recovery mümkün değil. Hangi persistence motoru / segment formatı seçilmeli? From-scratch yazma maliyeti ne?

## Port

- **Port Interface:** `internal/port/wal.go: WAL` — one interface, one file location (future `internal/port/wal.go`)
- **Signature:** `type WAL interface { Append(topic string, e Event) (Offset uint64, error); Read(topic string, from Offset, limit int) ([]Event, error); Replay(topic string) iter.Seq2[Event, error]; Sync() error; Close() error }` — `Replay` push-channel değil pull iterator (`iter.Seq2`, Go 1.23+; hedef Go 1.27): aynı goroutine'de zero-copy akış, caller `break` edince `yield` false döner; CRC bozuk record'da `(Event{}, ErrCorrupt)` yield edilip stream sonlanır.
- **Scope:**
  - Owns: segment file layout, per-record CRC + truncate-to-last-valid recovery (torn write'ta son geçerli offset'e geri sar), fsync lifecycle
  - Owns: buffer lifecycle — `Append` çağıran taraf `Sync` onayından sonra veriyi unutabilir; adapter buffer'ı kendi havuzunda (`sync.Pool`/arena) yönetir
  - Adapter-internal: reactive flush pipeline (Pebble WriteQueue tarzı — producer'lar kuyruğa teslim eder, tek flusher coalescing ile TEK write+fsync, bekleyenlere sync callback); arena/buffer reuse implementasyon detayıdır, port'a sızmaz
  - NOT owns: index/cache (ayrı port), consensus/quorum (ADR-002 `Consensus` port), batch/sync politika knob'ları (ConfigSource ADR-012)
- **Adapters:** Pebble LSM, BadgerDB, RocksDB/grocksdb, Kafka segment klonu, from-scratch file+CRC+fsync

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| Pebble LSM | Battle-tested (CockroachDB), Go native, range scan hızlı | LSM compaction tuning karmaşık, WAL değil LSM | 0 hafta (kütüphane) | Hızlı durability, Go ekibi LSM'i öğrenmeye gerek yoksa | embedded-kv |
| BadgerDB | Go native, LSM + value log, iyi write throughput | GC tuning, Dgraph'a bağımlı tasarım | 0 hafta | Write-heavy, pure-Go isteniyorsa | embedded-kv |
| RocksDB/grocksdb | En olgun LSM, yüksek performans, wide adoption | CGo, build karmaşıklığı, Go değil | 0 hafta + CGo maliyeti | Maksimum perf, CGo kabul edilebilirse | cgo-lsm |
| Kafka segment klonu | Segment + index formatı kanıtlı, replay çok hızlı | Kendi implementasyonu gerekir, compaction yok | 2-3 hafta, ~1000 LOC (segment+index) | Kafka uyumlu arşiv/replay isteniyorsa | segment-log |
| from-scratch file+CRC+fsync | Segment/CRC/fsync/recovery tam öğrenme, sıfır bağımlılık | En yüksek risk, fsync/recovery bug'ları pahalı | 3-4 hafta, ~1500 LOC | Öğrenme ve sıfır-dep hedefi (v2 felsefesi) | from-scratch |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- Pebble: https://github.com/cockroachdb/pebble
- BadgerDB: https://github.com/dgraph-io/badger
- Kafka log format: https://kafka.apache.org/documentation/#log
- `proto/Event.proto:1-12` — Event gövdesi WAL'a yazılacak payload
