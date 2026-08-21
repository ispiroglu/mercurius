# 07 — Testing Strategy (İnanılmaz Testsuite)

> **Hedef:** `go test ./...` tek komutla her şey, flaky 0, coverage %80+.

## Mevcut Durum

*   2 dosya, 6 test, hepsi integration (`test/event_routing_test.go:53`), `time.Sleep` ile senkronizasyon, `reflect.DeepEqual` + `assert.Equal(true, ...)` anti-pattern
*   `go test` CI'da kapalı (`ci.yml:36`), coverage %5-10
*   `kafka-cmp` ad-hoc `main.go` (benchmark değil), `rabbitmq-cmp` kırık (main.go yok)

## Katmanlar

| Katman | Ne Test Eder | Tool | Nerede Koşar |
|--------|--------------|------|--------------|
| **Unit** | `wal`, `raft/state`, `consume/retry`, `stream_pool` — mock, table-driven | `go test -race -count=100` | Her push, 30s |
| **Integration** | Gerçek WAL + Raft 3 node (testcontainers-go), gRPC publish/subscribe | `testcontainers-go` + `ghz` | Her push, 2m |
| **Contract** | Proto breaking change | `buf breaking` | PR'da |
| **E2E** | `helm install` -> kind cluster -> publish 10k -> verify | `kind` + `helm test` + `k6` | Nightly + pre-release |
| **Bench** | Throughput (msg/s), p99 latency, WAL fsync latency | `testing.B` + `ghz` + `benchstat` | Nightly |
| **Chaos** | `kill -9 leader`, `partition 2-1`, `WAL corruption`, `slow disk` | `toxiproxy` + `pumba` | Nightly |
| **Property** | `rapid` (Go quickcheck) — "her publish en az 1 ack" | `p casing/rapid` | PR'da |

## Unit Örnek

```go
func TestWAL_AppendAndReplay(t *testing.T) {
    w := NewWAL(t.TempDir(), 64<<20)
    for i := 0; i < 100; i++ {
        _ = w.Append(&Event{ID: fmt.Sprint(i), Topic: "t", Body: []byte("hi")})
    }
    w.Close()
    // crash simulation: reopen
    w2 := OpenWAL(t.TempDir())
    events, _ := w2.Replay(0)
    assert.Len(t, events, 100)
}
```

## Integration Örnek (testcontainers)

```go
func TestCluster_PublishQuorum(t *testing.T) {
    cluster := NewTestCluster(t, 3) // 3 container
    leader := cluster.Leader()
    _ = leader.Publish("t", []byte("hi"))
    // kill leader
    cluster.Kill(leader)
    newLeader := cluster.WaitLeader(5 * time.Second)
    events := newLeader.Subscribe("t", 0)
    assert.Contains(t, events, "hi") // data not lost
}
```

## Chaos Matrisi

| Senaryo | Beklenti |
|---------|----------|
| Leader `kill -9` mid-replicate | Yeni leader seçilir, mesaj kaybolmaz |
| Network partition 2-1 (toxiproxy) | Minority yazamaz (`NotLeader`), majority devam |
| WAL segment corrupt (son byte flip) | Recovery truncate, önceki mesajlar korunur |
| Slow disk (toxiproxy latency 500ms) | `fsync latency` metric artar, ama ACK gecikir (timeout değil) |
| Consumer `Nack` 5 kez | DLQ'ya gider, `consume_log` dlq |

## Coverage Hedef

*   `internal/wal`: %85
*   `internal/raft`: %80 (election en zor)
*   `internal/stretch`: %70
*   Toplam: %80, `go test -coverprofile` + `codecov`

## CI

```yaml
# .github/workflows/ci.yml (v2)
jobs:
  lint: { uses: golangci/golangci-lint-action@v6 }
  unit: { run: go test ./... -race -count=1 -coverprofile=cover.out }
  contract: { run: buf breaking --against origin/master }
  integration: { run: go test ./test/integration -tags=integration -count=1 }
  bench: { if: nightly, run: go test ./test/bench -bench=. -benchmem }
```

## Araştırma Görevleri

- [ ] `testcontainers-go` ile etcd/raft test örnekleri
- [ ] `buf` proto lint/breaking
- [ ] `toxiproxy` Go client
- [ ] `rapid` property test
