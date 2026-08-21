# 05 — Operability (Helm, Self-Hostable, Observability)

> **Soru:** Kullanıcı `helm install mercurius` tek komutla nasıl kurar? Neyi görür, nasıl upgrade eder?
> **Karar yok** — ADR-005'te seçilecek.

## Helm — Nedir, Neden?

Helm = K8s için paket yöneticisi. Chart = template + values.

```
chart/mercurius/
  Chart.yaml          // name: mercurius, version: 0.2.0, appVersion: v2
  values.yaml         // default config (replicas, storage, dc)
  values.schema.json  // validation
  templates/
    statefulset.yaml  // broker nodes (3 replika, PVC)
    service.yaml      // headless + gateway
    configmap.yaml    // mercurius.yaml
    pdb.yaml          // PodDisruptionBudget
    servicemonitor.yaml // prometheus-operator
    tests/            // helm test
  NOTES.txt           // install sonrası çıktı
```

**Neden StatefulSet, Deployment değil?**

| Resource | Ne Zaman |
|----------|----------|
| **StatefulSet** | Broker node (stable identity `mercurius-0`, stable storage `pvc-mercurius-0`, ordered start) |
| **Deployment** | Gateway (stateless, scale kolay), Stretch Gateway |
| **Headless Service** | Raft peer discovery (`mercurius-0.mercurius-headless:9000`) |
| **PVC** | Per-node WAL storage (`10Gi` default) |
| **PDB** | `minAvailable: 2` (3 node'da 1 node drain edilebilir, quorum korunur) |
| **HPA** | Gateway için (broker için HPA yok — stateful) |

Tek komut:

```bash
helm repo add mercurius https://ispiroglu.github.io/mercurius
helm install my-broker mercurius/mercurius \
  --set cluster.nodes=3 \
  --set storage.size=20Gi \
  --set stretch.enabled=true \
  --set stretch.peerDC=gateway.dc2.example.com
```

## Self-Hostable — Kolay Kurulum

| Katman | Seçenekler | Not |
|--------|------------|-----|
| **Single binary** | `./mercurius --config mercurius.yaml` | Docker olmadan, systemd ile |
| **Container** | `ghcr.io/ispiroglu/mercurius:v2` | Distroless, non-root (v1'de `appuser 10001` var) |
| **Compose** | `docker compose up` (dev için) | Prod değil, sadece local |
| **Helm** | K8s prod | Asıl hedef |

Config:

```yaml
# mercurius.yaml
node:
  id: node-1
  dc: dc1
  listen: 0.0.0.0:9000
wal:
  dir: /data/wal
  segmentSize: 64MiB
  fsync: batch # every | batch | async
raft:
  bootstrap: true # ilk kurulumda
  peers: [mercurius-0:9000, mercurius-1:9000, mercurius-2:9000]
stretch:
  enabled: false
  peer: gateway.dc2:9000
  tls: true
```

Upgrade: `helm upgrade my-broker mercurius/mercurius --version 0.3.0` -> StatefulSet rolling update (ordered, `partition: 0`).

## Observability — Neyi Ölçersin?

### Metrics (Prometheus, `/metrics`)

| Metrik | Tip | Açıklama |
|--------|-----|----------|
| `mercurius_publish_latency_ms` | histogram | p50/p99, label `topic` |
| `mercurius_wal_fsync_latency_ms` | histogram | fsync süresi |
| `mercurius_raft_commit_latency_ms` | histogram | quorum commit süresi |
| `mercurius_subscribe_lag` | gauge | `latest_offset - consumer_offset` per subscriber |
| `mercurius_dlq_depth` | gauge | Dead letter queue boyutu |
| `mercurius_retry_total` | counter | Retry count, label `topic, status` |
| `mercurius_stretch_lag_ms` | gauge | Inter-DC replication lag |
| `mercurius_wal_segments` | gauge | Segment sayısı, disk kullanımı |

v1'de sadece `promhttp.Handler` var (`mercurius.go:40`), metrik 0.

### Tracing (OTEL + Tempo)

```
Publish span: gateway -> WAL -> Raft -> ACK (trace id propagate)
Subscribe span: replay -> live tail -> handler -> ACK
Stretch span: tailer -> ship -> ingest
```

OTEL SDK: `go.opentelemetry.io/otel`, exporter OTLP -> Tempo (`prometheus/tempo.yaml` var ama entegrasyon yok).

### Logs (Zap + Loki)

v1'de `logger.NewLogger()` per-struct, `DevelopmentConfig` renkli console. v2'de:

*   JSON prod, console dev (`--log-format json`)
*   `trace_id` her log'a ekle (OTEL baggage)
*   Loki `promtail` ile toplanır

### Grafana

`prometheus/datasource.yaml` var ama Grafana servisi yok. v2 Helm chart'a `grafana` dependency veya `ServiceMonitor` ekle.

## Modern Alternatifler

| Tool | Ne İçin | Artı/Eksi |
|------|---------|-----------|
| **Helm** | K8s paket | Standart, herkes bilir. Template karmaşık olabilir |
| **Kustomize** | K8s overlay | Helm'siz, basit. Ama paket değil, repo klonlamak gerek |
| **Nix** | Reproducible build | Güçlü ama öğrenme eğrisi dik, K8s entegrasyonu zayıf |
| **Operator (kubebuilder)** | Custom controller | Helm'den güçlü (auto-failover logic), ama yazması 2-3 hafta |
| **Docker Compose** | Local dev | Helm yerine değil, yanında |
| **OTEL** | Tracing | Standart, Tempo/Jaeger uyumlu |
| **Prometheus + Grafana** | Metrics | Standart |
| **Loki** | Logs | Prometheus gibi, Grafana ile entegre |

**From-scratch maliyet:** Helm chart **1 hafta**, Operator **+2-3 hafta**. Başta Helm yeterli, operator sonra.

## Araştırma Görevleri

- [ ] Helm chart best practices: `helm create` + `helm lint` + `helm template`
- [ ] StatefulSet `volumeClaimTemplates` nasıl çalışır — PVC per pod
- [ ] OTEL Go `TracerProvider` + `Propagator` gRPC interceptor ile nasıl eklenir
- [ ] `ServiceMonitor` vs `PodMonitor` farkı (prometheus-operator)

## Karar Bekliyor — ADR-005

| Soru | Seçenekler |
|------|------------|
| Paket | Helm vs Kustomize vs Operator (başta Helm, sonra Operator?) |
| Config | File vs env vs flag (cobra/viper vs koanf) |
| Observability | OTEL+Tempo vs sadece Prometheus vs Datadog |
| Storage class | Dynamic PVC vs hostPath (dev) |

> Hepsi `proposed`.
