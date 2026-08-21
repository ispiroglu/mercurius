# ADR-012: Operability — Config & Deploy

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/05-operability.md

## Bağlam

v1 tek binary, config ve deploy soyutlanmamış. Production için Helm StatefulSet + PVC + PDB + headless service, Kustomize veya Operator gerekli. Hangi deploy modeli? Helm mi Kustomize mı Operator mü? `chart/mercurius/` layout'u ne olacak?

## Port

- **Port Interface:** `internal/port/config.go: ConfigSource` — one interface, one file location (future `internal/port/config.go`)
- **Signature:** `type ConfigSource interface { Load(path string) (Config, error); Watch() <-chan Config }`
- **Scope:**
  - Owns: config loading/watching, deploy manifests (Helm/Kustomize/Operator)
  - NOT owns: observability/metrics (ADR-013 `Metrics` port), TLS (ADR-011)
- **Adapters:** Helm StatefulSet+PVC+PDB+Headless, Kustomize, Operator/kubebuilder, systemd single binary, Compose dev only

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| Helm StatefulSet+PVC+PDB+Headless | Standard K8s deploy, PVC ile durable, PDB ile availability | Helm templating karmaşık, chart maintenance | 2-3 hafta | Production K8s, standard Helm isteniyorsa | helm-k8s |
| Kustomize | Basit overlay, Helm'e göre daha az templating | Helm kadar zengin değil, chart ekosistemi yok | 1-2 hafta | Basit K8s deploy, Helm'siz isteniyorsa | kustomize-k8s |
| Operator/kubebuilder | Otomatik lifecycle, CRD ile topic yönetimi | En karmaşık, kubebuilder öğrenme eğrisi | 4-6 hafta | Otomatik topic/partition lifecycle isteniyorsa | operator |
| systemd single binary | En basit, K8s yok, bare-metal uyumlu | Scale ve HA yok, single node | 1 hafta | Bare-metal/single-node isteniyorsa | systemd |
| Compose dev only | Dev için hızlı, basit | Production'a uygun değil | 0.5 hafta | Sadece local dev | compose-dev |

### Helm Chart Layout

```
chart/mercurius/
  Chart.yaml
  values.yaml
  templates/
    statefulset.yaml   # WAL PVC, headless service
    service.yaml       # headless + client service
    pdb.yaml           # PodDisruptionBudget
    configmap.yaml     # mercurius.yaml
    secret.yaml        # TLS certs
```

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- Helm best practices
- StatefulSet + PVC + PDB pattern
- `05-operability.md`
