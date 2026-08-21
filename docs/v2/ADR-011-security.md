# ADR-011: Security — Auth & TLS

- **Durum:** proposed
- **Tarih:** 2026-08-21
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/05-operability.md

## Bağlam

v1 `pkg/client/grpc_dial.go:8` `insecure.NewCredentials()` — TLS yok, auth yok, ACL yok. Multi-tenant topic için per-topic authz, mTLS ve JWT gerekli. Hangi auth modeli? mTLS mi JWT mi SPIFFE mi?

## Port

- **Port Interface:** `internal/port/auth.go: Authenticator + TLSProvider` — one interface, one file location (future `internal/port/auth.go`)
- **Signature:** `type Authenticator interface { Authenticate(ctx context.Context) (Principal, error); Authorize(p Principal, topic string, op string) error }` + `type TLSProvider interface { ServerConfig() *tls.Config; ClientConfig() *tls.Config }`
- **Scope:**
  - Owns: authn/authz/TLS config, principal/ACL evaluation
  - NOT owns: transport framing (ADR-006 `Transport` port), config distribution (ADR-012)
- **Adapters:** mTLS, JWT, ACL per topic, No auth (v1 insecure grpc_dial.go:8), SPIFFE

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| mTLS | Güçlü authn, transport-level güvenlik, standard | Cert yönetimi karmaşık, rotation gerekir | 2-3 hafta | Service-to-service, güçlü authn şartsa | cert-based |
| JWT | Stateless, scalable, geniş adoption | Token lifecycle, revocation karmaşık | 1-2 hafta | User-facing, stateless auth isteniyorsa | token-based |
| ACL per topic | Fine-grained topic authz, multi-tenant | ACL store ve sync gerekir | 2 hafta | Per-topic multi-tenant authz şartsa | acl |
| No auth (v1 insecure grpc_dial.go:8) | Sıfır maliyet, hızlı | Güvensiz, production'a uygun değil | 0 hafta | Dev/test only | none |
| SPIFFE | Workload identity, otomatik cert, modern | Ekosistem genç, SPIRE kurulumu gerekir | 3-4 hafta | Workload identity ve otomatik rotation isteniyorsa | workload-identity |

## Karar

> Henüz verilmedi — proposed durumda bırak.

## Sonuçlar

Karar verilince doldurulacak.

## Referanslar

- `pkg/client/grpc_dial.go:8` — insecure dial
- SPIFFE/SPIRE docs
- mTLS in gRPC
