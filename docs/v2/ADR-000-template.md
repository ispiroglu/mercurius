# ADR-000: Başlık

- **Durum:** proposed | accepted | rejected | superseded
- **Tarih:** YYYY-MM-DD
- **Yazar:** @ispiroglu
- **İlgili Doküman:** docs/v2/0X-*.md

## Bağlam

Neden bu karar gerekiyor? Hangi sorun, hangi kısıt?

## Port

- **Port Interface:** `package/path: InterfaceName` — one interface, one file location (future `internal/port/*.go`)
- **Signature:** Go interface literal, e.g. `type WAL interface { Append(Event) (Offset, error); Replay(Offset) (<-chan Event, error); Close() error }`
- **Scope:** what the port owns vs what it explicitly does NOT own (scope boundary, 2 bullet lines)
- **Adapters:** list of adapter candidates that will be compared in Alternatifler table (names must match table rows)

## Alternatifler

| Alternatif | Artı | Eksi | From-Scratch Maliyet | Ne Zaman Seçilir | Adapter Type |
|------------|------|------|---------------------|------------------|--------------|
| A | | | örn: 3 hafta, 800 LOC | | |
| B | | | | | |
| C (kendin yaz) | öğrenme max | risk max | | | |

## Karar

> Henüz verilmedi — `proposed` durumda bırak.

Seçilen alternatif ve gerekçesi buraya yazılacak.

## Sonuçlar

Karar verildikten sonra ne kazanıldı, ne kaybedildi, geri dönüş maliyeti ne?

## Referanslar

- Linkler, paper'lar, blog postlar
