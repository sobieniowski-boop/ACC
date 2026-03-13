# Amazon Listing Registry

Status na `2026-03-03`.

## Cel

`Amazon Listing Registry` to staging i kanoniczny lookup tożsamości produktu na Amazonie.
Źródłem jest Google Sheet:

- `gid=400534387`
- zawiera m.in. `Merchant SKU`, `Nr art.`, `EAN`, `ASIN (ADSY)`, `Parent Asin`, `Parent/Child`, `Marka`, `Nazwa`

Registry nie jest źródłem kosztów ani fee.
Służy wyłącznie do:
- `SKU -> internal_sku`
- `SKU/EAN/ASIN` cross-map
- parent/child listing context
- wzbogacania `title_preferred`, `ean`, `parent_asin`
- naprawy brakujących mapowań produktu

## Tabele

- `dbo.acc_amazon_listing_registry`
- `dbo.acc_amazon_listing_registry_sync_state`

Najważniejsze pola:
- `merchant_sku`
- `merchant_sku_alt`
- `internal_sku`
- `ean`
- `asin`
- `parent_asin`
- `brand`
- `product_name`
- `listing_role`
- `launch_type`
- `category_1`
- `category_2`
- `row_hash`
- `source_gid`
- `synced_at`

## Sync

Serwis:
- `apps/api/app/services/amazon_listing_registry.py`

Job type:
- `sync_amazon_listing_registry`

Runtime:
- schema bootstrap w `apps/api/app/main.py`
- job runner w `apps/api/app/connectors/mssql/mssql_store.py`
- scheduler hook w `apps/api/app/scheduler.py`

Sync:
- pobiera CSV z Google Sheet
- liczy `source_hash`
- jeśli hash się nie zmienił, zwraca `skipped`
- jeśli źródło się zmieniło, odświeża snapshot dla `source_gid`

## Aktualny stan wdrożenia

Załadowany snapshot:
- `17 997` rekordów

Registry jest już używany w:
- `Missing COGS` i `Data Quality`
- fallback `SKU -> ISK -> Oficjalny XLSX`
- `AI Product Matcher` jako exact hint do zawężania kandydatów
- `order_pipeline`:
  - backfill `acc_product`
  - enrich existing `acc_product`
  - linkowanie `acc_order_line.product_id`
- `FBA Ops`:
  - inventory title/context
  - inbound line context
  - aged/stranded title fallback
- `Finance Center`:
  - ledger enrichment (`asin/internal_sku/title_preferred`)

## Ważne zasady

- Registry nie wpisuje kosztów do bazy.
- Registry nie zastępuje `acc_purchase_price`.
- AI może korzystać z registry do identyfikacji produktu, ale nie do zgadywania ceny zakupu.
- Oficjalna cena zakupu dalej pochodzi z:
  - `acc_purchase_price`
  - `00. Oficjalne ceny zakupu dla sprzedaży.xlsx`
  - holding / ERP
  - wpis manualny

## Produkcyjna interpretacja

To jest źródło prawdy o relacjach listingowych Amazon, a nie o finansach produktu.

Najważniejsze skutki:
- mniej brakujących `product_id`
- lepsze `title_preferred`
- lepsze `internal_sku` hints
- mniejsza zależność runtime od live Google Sheet

