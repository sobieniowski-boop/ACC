# Profit v2 update (2026-03-05, pre CM refactor)

Ten wpis dokumentuje zmiany dowiezione po ostatniej aktualizacji i przed kolejnym twardym przepieciem modelu CM1/CM2/NP.

## 1) ASIN-first i parent rollups

Backend `profit/v2/products`:
- domyslny `group_by` ustawiony na `asin_marketplace`,
- wspierane tryby: `asin_marketplace`, `asin`, `parent_marketplace`, `parent`, plus tryby SKU do debugu.

Frontend `Product Profit Table`:
- domyslnie pracuje na ASIN+MP,
- ma przelacznik grupowania ASIN/Parent/SKU,
- akcje (drilldown/pricing/content/task) w trybach agregowanych uzywaja `sample_sku`.

## 2) Spojnosc API typow grup

W modelach API/UI dodano pola grupowania:
- `entity_type`,
- `group_key`,
- `sample_sku`,
- `parent_asin`,
- `sku_count`,
- `child_count`,
- dla what-if dodatkowo `offer_count`.

Cel: jednoznacznie odroznic wiersz SKU od agregatu ASIN/Parent i uniknac blednych akcji na technicznym kluczu grupy.

## 3) KPI dashboard: fallback CM na poziomie order

W KPI fallback dla brakujacego `contribution_margin_pln` liczony jest z:
- `revenue_pln - cogs_pln - amazon_fees_pln - ads_cost_pln - logistics_pln`.

To ogranicza sytuacje, gdzie dashboard pokazuje 0 lub losowe odchylenia przy niepelnym polu marzy na naglowku zamowienia.

## 4) Top drivers: ASIN-first

Top drivers/leaks zostaly przepiete na logike ASIN-first (zamiast twardego SKU-only), z zachowaniem filtrow globalnych i zgodnoscia z tabela produktowa v2.

## 5) Stan na ten moment (przed kolejnym krokiem)

- warstwa grupowania i nawigacji jest juz produkcyjna,
- warstwa logistyczna what-if (Plan/Observed/Decision) jest wdrozona,
- kolejny krok to twarde dopiecie semantyki kosztowej CM1/CM2/NP (returns_net, FBA storage components, overhead pools).

## 6) Post-refactor (CM semantics hard switch completed)

Zakres wdrozenia:
- backend `profit_engine.py`:
  - `ensure_profit_cost_model_schema()` tworzy/utrzymuje:
    - `acc_profit_cost_config` (np. `return_handling_per_unit_pln`),
    - `acc_profit_overhead_pool` (okres, MP, metoda alokacji, confidence),
  - `get_product_profit_table()`:
    - CM1: `revenue - cogs - amazon_fees - logistics`,
    - CM2: `CM1 - ads - returns_net - storage - aged - removal - liquidation`,
    - NP: `CM2 - overhead_allocated`,
  - `get_product_what_if_table()`:
    - te same komponenty CM1/CM2/NP co w realized,
    - grupowania ASIN/Parent przeliczone na nowej formule (usuniety stary fallback `cm2 = cm1 - ads - logistics`),
    - summary zwraca pelne totals: returns, FBA components, overhead, NP.
- schematy API (`apps/api/app/schemas/profit_v2.py`) i typy frontendu (`apps/web/src/lib/api.ts`):
  - dodane pola komponentowe CM2 i NP dla item/summary.
- UI `ProductProfitTable.tsx`:
  - realized: kolumny `Returns Net`, `FBA Storage/Aged/Removal/Liquidation`, `Overhead`, `NP`.
  - what-if:
    - sortowanie/loss filter respektuje wybrany `profit_mode` (CM1/CM2/NP),
    - tabela pokazuje komponenty CM2 + NP,
    - kafle summary pokazuja wybrany mode oraz CM2/NP rownolegle.

Weryfikacja techniczna:
- `python -m py_compile apps/api/app/services/profit_engine.py apps/api/app/schemas/profit_v2.py` -> OK
- `npm --prefix apps/web run -s build` -> OK

## 7) Incremental update (2026-03-06)

Zakres zmian po hard-switch CM:

- `Realized` revenue dla `profit/v2/products` obejmuje teraz takze `ShippingCharge` z finance feedu:
  - zrodlo: `acc_finance_transaction`,
  - alokacja na line-item po `item_price` (fallback po `quantity`),
  - aktywne tylko dla `MFN/FBM` (bez doliczania dla `AFN/FBA`).

- Do item-level API i UI dopiete metryki operacyjne:
  - `return_rate`,
  - `tacos`,
  - `days_of_cover`,
  - `shipping_match_pct`,
  - `finance_match_pct`,
  - oraz jawne pole `shipping_charge_pln`.

- Frontend `Product Profit Table`:
  - tryb `Realized` ma teraz rozwijanie `parent -> child` (jak w what-if),
  - child rows sa pobierane dynamicznie przez `parent_asin + group_by=asin_marketplace`,
  - dziala bez dodatkowego endpointu.

- Poprawka alokacji kosztow FBA component pools:
  - dodane marketplace-level weight totals,
  - usuniete zjawisko zawyzania storage/aged/removal/liquidation w waskich filtrach.

Uwaga operacyjna:
- kod i typy sa zaktualizowane; brak twardego restartu procesu API byl celowy (zgodnie z decyzja operacyjna),
- jesli API nie dziala w hot-reload, nowe zmiany aktywuja sie po kolejnym restarcie procesu.

## 8) Full CM2/NP wiring + engine unification (2026-03-07)

Zakres zmian:

### 8a) Unifikacja silników profit — V1 → V2
- `app/api/v1/profit.py` przepisany od zera — trasy V1 (`/profit/orders`, `/profit/by-sku`, `/profit/export`) teraz delegują do V2 engine (`profit_engine.py`).
- `mssql_store.py` profit functions nie są już wywoływane (pozostają w pliku, ale route'y nie importują).

### 8b) Backfill amount_pln w acc_finance_transaction
- Skrypt `scripts/backfill_finance_amount_pln.py` zaktualizował **1,587,934 rekordów** z `amount_pln = 0`.
- Metoda: `UPDATE TOP(25000)` z `OUTER APPLY acc_exchange_rate` (binary search na dacie FX).
- Finance import (`order_pipeline.py`, `sync_service.py`) naprawiony — `amount_pln` obliczane przy INSERT.

### 8c) Klasyfikacja charge_types — `_classify_finance_charge()`
- Nowa funkcja mapująca ~49 charge_types z `acc_finance_transaction` do warstw P&L:
  - **CM2 layer:** 7 bucketów (storage, aged, removal, liquidation, refund_cost, shipping_surcharge, fba_inbound).
  - **NP layer:** 3 buckety (service_fee, adjustment, other_overhead).
  - **Skip:** Principal/Tax/ShippingCharge (revenue), Commission/FBAPerUnitFulfillmentFee (już na order_line jako CM1 fees).
- Zwraca `{"layer": "cm2"|"np", "bucket": str, "sign": 1|-1}` lub `None`.

### 8d) Rozszerzenie CM2 — 4 → 7 bucketów
- `_load_fba_component_pools()` przepisane — używa `_classify_finance_charge()` + `ft.amount_pln` (nie hardcoded FX).
- `_allocate_fba_component_costs()` przepisane — generyczny alokator z `_KEY_MAP`.
- Nowe buckety: `refund_cost → refund_finance_pln`, `shipping_surcharge → shipping_surcharge_pln`, `fba_inbound → fba_inbound_fee_pln`.
- Formuła CM2: `cm2 = cm1 - ads - returns - storage - aged - removal - liquidation - refund_fin - ship_surcharge - fba_inbound`.

### 8e) NP auto-detekcja z finance
- `_load_overhead_pools()` rozszerzony — ładuje z dwóch źródeł:
  1. `acc_profit_overhead_pool` (manual admin config).
  2. `acc_finance_transaction` — charge_types z `layer="np"` (ServiceFee, Adjustment, BalanceAdjustment, PostageBilling).
- Manualne poole mają pierwszeństwo (`pool_name` dedup).

### 8f) ShippingCharge w CM1 revenue
- Pool-based approach: łączna kwota per marketplace z finance, dystrybucja proporcjonalnie wg revenue share.
- Eliminuje heavy JOIN finance × order_line (timeout w poprzednim podejściu).
- `r["shipping_charge_pln"]` wstrzykiwane do produktów → `rev = line_revenue + shipping_charge_pln`.

### 8g) Schema API
- 3 nowe pola w `ProductProfitItem` (`schemas/profit_v2.py`): `refund_finance_pln`, `shipping_surcharge_pln`, `fba_inbound_fee_pln`.

### Wyniki testowe (1-7 marca 2026)
```
Revenue:   1,824,734.23 PLN
CM1:         864,441.92 PLN (47.4%)
CM2:         507,776.23 PLN (27.8%)
NP:          166,005.00 PLN  (9.1%)
```

Pełna dokumentacja techniczna: `docs/PROFIT_CM2_NP_WIRING_2026-03-07.md`.
