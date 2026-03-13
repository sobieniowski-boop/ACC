# Profit CM2/NP Full Wiring (2026-03-07)

## Kontekst

Przed tą zmianą silnik P&L (`profit_engine.py`) miał architekturę CM1/CM2/NP, ale:
- CM2 uwzględniał tylko 4 buckety FBA (storage, aged, removal, liquidation)
- NP overhead pools były puste (acc_profit_overhead_pool = 0 wierszy)
- ShippingCharge (opłata za przesyłkę od klienta) nie była w revenue
- `amount_pln` w `acc_finance_transaction` = 0 wszędzie (1.59M rekordów)
- Istniały 3 równoległe silniki profit: `profit_engine.py` (V2), `mssql_store.py` (V1), `profit_service.py` (write-back)
- ~49 charge_types w finance, tylko 3 klasyfikowane

## Co zostało zrobione

### 1. Unifikacja silników — V1 routes → V2 engine
**Plik:** `app/api/v1/profit.py`

Przeredagowany od zera. Trasy `/profit/orders`, `/profit/by-sku`, `/profit/export` teraz korzystają z V2 engine:
- `_get_profit_orders_unified()` — order-level profit z formułami V2 (netto revenue on-the-fly)
- `/profit/by-sku` deleguje do `get_product_profit_table(group_by="sku")`
- Usunięte importy z `mssql_store.py`

### 2. Backfill amount_pln w finance transactions
**Skrypt:** `scripts/backfill_finance_amount_pln.py`

- Zaktualizowano **1,587,934 rekordów** w 386 sekund (~4,800 rows/s)
- Metoda: `UPDATE TOP(25000)` z `OUTER APPLY acc_exchange_rate` (binary search na dacie)
- Po backfillu: 0 rekordów z `amount_pln = 0`

### 3. Fix importu finance — amount_pln przy INSERT
**Pliki:** `app/services/order_pipeline.py`, `app/services/sync_service.py`

- `_insert_finance_rows()` w order_pipeline: dodane `_load_fx_cache()` + `_get_fx_rate()` (binary search)
- `sync_finances()` w sync_service: dodane `_calc_pln()` helper z preloadem kursów
- Wszystkie 3 punkty tworzenia `FinanceTransaction` teraz obliczają `amount_pln` i `exchange_rate`

### 4. Kompleksowa klasyfikacja charge_types
**Plik:** `app/services/profit_engine.py` — `_classify_finance_charge()`

Nowa funkcja klasyfikująca KAŻDY charge_type z `acc_finance_transaction` do layera P&L:

| Layer | Buckety | Przykładowe charge_types |
|-------|---------|--------------------------|
| CM2 | `fba_storage` | FBAStorageFee, FBALongTermStorageFee |
| CM2 | `fba_aged` | FBALongTermStorageFee (z transaction_type StorageFee) |
| CM2 | `fba_removal` | RemovalComplete, FBACustomerReturnPerUnitFee |
| CM2 | `fba_liquidation` | LiquidationsProceeds |
| CM2 | `refund_cost` | ReturnPostage, ReturnShipping, RestockingFee |
| CM2 | `shipping_surcharge` | ShippingHB, ShippingChargeback |
| CM2 | `fba_inbound` | FBAInventoryPlacementServiceFee |
| NP | `service_fee` | ServiceFee, SubscriptionFee |
| NP | `adjustment` | Adjustment, BalanceAdjustment |
| NP | `other_overhead` | PostageBilling, inne nieznane |
| Skip | — | Principal, Tax, ShippingCharge, ShippingTax (revenue) |
| Skip | — | Commission, FBAPerUnitFulfillmentFee (CM1 fees already on order_line) |

### 5. Rozszerzenie pool CM2 — `_load_fba_component_pools()`
**Plik:** `app/services/profit_engine.py`

Przed: ładował tylko 4 buckety (storage/aged/removal/liquidation) z hardcoded FX fallback.
Po: ładuje 7 bucketów CM2 używając `_classify_finance_charge()` i `ft.amount_pln` (backfilled).

Buckety:
```
storage, aged, removal, liquidation, refund_cost, shipping_surcharge, fba_inbound
```

### 6. Rozszerzenie alokacji CM2 — `_allocate_fba_component_costs()`
**Plik:** `app/services/profit_engine.py`

Generyczny alokator z `_KEY_MAP`:
```python
_KEY_MAP = {
    "storage": "fba_storage_fee_pln",
    "aged": "fba_aged_fee_pln",
    "removal": "fba_removal_fee_pln",
    "liquidation": "fba_liquidation_fee_pln",
    "refund_cost": "refund_finance_pln",
    "shipping_surcharge": "shipping_surcharge_pln",
    "fba_inbound": "fba_inbound_fee_pln",
}
```

Alokacja: AFN units (primary) → revenue (fallback) per marketplace.

### 7. NP overhead z finance transactions
**Plik:** `app/services/profit_engine.py` — `_load_overhead_pools()`

Teraz ładuje z DWÓCH źródeł:
1. `acc_profit_overhead_pool` — ręcznie skonfigurowane poole (admin)
2. `acc_finance_transaction` — auto-detekcja charge_types z `layer="np"` (service_fee, adjustment, other_overhead)

Manualne poole mają pierwszeństwo (by `pool_name`).

### 8. ShippingCharge w revenue CM1
**Plik:** `app/services/profit_engine.py`

Pool-based approach:
- Łączna kwota `ShippingCharge + ShippingTax` per marketplace z `acc_finance_transaction`
- Dystrybucja do produktów proporcjonalnie wg revenue share
- Szybkie zapytanie (~0.5s vs stary JOIN order_line: timeout)
- Wstrzykiwane do `r["shipping_charge_pln"]` przed loop przetwarzania
- Już podpięte: `rev = line_revenue + shipping_charge_pln`

### 9. Aktualizacja schematów API
**Plik:** `app/schemas/profit_v2.py`

Nowe pola w `ProductProfitItem`:
- `refund_finance_pln` — koszty zwrotów z finance (ReturnPostage itp.)
- `shipping_surcharge_pln` — surcharge/penalty Amazon (ShippingHB)
- `fba_inbound_fee_pln` — FBA inbound placement fee

### 10. Aktualizacja formuły CM2
```python
cm2 = cm1 - ads - returns_net
       - storage - aged - removal - liquidation
       - refund_finance - shipping_surcharge - fba_inbound
```

## Wyniki testowe (1-7 marca 2026)

```
Revenue:   1,824,734.23 PLN
CM1:         864,441.92 PLN (47.4%)
CM2:         507,776.23 PLN (27.8%)
NP:          166,005.00 PLN  (9.1%)
```

Przykład B0B2S7H1F6:
```
Revenue: 20,943.63 + Ship: 293.67
COGS: 2,591.44  Fees: 6,503.19
--- CM1: 11,849.00 (56.58%)
Ads: 78.31  Returns: 55.52
Storage: 13,050.79  Aged: 76.56  Removal: 1,212.95
Refund fin: 148.24  Ship surcharge: 373.79
--- CM2: -3,147.16 (-15.03%)
Overhead: 214.67
--- NP: -3,361.83 (-16.05%)
```

## Dane — uwagi

- Finance data w `acc_finance_transaction` zaczyna się od **2025-09-03**
- FBAStorageFee: dane dopiero od **2026-03-03** (792 rekordów, ~198K PLN)
- ShippingCharge: 76K rekordów, 1.3M PLN total
- Zapytania wcześniejsze niż 2025-09 zwrócą zerowe CM2/NP (brak danych finance)
- `amount_pln` jest wypełnione dla WSZYSTKICH 1.59M rekordów (FX via acc_exchange_rate)

## Pliki zmodyfikowane

| Plik | Zmiana |
|------|--------|
| `app/api/v1/profit.py` | Przebudowa V1 routes → V2 engine |
| `app/services/profit_engine.py` | `_classify_finance_charge()`, `_load_fba_component_pools()` (7 CM2), `_allocate_fba_component_costs()` (generyczny), `_load_overhead_pools()` (+ NP z finance), shipping pool, CM2 formula |
| `app/services/order_pipeline.py` | `_insert_finance_rows()` + `_load_fx_cache()` + `_get_fx_rate()` |
| `app/services/sync_service.py` | `_calc_pln()` helper, amount_pln w 3 punktach tworzenia |
| `app/schemas/profit_v2.py` | +3 pola CM2 w `ProductProfitItem` |
| `scripts/backfill_finance_amount_pln.py` | Jednorazowy backfill 1.59M rekordów |

## Znane ograniczenia

- `_load_finance_lookup()` w profit_engine.py — martwy kod (nigdy nie wywoływany). Do wyczyszczenia.
- `profit_service.py` — wciąż pisze brutto revenue do `acc_order`. Mniej istotne bo V2 liczy on-the-fly.
- `mssql_store.py` — profit functions nadal istnieją, ale nie są wywoływane. Do usunięcia.
- Frontend jeszcze nie wyświetla nowych pól CM2 (refund_finance, shipping_surcharge, fba_inbound) — wymaga dodania kolumn do `ProductProfitTable.tsx`.

## Co dalej

- [ ] Audyt brakujących opłat z SP-API (niektóre charge_types nie pojawiają się w DB)
- [ ] Czyszczenie martwego kodu (_load_finance_lookup, profit functions w mssql_store)
- [ ] Frontend: kolumny CM2 rozszerzone w ProductProfitTable
- [ ] Monitorowanie pokrycia danych finance (dashboard z coverage % per charge_type)
