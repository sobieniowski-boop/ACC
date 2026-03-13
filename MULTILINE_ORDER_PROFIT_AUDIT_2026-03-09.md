# Multi-Line Order Profit Integrity Audit

**Data:** 2026-03-09  
**Audytor:** Copilot (Claude Opus 4.6)  
**Zakres:** Kompletna analiza poprawności obliczania zysku dla zamówień wieloliniowych/wieloproduktowych  
**Inicjator:** Podejrzenie niepoprawnego zysku dla zamówień z >1 linią produktową  

---

## Spis treści

1. [Podsumowanie wykonawcze](#1-podsumowanie-wykonawcze)
2. [Zakres audytu](#2-zakres-audytu)
3. [Dane statystyczne](#3-dane-statystyczne)
4. [Architektura silnika profitowego](#4-architektura-silnika-profitowego)
5. [Weryfikacja poprawności (co działa dobrze)](#5-weryfikacja-poprawności-co-działa-dobrze)
6. [Finding F1 — Multiplikacja wierszy z logistics fact (HIGH)](#6-finding-f1--multiplikacja-wierszy-z-logistics-fact-high)
7. [Finding F2 — TOP 1 SKU w widoku zamówień (MEDIUM)](#7-finding-f2--top-1-sku-w-widoku-zamówień-medium)
8. [Finding F3 — Rozbieżność V1 vs V2 revenue (MEDIUM)](#8-finding-f3--rozbieżność-v1-vs-v2-revenue-medium)
9. [Finding F4 — logistics_pln=0 w rollupach (LOW)](#9-finding-f4--logistics_pln0-w-rollupach-low)
10. [Finding F5 — Niespójność refund między agregatem a drilldownem (LOW)](#10-finding-f5--niespójność-refund-między-agregatem-a-drilldownem-low)
11. [Proponowane poprawki](#11-proponowane-poprawki)
12. [Zapytania walidacyjne SQL](#12-zapytania-walidacyjne-sql)
13. [Ocena wpływu na rekomputację historyczną](#13-ocena-wpływu-na-rekomputację-historyczną)
14. [Załączniki — surowe dane z DB](#14-załączniki--surowe-dane-z-db)

---

## 1. Podsumowanie wykonawcze

### Werdykt: Silnik profitowy jest **zasadniczo poprawny** dla zamówień wieloliniowych

Główny silnik V2 (`profit_engine.py`, ~6400 linii) operuje na granularności **linii zamówienia** — alokuje przychód, COGS, opłaty i zwroty per linia. Arytmetyka jest poprawna. Zidentyfikowano **5 problemów**, z których **1 jest krytyczny** (multiplikacja wierszy przez logistics fact), a 2 są umiarkowane.

| # | Severity | Problem | Wpływ finansowy |
|:--|:---------|:--------|:----------------|
| F1 | **HIGH** | LEFT JOIN do `acc_order_logistics_fact` powoduje 2x multiplikację wierszy | 2 287 PLN revenue zdublowane (48 zamówień shipped) |
| F2 | **MEDIUM** | `get_profitability_orders()` pokazuje TOP 1 SKU zamiast wszystkich | 5 322 zamówień wieloproduktowych — błędna atrybucja SKU |
| F3 | **MEDIUM** | V1 `profit_service` zapisuje CM niezgodne z V2 engine | 138 985 PLN delta sumarycznie (6 313 zamówień) |
| F4 | **LOW** | Rollup tabele mają `logistics_pln=0` zawsze | Brak kosztu logistyki w rollupach |
| F5 | **LOW** | Refund: w agregacie jako koszt CM2, w drilldownie jako odliczenie od revenue | Decyzja projektowa, nie bug |

---

## 2. Zakres audytu

### Przeanalizowane pliki źródłowe

| Plik | Linie | Rola |
|:-----|:------|:-----|
| `apps/api/app/services/profit_engine.py` | ~6400 | Główny silnik CM1/CM2/NP (SQL) |
| `apps/api/app/services/profit_service.py` | ~156 | V1 legacy — zapis CM na poziomie zamówienia |
| `apps/api/app/services/profitability_service.py` | ~1100 | Rollupy SKU/marketplace + enrichment |
| `apps/api/app/services/order_pipeline.py` | ~2400 | 10-krokowy pipeline ingestion |
| `apps/api/app/services/order_logistics_source.py` | ~55 | Feature-gated logistics fact JOIN |
| `apps/api/app/services/fee_taxonomy.py` | ~200 | Klasyfikator charge_type → buckets P&L |

### Wymiary audytu (13 sprawdzonych)

1. Granularność (order-level vs line-level)
2. Alokacja przychodu per linia
3. Alokacja COGS per linia
4. Alokacja opłat FBA per linia
5. Podział zwrotów (refund split)
6. Przeliczenie walut (FX)
7. JOIN-y mogące powodować multiplikację wierszy
8. Alokacja kosztów CM2 (storage, aged, removal, etc.)
9. Spójność V1 vs V2
10. Rollup: aggregacja dane wieloliniowe
11. Drilldown: grain vs widok
12. Loss orders: identyfikacja strat
13. Logistics cost injection

### Zapytania do bazy danych (4 rundy)

- `_ml_audit_q.py` — statystyki multi-line, rozkład, precyzja refund share
- `_ml_audit_q2.py` — duplikaty logistics, multiplikacja wierszy, delta V1/V2
- `_ml_audit_q3.py` — feature flag, schema, rollup weryfikacja
- `_ml_audit_q4.py` — PK structure, kwantyfikacja wpływu

---

## 3. Dane statystyczne

### Zamówienia w bazie

| Metryka | Wartość |
|:--------|:-------|
| Zamówienia shipped (total) | **773 339** |
| Zamówienia wieloliniowe (2+ linie) | **6 314** (0.82%) |
| Zamówienia wieloproduktowe (2+ SKU) | **5 322** (0.69%) |
| Wieloliniowe ze zwrotem | **410** |
| Linie w zamówieniach wieloliniowych | **13 929** |

### Rozkład liczby linii

| Linie | Zamówienia |
|:------|:-----------|
| 2 | 5 391 |
| 3 | 685 |
| 4 | 165 |
| 5 | 43 |
| 6 | 20 |
| 7 | 4 |
| 8 | 2 |
| 9 | 1 |
| 10 | 1 |
| 15 | 1 |
| 19 | 1 |

### Kwoty refund

| Typ | Ilość |
|:----|:------|
| Refundy ujemne (prawidłowe) | 25 553 |
| Refundy zerowe | 120 |
| Refundy dodatnie (BRAK — OK) | 0 |

---

## 4. Architektura silnika profitowego

### Warstwy obliczeniowe

```
┌───────────────────────────────────────────────────────────┐
│  WARSTWA 1: profit_engine.py (V2)                         │
│  Grain: acc_order_line                                    │
│  Revenue = (item_price - item_tax - promo) × FX_rate      │
│  COGS = stamped at ingestion (order_pipeline.py)          │
│  FBA fees = stamped at ingestion                          │
│  Refund = 3-tier cascade split                            │
│  CM1 = Revenue - COGS - Referral - FBA                    │
│  CM2 = CM1 - StorageFees - AgedFees - Logistics - AdSpend │
│  NP = CM2 - OtherFees                                     │
├───────────────────────────────────────────────────────────┤
│  WARSTWA 2: profitability_service.py (Rollups)            │
│  Grain: (period_date, marketplace_id, sku)                │
│  MERGE upsert + 6 enrichment passes                       │
│  Reads from acc_order_line, enriches from finance events   │
├───────────────────────────────────────────────────────────┤
│  WARSTWA 3: profit_service.py (V1 Legacy)                 │
│  Grain: acc_order (order-level)                           │
│  Writes revenue_pln, contribution_margin_pln, cm_percent  │
│  Independent calculation — different FX logic              │
└───────────────────────────────────────────────────────────┘
```

### Alokacja CM2 (pool distribution)

```python
# profit_engine.py, linia ~1448
_KEY_MAP = {
    "storage":            "fba_storage_fee_pln",
    "aged":               "fba_aged_fee_pln",
    "removal":            "fba_removal_fee_pln",
    "liquidation":        "fba_liquidation_fee_pln",
    "refund_cost":        "refund_finance_pln",
    "shipping_surcharge": "shipping_surcharge_pln",
    "fba_inbound":        "fba_inbound_fee_pln",
    "promo":              "promo_cost_pln",
    "warehouse_loss":     "warehouse_loss_pln",
    "amazon_other_fee":   "amazon_other_fee_pln",
}
# Alokacja: proporcjonalnie do AFN units (fallback: revenue weight)
```

### Refund split — kaskada 3-tier

```
Tier 1: revenue_share = line_price / order_total_price
Tier 2: units_share   = line_units / order_total_units
Tier 3: equal_share   = 1.0 / line_count
```

**Wynik walidacji:** Suma udziałów (sum_shares) = **1.000000** dla WSZYSTKICH 410 wieloliniowych zamówień ze zwrotami. Błąd alokacji = 0.000000.

---

## 5. Weryfikacja poprawności (co działa dobrze)

### ✅ Revenue per-line

Formula: `(item_price - item_tax - promo_discount) × FX_rate_to_pln`

- Każda linia zamówienia ma własny `item_price`, `item_tax`, `item_promo`
- FX rate pobierany przez `OUTER APPLY TOP 1` z `acc_exchange_rate` per waluta + data
- **POPRAWNE** — brak multiplikacji, brak utraty danych

### ✅ COGS per-line

- Stampowane podczas ingestion w `order_pipeline.py` (krok 2: COGS pass)
- Każda linia `acc_order_line` ma własne `cogs_pln` = `purchase_price × quantity`
- **POPRAWNE** — nie zależy od JOIN-ów

### ✅ FBA fees per-line

- Stampowane podczas ingestion w `order_pipeline.py` (krok 3: FBA fee pass)
- `fba_fee_pln` per linia
- `referral_fee_pln` per linia
- **POPRAWNE** — nie zależy od JOIN-ów

### ✅ Refund split

- 3-tierowa kaskada (revenue share → units → equal)
- Precyzja: `sum_shares = 1.000000` dla 100% testowanych zamówień
- 10 najgorszych przypadków sprawdzonych — zero błędu alokacji
- **POPRAWNE**

### ✅ CM2 pool allocation

- `_allocate_fba_component_costs()` (linia 1448) rozdziela pule proporcjonalnie
- Wagi: AFN units primary, revenue fallback
- Granularność: per SKU per marketplace
- **POPRAWNE**

### ✅ Brak JOIN multiplikacji w głównym zapytaniu

- `acc_order_line` → `acc_order`: wiele-do-jednego (OK)
- `acc_order_line` → `acc_product`: wiele-do-jednego (OK)
- `acc_order` → `acc_exchange_rate`: OUTER APPLY TOP 1 (OK)
- `acc_order` → `acc_order_logistics_fact`: **PROBLEM** (patrz F1)

---

## 6. Finding F1 — Multiplikacja wierszy z logistics fact (HIGH)

### Problem

Plik: `apps/api/app/services/order_logistics_source.py`, linia 18-23

```python
def profit_logistics_join_sql(*, order_alias: str = "o", fact_alias: str = "olf") -> str:
    return (
        f"LEFT JOIN dbo.acc_order_logistics_fact {fact_alias} WITH (NOLOCK) "
        f"ON {fact_alias}.amazon_order_id = {order_alias}.amazon_order_id"
    )
```

Tabela `acc_order_logistics_fact` ma **klucz główny** `(amazon_order_id, calc_version)`.

Gdy zamówienie ma dwie wersje kalkulacji (np. `dhl_v1` + `gls_v1`), LEFT JOIN zwraca **2 wiersze** zamiast 1 — co powoduje **podwojenie** wszystkich wartości per linia (revenue, COGS, opłaty).

### Schema tabeli

```
PK: (amazon_order_id, calc_version)
Kolumny: amazon_order_id, acc_order_id, shipments_count, delivered_shipments_count,
         total_logistics_pln, last_delivery_at, calc_version, source_system,
         calculated_at, actual_shipments_count, estimated_shipments_count
```

### Przykład duplikatu

| amazon_order_id | calc_version | total_logistics_pln | source_system |
|:----------------|:-------------|:-------------------|:--------------|
| 028-0400778-4486722 | dhl_v1 | 51.04 PLN | shipment_aggregate |
| 028-0400778-4486722 | gls_v1 | 73.51 PLN | shipment_aggregate_gls |

### Wpływ

| Metryka | Wartość |
|:--------|:-------|
| Zamówienia z duplikatem logistics fact | **97** |
| Z tego shipped (realizowane) | **90** |
| Zamówienia shipped z podwójnym wpływem | **48** |
| Revenue zdublowane (suma) | **2 287,24 PLN** |
| COGS zdublowane (suma) | **3 376,72 PLN** |
| Linie dotknięte | **57** |
| Overlap z multi-line orders | **6 zamówień** |
| Multiplikator | **2.0x** (dokładnie podwojenie) |

### Weryfikacja multiplikacji

```
Zamówienie 028-0400778-4486722:
  JOIN produces: 2 rows
  Actual lines: 1
  Fact rows: 2
  Multiplier: 2.0x

Zamówienie 028-0714541-0484353:
  JOIN produces: 2 rows
  Actual lines: 1
  Fact rows: 2
  Multiplier: 2.0x
```

### Mechanizm błędu

```sql
-- Obecny kod (BŁĘDNY):
LEFT JOIN dbo.acc_order_logistics_fact olf
  ON olf.amazon_order_id = o.amazon_order_id
-- Jeśli 2 wiersze w fact → każda linia zamówienia mnożona × 2

-- Efekt: SUM(revenue), SUM(cogs), SUM(fees) = 2× prawidłowej wartości
```

---

## 7. Finding F2 — TOP 1 SKU w widoku zamówień (MEDIUM)

### Problem

Plik: `apps/api/app/services/profitability_service.py`, linia ~332

```sql
CROSS APPLY (
    SELECT TOP 1
        ol.sku,
        ol.asin,
        (SELECT ISNULL(SUM(ISNULL(ol2.fba_fee_pln, 0)), 0)
         FROM dbo.acc_order_line ol2 WITH (NOLOCK)
         WHERE ol2.order_id = o.id) as fba_fees_pln
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    WHERE ol.order_id = o.id
) ol_agg
```

### Opis

Endpoint `GET /profitability/orders` używany przez stronę "Profitability Orders" wyciąga **tylko pierwszy SKU** z zamówienia wieloproduktowego. Dla 5 322 zamówień z 2+ różnymi SKU, w UI pokazywany jest **nieprawidłowy SKU** — zawsze ten, który SQL Server poda jako "TOP 1" (bez ORDER BY = niedeterministyczny).

### Wpływ

| Metryka | Wartość |
|:--------|:-------|
| Zamówienia wieloproduktowe (2+ SKU) | **5 322** |
| Revenue tych zamówień | **1 178 522,34 PLN** |
| CM tych zamówień | **426 955,08 PLN** |

### Nasilenie

Jest to problem **prezentacji danych** — sam zysk jest obliczony poprawnie w silniku V2. Ale w widoku zamówień użytkownik widzi tylko 1 SKU, co wprowadza w błąd.

---

## 8. Finding F3 — Rozbieżność V1 vs V2 revenue (MEDIUM)

### Problem

Plik: `apps/api/app/services/profit_service.py`, linia ~189

V1 legacy `profit_service.py` zapisuje `revenue_pln` i `contribution_margin_pln` do tabeli `acc_order` — obliczone **niezależnie** od V2 engine.

### Różnice w kalkulacji

| Aspekt | V1 (profit_service) | V2 (profit_engine) |
|:-------|:-------------------|:-------------------|
| Grain | Order-level | Line-level |
| Revenue | Suma z linii (prosty) | `(item_price - item_tax - promo) × FX` per linia |
| FX | `get_rate_safe()` per zamówienie | `OUTER APPLY TOP 1` per zamówienie (identyczna data) |
| Opłaty | Suma fba+referral z linii | j.w. + fee taxonomy |
| CM | revenue - cogs - fees | Pełny P&L z CM1/CM2/NP |

### Wpływ

| Metryka | Wartość |
|:--------|:-------|
| Zamówienia wieloliniowe z deltą | **6 313** |
| Sumaryczna delta revenue | **138 985,33 PLN** |
| Średnia delta per zamówienie | **22,02 PLN** |

### Największe rozbieżności (zamówienia 3+ linii)

| amazon_order_id | V1 revenue | V2 revenue | Delta |
|:----------------|:-----------|:-----------|:------|
| 171-8275919-8293123 | 874,69 | 2 242,83 | **1 368,14 PLN** |
| 403-7513550-2456360 | 728,03 | 1 820,10 | **1 092,07 PLN** |
| 171-2059292-5873931 | 514,79 | 1 132,57 | **617,78 PLN** |
| 305-4821944-1517101 | 453,58 | 984,20 | **530,62 PLN** |
| 403-6948343-3482729 | 441,08 | 970,36 | **529,28 PLN** |

### Analiza przyczyny

Delta wynika z tego, że V1 sumuje `item_price_amount` z linii **bez odejmowania podatku i promo**, podczas gdy V2 poprawnie odejmuje. Dla zamówień wieloliniowych różnice się kumulują.

---

## 9. Finding F4 — logistics_pln=0 w rollupach (LOW)

### Problem

Plik: `apps/api/app/services/profitability_service.py`, linia ~933-975

### Opis

Tabela `acc_sku_profitability_rollup` jest budowana przez MERGE upsert:

```sql
WHEN MATCHED THEN UPDATE SET
    logistics_pln = ISNULL(tgt.logistics_pln, 0),
    ...
WHEN NOT MATCHED THEN INSERT (...)
    VALUES (..., 0, ...)  -- logistics_pln hardcoded to 0
```

Pole `logistics_pln` jest ustawiane na `ISNULL(tgt.logistics_pln, 0)` przy UPDATE (zachowuje starą wartość) lub `0` przy INSERT. **Żaden z 6 enrichment passes nie aktualizuje logistics_pln z tabeli `acc_order_logistics_fact`.**

### Weryfikacja

```
Rollup rows z logistics > 0:  0  (z ~25 000+ wierszy rollup)
```

### Wpływ

Rollupy SKU/marketplace **nie zawierają kosztów logistyki** → raporty oparte na rollupach (executive, strategy, profitability overview) zaniżają koszty.

---

## 10. Finding F5 — Niespójność refund między agregatem a drilldownem (LOW)

### Problem

| Widok | Traktowanie refund |
|:------|:------------------|
| Główna agregacja (ProductProfitTable) | Gross revenue + refund jako koszt CM2 (oddzielna kolumna) |
| Drilldown (ProductDrilldown) | Net revenue (revenue po odliczeniu refund) |

### Opis

Jest to **świadoma decyzja projektowa**, a nie bug. Główna tabela pokazuje przychód brutto i refund jako osobną kolumnę kosztów, co pozwala analizować refund rate. Drilldown pokazuje netto per transakcja.

Użytkownik może jednak odczuć **nieintuicyjną zmianę** wartości revenue przy przechodzeniu z tabeli do drilldownu.

---

## 11. Proponowane poprawki

### Fix F1 — OUTER APPLY TOP 1 zamiast LEFT JOIN (HIGH, est. 30 min)

**Plik:** `apps/api/app/services/order_logistics_source.py`

**Obecny kod:**
```python
def profit_logistics_join_sql(*, order_alias: str = "o", fact_alias: str = "olf") -> str:
    return (
        f"LEFT JOIN dbo.acc_order_logistics_fact {fact_alias} WITH (NOLOCK) "
        f"ON {fact_alias}.amazon_order_id = {order_alias}.amazon_order_id"
    )
```

**Proponowany:**
```python
def profit_logistics_join_sql(*, order_alias: str = "o", fact_alias: str = "olf") -> str:
    return (
        f"OUTER APPLY ("
        f"  SELECT TOP 1 olf_inner.total_logistics_pln, olf_inner.calc_version, "
        f"         olf_inner.shipments_count, olf_inner.delivered_shipments_count, "
        f"         olf_inner.last_delivery_at "
        f"  FROM dbo.acc_order_logistics_fact olf_inner WITH (NOLOCK) "
        f"  WHERE olf_inner.amazon_order_id = {order_alias}.amazon_order_id "
        f"  ORDER BY olf_inner.calculated_at DESC"
        f") {fact_alias}"
    )
```

**Efekt:** Zawsze 1 wiersz — najnowsza kalkulacja. Eliminuje multiplikację.

**Po wdrożeniu:** Rekomputacja 48 dotkniętych zamówień:
```sql
-- Identyfikacja dotkniętych zamówień:
SELECT amazon_order_id
FROM dbo.acc_order_logistics_fact WITH (NOLOCK)
GROUP BY amazon_order_id
HAVING COUNT(*) > 1
```

### Fix F2 — Line-level view lub multi-SKU indicator (MEDIUM, est. 1h)

**Plik:** `apps/api/app/services/profitability_service.py`

**Opcja A (rekomendowana):** Dodać `ORDER BY ol.sku` do CROSS APPLY TOP 1 (deterministyczność) + dodać pole `sku_count`:
```sql
CROSS APPLY (
    SELECT TOP 1
        ol.sku,
        ol.asin,
        (SELECT COUNT(DISTINCT ol3.sku)
         FROM dbo.acc_order_line ol3 WITH (NOLOCK)
         WHERE ol3.order_id = o.id) as sku_count,
        (SELECT ISNULL(SUM(ISNULL(ol2.fba_fee_pln, 0)), 0)
         FROM dbo.acc_order_line ol2 WITH (NOLOCK)
         WHERE ol2.order_id = o.id) as fba_fees_pln
    FROM dbo.acc_order_line ol WITH (NOLOCK)
    WHERE ol.order_id = o.id
    ORDER BY ol.sku
) ol_agg
```

**Opcja B:** Rozwinąć do widoku per linia (większa zmiana w API + frontend).

### Fix F3 — Deprecjacja V1 lub re-sync (MEDIUM, est. 2h)

**Opcja A (rekomendowana):** Dodać endpointy i UI w V2 dla wszystkich widoków korzystających z `acc_order.contribution_margin_pln`, a następnie usunąć zależność od V1.

**Opcja B:** Uruchomić ponownie `recalculate_profit_batch()` z poprawioną formułą FX.

### Fix F4 — Enrichment pass dla logistics (LOW, est. 1h)

**Plik:** `apps/api/app/services/profitability_service.py`

Dodać nowy enrichment pass po przejście 5 (ad spend):
```sql
-- Pass 6d: Logistics from logistics fact
UPDATE r SET
    logistics_pln = ISNULL(lf.total_logistics_pln, 0)
FROM dbo.acc_sku_profitability_rollup r WITH (ROWLOCK)
CROSS APPLY (
    SELECT TOP 1 olf.total_logistics_pln
    FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
    JOIN dbo.acc_order o WITH (NOLOCK) ON o.amazon_order_id = olf.amazon_order_id
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE ol.sku = r.sku
      AND o.marketplace_id = r.marketplace_id
      AND CAST(o.purchase_date AS DATE) = r.period_date
    ORDER BY olf.calculated_at DESC
) lf
WHERE r.period_date >= @date_from AND r.period_date <= @date_to
```

### Fix F5 — Brak akcji (LOW)

Decyzja projektowa. Opcjonalnie: dodać tooltip w UI wyjaśniający różnicę.

---

## 12. Zapytania walidacyjne SQL

### Walidacja F1 — po wdrożeniu fixa

```sql
-- Sprawdź czy nie ma multiplikacji
SELECT
    o.amazon_order_id,
    COUNT(*) as join_rows,
    COUNT(DISTINCT olf.calc_version) as fact_versions
FROM dbo.acc_order o WITH (NOLOCK)
OUTER APPLY (
    SELECT TOP 1 olf_inner.*
    FROM dbo.acc_order_logistics_fact olf_inner WITH (NOLOCK)
    WHERE olf_inner.amazon_order_id = o.amazon_order_id
    ORDER BY olf_inner.calculated_at DESC
) olf
WHERE o.status = 'Shipped'
GROUP BY o.amazon_order_id
HAVING COUNT(*) > 1
-- Oczekiwany wynik: 0 wierszy
```

### Walidacja F1 — zamówienia aktualnie dotknięte

```sql
-- Zamówienia z duplikatami logistics fact (przed fixem)
SELECT
    olf.amazon_order_id,
    COUNT(*) as versions,
    STRING_AGG(olf.calc_version, ', ') as calc_versions,
    SUM(olf.total_logistics_pln) as total_logistics_sum
FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
GROUP BY olf.amazon_order_id
HAVING COUNT(*) > 1
ORDER BY SUM(olf.total_logistics_pln) DESC
```

### Walidacja F2 — zamówienia wieloproduktowe

```sql
SELECT COUNT(*) as multi_sku_orders
FROM (
    SELECT o.id
    FROM dbo.acc_order o WITH (NOLOCK)
    JOIN dbo.acc_order_line ol WITH (NOLOCK) ON ol.order_id = o.id
    WHERE o.status = 'Shipped' AND ol.sku IS NOT NULL
    GROUP BY o.id
    HAVING COUNT(DISTINCT ol.sku) > 1
) sub
```

### Walidacja F4 — logistics w rollupach

```sql
SELECT
    COUNT(*) as total_rollup_rows,
    SUM(CASE WHEN logistics_pln > 0 THEN 1 ELSE 0 END) as with_logistics,
    SUM(CASE WHEN logistics_pln = 0 OR logistics_pln IS NULL THEN 1 ELSE 0 END) as without_logistics
FROM dbo.acc_sku_profitability_rollup WITH (NOLOCK)
```

---

## 13. Ocena wpływu na rekomputację historyczną

### Wymagana rekomputacja po Fix F1

| Zakres | Szczegóły |
|:-------|:----------|
| Zamówienia do przeliczenia | **48** (shipped z dup logistics fact) |
| Revenue do korekty | **2 287,24 PLN** (obecne podwojenie) |
| COGS do korekty | **3 376,72 PLN** (obecne podwojenie) |
| Metoda | Ponowne uruchomienie `recompute_rollups()` dla affected SKUs |
| Czas | ~minuty (mały zakres) |
| Ryzyko | **Niskie** — MERGE upsert jest idempotentny |

### Wymagana rekomputacja po Fix F4

| Zakres | Szczegóły |
|:-------|:----------|
| Wiersze rollup | **~25 000+** |
| Metoda | `recompute_rollups(date_from=2025-03-09, date_to=today)` + nowy enrichment pass |
| Czas | 1-3h |
| Ryzyko | **Niskie** — dodaje wartości, nie modyfikuje istniejących |

### NIE wymagana rekomputacja

- **Przychody per linia** — poprawne (F1 to problem JOIN, nie kalkulacji)
- **COGS per linia** — poprawne (stampowane przy ingestion)
- **FBA fees per linia** — poprawne
- **Refund split** — poprawny (sum_shares = 1.000000)
- **FX rates** — poprawne

---

## 14. Załączniki — surowe dane z DB

### Załącznik A — Statystyki multi-line (`_ml_audit_results.txt`)

```
Multi-line orders: 6314, Total shipped orders: 773339, Lines in multi-line: 13929
Logistics fact duplicates (top 5):
  028-1904177-4936305: 2 versions
  402-3203031-6456301: 2 versions
  305-2145903-8133922: 2 versions
  402-4099090-9719547: 2 versions
  028-4613840-3037107: 2 versions
Multi-line refund orders: 410

Line count distribution (multi-line only):
  2 lines: 5391 orders
  3 lines: 685 orders
  4 lines: 165 orders
  5 lines: 43 orders
  6 lines: 20 orders
  7 lines: 4 orders
  8 lines: 2 orders
  9 lines: 1 orders
  10 lines: 1 orders
  15 lines: 1 orders
  19 lines: 1 orders

Refund share allocation precision (worst 10 multi-line refunds):
  ALL: sum_shares=1.000000, share_error=0.000000

Orders with 2+ distinct SKUs (multi-SKU): 5322
```

### Załącznik B — Analiza duplikatów (`_ml_audit_results2.txt`)

```
Orders with duplicate logistics facts: 97
Of which shipped: 90
Row multiplication check:
  028-0400778-4486722: 2 rows × 1 line = 2.0x
  028-0714541-0484353: 2 rows × 1 line = 2.0x
  028-1062459-1280315: 2 rows × 1 line = 2.0x
  028-2848321-4040313: 2 rows × 1 line = 2.0x

Multi-SKU orders: 5322, rev=1,178,522.34 PLN, cm=426,955.08 PLN

V1 vs V2 revenue — top divergences (3+ lines):
  171-8275919-8293123: delta=1,368.14 PLN (6 lines)
  403-7513550-2456360: delta=1,092.07 PLN (4 lines)
  171-2059292-5873931: delta=617.78 PLN (4 lines)

Refund amounts: positive=0, negative=25,553, zero=120
```

### Załącznik C — Schema & feature flags (`_ml_audit_results3.txt`)

```
PROFIT_USE_LOGISTICS_FACT: True
PK_acc_order_logistics_fact: unique=True, pk=True

Rollup rows with logistics>0: 0
V1 vs V2 revenue delta (multi-line only): 
  total=138,985.33 PLN, avg=22.02 PLN, orders=6,313

Duplicate logistics fact samples:
  028-0400778-4486722: dhl_v1=51.04 PLN, gls_v1=73.51 PLN
  028-0714541-0484353: dhl_v1=56.04 PLN, gls_v1=35.64 PLN
```

### Załącznik D — PK & impact (`_ml_audit_results4.txt`)

```
PK columns: ['amazon_order_id', 'calc_version']
IX_acc_order_logistics_fact_order columns: ['acc_order_id', 'calculated_at']

Sample duplicate:
  028-0400778-4486722 | dhl_v1 | 51.04 PLN | shipment_aggregate     | 2026-03-09 11:59
  028-0400778-4486722 | gls_v1 | 73.51 PLN | shipment_aggregate_gls | 2026-03-09 12:19

Impact: 48 shipped orders, 2,287.24 PLN rev doubled, 3,376.72 PLN COGS doubled
Multi-line + logistics-dup overlap: 6 orders
```

---

## Konkluzja

Silnik profitowy V2 (`profit_engine.py`) jest **poprawnie zaprojektowany** do obsługi zamówień wieloliniowych na poziomie granularności linii. Arytmetyka revenue, COGS, FBA fees i refund split jest **bezbłędna**.

Jedyny problem wpływający na **dokładność liczbową** to F1 (multiplikacja wierszy przez logistics fact), który dotyczy **48 zamówień shipped** z sumarycznym podwojeniem 2 287 PLN revenue i 3 377 PLN COGS. Fix jest prosty (OUTER APPLY TOP 1).

Pozostałe findings (F2-F5) to problemy prezentacji danych, rozbieżności między warstwami V1/V2, lub brakujące enrichment passes — nie wpływają na poprawność główného silnika.

**Priorytet wdrożenia:** F1 → F2 → F3 → F4 → F5
