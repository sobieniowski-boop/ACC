# Koszt kuriera per order — 2026 + Instrukcje audytu źródeł logistyki

> **Cel dokumentu**: (1) Tabele z danymi kosztów kuriera od początku 2026,
> (2) Kompletne instrukcje krok-po-kroku, aby inny Copilot mógł zweryfikować,
> że KAŻDY moduł pobiera koszty kuriera z prawidłowego źródła.
>
> **Stan na**: 2026-03-10 · **Baza**: `NetfoxAnalityka` na `acc-sql-kadax.database.windows.net`

---

## Spis treści

1. [Architektura przepływu danych logistycznych](#1-architektura-przepływu-danych-logistycznych)
2. [Feature flag PROFIT_USE_LOGISTICS_FACT](#2-feature-flag)
3. [Centralny moduł: order_logistics_source.py](#3-centralny-moduł)
4. [Tabele bazy danych](#4-tabele-bazy-danych)
5. [Dane: koszty kuriera 2026](#5-dane-koszty-kuriera-2026)
6. [Audyt modułów — checklist](#6-audyt-modułów--checklist)
7. [Procedura weryfikacji dla Copilota](#7-procedura-weryfikacji-dla-copilota)
8. [Znane problemy i dług techniczny](#8-znane-problemy)

---

## 1. Architektura przepływu danych logistycznych

```
FAKTURY DHL / GLS
      │
      ▼
acc_shipment_cost          ← koszt per przesyłka (net, fuel, toll, gross)
      │
      ▼
acc_order_logistics_fact   ← agregacja na poziom zamówienia (total_logistics_pln)
      │                       Populowane przez: dhl_logistics_aggregation.py,
      │                                        gls_logistics_aggregation.py,
      │                                        order_logistics_estimation.py
      │
      ├──► LIVE SQL (OUTER APPLY) ──► profit_engine.py, profitability_service.py
      │        ↑ przez helpery z order_logistics_source.py
      │
      └──► ROLLUP (step 4d) ──► acc_sku_profitability_rollup.logistics_pln
                                    ↑ prorata wg udziału revenue per SKU-line
                                    │
                                    └──► get_profitability_overview()
                                         get_profitability_products()
                                         acc_marketplace_profitability_rollup
```

**STARY flow (legacy)**: `acc_order.logistics_pln` — kolumna na tabeli zamówień,
wypełniana w starym pipeline. Używana jako FALLBACK gdy brak rekordu w `acc_order_logistics_fact`.

**NOWY flow (fact)**: `acc_order_logistics_fact.total_logistics_pln` — suma wszystkich
kosztów przesyłek (DHL+GLS) przypisanych do zamówienia. Deduplikacja: `TOP 1 ORDER BY calculated_at DESC`.

---

## 2. Feature flag

| Klucz                        | Domyślna w kodzie | Wartość runtime |
|------------------------------|-------------------|-----------------|
| `PROFIT_USE_LOGISTICS_FACT`  | `False`           | **`True`**      |

- Plik: `apps/api/app/core/config.py` (settings)
- Odczyt: `order_logistics_source.py` → `profit_uses_logistics_fact()`
- Gdy `True`: OUTER APPLY do `acc_order_logistics_fact` + fallback na `acc_order.logistics_pln`
- Gdy `False`: tylko `acc_order.logistics_pln` (legacy)

**Weryfikacja**:
```python
# Sprawdź aktualną wartość flagi:
from app.core.config import settings
print(getattr(settings, "PROFIT_USE_LOGISTICS_FACT", "NOT SET"))
```

---

## 3. Centralny moduł

**Plik**: `apps/api/app/services/order_logistics_source.py` (~183 linie)

Wszystkie moduły POWINNY używać helperów z tego pliku zamiast pisać własne SQL.

### Funkcje (Raw SQL — pymssql):

| Funkcja | Opis | Zwraca gdy flag=True | Zwraca gdy flag=False |
|---------|------|----------------------|----------------------|
| `profit_logistics_join_sql(order_alias, fact_alias)` | Fragment OUTER APPLY | `OUTER APPLY (SELECT TOP 1 olf_inner.total_logistics_pln FROM acc_order_logistics_fact olf_inner WHERE olf_inner.amazon_order_id = o.amazon_order_id ORDER BY olf_inner.calculated_at DESC) olf` | `""` (pusty string) |
| `profit_logistics_value_sql(order_alias, fact_alias)` | Wyrażenie wartości | `ISNULL(CAST(olf.total_logistics_pln AS FLOAT), CAST(ISNULL(o.logistics_pln, 0) AS FLOAT))` | `CAST(ISNULL(o.logistics_pln, 0) AS FLOAT)` |

### Funkcje (SQLAlchemy — async):

| Funkcja | Opis |
|---------|------|
| `profit_logistics_join_sqla(statement, order_model, fact_model)` | Dodaje lateral/outerjoin do `_olf_latest` subquery |
| `profit_logistics_value_sqla(order_model, fact_model)` | Zwraca `COALESCE(fact, legacy)` expression |
| `resolve_profit_logistics_pln(db, amazon_order_id, legacy_logistics_pln)` | Async resolver — zwraca float dla jednego zamówienia |

### Import w modułach konsumujących:
```python
from app.services.order_logistics_source import profit_logistics_join_sql, profit_logistics_value_sql
```

---

## 4. Tabele bazy danych

### `acc_order_logistics_fact`
```
amazon_order_id          VARCHAR     ← klucz join z acc_order
acc_order_id             INT         ← FK do acc_order.id
shipments_count          INT
delivered_shipments_count INT
total_logistics_pln      DECIMAL     ← ★ GŁÓWNA WARTOŚĆ: suma kosztów kuriera
last_delivery_at         DATETIME
calc_version             VARCHAR     ← np. "dhl_v1", "gls_v1"
source_system            VARCHAR
calculated_at            DATETIME    ← ★ do deduplikacji (ORDER BY DESC)
actual_shipments_count   INT
estimated_shipments_count INT
```

### `acc_shipment_cost`
```
id, shipment_id, cost_source, currency,
net_amount, fuel_amount, toll_amount, gross_amount,
invoice_number, invoice_date, billing_period,
is_estimated, raw_payload_json, created_at, updated_at
```

### `acc_order.logistics_pln` (legacy kolumna)
Kolumna FLOAT na tabeli `acc_order`. Stara wartość z poprzedniego pipeline.

### `acc_sku_profitability_rollup`
Pre-obliczona tabela rollup (dziennie × SKU × marketplace).
Kolumna `logistics_pln` wypełniana w kroku 4d rekomputacji (`recompute_rollups()`).

---

## 5. Dane: koszty kuriera 2026

### 5.1 Grand Totals
| Metryka | Wartość |
|---------|---------|
| Zamówienia ogółem | 114 558 |
| Z rekordem w fact | 44 013 (38.4%) |
| Fact łącznie PLN | 1 337 300.68 |
| Legacy łącznie PLN | 708 788.38 |
| Średni koszt (non-zero fact) | **31.14 PLN** |

### 5.2 Miesięcznie per marketplace (z acc_order_logistics_fact)

| Miesiąc | Marketplace | Orders | Z Fact | Total PLN | Avg (non-zero) |
|---------|-------------|--------|--------|-----------|-----------------|
| 2026-01 | A13V1IB3VIYZZH (PL) | 5 176 | 3 992 | 170 697.89 | 42.82 |
| 2026-01 | A1805IZSGTT6HS (NL) | 853 | 642 | 23 789.19 | 37.05 |
| 2026-01 | A1C3SOZRARQ6R3 (CZ) | 521 | 338 | 3 840.98 | 11.43 |
| 2026-01 | A1PA6795UKMFR9 (DE) | 28 650 | 19 757 | 523 202.33 | 26.48 |
| 2026-01 | A1RKKUPIHCS9HS (ES) | 1 291 | 714 | 41 818.89 | 58.57 |
| 2026-01 | A28R8C7NBKEWEA (SE) | 114 | 58 | 5 538.52 | 95.49 |
| 2026-01 | A2NODRKZP88ZB9 (BE) | 449 | 265 | 13 391.62 | 50.53 |
| 2026-01 | AMEN7PMS3EDWL (IT) | 973 | 718 | 29 584.50 | 41.26 |
| 2026-01 | APJ6JRA9NG5V4 (FR) | 4 850 | 3 231 | 140 426.00 | 43.48 |
| 2026-02 | A13V1IB3VIYZZH (PL) | 6 484 | 1 669 | 40 193.46 | 42.99 |
| 2026-02 | A1805IZSGTT6HS (NL) | 907 | 439 | 13 405.56 | 31.39 |
| 2026-02 | A1C3SOZRARQ6R3 (CZ) | 524 | 91 | 713.34 | 11.32 |
| 2026-02 | A1PA6795UKMFR9 (DE) | 32 656 | 9 787 | 238 333.48 | 24.35 |
| 2026-02 | A1RKKUPIHCS9HS (ES) | 1 342 | 284 | 14 696.75 | 59.50 |
| 2026-02 | A28R8C7NBKEWEA (SE) | 131 | 28 | 2 085.90 | 83.44 |
| 2026-02 | A2NODRKZP88ZB9 (BE) | 486 | 145 | 6 254.13 | 44.36 |
| 2026-02 | AMEN7PMS3EDWL (IT) | 1 014 | 198 | 5 248.69 | 37.49 |
| 2026-02 | APJ6JRA9NG5V4 (FR) | 5 352 | 1 657 | 64 079.45 | 43.56 |
| 2026-03 | (wszystkie) | 22 785 | **0** | **0.00** | — |

> **Uwaga**: Marzec 2026 — brak danych logistycznych. Pipeline DHL/GLS nie przetworzyło faktur za marzec.

### 5.3 Legacy vs Fact — porównanie

| Miesiąc | Orders | Legacy PLN | Fact PLN | Delta | Leg-only | Fact-only | Różne |
|---------|--------|------------|----------|-------|----------|-----------|-------|
| 2026-01 | 42 877 | 526 585.11 | 952 289.91 | -425 704.80 | 12 | 11 406 | 182 |
| 2026-02 | 48 896 | 182 203.27 | 385 010.77 | -202 807.50 | 1 054 | 8 036 | 8 |
| 2026-03 | 22 785 | 0.00 | 0.00 | 0.00 | 0 | 0 | 0 |

> Fact zawiera WIĘCEJ niż legacy — fact obejmuje zamówienia z przesyłkami, których legacy nie ujmuje.

### 5.4 Rozkład per przewoźnik

| Miesiąc | Przewoźnik | Przesyłki | Koszt łącznie | Średni koszt |
|---------|------------|-----------|---------------|--------------|
| 2026-01 | DHL | 13 407 | 545 399.56 PLN | **40.68 PLN** |
| 2026-01 | GLS | 28 039 | 583 698.73 PLN | **26.29 PLN** |
| 2026-02 | DHL | 11 178 | 410 466.06 PLN | **36.72 PLN** |
| 2026-02 | GLS | 9 499 | 186 282.17 PLN | **29.22 PLN** |

> DHL jest droższy (~37-41 PLN/szt) niż GLS (~26-29 PLN/szt). Wszystkie koszty za styczeń i luty to koszty rzeczywiste (0 estimated).

### 5.5 Rollup table (`acc_sku_profitability_rollup`)

| Miesiąc | Marketplace | SKU-days | Total PLN | Non-zero | Zero |
|---------|-------------|----------|-----------|----------|------|
| 2026-01 | A13V1IB3VIYZZH | 4 917 | 170 698.51 | 3 913 | 1 004 |
| 2026-01 | A1805IZSGTT6HS | 873 | 23 789.22 | 663 | 210 |
| 2026-01 | A1C3SOZRARQ6R3 | 516 | 3 840.19 | 337 | 179 |
| 2026-01 | A1PA6795UKMFR9 | 16 979 | 523 201.16 | 12 735 | 4 244 |
| 2026-01 | A1RKKUPIHCS9HS | 1 206 | 41 853.11 | 696 | 510 |
| 2026-01 | A28R8C7NBKEWEA | 115 | 5 592.65 | 61 | 54 |
| 2026-01 | A2NODRKZP88ZB9 | 481 | 13 391.62 | 295 | 186 |
| 2026-01 | AMEN7PMS3EDWL | 970 | 29 584.95 | 720 | 250 |
| 2026-01 | APJ6JRA9NG5V4 | 4 229 | 140 493.10 | 3 049 | 1 180 |
| 2026-02 | A13V1IB3VIYZZH | 5 903 | 40 193.56 | 936 | 4 967 |
| 2026-02 | A1805IZSGTT6HS | 931 | 13 405.56 | 451 | 480 |
| 2026-02 | A1C3SOZRARQ6R3 | 547 | 713.19 | 66 | 481 |
| 2026-02 | A1PA6795UKMFR9 | 19 617 | 238 332.52 | 7 609 | 12 008 |
| 2026-02 | A1RKKUPIHCS9HS | 1 220 | 14 696.72 | 244 | 976 |
| 2026-02 | A28R8C7NBKEWEA | 136 | 2 085.89 | 27 | 109 |
| 2026-02 | A2NODRKZP88ZB9 | 504 | 6 254.12 | 156 | 348 |
| 2026-02 | AMEN7PMS3EDWL | 1 028 | 5 248.76 | 147 | 881 |
| 2026-02 | APJ6JRA9NG5V4 | 4 496 | 64 079.46 | 1 414 | 3 082 |
| 2026-03 | (wszystkie) | 14 369 | **0.00** | 0 | 14 369 |

> Rollup totale są zgodne z fact table (różnica ± kilka PLN z zaokrągleń proration).

### 5.6 AFN (FBA) — koszt logistyki

| Miesiąc | Zamówienia AFN | Legacy ≠ 0 | Fact ≠ 0 | Legacy total | Fact total |
|---------|---------------|------------|----------|-------------|-----------|
| 2026-01 | 12 377 | 0 | 0 | 0.00 | 0.00 |
| 2026-02 | 18 713 | 0 | 0 | 0.00 | 0.00 |
| 2026-03 | 6 882 | 0 | 0 | 0.00 | 0.00 |

> FBA zamówienia **NIE** mają kosztów kuriera — Amazon obsługuje logistykę. To poprawne zachowanie.

---

## 6. Audyt modułów — checklist

### Legenda statusów
- ✅ **OK** — używa centralnych helperów z `order_logistics_source.py`
- ✅🗃 **OK-ROLLUP** — czyta z pre-obliczonej tabeli rollup (poprawne architekturalnie)
- ⚠️ **UWAGA** — duplikuje logikę helperów (działa poprawnie, ale dług techniczny)

### 6.1 profit_engine.py

| Funkcja | Linia | Status | Mechanizm |
|---------|-------|--------|-----------|
| `get_product_profit_table()` | ~1773 | ✅ OK | `profit_logistics_join_sql()` + `profit_logistics_value_sql()` w live OUTER APPLY |
| `get_product_what_if_table()` | ~2748 | ✅ OK | `profit_logistics_join_sql()` + `profit_logistics_value_sql()` (linie ~3252-3253) |
| `export_product_profit_xlsx()` | ~4079 | ✅ OK | Wrapper — wywołuje `get_product_profit_table()`, potem formatuje do Excel |
| `get_product_drilldown()` | ~4608 | ✅ OK | `profit_logistics_join_sql()` + `profit_logistics_value_sql()` (linie ~4621-4622) |
| `get_loss_orders()` | ~4891 | ✅ OK | `profit_logistics_join_sql()` + `profit_logistics_value_sql()` (linie ~4909-4910) |
| `get_profit_kpis()` | ~6631 | ✅ OK | Helpery wewnątrz `_compute_kpis()` closure (linie ~6662-6663) |
| `_apply_manual_price_to_internal_sku()` | ~6771 | ✅ OK | `profit_logistics_join_sql()` + `profit_logistics_value_sql()` (linie ~6830-6831) |

**Import** (linia ~48):
```python
from app.services.order_logistics_source import profit_logistics_join_sql, profit_logistics_value_sql
```

### 6.2 profitability_service.py

| Funkcja | Linia | Status | Mechanizm |
|---------|-------|--------|-----------|
| `get_profitability_overview()` | ~181 | ✅ OK | Główne KPI z rollup; sekcja loss orders używa helperów (linie ~315-316) |
| `get_profitability_orders()` | ~427 | ✅ OK | `profit_logistics_join_sql()` + `profit_logistics_value_sql()` (linie ~435-436) |
| `get_profitability_products()` | ~594 | ✅🗃 OK-ROLLUP | Czyta SUM(r.logistics_pln) z `acc_sku_profitability_rollup` (linia ~651) |
| `recompute_rollups()` step 4d | ~1098 | ⚠️ UWAGA | Własny OUTER APPLY TOP 1 do `acc_order_logistics_fact` — duplikuje logikę helperów |

### 6.3 Pipeline — populowanie acc_order_logistics_fact

| Moduł | Linia | Opis |
|-------|-------|------|
| `dhl_logistics_aggregation.py` | ~64 | INSERT INTO acc_order_logistics_fact z przesyłek DHL |
| `gls_logistics_aggregation.py` | ~65 | INSERT INTO acc_order_logistics_fact z przesyłek GLS |
| `order_logistics_estimation.py` | ~650 | INSERT INTO acc_order_logistics_fact (estimated costs) |

### 6.4 API Endpoints (`profit_v2.py`)

| Endpoint | Linia | Wywołuje | Status |
|----------|-------|----------|--------|
| `GET /profit/v2/products` | ~56 | `get_product_profit_table()` lub rollup path | ✅ OK |
| `GET /profit/v2/orders` | ~771 | `get_profitability_orders()` | ✅ OK |
| `GET /profit/v2/overview` | ~740 | `get_profitability_overview()` | ✅ OK |
| `GET /profit/v2/kpis` | — | `get_profit_kpis()` | ✅ OK |
| `GET /profit/v2/what-if` | ~202 | `get_product_what_if_table()` | ✅ OK |
| `GET /profit/v2/products/export.xlsx` | ~253 | `export_product_profit_xlsx()` | ✅ OK |
| `GET /profit/v2/drilldown` | ~301 | `get_product_drilldown()` | ✅ OK |
| `GET /profit/v2/loss-orders` | ~340 | `get_loss_orders()` | ✅ OK |

### 6.5 Frontend (React)

6 stron wyświetla `logistics_pln`; 10 TypeScript typów referencuje to pole.
Frontend NIE oblicza logistyki — jedynie wyświetla wartości zwrócone z API.
**Żadna weryfikacja frontendu nie jest potrzebna** — poprawność zależy od backendu.

---

## 7. Procedura weryfikacji dla Copilota

### Krok 1: Zweryfikuj feature flag

```bash
# Sprawdź w pliku .env:
grep -i "PROFIT_USE_LOGISTICS_FACT" apps/api/.env
# Spodziewany wynik: PROFIT_USE_LOGISTICS_FACT=true
```

Jeśli flaga = `false` lub brak → system używa TYLKO legacy `acc_order.logistics_pln`.

### Krok 2: Zweryfikuj import helpera w każdym module

Dla `profit_engine.py`:
```bash
grep -n "from app.services.order_logistics_source" apps/api/app/services/profit_engine.py
# Spodziewany: import profit_logistics_join_sql, profit_logistics_value_sql
```

Dla `profitability_service.py`:
```bash
grep -n "from app.services.order_logistics_source" apps/api/app/services/profitability_service.py
# Spodziewany: import obu helperów
```

### Krok 3: Zweryfikuj że każda funkcja live SQL używa helpera

Dla KAŻDEJ funkcji z tabeli w sekcji 6.1 i 6.2 (status ✅ OK):

1. Znajdź definicję funkcji:
   ```bash
   grep -n "def get_product_profit_table" apps/api/app/services/profit_engine.py
   ```
2. Przeczytaj ciało funkcji (~100-200 linii po definicji)
3. Zweryfikuj obecność wywołań:
   ```python
   logistics_join = profit_logistics_join_sql()    # lub z aliasami
   logistics_val  = profit_logistics_value_sql()   # lub z aliasami
   ```
4. Zweryfikuj że `logistics_join` jest wstawiony do SQL query (w FROM/JOIN clause)
5. Zweryfikuj że `logistics_val` jest użyty w SELECT jako alias `logistics_pln`

### Krok 4: Zweryfikuj rollup recomputation (step 4d)

```bash
grep -n "4d.*[Ll]ogistics\|logistics.*fact\|acc_order_logistics_fact" apps/api/app/services/profitability_service.py
```

Sprawdź że `recompute_rollups()` w kroku 4d:
- Używa `OUTER APPLY (SELECT TOP 1 ... FROM acc_order_logistics_fact ... ORDER BY calculated_at DESC)`
- Ma fallback: `ISNULL(fact, ISNULL(o.logistics_pln, 0))`
- Proratuje logistics na SKU-lines proporcjonalnie do `line_net / order_net`

**Znana uwaga**: Ten krok duplikuje logikę helpera zamiast go wywoływać.
To działa poprawnie, ale gdyby helper się zmienił, ten SQL musi być zaktualizowany ręcznie.

### Krok 5: Zweryfikuj spójność danych (opcjonalnie)

Uruchom SQL (read-only):
```sql
-- Ile zamówień ma fact vs legacy w bieżącym miesiącu?
SELECT
    FORMAT(o.purchase_date, 'yyyy-MM') AS month,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN f.total_logistics_pln IS NOT NULL THEN 1 ELSE 0 END) AS with_fact,
    SUM(CASE WHEN o.logistics_pln > 0 THEN 1 ELSE 0 END) AS with_legacy,
    ROUND(SUM(ISNULL(f.total_logistics_pln, 0)), 2) AS fact_total,
    ROUND(SUM(ISNULL(o.logistics_pln, 0)), 2) AS legacy_total
FROM dbo.acc_order o WITH (NOLOCK)
OUTER APPLY (
    SELECT TOP 1 olf.total_logistics_pln
    FROM dbo.acc_order_logistics_fact olf WITH (NOLOCK)
    WHERE olf.amazon_order_id = o.amazon_order_id
    ORDER BY olf.calculated_at DESC
) f
WHERE o.purchase_date >= '2026-01-01'
  AND o.status IN ('Shipped', 'Unshipped')
GROUP BY FORMAT(o.purchase_date, 'yyyy-MM')
ORDER BY 1;
```

### Krok 6: Szukanie „rogue" odniesień (bypass helperów)

```bash
# Szukaj bezpośrednich odwołań do acc_order_logistics_fact POZA helperem:
grep -rn "acc_order_logistics_fact" apps/api/app/services/ --include="*.py" \
  | grep -v "order_logistics_source.py" \
  | grep -v "aggregation.py" \
  | grep -v "estimation.py"
```

**Spodziewany wynik**: Jedyny hit to `profitability_service.py` w `recompute_rollups()` step 4d
(znany dług techniczny — patrz sekcja 8).

Jeśli znajdziesz INNE odwołania → to potencjalny bug — moduł pomija helpera.

```bash
# Szukaj bezpośrednich odwołań do o.logistics_pln POZA helperem:
grep -rn "\.logistics_pln" apps/api/app/services/ --include="*.py" \
  | grep -v "order_logistics_source.py" \
  | grep -v "test_" \
  | grep -v "rollup"
```

---

## 8. Znane problemy

### 8.1 Duplikacja logiki w `recompute_rollups()` step 4d

**Co**: `profitability_service.py` linia ~1098 — krok 4d rekomputacji rollup-ów
pisze własny `OUTER APPLY TOP 1 ... FROM acc_order_logistics_fact ORDER BY calculated_at DESC`
zamiast wywoływać `profit_logistics_join_sql()`.

**Dlaczego to problem**: Jeśli logika helpera się zmieni (np. dodanie warunku `WHERE source_system = 'latest'`),
rollup recomputation NIE odziedziczy tej zmiany.

**Dlaczego tak jest**: Step 4d operuje na pymssql raw SQL w BATCH UPDATE (nie per-row),
a helpery zwracają fragmenty SQL — technicznie możliwe do użycia,
ale refactor wymagałby dostosowania struktury CTE.

**Status**: Działa poprawnie. Logika jest identyczna z helperem.
Niska priorytet do refactoru — monitorować przy zmianach helpera.

### 8.2 Marzec 2026 — brak danych logistycznych

22 785 zamówień z marca nie ma rekordów w `acc_order_logistics_fact`.
Pipeline DHL/GLS nie przetworzyło jeszcze faktur za marzec.
`acc_order.logistics_pln` również = 0.00 dla tych zamówień.

### 8.3 Luty 2026 — częściowe pokrycie

Tylko ~30% zamówień lutego ma rekordy w fact table (vs ~73% w styczniu).
Możliwe opóźnienie w imporcie faktur DHL/GLS za luty.

### 8.4 Różnica Fact vs Legacy

Fact (1 337 300.68 PLN) jest prawie 2× legacy (708 788.38 PLN).
To oczekiwane — fact obejmuje zamówienia bez starej wartości legacy
(11 406 zamówień w styczniu ma fact ale nie legacy).