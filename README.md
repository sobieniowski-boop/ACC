# Amazon Command Center (ACC)

**Centrum dowodzenia dla Dyrektora E-commerce** — standalone monorepo delivering real-time KPI intelligence, order-level profit analytics, inventory management, ads monitoring, and AI-powered recommendations across all KADAX Amazon marketplaces.

---

## Operational Docs

- `docs/AZURE_SQL_SETUP.md` - Azure SQL setup
- `docs/PRODUCT_TASK_WORKFLOW.md` - Product Tasks workflow, SLA alerts, and owner auto-assignment rules
- `docs/FBA_OPS.md` - FBA Ops module, production fallback strategy, diagnostics, and current marketplace caveats
- `docs/FINANCE_CENTER.md` - Finance Center import path, canonical dashboard endpoint, section truth states, bank-import blocker, and current trust limits
- `docs/AMAZON_LISTING_REGISTRY.md` - listing registry staging from Google Sheet, SKU/EAN/ASIN cross-map, and product identity fallback rules
- `docs/MANAGE_ALL_INVENTORY.md` - Inventory 360 module: cached Azure-SQL read path, Sales & Traffic sync, safe Feeds apply/rollback, and current traffic/live-runtime caveats
- `docs/FAMILY_RESTRUCTURE_2026-03-06.md` - Family Restructure v3 pipeline: 7-step DE→target MP replication (PTD validation, PIM enrichment, GPT translation, create parent, reassign children)
- `docs/PROFIT_CM2_NP_WIRING_2026-03-07.md` - P&L Engine V2 unification: V1→V2 delegation, CM2 7-bucket, NP auto-detect
- `GET /api/v1/health/order-sync` - order sync watermark health and recovery status per marketplace

> **v2.13 (2026-03-08):** ECB backup connector, traffic scheduler (04:30), 23 CHECK constraints, sync_runner job logging, ad_spend/refund enrichment in profitability rollups, bidirectional marketplace expansion with DE-first priority, frontend NaN safety + TACOS label.

---

## Architecture

```
C:\ACC\
├── apps/
│   ├── api/                   # FastAPI backend (Python 3.12)
│   │   ├── app/
│   │   │   ├── core/          # config, database, security, redis
│   │   │   ├── models/        # SQLAlchemy async models (17 tabel z prefiksem acc_)
│   │   │   ├── schemas/       # Pydantic v2 schemas
│   │   │   ├── api/v1/        # REST routes (auth, kpi, profit, alerts, jobs)
│   │   │   ├── api/ws.py      # WebSocket (jobs, alerts)
│   │   │   ├── connectors/    # SP-API + MSSQL + NBP + ECB adapters
│   │   │   ├── services/      # profit_service, sync_service, ai_service, ai_product_matcher
│   │   │   ├── jobs/          # Background job modules
│   │   │   └── worker.py      # Celery app + beat schedule
│   │   ├── migrations/        # Alembic (async) — legacy, nie uruchamiać
│   │   └── scripts/           # seed_demo.py
│   └── web/                   # React 18 + TypeScript + shadcn/ui
│       └── src/
│           ├── pages/         # Dashboard, ProfitExplorer, Alerts, Jobs
│           ├── components/    # Layout, Sidebar, TopBar
│           ├── lib/           # api.ts (axios), utils.ts
│           └── store/         # Zustand auth store
└── .env                       # Credentials (gitignored)
```

---

## Stack

| Warstwa | Technologia |
|---------|-------------|
| API | FastAPI 0.115 + SQLAlchemy 2.0 async + Alembic |
| Task queue | Local async (brak Celery/Redis w deploy) |
| Baza danych | **MSSQL** (NetfoxAnalityka) — migracja z PostgreSQL |
| AI | OpenAI GPT-5.2 |
| Frontend | React 18 + TypeScript + Vite + shadcn/ui + Recharts |
| Auth | JWT (access 8h + refresh 30d) + RBAC (5 ról) |
| Źródła danych | Amazon SP-API (KADAX) + MSSQL NetfoxAnalityka + NBP API + ECB XML |
| Kursy walut | **NBP API** (tabela A) — primary; **ECB XML** — backup (30 walut EUR-based) |
| Serwer | Windows Server (192.168.49.97), Python 3.12.8, uvicorn |

---

## Current Modules

Operationally important modules in current ACC:
- Profit / Profit V2 (COGS coverage **99.34%**, 2-pass stamping pipeline)
- Data Quality (inline price editing, map-and-price, AI Product Matcher)
- Amazon Listing Registry (Google Sheet staging -> MSSQL identity lookup)
- Manage All Inventory
  - Azure-SQL-optimized read path via `acc_inv_item_cache`
  - Sales & Traffic import now uses `GET_SALES_AND_TRAFFIC_REPORT`
  - live `JSON_LISTINGS_FEED` apply/rollback is enabled for safe draft types (`reparent`, `update_theme`)
- Family Mapper (+ Restructure v3: DE→EU marketplace family replication with PIM enrichment & GPT translation)
- Content Ops
- FBA Ops
- Finance Center
  - canonical dashboard endpoint: `GET /api/v1/finance/dashboard`
  - section statuses distinguish `real_data`, `partial`, `blocked_by_missing_bank_import`, and `no_data`
- Pricing / Buy Box
  - page now respects `?sku=...` in URL
  - empty state is truthful: no offers appear until `sync_pricing` populates `acc_offer`

`FBA Ops` is active and uses ACC-native patterns:
- FastAPI router: `apps/api/app/api/v1/fba_ops.py`
- service layer: `apps/api/app/services/fba_ops/service.py`
- frontend pages under `apps/web/src/pages/Fba*.tsx`
- schema bootstrap via `ensure_fba_schema()`
- APScheduler jobs, not a separate worker-only runtime

See `docs/FBA_OPS.md` for current scope, endpoints, diagnostics, and Amazon fallback behavior.
See `docs/FINANCE_CENTER.md` for current finance import path, dashboard truth states, bank-import dependency, background jobs, payout groups, completeness alerts, and production caveats.
See `docs/AMAZON_LISTING_REGISTRY.md` for the product identity staging layer used by Missing COGS, AI matcher, FBA Ops, and finance enrichment.
See `docs/PROFIT_TKL_SQL_CACHE.md` for what-if logistics TKL SQL cache (cold-start mitigation + refresh job).
See `docs/PROFIT_WHATIF_LOGISTICS_MODEL.md` for Plan/Observed/Decision courier cost logic and execution drift rules.

---

## Deployment (Windows Server — produkcja)

ACC działa bezpośrednio na Windows Server (bez Docker):

```
Serwer:    192.168.49.97 (PCNETFOX_001)
Backend:   uvicorn :8000  (Windows Task Scheduler: ACC-Backend)
Frontend:  Vite dev :3010
Python:    C:\ACC\.venv\Scripts\python.exe (3.12.8)
MSSQL:     192.168.230.120:11901 / NetfoxAnalityka
ODBC:      "SQL Server" (stary driver — jedyny dostępny)
SSH:       copilot-dev@192.168.49.97 (key auth)
```

### Restart backendu
```powershell
schtasks /end /tn ACC-Backend
ping 127.0.0.1 -n 3 >nul
echo. > C:\ACC\backend.log
schtasks /run /tn ACC-Backend
```

### Weryfikacja
```powershell
curl -s http://localhost:8000/docs -o nul -w "%{http_code}"   # → 200
curl -s http://localhost:3010 -o nul -w "%{http_code}"        # → 200
```

---

## Logowanie

```
Endpoint:  POST /api/v1/auth/token
Body:      {"email": "...", "password": "..."}
Response:  {"access_token": "...", "refresh_token": "..."}

Użytkownik: msobieniowski@netfox.pl / Mil53$SobAdhd
```

---

## Baza Danych — MSSQL

### Migracja PostgreSQL → MSSQL

Projekt był pierwotnie zaprojektowany na PostgreSQL. Migracja obejmowała:

1. **Zmiana connection string** na `mssql+aioodbc://` z `?TrustServerCertificate=yes`
2. **Prefiks tabel `acc_`** — wszystkie tabele mają prefiks dla uniknięcia kolizji z istniejącymi tabelami NetfoxAnalityka
3. **Model `AccOrder`** zamiast `Order` — słowo `ORDER` jest zarezerwowane w SQL Server
4. **UUID jako `UNIQUEIDENTIFIER`** — MSSQL natywny typ
5. **DATETIMEOFFSET → date** — kolumna `purchase_date` zwraca string z offsetem, np. `"2026-02-19 12:07:40.0000000 +00:00"` — wymagana konwersja przez `_to_date()`

### Tabele (17)

| Tabela | Rows (02.03.2026) | Opis |
|--------|-------------------|------|
| `acc_order` | ~101,000+ | Zamówienia Amazon (z revenue_pln) |
| `acc_order_line` | ~152,000+ | Pozycje zamówień (99.34% z COGS) |
| `acc_product` | 319 | Produkty (ASIN/SKU) |
| `acc_purchase_price` | 3,035+ | Historia cen zakupu (holding, xlsx) |
| `acc_product_match_suggestion` | — | AI match suggestions (GPT-4o, human-in-the-loop) |
| `acc_exchange_rate` | 468+ | Kursy walut NBP |
| `acc_inventory_snapshot` | 50 | Stany magazynowe FBA |
| `acc_marketplace` | 9 | Marketplace'y (DE, FR, IT, ES, NL, BE, PL, SE, IE) |
| `acc_user` | 1 | Użytkownicy |
| `acc_job_run` | ~15 | Historia uruchomień jobów (legacy — zastąpione przez `acc_al_jobs`) |
| `acc_al_jobs` | 1006+ | 🆕 Aktywny system jobów (status, trigger_source, progress, retry) |
| `acc_offer` | 0 | Oferty cenowe (do uzupełnienia) |
| `acc_finance_transaction` | 0 | Transakcje finansowe |
| `acc_ads_profile` | 10 | Profile reklamowe (EU marketplaces) |
| `acc_ads_campaign` | 5,083 | Kampanie reklamowe (SP+SB+SD) |
| `acc_ads_campaign_day` | 0 | Dane dzienne kampanii (penduje) |
| `acc_plan_month` | 0 | Plany miesięczne |
| `acc_plan_line` | 0 | Pozycje planów |
| `acc_alert` | 0 | Alerty |
| `acc_alert_rule` | 0 | Reguły alertów |
| `acc_ai_recommendation` | 0 | Rekomendacje AI |

### Konfiguracja połączenia

Plik: `app/core/config.py` → klasa `Settings`

```python
MSSQL_SERVER = "192.168.230.120"
MSSQL_PORT = 11901
MSSQL_DATABASE = "NetfoxAnalityka"
MSSQL_USER = "msobieniowski"
MSSQL_PASSWORD = "IS(*s78^&UYJ9yhyghui*"
```

**UWAGA na atrybuty:** Używaj `settings.MSSQL_SERVER` (nie `MSSQL_HOST`) i `settings.MSSQL_DATABASE` (nie `MSSQL_DB`).

---

## ODBC Driver — Ograniczenia (KRYTYCZNE)

Serwer ma TYLKO stary driver `"SQL Server"` (nie ODBC 17/18). To wymusza:

### 1. Brak MARS (Multiple Active Result Sets)
Nie można mieć dwóch otwartych kursorów jednocześnie. Każdy cursor musi być `close()` przed otwarciem następnego.

```python
# ❌ BŁĄD — dwa kursory naraz
cur1 = conn.cursor()
cur1.execute("SELECT ...")
cur2 = conn.cursor()  # HY000: connection busy!

# ✅ POPRAWNE — zamknij przed otwarciem
cur1 = conn.cursor()
cur1.execute("SELECT ...")
rows = cur1.fetchall()
cur1.close()
cur2 = conn.cursor()
cur2.execute("...")
```

### 2. HY104 (Invalid precision value)
Stary driver nie obsługuje wielu typów Python → SQL bind. Rozwiązanie: `CAST(? AS ...)` w SQL.

```python
# ❌ HY104
cur.execute("UPDATE t SET col = ? WHERE d >= ?", [3.14, "2026-02-19"])

# ✅ POPRAWNE
cur.execute(
    "UPDATE t SET col = CAST(? AS DECIMAL(14,2)) WHERE d >= CAST(? AS DATETIME2)",
    [str(3.14), "2026-02-19"]
)
```

### 3. Brak event handlerów SQLAlchemy
Event listenery (np. `@event.listens_for(Engine, 'connect')`) powodują błędy z tym driverem. Wszelkie hooks w `database.py` muszą być usunięte.

### 4. Wzorzec raw pyodbc + asyncio.to_thread()
Dla operacji wymagających wielu kursorów (batch UPDATE/INSERT) używamy raw pyodbc:

```python
import asyncio
import pyodbc

async def my_batch_operation():
    def _sync_work(conn_str):
        conn = pyodbc.connect(conn_str, autocommit=False)
        try:
            cur = conn.cursor()
            cur.execute("SELECT ...")
            rows = cur.fetchall()
            cur.close()
            
            for row in rows:
                cur2 = conn.cursor()
                cur2.execute("UPDATE ...", [...])
                cur2.close()
            
            conn.commit()
        except:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    await asyncio.to_thread(_sync_work, conn_str)
```

---

## Kursy Walut — NBP API + ECB Backup

### Źródło danych (primary)

**NBP API Tabela A** — oficjalne średnie kursy walut NBP.

```
Base URL:  https://api.nbp.pl/api/exchangerates/rates/a/
Endpoint:  GET /rates/a/{currency}/{date_from}/{date_to}/?format=json
Auth:      Brak (publiczne API)
Limit:     Max 367 dni w jednym zapytaniu
```

### Plik: `app/connectors/nbp.py`

```python
# Pojedynczy kurs (z fallbackiem do 5 dni wstecz)
rate = await fetch_nbp_rate("EUR", date(2026, 2, 19))
# → 4.222

# Zakres dat — bulk (1 request per walutę)
rates = await fetch_nbp_rates_range("EUR", date(2026, 1, 1), date(2026, 2, 26))
# → [(date(2026-01-02), 4.2451), (date(2026-01-03), 4.2398), ...]

# Wiele walut, jedna data
all_rates = await fetch_all_currencies(["EUR", "GBP", "SEK"], date(2026, 2, 19))
# → {"EUR": 4.222, "GBP": 4.891, "SEK": 0.392}
```

### Waluty w Tabeli A NBP

Tabela A zawiera ~30 walut, w tym:
- **EUR, GBP, SEK, TRY, USD, CZK, DKK, NOK, CHF** — bezpośrednio dostępne

### Waluty SPOZA Tabeli A — cross-rate

**AED** (dirham) i **SAR** (rial) NIE SĄ w Tabeli A NBP.
Obliczane przez kurs krzyżowy z USD:

```
AED/PLN = USD/PLN ÷ 3.6725   (AED peg do USD: 1 USD = 3.6725 AED)
SAR/PLN = USD/PLN ÷ 3.7500   (SAR peg do USD: 1 USD = 3.7500 SAR)
```

Źródło: `source = "NBP-cross"` w tabeli `acc_exchange_rate`.

### Model: `acc_exchange_rate`

| Kolumna | Typ | Opis |
|---------|-----|------|
| `id` | UNIQUEIDENTIFIER | PK |
| `rate_date` | DATE | Data kursu |
| `currency` | VARCHAR(5) | Kod waluty (EUR, GBP, etc.) |
| `rate_to_pln` | DECIMAL(10,6) | Kurs do PLN |
| `source` | VARCHAR(50) | "NBP" lub "NBP-cross" |
| `created_at` | DATETIME2 | Timestamp |

UniqueConstraint: `(rate_date, currency)` — jeden kurs na dzień na walutę.

### Sync kursów: `sync_exchange_rates`

Plik: `app/services/sync_service.py` → `async def sync_exchange_rates()`

**Mechanizm:**
1. Pobiera aktywne marketplace'y z bazy → wyciąga unikalne waluty (bez PLN)
2. Bulk fetch z NBP API: 1 HTTP request per walutę (zamiast N requestów dzień-po-dniu)
3. Jeśli jest AED lub SAR → zawsze pobiera też USD → przelicza cross-rate
4. INSERT via raw pyodbc (omija HY104 starego drivera)
5. Deduplikacja: sprawdza istniejące `(rate_date, currency)` przed insertem

**Triggering:**
```bash
POST /api/v1/jobs/run
{
    "job_type": "sync_exchange_rates",
    "marketplace_id": "A1PA6795UKMFR9",
    "params": {"days_back": 30}
}
```

### Stan kursów (26.02.2026)

| Waluta | Źródło | Ilość | Zakres kursów | Okres |
|--------|--------|-------|---------------|-------|
| EUR | NBP | 78 | 4.2009 – 4.2597 | 03.11.2025 – 26.02.2026 |
| GBP | NBP | 78 | 4.7854 – 4.9015 | 03.11.2025 – 26.02.2026 |
| SEK | NBP | 78 | 0.3836 – 0.3996 | 03.11.2025 – 26.02.2026 |
| TRY | NBP | 78 | 0.0807 – 0.0880 | 03.11.2025 – 26.02.2026 |
| AED | NBP-cross | 78 | 0.9543 – 1.0100 | 03.11.2025 – 26.02.2026 |
| SAR | NBP-cross | 78 | 0.9345 – 0.9891 | 03.11.2025 – 26.02.2026 |

**WAŻNE:** Wcześniej (przed 26.02.2026) tabela zawierała 540 rekordów „seed" z hardcoded kursem 4.25 EUR/PLN.
Zostały usunięte i zastąpione realnymi kursami NBP API.

### Weekendy i święta

NBP nie publikuje kursów w weekendy i święta. Zapytanie `rate_date <= ?` z `ORDER BY rate_date DESC` automatycznie użyje kursu z ostatniego dnia roboczego (piątek).

### ECB Exchange Rate Backup (🆕 v2.13)

**Plik:** `app/connectors/ecb.py`
**Tabela:** `ecb_exchange_rate`
**Scheduler:** codziennie 02:30 (`sync-ecb-exchange-rates-daily`)
**Sync runner:** `python sync_runner.py --ecb-rates`

ECB XML feed (`eurofxref-hist-90d.xml`) jako backup dla NBP. 30 walut EUR-based.
NBP pozostaje primary source (`acc_exchange_rate`), ECB jest w osobnej tabeli.

```python
from app.connectors.ecb import fetch_ecb_rates
rates = await fetch_ecb_rates(days_back=90)
# → [{"rate_date": "2026-03-07", "source_currency": "EUR", "target_currency": "USD", "rate": 1.1561}, ...]
```

---

## Kalkulator Zysku — Profit Service

Plik: `app/services/profit_service.py`

### Zasada przeliczania revenue

**Każde zamówienie jest przeliczane po kursie NBP z dnia zakupu (`purchase_date`):**

```
revenue_pln = order_total × kurs_NBP_z_dnia_purchase_date
```

Np. zamówienie z 25.02 → kurs EUR/PLN z 25.02 (4.2183) → `revenue_pln = order_total × 4.2183`

### Lookup kursu walutowego

```sql
SELECT TOP 1 rate_to_pln 
FROM acc_exchange_rate 
WHERE currency = ? AND rate_date <= ? 
ORDER BY rate_date DESC
```

Szuka kursu z dokładnie tego dnia, a jeśli brak (weekend) — bierze z ostatniego dnia publikacji.

### Funkcja `recalculate_profit_batch()`

**Implementacja: raw pyodbc** (nie SQLAlchemy) — ze względu na ograniczenia ODBC.

Algorytm:
1. **SELECT** zamówienia `WHERE status='Shipped' AND purchase_date >= CAST(? AS DATETIME2)`
2. **SELECT** wszystkie linie zamówień (`acc_order_line`) za jednym zapytaniem (bulk)
3. **SELECT** kursy walut per (currency, date) — z cache'em (dict fx_cache)
4. **Kalkulacja** per zamówienie:
   - `revenue_pln = order_total × fx_rate`
   - `total_cogs = Σ(cogs_pln × qty)` per linia
   - `total_fees = Σ((fba_fee + referral_fee) × qty)` per linia
   - `cm = revenue_pln - total_cogs - total_fees`
   - `cm_percent = cm / revenue_pln × 100`
5. **UPDATE** zamówienie: `revenue_pln`, `cogs_pln`, `amazon_fees_pln`, `contribution_margin_pln`, `cm_percent`

### Helper: `_to_date(val)`

Bezpieczna konwersja `purchase_date` z MSSQL DATETIMEOFFSET:
- `datetime` → `.date()`
- `date` → passthrough
- `str` (np. `"2026-02-19 12:07:40.0000000 +00:00"`) → `fromisoformat()` → `.date()`
- `None` → `date.today()`

### Triggering

```bash
POST /api/v1/jobs/run
{
    "job_type": "calc_profit",
    "marketplace_id": "A1PA6795UKMFR9",
    "params": {"days_back": 10}
}
```

**UWAGA:** Domyślne `days_back=1`. Dla przetworzenia starszych zamówień podaj większą wartość w `params`.

### Weryfikacja (26.02.2026)

```
Orders:     351 (wszystkie Shipped)
Revenue:    37,883.95 PLN
Avg order:  107.93 PLN
EUR rate:   4.2220 (19.02.2026 — data wszystkich obecnych zamówień)
CM:         100% (brak danych COGS/fees — do uzupełnienia)
```

Każde zamówienie ma `revenue_pln` zgodne z `order_total × kurs_NBP_z_dnia_purchase_date`.

---

## Synchronizacja Danych — Sync Service

Plik: `app/services/sync_service.py` (~1000 linii)

### Dostępne joby

| Job type | Funkcja | Co robi |
|----------|---------|---------|
| `sync_orders` | `sync_orders()` | Pobiera zamówienia z SP-API |
| `sync_finances` | `sync_finances()` | Transakcje finansowe SP-API |
| `sync_inventory` | `sync_inventory()` | Stany FBA z SP-API |
| `sync_exchange_rates` | `sync_exchange_rates()` | Kursy NBP API (bulk) |
| `sync_ecb_exchange_rates` | `sync_ecb_exchange_rates()` | 🆕 Kursy ECB XML (backup, 30 walut) |
| `inventory_sync_sales_traffic` | `sync_inventory_sales_traffic()` | 🆕 Sales & Traffic reports (scheduler 04:30) |
| `calc_profit` | `recalculate_profit_batch()` | Przelicza revenue/CM |
| `sync_products` | `sync_products()` | Katalog produktów SP-API |

### Kolejność uruchomienia (Initial Data Sync)

```
1. sync_orders         → acc_order + acc_order_line
2. sync_products       → acc_product
3. sync_exchange_rates → acc_exchange_rate (kursy NBP)
4. calc_profit         → aktualizacja revenue_pln w acc_order
5. sync_inventory      → acc_inventory_snapshot (opcjonalne)
6. sync_finances       → acc_finance_transaction (opcjonalne)
```

### Job API

```bash
# Trigger
POST /api/v1/jobs/run
Content-Type: application/json
Authorization: Bearer <token>

{
    "job_type": "sync_exchange_rates",
    "marketplace_id": "A1PA6795UKMFR9",
    "params": {"days_back": 30}
}

# Status
GET /api/v1/jobs/{job_id}

# Response
{
    "id": "...",
    "status": "success",        # pending | running | success | failure
    "records_processed": 468,
    "progress_pct": 100,
    "error_message": "-",
    "started_at": "...",
    "finished_at": "..."
}
```

---

## KPI Dashboard

### Endpoint

```
GET /api/v1/kpi/summary?date_from=2026-02-01&date_to=2026-02-28
```

### Zwracane wartości

```json
{
    "revenue_pln": 37883.95,
    "orders_count": 351,
    "avg_order_pln": 107.93,
    "cm_pct": 100.0,
    "by_marketplace": [
        {
            "marketplace_id": "A1PA6795UKMFR9",
            "name": "Amazon.de",
            "revenue_pln": 37883.95,
            "orders_count": 351
        }
    ]
}
```

**Uwaga:** `cm_pct = 100%` ponieważ brak jeszcze danych COGS i fees.

---

## Marketplace'y

| Marketplace ID | Kraj | Waluta | Domena |
|----------------|------|--------|--------|
| `A1PA6795UKMFR9` | Niemcy (DE) | EUR | amazon.de |
| `A13V1IB3VIYZZH` | Francja (FR) | EUR | amazon.fr |
| `APJ6JRA9NG5V4` | Włochy (IT) | EUR | amazon.it |
| `A1RKKUPIHCS9HS` | Hiszpania (ES) | EUR | amazon.es |
| `A1805IZSGTT6HS` | Holandia (NL) | EUR | amazon.nl |
| `AMEN7PMS3EDWL` | Belgia (BE) | EUR | amazon.com.be |
| `A1C3SOZRARQ6R3` | Polska (PL) | PLN | amazon.pl |
| `A2NODRKZP88ZB9` | Szwecja (SE) | SEK | amazon.se |
| `A28R8C7NBKEWEA` | Irlandia (IE) | EUR | amazon.ie |

> **Uwaga (v2.13):** Marketplace whitelist ograniczony do 9 aktywnych rynków EU. GB/AE/SA/TR usunięte z aktywnego pipeline'u.

---

## Połączenie SP-API (Amazon Selling Partner API)

Plik: `app/connectors/amazon_sp_api/client.py`

```python
# Retry automatyczny na 429 (Too Many Requests)
# Exponential backoff: 1s, 2s, 4s (max 3 próby)
# Base URL: https://sellingpartnerapi-eu.amazon.com
```

### Używane endpointy

| Endpoint | Plik | Co pobiera |
|----------|------|-----------|
| `GET /orders/v0/orders` | `orders.py` | Lista zamówień (paginacja NextToken) |
| `GET /orders/v0/orders/{id}/orderItems` | `orders.py` | Pozycje zamówienia |
| `GET /finances/v0/financialEvents` | `finances.py` | Transakcje finansowe |
| `GET /fba/inventory/v1/summaries` | `inventory.py` | Stany FBA per ASIN/SKU |
### Amazon Ads API (NOWE — Marzec 2026)

**Connector:** `app/connectors/amazon_ads_api/` — osobne credentials od SP-API.

```python
# Base URL: https://advertising-api-eu.amazon.com
# Auth: LWA OAuth (AMAZON_ADS_CLIENT_ID/SECRET/REFRESH_TOKEN)
# Headers: Amazon-Advertising-API-ClientId, Amazon-Advertising-API-Scope (profile_id)
```

| Endpoint | Plik | Co pobiera |
|----------|------|------------|
| `GET /v2/profiles` | `profiles.py` | Profile reklamowe (10 EU marketplace) |
| `POST /sp/campaigns/list` | `campaigns.py` | Kampanie Sponsored Products (v3) |
| `POST /sb/v4/campaigns/list` | `campaigns.py` | Kampanie Sponsored Brands (v4) |
| `POST /sd/campaigns/list` | `campaigns.py` | Kampanie Sponsored Display (v3) |
| `POST /reporting/reports` | `reporting.py` | Raporty dzienne (async: create → poll → download) |

**Sync service:** `app/services/ads_sync.py` — profiles → campaigns → daily reports → MERGE Azure SQL.
**Scheduler:** codziennie 07:00 (`_sync_ads()`, `max_instances=1`).
**Stan:** 10 profili, 5,083 kampanii, daily reports pending (Amazon report generation wolny).
---

## Integracja MSSQL (NetfoxAnalityka)

Plik: `app/connectors/mssql/netfox.py`

### Konfiguracja

```
SERVER=192.168.230.120,11901
DATABASE=NetfoxAnalityka
UID=msobieniowski
PWD=IS(*s78^&UYJ9yhyghui*
DRIVER={SQL Server}       ← stary driver, jedyny dostępny!
```

### SCHEMA MAPPING

Na górze pliku `netfox.py` jest dataclass `_SchemaMap` mapujący nazwy tabel/kolumn Comarch XL:

```python
@dataclass
class _SchemaMap:
    tbl_products: str = "dbo.Kartoteki"
    col_sku:      str = "Symbol"
    col_ean:      str = "EAN"
    col_purchase_price: str = "CenaZakupu"
```

### Dostępne funkcje

```python
from app.connectors.mssql.netfox import (
    get_product_costs,
    get_warehouse_stock,
    get_open_purchase_orders,
    get_products_with_stock,
    test_connection,
)
```

**Wszystkie wywołania MSSQL są synchroniczne** (pyodbc nie ma async). W FastAPI używaj:
```python
result = await asyncio.to_thread(get_product_costs, skus)
```

---

## Auth / Role

### Logowanie

```bash
POST /api/v1/auth/token
{"email": "msobieniowski@netfox.pl", "password": "Mil53$SobAdhd"}
# → {"access_token": "...", "refresh_token": "..."}
```

### Role

| Rola | Dostęp |
|------|--------|
| `admin` | Pełny + zarządzanie użytkownikami |
| `director` | Pełny odczyt/zapis + planowanie |
| `category_mgr` | Własne kategorie + pricing |
| `ops` | Zamówienia + magazyn + joby |
| `analyst` | Tylko odczyt |

### Auth flow (frontend)

```
Login.tsx → login(email, password) → /auth/token
  → authStore.setTokens(access, refresh) → localStorage (Zustand)

Każdy request → api.ts interceptor → Bearer token
401 → interceptor → /auth/refresh → nowy token
  → refresh też 401 → logout() → /login
```

---

## Frontend

### Strony

| Strona | API calls | Opis |
|--------|-----------|------|
| Dashboard | `getKPISummary`, `getRevenueChart` | KPI + wykres revenue |
| ProfitExplorer | `getProfitOrders` | P&L per zamówienie |
| Pricing | `getPricingOffers`, `getBuyBoxStats` | Oferty cenowe |
| Planning | `getPlanMonths`, `getPlanVsActual` | Plan vs actual |
| Inventory | `getInventory`, `getReorderSuggestions` | Stany FBA |
| Ads | `getAdsSummary`, `getAdsChart` | Kampanie reklamowe |
| AI Recommendations | `getAIRecommendations` | Sugestie AI |
| Alerts | `getAlerts`, `markAlertRead` | Alerty |
| Jobs | `getJobs`, `runJob` | Zarządzanie jobami |

### Design system

```css
--bg-primary: #060d1a       /* najciemniejsze tło */
--bg-secondary: #0f172a     /* karty */
--accent: #FF9900           /* Amazon orange */
--text: #ffffff
--text-muted: rgba(255,255,255,0.5)
```

---

## Zmienne Środowiskowe (.env)

| Zmienna | Opis |
|---------|------|
| `MSSQL_SERVER` | Serwer MSSQL (192.168.230.120) |
| `MSSQL_PORT` | Port MSSQL (11901) |
| `MSSQL_DATABASE` | Baza danych (NetfoxAnalityka) |
| `MSSQL_USER` | Użytkownik MSSQL |
| `MSSQL_PASSWORD` | Hasło MSSQL |
| `SECRET_KEY` | JWT signing secret (**zmienić w prod!**) |
| `SP_API_CLIENT_ID` | Amazon LWA client ID |
| `SP_API_CLIENT_SECRET` | Amazon LWA client secret |
| `SP_API_REFRESH_TOKEN` | Amazon seller refresh token |
| `SP_API_SELLER_ID` | Amazon seller ID |
| `AMAZON_ADS_CLIENT_ID` | Amazon Ads API LWA client ID |
| `AMAZON_ADS_CLIENT_SECRET` | Amazon Ads API LWA client secret |
| `AMAZON_ADS_REFRESH_TOKEN` | Amazon Ads API refresh token |
| `AMAZON_ADS_REGION` | Amazon Ads region (EU/NA/FE) |
| `OPENAI_API_KEY` | OpenAI API key |

---

## Historia Zmian

### 26.02.2026 — Naprawa kursów walut i kalkulacji revenue

**Problem:** Wszystkie 540 kursów walut w bazie pochodziło z seed'a (`source="seed"`) z hardcoded kursem EUR/PLN = 4.25. Revenue_pln albo nie były wyliczone, albo używały niepoprawnych kursów.

**Wykonane zmiany:**

1. **Usunięto 540 seedowych kursów** — zastąpiono 468 realnymi kursami z NBP API
2. **Pobrano kursy z NBP API** za okres 03.11.2025 – 26.02.2026:
   - EUR, GBP, SEK, TRY — bezpośrednio z API `https://api.nbp.pl/api/exchangerates/rates/a/{cur}/{from}/{to}/`
   - AED, SAR — obliczone z kursu USD przez cross-rate (AED peg: 3.6725, SAR peg: 3.7500)
3. **Przeliczono revenue** dla 351 zamówień: 38,135.73 PLN (kurs seed 4.25) → **37,883.95 PLN** (kurs NBP 4.2220)
4. **Naprawiono `sync_exchange_rates`** w `sync_service.py`:
   - Bulk fetch (1 HTTP per walutę zamiast 30+ requestów dzień-po-dniu)
   - Obsługa AED/SAR przez USD cross-rate
   - Raw pyodbc INSERT (omija HY104 starego drivera ODBC)
   - Deduplikacja — nie wstawia duplikatów
5. **Naprawiono `recalculate_profit_batch`** w `profit_service.py`:
   - Raw pyodbc via `asyncio.to_thread()` (zamiast SQLAlchemy — MARS issues)
   - Helper `_to_date()` — konwersja MSSQL DATETIMEOFFSET string → date
   - `CAST(? AS DATETIME2)` / `CAST(? AS DECIMAL)` — omija HY104
   - Kurs per zamówienie (nie jeden efektywny dla wszystkich)

### Wcześniej — Migracja PostgreSQL → MSSQL

1. **Zmiana bazy** z PostgreSQL na MSSQL (NetfoxAnalityka)
2. **Prefiks `acc_`** na wszystkich tabelach (17 tabel)
3. **Model `AccOrder`** (zamiast `Order` — reserved keyword)
4. **Usunięcie eventlistenerów** z `database.py` (łamały stary driver ODBC)
5. **Naprawa logowania** — endpoint `/auth/token`, pole `email`
6. **Synchronizacja danych** z SP-API: 351 zamówień, 379 linii, 319 produktów, 50 inventory

---

## Znane Ograniczenia

### Krytyczne

1. **ODBC Driver "SQL Server"** — stary driver bez MARS, bez HY104 support. Wszelkie batch operacje muszą używać raw pyodbc z pattern'em cursor open/close.

2. **AED/SAR nie ma w NBP Tabela A** — obliczane z USD cross-rate. Przy dużej zmienności USD/AED peg (która praktycznie nie występuje, bo oba są pegged) kurs może być niedokładny.

3. **COGS coverage 99.34%** — 151,648/152,662 order lines z ceną zakupu. Pozostałe ~1,014 lines to głównie Amazon bundles (AI matcher w fazie review).

4. **Brak Amazon fees** — `fba_fee_pln = 0`, `referral_fee_pln = 0`. CM1 zawyżone.

5. **Brak Redis** — backend używa `_NoopRedis` fallback (w pamięci). Cache KPI jest krótkotrwały.

### Architektoniczne

6. **Celery nie działa** — na serwerze produkcyjnym brak Redis/broker. Joby uruchamiane przez lokalny async (`asyncio.create_task`).

7. **`purchase_date` jako string** — MSSQL DATETIMEOFFSET zwraca string przez stary driver. Każde porównanie/konwersja musi przejść przez `_to_date()`.

8. **SCP race condition** — tworzenie pliku i natychmiastowe SCP może dać 0 bajtów. Weryfikuj `dir` po przesyłce.

---

## Roadmapa

### ✅ Zrobione

- [x] Migracja PostgreSQL → MSSQL
- [x] Synchronizacja zamówień SP-API (101K+ orders, 116K+ lines)
- [x] Synchronizacja produktów (319)
- [x] Synchronizacja inventory (50 snapshots)
- [x] Kursy walut z NBP API (realne, nie seed)
- [x] Cross-rate AED/SAR z USD
- [x] Kalkulacja revenue_pln per zamówienie po kursie z dnia zakupu
- [x] Dashboard KPI endpoint
- [x] **Amazon Ads API** — profiles (10), campaigns SP/SB/SD (5,083), reporting (v3 async)
- [x] **Security** — usunięto hardcoded credentials, .gitignore zaktualizowany
- [x] **Order backfill** — Reports API TSV, dual MERGE (orders + lines)
- [x] **Deep Health endpoint** — Azure SQL + Redis + SP-API concurrent check

### 🟨 Do zrobienia

**Priorytet 1 — Dane kosztowe:**
- [x] ~~Podłączyć COGS~~ — DONE (99.34% coverage, 2-pass pipeline, XLSX import, ASIN cross-lookup)
- [x] ~~AI Product Matcher~~ — DONE (GPT-4o matching ~122 unmapped bundles, BOM decomposition, human-in-the-loop)
- [x] ~~Data Quality UI~~ — DONE (inline price editing + map-and-price for unmapped products)
- [ ] Podłączyć Amazon fees (FBA + referral)
- [ ] Realna kalkulacja CM1 (contribution margin)
- [ ] Ads daily reports → CM1 integration (`ads_cost_pln` allocation per order)

**Priorytet 2 — Automatyzacja:**
- [x] ~~Windows Task Scheduler: cykliczny sync (orders + rates + profit)~~
- [x] ~~Amazon Ads API (ACoS historyczny)~~ — DONE
- [ ] Auto-pricing SP-API

**Priorytet 3 — Uzupełnienie funkcji:**
- [ ] Export do Excel
- [ ] Powiadomienia email
- [ ] Porównanie marketplace'ów
- [ ] Historia alertów

**Priorytet 4 — Dług techniczny:**
- [ ] Testy jednostkowe (pytest + httpx.AsyncClient)
- [ ] Testy frontend (vitest)
- [ ] CI/CD (GitHub Actions)
- [ ] Health check endpoint
- [ ] Logi produkcyjne (structlog → Sentry)

---

## Kontakty i Zasoby

| Zasób | Link |
|-------|------|
| Amazon SP-API docs | https://developer-docs.amazon.com/sp-api/ |
| Amazon Seller Central | https://sellercentral.amazon.de |
| NBP API kursy walut | https://api.nbp.pl |
| NBP Tabela A (lista walut) | https://api.nbp.pl/api/exchangerates/tables/a/ |
| FastAPI docs | https://fastapi.tiangolo.com |
| Serwer ACC | 192.168.49.97:8000 (backend), :3010 (frontend) |
| MSSQL NetfoxAnalityka | 192.168.230.120:11901 |

---

*Ostatnia aktualizacja: 3 marca 2026*
*Autor: GitHub Copilot (Claude Opus 4.6) + Dyrektor E-commerce KADAX*
