# Amazon Ads API — Plan Wdrożenia

> **Data:** 2 Marca 2026  
> **Status:** ✅ **WDROŻONY** — Fazy 1-3 zakończone, Faza 4-5 pending  
> **Scope:** Zarządzanie i raportowanie kampanii SP/SB/SD na 13 marketplace'ach  

---

## 1. Co mamy już gotowe w ACC

ACC ma **kompletny front-end i back-end** dla modułu Ads — ale **bez podłączenia do prawdziwego API**:

| Warstwa | Plik | Stan |
|---------|------|------|
| Model DB | `app/models/ads.py` | `acc_ads_campaign` + `acc_ads_campaign_day` (SQLAlchemy) |
| Schemas | `app/schemas/ads_schema.py` | CampaignOut, AdsSummaryResponse, AdsChartResponse, TopCampaignRow, BudgetRecommendation |
| API Routes | `app/api/v1/ads.py` | `/ads/campaigns`, `/ads/summary`, `/ads/chart`, `/ads/top-campaigns` |
| Frontend | `apps/web/src/pages/Ads.tsx` | KPI tiles (spend, sales, ACoS, clicks), Spend vs Sales chart, Top Campaigns table |
| Frontend API | `apps/web/src/lib/api.ts` | Typy + wywołania (getAdsSummary, getAdsChart, getTopCampaigns) |
| CM1 Integration | `profit_engine.py`, `mssql_store.py` | `acc_order.ads_cost_pln` — wchodzi do kalkulacji marży CM1 |
| Planning | `planning.py` | `budget_ads_pln` per marketplace/miesiąc |

**Brakuje TYLKO:** Connector do Amazon Ads API + sync service.

---

## 2. Amazon Ads API vs SP-API — kluczowe różnice

| | SP-API (mamy) | Amazon Ads API (nowe) |
|---|---|---|
| **Host** | `sellingpartnerapi-eu.amazon.com` | `advertising-api-eu.amazon.com` |
| **Auth** | LWA OAuth (SP-API app) | LWA OAuth (**osobna** aplikacja/credentials) |
| **Profile** | Marketplace ID | **Advertising Profile ID** (per marketplace) |
| **Credentials** | `SP_API_CLIENT_ID/SECRET` | `AMAZON_ADS_CLIENT_ID/SECRET` (osobne!) |
| **Rate limits** | 0.5-15 req/s per endpoint | 10 req/s (ogólny) |
| **Dane** | Zamówienia, produkty, finanse | Kampanie, wydatki, ACoS, ROAS, impressions |

> **UWAGA:** Amazon Ads API wymaga **OSOBNYCH** credentials (client_id, client_secret, refresh_token).  
> Mogą używać tego samego LWA app co SP-API — ale profil reklamowy jest inny koncept niż marketplace.

---

## 3. Co daje nam Amazon Ads API

### 3.1 Dane, które nasi trafią do ACC

| Dane | API Endpoint | Tabela ACC | Częstotliwość |
|------|-------------|------------|---------------|
| **Lista kampanii** (SP, SB, SD) | `GET /sp/campaigns`, `/sb/campaigns`, `/sd/campaigns` | `acc_ads_campaign` | 1x dziennie |
| **Dzienny performance** (impressions, clicks, spend, sales, ACoS) | `POST /reporting/reports` → download | `acc_ads_campaign_day` | 1x dziennie |
| **Budżety kampanii** | W ramach campaigns endpoint | `acc_ads_campaign.daily_budget` | 1x dziennie |
| **Targeting keywords/targets** | `/sp/keywords`, `/sp/targets` | 🔮 Faza 2 | Opcjonalnie |

### 3.2 Wartość biznesowa dla KADAX

1. **Pełny CM1 z ACoS** — teraz `ads_cost_pln` w profit jest 0. Po wdrożeniu: prawdziwe koszty reklamy per marketplace/dzień w kalkulacji marży
2. **Dashboard ACoS/ROAS** — Ads.tsx już istnieje, tylko czeka na dane
3. **Budget monitoring** — alert gdy ACoS > próg lub budget wyczerpany
4. **TACoS (Total ACoS)** — stosunek ad spend do total sales (nie tylko ad-attributed)
5. **Cross-marketplace comparison** — który marketplace ma najlepszy ROAS
6. **Planning integration** — porównanie planned vs actual ad spend

---

## 4. Onboarding — kroki manualne (Miłosz)

### Krok 1: Developer Account
Ze screena widzę komunikat *"This Amazon.com account isn't associated with a developer account"*.

Opcje:
- **A) "Switch accounts"** — jeśli KADAX ma osobne konto developer (np. to samo co SP-API)
- **B) "Create a developer.amazon.com account"** — jeśli nie

> **Rekomendacja:** Najpierw "Switch accounts" i zaloguj się na konto, które ma SP-API już skonfigurowane. Jeśli to konto KADAX Seller, powinno być powiązane z developer account.

### Krok 2: Utwórz/Wybierz LWA Application
1. Idź na `developer.amazon.com` → **Login with Amazon** → **Security Profiles**
2. Możesz użyć **tego samego** Security Profile co SP-API albo stworzyć nowy
3. Zanotuj: **Client ID** + **Client Secret**

### Krok 3: Połącz LWA App z Ads API
1. Na `advertising.amazon.com/developer/overview` → "Submit Amazon application"
2. Wybierz swoją LWA Application z dropdown
3. Scope: `advertising::campaign_management` (already approved)

### Krok 4: OAuth consent — uzyskaj Refresh Token
```
Step 1: GET https://www.amazon.com/ap/oa
  ?client_id={LWA_CLIENT_ID}
  &scope=advertising::campaign_management
  &response_type=code
  &redirect_uri=https://localhost  (tymczasowy)

Step 2: Z kodu autoryzacyjnego wymień na token:
POST https://api.amazon.com/auth/o2/token
  grant_type=authorization_code
  code={AUTH_CODE}
  redirect_uri=https://localhost
  client_id={LWA_CLIENT_ID}
  client_secret={LWA_CLIENT_SECRET}

→ Dostaniesz: refresh_token (TRWAŁY, zapisz w .env)
```

### Krok 5: Dodaj do `.env`
```env
# Amazon Ads API
AMAZON_ADS_CLIENT_ID=amzn1.application-oa2-client.xxxxx
AMAZON_ADS_CLIENT_SECRET=xxxxx
AMAZON_ADS_REFRESH_TOKEN=Atzr|xxxxx
AMAZON_ADS_REGION=EU           # EU | NA | FE
```

---

## 5. Plan implementacji technicznej

### Faza 1 — Connector + Profile Discovery (1 dzień)

```
apps/api/app/connectors/amazon_ads_api/
├── __init__.py
├── client.py        ← AdsAPIAuth (LWA, osobny od SP-API) + AdsAPIClient (httpx)
├── profiles.py      ← GET /v2/profiles → mapowanie profile_id ↔ marketplace_id
├── campaigns.py     ← GET /sp/campaigns, /sb/campaigns, /sd/campaigns
└── reporting.py     ← POST /reporting/reports → poll → download (GZIP JSON)
```

**`client.py`** — wzorowany na SP-API `client.py`:
```python
ADS_API_HOST = "https://advertising-api-eu.amazon.com"
LWA_TOKEN_URL = "https://api.amazon.com/auth/o2/token"

class AdsAPIAuth:
    """LWA token cache for Amazon Ads API (separate credentials from SP-API)."""
    
class AdsAPIClient:
    """Thin async HTTP client for Amazon Ads API.
    Headers: Amazon-Advertising-API-ClientId, Amazon-Advertising-API-Scope (profile_id)
    """
```

**`profiles.py`** — kluczowy discovery step:
```python
async def get_profiles() -> list[dict]:
    """GET /v2/profiles → returns [{profileId, countryCode, accountInfo, ...}]
    Maps each profile to our MARKETPLACE_REGISTRY by countryCode.
    """
```

### Faza 2 — Sync Service + Daily Reports (1-2 dni)

```
apps/api/app/services/ads_sync.py  ← Główny sync orchestrator
```

Flow:
1. `sync_profiles()` → cache `acc_ads_profile` (profile_id ↔ marketplace_id)
2. `sync_campaigns()` → per profile: GET SP/SB/SD campaigns → MERGE `acc_ads_campaign`
3. `sync_daily_reports()` → per profile: request report (last 3 days) → download → MERGE `acc_ads_campaign_day`

**Reporting flow (API v3):**
```python
# 1. Request report
POST /reporting/reports
{
    "reportTypeId": "spCampaigns",        # lub sbCampaigns, sdCampaigns
    "timeUnit": "DAILY",
    "groupBy": ["campaign"],
    "columns": ["campaignName", "impressions", "clicks", "spend", "sales7d", 
                 "orders7d", "unitsSold7d", "campaignBudgetAmount"],
    "dateRange": {"startDate": "2026-03-01", "endDate": "2026-03-01"},
    "format": "GZIP_JSON"
}

# 2. Poll → GET /reporting/reports/{reportId}
# 3. Download → GZIP'd JSON array
```

### Faza 3 — Scheduler + MERGE to Azure SQL (0.5 dnia)

```python
# scheduler.py — dodać:
scheduler.add_job(sync_ads_daily, "cron", hour=7, minute=0, id="ads_sync")
```

**MERGE patterns** — tak samo jak `backfill_via_reports.py`:
- `_bulk_upsert_campaigns()` → MERGE INTO `acc_ads_campaign`
- `_bulk_upsert_campaign_days()` → MERGE INTO `acc_ads_campaign_day`
- `connect_acc()` + pymssql_compat (? → %s)
- WITH (NOLOCK) na reads, SET LOCK_TIMEOUT 30000 na writes

### Faza 4 — CM1 Integration (0.5 dnia)

Alokacja `ads_cost_pln` per order:
```
ads_cost_per_order = daily_marketplace_spend / daily_marketplace_orders
```
- Per marketplace per day: łączny spend / ilość zamówień
- UPDATE `acc_order SET ads_cost_pln = X WHERE marketplace_id = Y AND purchase_date = Z`
- Uruchamiać po sync_ads_daily

### Faza 5 — Historyczny backfill (1 dzień)

Amazon Ads API daje dane historyczne do **60 dni wstecz**.
- Jednorazowy backfill: requestować reports per marketplace × per dzień × last 60 days
- ~60 × 13 = 780 raportów (Rate limit: 10/s → ok w kilka minut)
- Po backfill: dashboard ma od razu 60 dni danych

---

## 6. Nowe tabele / schema changes

### `acc_ads_profile` (NOWE)
```sql
CREATE TABLE acc_ads_profile (
    profile_id        BIGINT        PRIMARY KEY,
    marketplace_id    NVARCHAR(30)  NOT NULL REFERENCES acc_marketplace(id),
    country_code      NVARCHAR(5)   NOT NULL,
    account_type      NVARCHAR(20)  NOT NULL,  -- seller | vendor
    account_name      NVARCHAR(200),
    last_synced_at    DATETIME2,
    created_at        DATETIME2     DEFAULT GETUTCDATE()
);
```

### `acc_ads_campaign` (JUŻ ISTNIEJE w models, może wymagać ALTER)
Kolumny do dodania:
```sql
ALTER TABLE acc_ads_campaign ADD profile_id BIGINT REFERENCES acc_ads_profile(profile_id);
ALTER TABLE acc_ads_campaign ADD ad_type NVARCHAR(5);  -- SP, SB, SD
```

### `acc_ads_campaign_day` (JUŻ ISTNIEJE w models)
- Schema wygląda kompletnie (impressions, clicks, spend, sales_7d, orders_7d, acos, roas, spend_pln, sales_pln)
- Może potrzebować: `units_7d`, `cpc`, `ctr` (kalkulowane lub stored)

---

## 7. Config changes

```python
# config.py — dodać:
# Amazon Ads API
AMAZON_ADS_CLIENT_ID: str = ""
AMAZON_ADS_CLIENT_SECRET: str = ""
AMAZON_ADS_REFRESH_TOKEN: str = ""
AMAZON_ADS_REGION: str = "EU"  # EU | NA | FE

@property
def amazon_ads_enabled(self) -> bool:
    return bool(self.AMAZON_ADS_CLIENT_ID and self.AMAZON_ADS_REFRESH_TOKEN)
```

---

## 8. Timeline & priorytety

| Krok | Co | Kto | Czas | Status |
|------|-----|-----|------|--------|
| **0** | Onboarding (developer account + OAuth) | Miłosz | 30 min | ✅ DONE |
| **1** | `amazon_ads_api/client.py` + `profiles.py` | Dev | 2-3h | ✅ DONE |
| **2** | `campaigns.py` + `reporting.py` | Dev | 3-4h | ✅ DONE |
| **3** | `ads_sync.py` (sync service) | Dev | 4h | ✅ DONE |
| **4** | Schema (CREATE/ALTER tables) | Dev | 30 min | ✅ DONE (DDL migration) |
| **5** | Scheduler job + CM1 alokacja | Dev | 2h | 🟡 Scheduler DONE, CM1 alokacja pending |
| **6** | Historical backfill (60 dni) | Dev | 2h | ⚠️ Daily reports timeout — Amazon wolno generuje |
| **7** | Test dashboard end-to-end | Dev | 1h | ⚠️ Campaigns OK, reports pending |

**Total: ~2 dni robocze** (po onboarding).  
**Dashboard Ads.tsx + API routes → działają od razu** — zero zmian potrzebnych.

---

## 9. Rate Limits & Best Practices

| Operacja | Limit | Strategia |
|----------|-------|-----------|
| Report request | 10 req/s | Sequential per profile, sleep 0.2s |
| Report download | Unlimited (S3 URL) | One at a time, async |
| Campaign list | 10 req/s | Paginated, all in one call |
| Profile list | 10 req/s | Single call, cache result |

**Caching:**
- Profiles → cache w Redis (TTL 24h)
- Campaign list → refresh daily
- Daily reports → MERGE (idempotent, safe to re-run)

---

## 10. Ryzyka & mitigacja

| Ryzyko | Prawdopodobieństwo | Mitigacja |
|--------|-------------------|-----------|
| Ads API credentials differ from SP-API | Prawie pewne | Osobne env vars, osobny auth client |
| Brak profilu reklamowego na niektórych marketplace | Możliwe (np. TR, IE) | Graceful skip, log warning |
| Historical data only 60 days | Pewne | Uruchomić sync jak najszybciej, gromadzić od teraz |
| Rate limiting | Niskie | Sleep 0.2s between calls, retry with backoff |
| Report format changes | Niskie | Versioned API (v3), defensive parsing |

---

*Autor: GitHub Copilot (Claude Opus 4.6) — 2 Marca 2026*

---

## 11. Status Implementacji (aktualizacja 2 Marca 2026 wieczorem)

### Co zostało zaimplementowane

**Faza 1 — Connector + Profile Discovery: ✅ DONE**
- `client.py` — LWA auth z osobnymi credentials, async HTTP client, `extra_headers` support
- `profiles.py` — 10 profili EU (DE, FR, IT, ES, PL, NL, SE, BE, UK, TR)
- Mapowanie `countryCode` → SP-API `marketplace_id` via `MARKETPLACE_REGISTRY`

**Faza 2 — Campaigns + Reporting: ✅ DONE**
- `campaigns.py` (210 linii):
  - SP: `POST /sp/campaigns/list` z `Accept: application/vnd.spCampaign.v3+json` (konieczne — inaczej 415)
  - SB: `POST /sb/v4/campaigns/list` z fallback na legacy `/sb/campaigns` (v3 dawał 404)
  - SD: `POST /sd/campaigns/list` (v3)
  - SB budget: obsługa jako dict LUB float (`isinstance(raw_budget, dict)`)
  - Wynik: 5,083 kampanii (DE: 4,219, FR: 395, IT: 231, PL: 11, ...)
- `reporting.py` (243 linii):
  - v3 async: create → poll → download GZIP JSON
  - Poprawione kolumny: SP `unitsSoldClicks7d`/`purchases7d`; SB/SD `cost`/`sales`/`unitsSold`/`purchases`
  - Parser `_parse_report_rows()` z OR chains na obie konwencje nazewnictwa
  - `max_polls=40`, `poll_interval=15s`

**Faza 3 — Sync Service + Scheduler: ✅ DONE**
- `ads_sync.py` (474 linii):
  - DDL migration: drop stare SQLAlchemy tables (UUID PK, brak `ad_type`), create nowe z composite PK
  - MERGE batched upsert: profiles, campaigns, campaign_day
  - PLN conversion via `acc_exchange_rate` z gap-filling
  - Rate limiting: raporty sekwencyjnie, 3s między typami, 5s między profilami
- `scheduler.py`: `_sync_ads()` o 07:00 (CronTrigger, max_instances=1)

### Co jeszcze nie działa

**Faza 4 — CM1 Integration: ⚠️ PENDING**
- `ads_cost_pln` per order = daily_marketplace_spend / daily_marketplace_orders
- Wymaga danych z `acc_ads_campaign_day` (która jest pusta)

**Faza 5 — Historical backfill: ⚠️ BLOCKED**
- Daily reports timeout po 40 polls (~10 min) — Amazon wolno generuje raporty
- Możliwe rozwiązania:
  1. Uruchomienie sync o 07:00 (off-peak) — scheduler już skonfigurowany
  2. Zwiększenie `max_polls` do 60+ (15 min wait)
  3. Retry logic z dłuższym polling

### Bezpieczeństwo
- Credentials w `.env` (AMAZON_ADS_CLIENT_ID/SECRET/REFRESH_TOKEN)
- Usunięto hardcoded CLIENT_ID/SECRET z `scripts/get_ads_refresh_token.py`
- `.gitignore` += `ads_tokens.json`, `*.tokens.json`, `tmp_*.py`

### Profile EU (z testu)
| Kraj | Profile ID | Marketplace ID | Kampanie (SP+SB+SD) |
|------|-----------|----------------|---------------------|
| DE | 2583515575894719 | A1PA6795UKMFR9 | 3,792 + 122 + 305 |
| FR | 2493898694024072 | A13V1IB3VIYZZH | 344 + 17 + 34 |
| IT | 2428774110140196 | APJ6JRA9NG5V4 | 205 + 6 + 20 |
| ES | 3175891521293524 | A1RKKUPIHCS9HS | 161 + 5 + 2 |
| PL | 2340543468250660 | A1C3SOZRARQ6R3 | 9 + 2 + 0 |
| NL | 4414157435569296 | A1805IZSGTT6HS | mniejsze |
| SE | 1434250854160131 | A2NODRKZP88ZB9 | mniejsze |
| BE | 1915712109695298 | AMEN7PMS3EDWL | mniejsze |
| UK | 1002265704230748 | A1F83G8C2ARO7P | mniejsze |
| TR | 3497618873647480 | A33AVAJ2PDY3EV | mniejsze |
