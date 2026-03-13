# Amazon Command Center — Developer Guide

> **Dla kogo:** Nowy developer, senior review, junior dev, AI coding assistant (Copilot/Cursor).  
> **Co znajdziesz:** Architektura, business logic, każdy plik systemu, przepływ danych, integracje, znane ograniczenia, roadmapa.  
> **Język kodu:** Python / TypeScript. **Język biznesu:** PL/EN mixed.

> **Aktualizacja 2026-03-08 v2.13:**
> - **🆕 ECB Exchange Rate Backup** — nowy connector `app/connectors/ecb.py` parsuje XML feed ECB (90d hist + daily). Nowa funkcja `sync_ecb_exchange_rates()` w `sync_service.py` zapisuje do tabeli `ecb_exchange_rate`. Scheduler: codziennie 02:30. Sync runner: `--ecb-rates`. NBP pozostaje primary, ECB jako backup (30 walut, ~1,814 kursów).
> - **🆕 Traffic Sync Pipeline** — `sync_inventory_sales_traffic` dodany do schedulera (04:30 daily, 90 dni lookback) + nowa flaga `--traffic` w sync_runner. Dane: `acc_inv_traffic_sku_daily` → `acc_inv_traffic_asin_daily` → `acc_inv_traffic_rollup`.
> - **🆕 23 CHECK Constraints** — hardening bazy danych: 23 CHECK constraints na 7 tabelach (`acc_order_line`, `acc_exchange_rate`, `ecb_exchange_rate`, `acc_order`, `acc_al_jobs`, `acc_inventory_snapshot`, `acc_sku_profitability_rollup`). Walidacja qty≥0, rate>0, status enums, progress 0-100 itd. Zastosowane WITH NOCHECK.
> - **🆕 Sync Runner Job Logging** — `sync_runner.py` przebudowany: każdy sync tworzy rekord `acc_al_jobs` via `create_job()`/`set_job_running()`/`set_job_success()`/`set_job_failure()`. Trigger source='cli', triggered_by='sync_runner'. Nowe job types: `sync_exchange_rates`, `sync_ecb_exchange_rates`, `sync_catalog`, `full_sync`.
> - **🔧 Ad Spend Enrichment (ACOS fix)** — `profitability_service.py` → `_enrich_rollup_from_finance()` rozszerzony o krok ad_spend: LEFT JOIN `acc_ads_product_day` → UPDATE `ad_spend_pln` w `acc_sku_profitability_rollup`. Wynik: 38,973 wierszy z realnym ad_spend (łącznie 589,923 PLN). ACOS i TACoS teraz realne (DE TACoS 8.4%, IT 1.8%).
> - **🔧 Refund Units Enrichment** — nowy krok w `_enrich_rollup_from_finance()`: agregacja z `acc_order` WHERE `is_refund=1` → UPDATE `refund_units`. Wynik: 7,464 wierszy z realnymi danymi refund (9,695 zwróconych jednostek).
> - **🔧 Strategy Expansion Bidirectional + DE-first** — `strategy_service.py` → `_detect_marketplace_expansion()` kompletnie przepisany: 3-fazowy pipeline:
>   - Faza 1: zapytanie o wszystkie aktywne SKU na WSZYSTKICH marketplace'ach (próg: rev>500, margin>5%)
>   - Faza 2 (DE-first): SKU sprzedające się na nie-DE marketplace'ach, ale brakujące na DE. Revenue estimate 25%, urgency=40, strategic_fit=90. Limit 60.
>   - Faza 3 (general): wszystkie inne ekspansje bidirectional (DE→others + others→others), 15% estimate, limit 300.
>   - Wynik: 760 okazji (300 expansion: DE=60, ES=233, PL=246, itd.). Stare wpisy GB wyczyszczone.
> - **🔧 Frontend NaN Safety** — `formatPct()` i `formatCurrency()` w `utils.ts` teraz obsługują null/undefined/NaN → zwracają "—". Dodano label TACOS na karcie Ad Spend w ProfitOverview.
> - **Rollup Recompute** — `recompute_rollups(days_back=365)`: 93,292 wierszy SKU, 741 marketplace (z realnymi ACOS, ad_spend, refund).
> - **Dokumentacja techniczna:** aktualizacja DEVELOPER_GUIDE.md, COPILOT_CONTEXT.md, README.md.
>
> **Aktualizacja 2026-03-07 v2.12:**
> - **🆕 P&L Engine Unification** — 3 równoległe silniki profit (profit_engine.py V2, mssql_store.py V1, profit_service.py write-back) zunifikowane: V1 routes teraz delegują do V2 engine.
> - **amount_pln backfill** — 1,587,934 rekordów w `acc_finance_transaction` zaktualizowanych (wcześniej `amount_pln = 0`).
> - **Kompleksowa klasyfikacja charge_types** — `_classify_finance_charge()` mapuje 49 typów opłat Amazon do warstw CM2/NP.
> - **CM2 rozszerzony** — 4→7 bucketów kosztowych: +refund_cost, +shipping_surcharge, +fba_inbound (obok storage/aged/removal/liquidation).
> - **NP auto-detekcja** — overhead pools ładowane z DWÓCH źródeł: `acc_profit_overhead_pool` (manual) + `acc_finance_transaction` NP-layer charges (auto).
> - **ShippingCharge w CM1** — opłata za przesyłkę od klienta (76K rekordów, 1.3M PLN) dodana do revenue pool-based per marketplace.
> - **Finance import fix** — `order_pipeline.py` i `sync_service.py` teraz obliczają `amount_pln`/`exchange_rate` przy INSERT.
> - **Nowe pola API** — `refund_finance_pln`, `shipping_surcharge_pln`, `fba_inbound_fee_pln` w `ProductProfitItem`.
> - **profit_engine.py** rozrósł się do ~6400 linii.
> - **Dokumentacja techniczna:** `docs/PROFIT_CM2_NP_WIRING_2026-03-07.md`.
>
> **Aktualizacja 2026-03-06 v2.11:**
> - **🆕 Family Restructure v3** — pełny pipeline replikacji struktury rodziny z DE na dowolny marketplace EU.
> - **Pipeline 7-krokowy:** PREFLIGHT → VALIDATE_THEME → AUDIT_CHILD_ATTRS → ENRICH_FROM_PIM → TRANSLATE_PARENT → CREATE_PARENT → REASSIGN_CHILD(s).
> - **Walidacja variation_theme** via SP-API Product Type Definition (`get_product_type_definition()`).
> - **Audyt atrybutów ALL children** — concurrent (semaphore 5, batch 20), nie sample. 100/100 checked for family 1367.
> - **Ergonode PIM enrichment** — lookup brakujących color/size w PIM, rozwiązywanie UUID→tekst via option maps, GPT-5.2 tłumaczenie na język docelowy, PATCH na target MP.
> - **Nowe funkcje ergonode.py:** `_build_option_map()`, `fetch_ergonode_variant_lookup()` — atrybuty: `wariant_kolor_tekst` (MULTI_SELECT), `wariant_text_rozmiar` (SELECT), `wariant_text_ilosc` (SELECT).
> - **Nowa funkcja listings.py:** `get_product_type_definition()` — SP-API PTD endpoint.
> - **GPT-5.2 tłumaczenie** parent attributes (item_name, bullet_point, product_description, generic_keyword) DE → target language.
> - **Frontend:** StepRow z 10 akcjami, dedykowany rendering VALIDATE_THEME / AUDIT_CHILD_ATTRS / ENRICH_FROM_PIM. ExecutionLog z summary badges.
> - **Dry-run test OK:** Family 1367 / FR — 109 steps, 0 errors, 100/100 audited, 18/19 PIM found.
> - **Dokumentacja techniczna:** `docs/FAMILY_RESTRUCTURE_2026-03-06.md`.
>
> **Aktualizacja 2026-03-05 v2.8:**
> - **🆕 Return Tracker** — pełny moduł śledzenia cyklu życia zwrotów Amazon (refund → FBA report → reconciliation → COGS recovery/write-off).
> - **Backend:** `return_tracker.py` (~1151 linii), `api/v1/returns.py` (9 endpointów).
> - **Tabele:** `acc_return_item`, `acc_fba_customer_return`, `acc_return_daily_summary`, `acc_return_sync_state`.
> - **Przepływ:** seed z `acc_order.is_refund=1` → FBA Customer Returns report → parse TSV → MERGE → reconcile (SELLABLE → cogs_recovered, DAMAGED → write_off, 45d → lost_in_transit) → daily summary.
> - **Incremental sync:** watermark per marketplace, backfill z chunkowaniem.
> - **Manual override:** `PUT /returns/items/{id}/status` — warehouse team, nota audytowa.
> - **Currency-safe:** wyłącznie kolumny `_pln`.
> - **Frontend:** backend API gotowe, strony React jeszcze nie zbudowane.
>
> **Aktualizacja 2026-03-05 v2.9:**
> - **🆕 SP-API Orders P0 hardening** — profile syncu + hash-skip + usage telemetry.
> - **Profile syncu:** `core_sync`, `ops_tracking`, `pii_support` w `step_sync_orders(sync_profile=...)`.
> - **Hash-skip:** `acc_order.sync_payload_hash` (bez `LastUpdateDate`) ogranicza reprocessingi i wywołania `orderItems` dla niezmienionych orderów.
> - **Telemetria API:** nowa tabela `acc_sp_api_usage_daily` (calls/success/errors/throttles/latency/rows per endpoint+marketplace+profile).
> - **Nowy endpoint diagnostyczny:** `GET /api/v1/health/sp-api-usage`.
> - **Dokumentacja techniczna:** `docs/SP_API_ORDER_SYNC_P0_2026-03-05.md`.
>
> **Aktualizacja 2026-03-05 v2.10:**
> - **🆕 P1 Taxonomy** — warstwa predykcji i review queue dla braków `brand/category/product_type`.
> - **Tabele:** `acc_taxonomy_node`, `acc_taxonomy_alias`, `acc_taxonomy_prediction`.
> - **Źródła predykcji:** `pim_exact`, `ean_match`, `embedding_match` + confidence score.
> - **Review queue API:** `/api/v1/inventory/taxonomy/refresh`, `/predictions`, `/predictions/{id}/review`.
> - **Integracja runtime:** fallback taxonomy podpięty do `Inventory` (`manage_inventory.py`) i `Profit` (`get_product_profit_table`).
> - **Job runtime:** nowy typ `sync_taxonomy` w `acc_al_jobs`.
> - **Live progres joba:** `sync_taxonomy` raportuje `processed/total/remaining/generated` w `progress_message`.
> - **Nightly scheduler:** `sync-taxonomy-nightly` (APScheduler) z parametrami `TAXONOMY_SYNC_*` (`enabled/hour/minute/limit/auto_apply/min_confidence`).
> - **Dokumentacja techniczna:** `docs/TAXONOMY_P1_2026-03-05.md`.
>
> **Aktualizacja 2026-03-05 v2.7:**
> - **🆕 FBA Fee Audit** — detekcja anomalii opłat FBA, szacowanie nadpłat, timeline per SKU, porównanie z cennikiem Amazon.
> - **Backend:** `fba_fee_audit.py` (~752 linii), 4 endpointy pod `/fba/fee-audit/*`.
> - **Currency fix:** `LAG() PARTITION BY sku, currency` zamiast `PARTITION BY sku` — eliminuje false positive'y z mieszania walut.
> - **EUR normalizacja:** `_load_fx_rates()` + `_to_eur()`, `overcharge_by_currency` breakdown.
> - **🔧 Currency Mixing Audit** — full codebase audit: 2 CRITICAL + 1 MEDIUM bugów naprawionych:
>   - `get_profit_by_sku()`: `SUM(item_price)` → `SUM(item_price * fx.rate_to_pln)` z `OUTER APPLY acc_exchange_rate`
>   - `sync_profit_snapshot()`: `revenue_net/revenue_gross` teraz mnożone przez `fx.rate_to_pln`
>   - `build_settlement_summaries()`: usunięty fallback `COALESCE(exchange_rate, 1)` → NULL gdy brak kursu
> - **🔧 Finance Dashboard** — usunięty hardcoded DE-only marketplace z Sync button; iteruje teraz po wszystkich 9 EU FBA.
> - **Dokumentacja:** `docs/CURRENCY_MIXING_AUDIT.md` — pełny raport audytu walutowego.
>
> **Aktualizacja 2026-03-04 v2.6:**
> - **Finance Dashboard** ma już kanoniczny backendowy endpoint `GET /api/v1/finance/dashboard`; frontend nie składa już górnych kafli z wielu osobnych query.
> - **Finance truth states** są jawne: `real_data`, `partial`, `blocked_by_missing_bank_import`, `no_data`.
> - **Bank import blocker** jest dziś świadomie komunikowany w dashboardzie; reconciliation pozostaje partial, dopóki `acc_fin_bank_line` jest puste.
> - **Pricing / Buy Box** respektuje `?sku=...` w URL i pokazuje uczciwy empty state, jeśli `sync_pricing` nie zasiliło `acc_offer`.
> - **Dashboard top drivers / leaks** reagują teraz na lokalne filtry dashboardu: `date`, `marketplace`, `fulfillment`, `brand`, `category`.
> - **Wazne:** `Global Filters` nadal nie są uniwersalnym kontraktem dla calego UI; `Dashboard` i `Pricing` zachowuja wlasny lokalny stan.
>
> **Aktualizacja 2026-03-03 v2.5:**
> - **🆕 Manage All Inventory hardening** — overview i główna tabela są zoptymalizowane pod Azure SQL przez `acc_inv_item_cache` + guard przeciw partial cache answers.
> - **Sales & Traffic:** `inventory_sync_sales_traffic` nie jest już placeholderem; używa `GET_SALES_AND_TRAFFIC_REPORT`, zapisuje do `acc_inv_traffic_asin_daily` / `acc_inv_traffic_sku_daily`, a rollupy odbudowują też cache inventory.
> - **Apply / rollback:** draft workflow używa już `JSON_LISTINGS_FEED`; auto-build jest bezpiecznie ograniczony do `reparent` i `update_theme`, a bardziej ryzykowne mutacje wymagają jawnego payloadu.
> - **Runtime note:** live backend został już podniesiony na nowym kodzie; smoke `/api/v1/inventory/overview` i `/api/v1/inventory/all` dla `DE` przeszedł poprawnie.
>
> **Aktualizacja 2026-03-03 v2.4:**
> - **🆕 Manage All Inventory** — nowy moduł `/inventory/*` jako shell decyzyjny inventory + listing + family + traffic coverage.
> - **Backend:** `schemas/manage_inventory.py`, `services/manage_inventory.py`, `api/v1/manage_inventory.py`.
> - **Frontend:** `InventoryOverview`, `ManageAllInventory`, `InventoryFamilies`, `InventoryDrafts`, `InventoryJobs`, `InventorySettings`.
> - **Schema:** `acc_inv_traffic_*`, `acc_inv_change_*`, `acc_inv_settings`, `acc_inv_category_cvr_baseline`.
> - **Runtime:** job types `inventory_sync_listings`, `inventory_sync_snapshots`, `inventory_sync_sales_traffic`, `inventory_compute_rollups`, `inventory_run_alerts`.
> - **Wazne:** modul nie udaje pelnego traffic truth; jesli `acc_inv_traffic_rollup` jest puste, UI i API jawnie oznaczaja coverage jako `partial`.
>
> **Aktualizacja 2026-03-03 v2.3:**
> - **🆕 Amazon Listing Registry** — staging identity registry z Google Sheet (`gid=400534387`) zapisany do `acc_amazon_listing_registry` + `acc_amazon_listing_registry_sync_state`.
> - **Nowy job:** `sync_amazon_listing_registry` z hash-based skip, bez live zaleznosci runtime od Google Sheet.
> - **Order pipeline:** registry zasila backfill `acc_product`, enrichment istniejacych produktow i linkowanie brakujacych `acc_order_line.product_id`.
> - **FBA Ops:** inventory, inbound oraz aged/stranded dostaja `internal_sku`, `ean`, `parent_asin` i lepsze `title_preferred` z registry.
> - **Finance Center:** ledger enrichment korzysta z registry (`asin`, `internal_sku`, `ean`, `title_preferred`, `parent_asin`, `listing_role`) jako warstwy identyfikacyjnej, nie kosztowej.
> - **Data Quality / Missing COGS:** bezpieczny fallback `SKU -> ISK -> Oficjalny XLSX` przechodzi teraz przez registry zamiast przez live lookup do arkusza.
>
> **Aktualizacja 2026-03-03 v2.2:**
> - **🆕 Order sync watermark hardening** — `sync_orders` nie jedzie juz po `now - 30 min`, tylko po trwalym watermarku `acc_order_sync_state` z overlapem i recovery po downtime.
> - **🆕 System health order sync** — `GET /api/v1/health/order-sync` + alert `order_sync_gap` + banner ryzyka na Finance Dashboard.
> - **🆕 Historical order catch-up** — zakres `2026-01-01 -> 2026-03-03` zostal dociagniety, a live incremental sync po watermarku domknal swieze luki `0_6d`.
> - **⚠️ Reports API caveat** — `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` traktujemy jako seller-account-wide bulk catch-up; nie wolno interpretowac jego progress/licznikow jako kanonicznej telemetrii per marketplace.
>
> **Aktualizacja 2026-03-03 v2.1:**
> - **🆕 Price source priority system** — 8-level cascade w `order_pipeline.py` Pass 2:
>   `manual(1) > import_xlsx(2) > xlsx_oficjalne(3) > holding(4) > erp_holding(5) > import_csv(6) > cogs_xlsx(7) > acc_product(8)`
> - **🆕 Holding ×1.04 multiplier** — Netfox Holding FIFO ceny systematycznie ~4% niższe od oficjalnego cennika. Mnożnik stosowany przy odczycie/stampowaniu (Opcja A), surowa cena w `acc_purchase_price` bez zmian.
> - **🆕 RPT_ duplicate fix** — 19,612 zduplikowanych order lines usunięte (Reports backfill tworzył duplikaty gdy real API line już istniała). `backfill_via_reports.py` MERGE wzmocniony guardem `NOT EXISTS`.
> - **🆕 import_cogs_xlsx.py** — wrzutki od zakupu (pliki z `cogs from sell/`) mają priorytet 2 (`import_xlsx`). Nadpisują wszystko oprócz `manual`. Respektują MAX_PURCHASE_PRICE_PLN=2000.
> - **Netfox ERP analysis** — sprawdzono 5 tabel BazaDanychSprzedaz* vs xlsx_oficjalne. Brak 1:1 źródła: TRADE=80%, JDG=77%, Holding=70%. Xlsx utrzymywany niezależnie.
>
> **Aktualizacja 2026-03-03 v2.0:**
> - **🆕 COGS Coverage ~99%** — pipeline fix (2-pass `step_stamp_purchase_prices`: acc_product + acc_purchase_price), XLSX import 3,035 SKUs, ASIN cross-lookup.
> - **🆕 Data Quality UI — inline price editing** — upsert purchase price + map-and-price for unmapped products.
> - **🆕 AI Product Matcher** — GPT-4o matching for ~122 unmapped Amazon bundles (BOM decomposition, human-in-the-loop).
> - **Backend:** `ai_product_matcher.py` (serwis), 4 nowe endpointy (`ai-match/run|suggestions|approve|reject`), `profit_engine.py` (+map_and_price, +upsert_purchase_price).
> - **Frontend:** `DataQuality.tsx` — inline price editing + AI Match Review Panel (approve/reject, BOM, confidence).
> - **DB:** `acc_product_match_suggestion` (AI match suggestions with status workflow).
>
> **Aktualizacja 2026-03-03 v1.10:**
> - **🆕 Finance Center hardening** — payout groups oparte o `financial_event_group_id`, nie o sam `settlement_id`.
> - **Background jobs:** `finance_sync_transactions`, `finance_prepare_settlements`, `finance_generate_ledger`, `finance_reconcile_payouts` wracają do UI od razu i lecą w tle.
> - **Progress reporting:** settlementy i ledger raportują postęp do `acc_al_jobs`.
> - **Completeness alerts:** systemowe alerty `finance_completeness_critical` i `finance_completeness_partial`.
> - **Gap diagnostics:** `GET /api/v1/finance/sync/gap-diagnostics`.
> - **Netfox health:** `GET /api/v1/health/netfox-sessions` + widget `Netfox Health`.
> - **Attribution repair:** historyczne payout groups poza `DE` zostaly odtworzone; aktywne marketplace maja juz imported finance rows.
> - **Coverage diagnostics UI:** dashboard pokazuje `imported_rows / tracked_groups / imported_orders`, `likely_gap_driver`, breakdown po wieku zamowien i kanale.
> - **Missing order diagnostics:** dashboard pokazuje tez `missing_order_rows`, `missing_order_distinct_orders`, `unmapped_rows` i profil brakow po wieku/event type; dominujacy wzorzec poza `DE` to swiezy order sync lag.
>
> **Aktualizacja 2026-03-02 v1.9:**
> - **🆕 FBA Ops** — nowy moduł operacyjny: overview, inventory, replenishment, inbound, aged/stranded, bundles, KPI scorecard.
> - **Backend:** `api/v1/fba_ops.py` + `services/fba_ops/service.py` + `schemas/fba_ops.py`.
> - **Frontend:** strony `FbaOverview`, `FbaInventory`, `FbaInbound`, `FbaAgedStranded`, `FbaBundles`, `FbaScorecard`.
> - **Dane:** inbound z SP-API Inbound API; inventory enrichment z Reports API + fallback do FBA Inventory API.
> - **Diagnostyka:** `GET /api/v1/fba/diagnostics/report-status` + tabela `acc_fba_report_diagnostic` + panel diagnostyczny na overview.
> - **Fallback produkcyjny:** dla marketplace'ów z trwałym `FATAL` planning report jest pomijany przez cooldown i zastępowany Inventory API; canonical stranded report nadal bywa `CANCELLED`.
>
> **Aktualizacja 2026-03-02 v1.8:**
> - **🆕 Amazon Ads API** — pełna integracja: connector (`amazon_ads_api/`) + sync service (`ads_sync.py`) + scheduler (07:00 daily)
> - **Connector:** `client.py` (LWA auth, osobne credentials), `profiles.py` (10 profili EU), `campaigns.py` (SP v3 + SB v4 + SD v3), `reporting.py` (v3 async reports)
> - **Dane:** 10 profili, 5,083 kampanii (SP+SB+SD), daily reports pending (Amazon report generation slow)
> - **Poprawki:** SP Accept header (415), SB v4 endpoint (404), SB budget float (crash), report columns (400), rate limiting (sequential + delays)
> - **Bezpieczeństwo:** Usunięto hardcoded CLIENT_ID/SECRET z `get_ads_refresh_token.py`, `.gitignore` += `ads_tokens.json`, `*.tokens.json`, `tmp_*.py`
> - **Order backfill update:** 101K+ orders, 116K+ order lines (z ~59K)
>
> **Aktualizacja 2026-03-02 v1.7:**
> - **Order Line backfill** — `backfill_via_reports.py` teraz wypełnia TAKŻE `acc_order_line` z tych samych TSV raportów (item-level data). Syntetyczny klucz `RPT_{order_id}_{sku}`. Schema change: `amazon_order_item_id` NVARCHAR(50) → NVARCHAR(150).
> - **Deep Health endpoint** — `GET /health/deep` sprawdza Azure SQL + Redis + SP-API (concurrent). Stary shallow `/health` zachowany.
> - **Backfill restart** — stary backfill (bez lines) zatrzymany, progress zresetowany, nowy backfill v2 (orders+lines) uruchomiony.
>
> **Aktualizacja 2026-02-28 v5:**
> - **🆕 Family Mapper** — kompletny moduł: 6 serwisów backend, 15 endpointów API, 4 strony React, 80 testów (pytest). Nightly job wyłączony (manual-only).
> - **Testy:** 80 testów pytest (conftest + test_master_key 28 + test_matching 10 + test_de_builder 21 + test_api_families 21). Frontend: `tsc --noEmit` + `vite build` clean.
>
> **Aktualizacja 2026-02-28 v4:**
> - **SAFETY HARDENING:** `SET LOCK_TIMEOUT 30000` na KAŻDYM `pyodbc.connect()` w systemie — zapobiega deadlockom.
> - **Query timeout:** 120s (`QUERY_TIMEOUT_SECONDS`) na pipeline queries.
> - **Courier costs:** Step 5.95 — mapowanie kosztów kurierów FBM (DHL/GLS) → `logistics_pln`. Batched DHL (50/query), temp tables.
> - **Backfill 10-fazowy:** Dodano fazę 9 (courier costs) do `backfill_full.py`.
> - **SellerBoard_Sync:** Wyłączony (Disabled) — nie jest już potrzebny.
> - Bazy danych: **MSSQL NetfoxAnalityka** (ACC dane) + **MSSQL NetfoxDistribution** (Subiekt GT ERP) — obie na `192.168.230.120:11901`.
> - **⚠️ NetfoxDistribution = READ ONLY!** Żadnego INSERT/UPDATE/DELETE. Tylko SELECT.
> - **⚠️ ITJK_DHL_Costs (13M wierszy, ZERO indeksów) — NIGDY nie rób CROSS APPLY ani JOIN bez batching!**
> - SKU mapping: 5-krokowa kaskada (Ergonode → GSheet → Baselinker → ASIN → SP-API Catalog). **1344/1406 produktów zmapowanych.**
> - Ceny zakupu: Holding FIFO + XLSX fallback (3-warstwowa architektura). **6702** XLSX + **2940** holding = ~6951 aktywnych cen.
> - **COGS pipeline naprawiony** — pokrycie: 75.7% linii zamówień, CM1 ≈ 75.5%.
> - **Dashboard** — Top 15 z oddzielnym select SKU/ASIN + internal_sku, presety dat PL, marketplace, FBA/FBM.
> - **FX Rate** — kurs NBP z dnia poprzedzającego datę zakupu (Art. 31a ustawy o VAT). 424+ kursów, daily sync.
> - **Netto Revenue** — `order_total / 1.{vat_rate}` — kolumna `vat_pln`.
> - Nie uruchamiamy migracji `alembic`, chyba że dostaniesz jawne polecenie.

---

## Spis treści

1. [Kontekst biznesowy](#1-kontekst-biznesowy)
2. [Architektura systemu](#2-architektura-systemu)
3. [Struktura repozytorium (każdy plik)](#3-struktura-repozytorium)
4. [Stack technologiczny — z uzasadnieniem](#4-stack-technologiczny)
5. [Instalacja i uruchomienie](#5-instalacja-i-uruchomienie)
6. [Zmienne środowiskowe](#6-zmienne-środowiskowe)
7. [Baza danych — modele i relacje](#7-baza-danych--modele-i-relacje)
8. [API — pełna mapa endpointów](#8-api--pełna-mapa-endpointów)
9. [WebSocket — real-time](#9-websocket--real-time)
10. [Autentykacja i RBAC](#10-autentykacja-i-rbac)
11. [Business logic — CM1/CM2/NP, FX, zysk](#11-business-logic--cm1cm2np-fx-zysk)
12. [Integracja SP-API (Amazon)](#12-integracja-sp-api-amazon)
13. [Integracja MSSQL (NetfoxAnalityka)](#13-integracja-mssql-netfoxanalityka)
14. [APScheduler — harmonogram zadań](#14-apscheduler--harmonogram-zadań)
15. [Frontend — strona po stronie](#15-frontend--strona-po-stronie)
16. [Składowe UI (shadcn/ui)](#16-składowe-ui-shadcnui)
17. [Znane ograniczenia i pułapki](#17-znane-ograniczenia-i-pułapki)
18. [Roadmapa — co zostało do zrobienia](#18-roadmapa--co-zostało-do-zrobienia)
19. [Konwencje i dobre praktyki](#19-konwencje-i-dobre-praktyki)
20. [Szybki szablon nowego modułu](#20-szybki-szablon-nowego-modułu)
21. [FBA Ops — status i fallbacki](#21-fba-ops--status-i-fallbacki)
22. [Finance Center — status i completeness](#22-finance-center--status-i-completeness)
23. [Amazon Listing Registry — staging i usage](#23-amazon-listing-registry--staging-i-usage)
24. [Manage All Inventory — status i ograniczenia](#24-manage-all-inventory--status-i-ograniczenia)
25. [Return Tracker — cykl życia zwrotów](#25-return-tracker--cykl-życia-zwrotów)
26. [FBA Fee Audit — detekcja anomalii opłat](#26-fba-fee-audit--detekcja-anomalii-opłat)
27. [Currency Mixing Audit — wyniki i fixy](#27-currency-mixing-audit--wyniki-i-fixy)
28. [Family Restructure — pipeline execute](#28-family-restructure--pipeline-execute)

---

## 1. Kontekst biznesowy

**KADAX** to polska firma e-commerce sprzedająca produkty na _Amazon.de, .pl, .fr, .it, .es, .se, .nl, .be, .at, .com, .ae, .sa_ — łącznie 12+ marketplace'ów.

**Amazon Command Center (ACC)** to wewnętrzne centrum dowodzenia dla **Dyrektora E-commerce** i jego zespołu. Zastępuje ręczne raporty Excel, kilkanaście zakładek Seller Central i codzienne pytania do analityków.

### Co system robi
| Funkcja | Źródło danych | Częstotliwość |
|---------|---------------|---------------|
| Przychód, zamówienia, marże w czasie rzeczywistym | SP-API Orders | co 15 min (pipeline) |
| Analiza zysku CM1 na poziomie zamówienia | SP-API + MSSQL | Codziennie 5:00 |
| Backfill produktów + mapowanie SKU | SP-API + Ergonode + GSheet + Baselinker | co 15 min (pipeline step 2-4) |
| Ceny zakupu (Holding FIFO + XLSX fallback) | MSSQL + XLSX z N:\ | Codziennie 2:00 (serwer) + 9:00 (lokalna stacja) |
| Buy Box i monitoring cen | SP-API Catalog | co 2h |
| Stan magazynowy FBA + DOI | SP-API Inventory | Codziennie 4:00 |
| Wydatki reklamowe i ACoS | SP-API Reports | Codziennie 3:00 |
| Planowanie budżetu miesięcznego | Własne dane | Na żądanie |
| Rekomendacje AI (GPT-5.2) | Wszystkie dane | Na żądanie |
| Stany i koszty zakupu z ERP | MSSQL NetfoxAnalityka | Codziennie 2:00 + pipeline |

### Definicja CM1/CM2/NP (marża wkładu)
```
CM1 = Przychód_PLN + ShippingCharge_PLN - COGS_PLN - Opłaty_Amazon_PLN - Logistyka_PLN
CM2 = CM1 - Reklama - Zwroty_netto - Storage - Aged - Removal - Liquidation - Refund_Finance - Shipping_Surcharge - FBA_Inbound
NP  = CM2 - Overhead_Allocated
CM1% = CM1 / Przychód_PLN × 100
```
- **COGS** = `netto_purchase_price_pln × 1.23` (VAT 23%)
- **Przychód_PLN** = `item_price × quantity × kurs_FX_NBP` (netto)
- **ShippingCharge** = pool z `acc_finance_transaction` per marketplace (dystrybucja wg revenue share)
- **Opłaty Amazon** = FBA fee + referral fee (z SP-API finance bridge)
- **CM2 buckety** = 7 kategorii kosztów z finance via `_classify_finance_charge()`
- **Overhead** = manual pools (`acc_profit_overhead_pool`) + auto-detect z finance NP-layer charges

---

## 2. Architektura systemu

```
┌─────────────────────────────────────────────────────────────────┐
│                         KADAX ACC                               │
│                                                                 │
│  ┌──────────────┐    HTTP/WS    ┌──────────────────────────┐   │
│  │   Browser    │◄─────────────►│  Nginx (port 3010)       │   │
│  │  React SPA   │               │  Reverse proxy /api → API│   │
│  └──────────────┘               └────────────┬─────────────┘   │
│                                              │                  │
│  ┌───────────────────────────────────────────▼──────────────┐  │
│  │  FastAPI (port 8000)                                      │  │
│  │  ├── /api/v1/* — REST (auth, kpi, profit, alerts, jobs,  │  │
│  │  │               pricing, planning, inventory, ads, ai)   │  │
│  │  └── /ws/*     — WebSocket (jobs progress, alerts push)  │  │
│  └───┬───────────────────────┬──────────────────────────────┘  │
│      │                       │                                  │
│  ┌───▼──────┐   ┌────────────▼─────────────────────────────┐   │
│  │MSSQL Netf│   │  Redis                                    │   │
│  │(230.120: │   │  DB0: async cache (KPI, sessions)         │   │
│  │11901)    │   │  DB1: Celery broker                       │   │
│  └──────────┘   │  DB2: Celery results                      │   │
│                 └─────────────────────────────────────────┬─┘   │
│                                                           │      │
│  ┌────────────────────────────────────────────────────────▼──┐  │
│  │  Celery Worker + Beat Scheduler                            │  │
│  │  ├── order_pipeline — co 15 min (10-step: sync+backfill+  │  │
│  │  │                    link+map+COGS+FX+finances+fees+       │  │
│  │  │                    courier_costs+profit)                  │  │
│  │  ├── sync_purchase_prices — 02:00 (Holding FIFO→MSSQL)    │  │
│  │  ├── sync_finances  — 03:00                               │  │
│  │  ├── sync_inventory — 04:00                               │  │
│  │  └── calc_profit    — 05:00                               │  │
│  └─────────────────────┬──────────────────────────────────┘    │
│                         │                                        │
│  ┌──────────────────────▼──────────────────────────────────┐   │
│  │  External APIs                                           │   │
│  │  ├── Amazon SP-API (eu / na)  — LWA OAuth               │   │
│  │  ├── MSSQL 192.168.230.120:11901 — NetfoxAnalityka ERP  │   │
│  │  ├── OpenAI API               — GPT-5.2                 │   │
│  │  └── NBP API                  — kursy walut              │   │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Przepływ danych (happy path)
```
SP-API Orders
    │
    ▼
order_pipeline.py (Celery, co 15 min)
    │
    ├── Step 1: Sync orders from SP-API → upsert acc_order + acc_order_line (raw pyodbc)
    ├── Step 2: Backfill missing acc_product rows (nowe SKU/ASIN combos)
    ├── Step 3: Link acc_order_line.product_id where NULL
    ├── Step 4: Map internal_sku (5-krokowa kaskada):
    │      a) Ergonode PIM (EAN → internal_sku)
    │      b) Google Sheet CSV (EAN → internal_sku)
    │      c) Baselinker MSSQL (EAN → internal_sku)
    │      d) Ergonode ASIN (asin_child/asin_parent → internal_sku)
    │      e) SP-API Catalog (ASIN → EAN → Ergonode/GSheet)
    ├── Step 5: Stamp purchase_price_pln on unstamped order lines
    ├── Step 5.5: COGS audit (validate_after_stamp)
    ├── Step 5.8: Sync exchange rates (NBP/ECB, 730 days)
    ├── Step 5.8b: Sync finances (Finances v2024-06-19 → acc_finance_transaction)
    ├── Step 5.9: Bridge fees (finance txns → fba_fee_pln/referral_fee_pln per line)
    ├── Step 5.95: Courier costs FBM (DHL/GLS → logistics_pln, batched, temp tables)
    └── Step 6: Calc profit (Revenue - COGS - Amazon Fees - Ads - Logistics = CM1; CM2 = CM1 - pools; NP = CM2 - overhead)
    │
    ▼
MSSQL: acc_order, acc_order_line, acc_product, acc_finance_transaction
    │
    ▼
sync_purchase_prices.py (serwer, 02:00)
    │ 3-warstwowa architektura:
    │ Layer 1: acc_purchase_price (historia cen, źródła: holding, xlsx, import_xlsx, manual)
    │ Layer 2: acc_order_line.purchase_price_pln (per-line stamp, 8-level priority)
    │ Layer 3: acc_product.netto_purchase_price_pln (cache)
    │ Źródła: xlsx_oficjalne (~7605) + Holding FIFO (~4083, ×1.04 at stamp) + import_xlsx (wrzutki)
    │
    │ push_xlsx_prices.py (lokalna stacja, Scheduled Task 09:00 + logon)
    │ Czyta XLSX z N:\, upsertuje zmienione ceny → acc_purchase_price
    │
    ▼
calc_profit.py (APScheduler, 05:00)
    │ pobiera COGS z acc_purchase_price
    │ pobiera kurs FX z MSSQL / fallback NBP
    │ liczy CM1 per order
    │
    ▼
MSSQL: acc_orders.contribution_margin_pln, cm_percent
    │
    ▼
/api/v1/profit/orders — FastAPI
    │
    ▼
ProfitExplorer.tsx — React
```

---

## 3. Struktura repozytorium

> Każdy katalog i plik z wyjaśnieniem co robi.

```
N:\AmazonCommandCenter\
│
├── .env                          ← SEKRET — dane dostępowe (gitignored)
├── .env.example                  ← Bezpieczny szablon bez haseł
├── .gitignore
├── docker-compose.yml            ← Całe środowisko: redis, api, worker, web (DB = zewnętrzny MSSQL)
├── README.md                     ← Skrócona instrukcja
├── DEVELOPER_GUIDE.md            ← Ten plik
│
├── apps/
│   │
│   ├── api/                      ════ BACKEND ════
│   │   ├── Dockerfile            ← python:3.12-slim, uvicorn, ODBC driver 18
│   │   ├── alembic.ini           ← Legacy po PostgreSQL (nieużywane w standardowym setupie)
│   │   ├── requirements.txt      ← Wszystkie zależności Python
│   │   │
│   │   ├── migrations/           ← Legacy po PostgreSQL
│   │   │   ├── env.py            ← Async runner Alembic (importuje wszystkie modele)
│   │   │   ├── script.py.mako    ← Szablon pliku migracji
│   │   │   └── versions/         ← Wygenerowane pliki migracji (początkowo puste)
│   │   │
│   │   ├── scripts/
│   │   │   ├── seed_demo.py      ← Seeduje: 12 marketplace, 2 userów, 4 alert rules, 90d FX
│   │   │   └── discover_mssql_schema.py ← Odkrywa schemat NetfoxAnalityka (uruchom raz!)
│   │   │
│   │   └── app/
│   │       ├── __init__.py
│   │       ├── main.py           ← FastAPI app, lifespan, CORS, Sentry, router mount
│   │       │
│   │       ├── core/             ════ INFRASTRUKTURA ════
│   │       │   ├── config.py     ← Wszystkie settings z .env (pydantic_settings)
│   │       │   ├── database.py   ← AsyncEngine, AsyncSession, get_db() dependency
│   │       │   ├── security.py   ← JWT encode/decode, bcrypt, Role enum, require_role()
│   │       │   └── redis_client.py ← Singleton aioredis, get_redis(), close_redis()
│   │       │
│   │       ├── models/           ════ SQLAlchemy MODELE ════
│   │       │   ├── __init__.py   ← Eksportuje wszystkie 17 klas
│   │       │   ├── user.py       ← User (email, hashed_password, role, last_login_at)
│   │       │   ├── marketplace.py← Marketplace (id=Amazon mkt ID, code, currency, tz)
│   │       │   ├── product.py    ← Product (sku, asin, ean, brand, category, cena_zakupu)
│   │       │   ├── offer.py      ← Offer (sku/mkt combo, price, buybox_price, fba_fee)
│   │       │   ├── order.py      ← AccOrder + OrderLine (CM1 fields)
│   │       │   ├── inventory.py  ← InventorySnapshot (qty_*, DOI, velocity_30d)
│   │       │   ├── finance.py    ← FinanceTransaction (type, amount_pln, fx_rate)
│   │       │   ├── ads.py        ← AdsCampaign + AdsCampaignDay (spend, sales, acos)
│   │       │   ├── plan.py       ← PlanMonth + PlanLine (targets, actuals, status)
│   │       │   ├── alert.py      ← AlertRule + Alert (triggered, severity, is_read)
│   │       │   ├── ai.py         ← AIRecommendation (type, title, action_items, status)
│   │       │   ├── job.py        ← JobRun (celery_task_id, progress_pct, status)
│   │       │   └── exchange_rate.py ← ExchangeRate (date, currency, rate_to_pln, NBP)
│   │       │
│   │       ├── schemas/          ════ PYDANTIC SCHEMAS ════
│   │       │   ├── __init__.py   ← Re-eksporty
│   │       │   ├── auth.py       ← TokenResponse, LoginRequest, UserOut, ChangePassword
│   │       │   ├── kpi.py        ← KPISummaryRequest/Response, MarketplaceKPI, Chart
│   │       │   ├── profit.py     ← ProfitOrderFilter, ProfitOrderOut, ProfitListResponse
│   │       │   ├── alerts.py     ← AlertRuleCreate/Out, AlertOut, AlertListResponse
│   │       │   ├── jobs.py       ← JobRunRequest, JobRunOut, JobListResponse
│   │       │   ├── pricing.py    ← OfferPriceOut, BuyBoxStatsOut, PriceUpdateRequest
│   │       │   ├── planning.py   ← PlanMonthOut, PlanLineOut, PlanVsActualResponse
│   │       │   ├── inventory.py  ← InventorySnapshotOut, OpenPOOut, ReorderSuggestionOut
│   │       │   ├── ads_schema.py ← CampaignOut, AdsSummaryResponse, TopCampaignRow
│   │       │   └── ai_rec.py     ← AIRecommendationOut, AIGenerateRequest, AIInsightSummary
│   │       │
│   │       ├── api/
│   │       │   ├── __init__.py
│   │       │   ├── ws.py         ← WebSocket: /ws/jobs/{id}, /ws/alerts (Redis pub/sub)
│   │       │   └── v1/
│   │       │       ├── __init__.py
│   │       │       ├── router.py        ← Łączy wszystkie 13 routerów pod /api/v1
│   │       │       ├── routes_health.py ← GET /health (shallow) + GET /health/deep (Azure SQL, Redis, SP-API concurrent check)
│   │       │       ├── routes_allegro.py← /allegro/* — TradeWatch/Allegro sales analytics
│   │       │       ├── auth.py          ← POST /token, /refresh, GET /me, POST /register
│   │       │       ├── kpi.py           ← GET /kpi/summary (cache 5min), /chart/revenue
│   │       │       ├── profit.py        ← GET /profit/orders, /profit/by-sku
│   │       │       ├── alerts.py        ← GET/POST /alerts, /alerts/rules
│   │       │       ├── jobs.py          ← POST /jobs/run, GET /jobs, /jobs/{id}
│   │       │       ├── pricing.py       ← GET /pricing/offers, /buybox-stats; POST /offers/update
│   │       │       ├── planning.py      ← GET/POST /planning/months; PATCH /months/{id}/status
│   │       │       ├── inventory_routes.py ← GET /inventory/, /open-pos, /reorder-suggestions
│   │       │       ├── ads.py           ← GET /ads/summary, /ads/chart, /ads/top-campaigns
│   │       │       ├── ai_rec.py        ← GET/POST /ai/recommendations, /ai/generate, /ai/summary
│   │       │       └── families.py      ← 🆕 15 endpointów Family Mapper (list, detail, children, links, coverage, review, fix-packages, triggers)
│   │       │
│   │       ├── connectors/       ════ ZEWNĘTRZNE API ════
│   │       │   ├── __init__.py
│   │       │   ├── nbp.py          ← Kursy walut NBP (fallback FX)
│   │       │   ├── ecb.py          ← 🆕 ECB XML feed parser (backup exchange rates, 30 walut EUR-based)
│   │       │   ├── ergonode.py      ← Ergonode PIM (EAN/ASIN lookup, fetch_ergonode_ean_lookup)
│   │       │   ├── amazon_sp_api/
│   │       │   │   ├── __init__.py
│   │       │   │   ├── client.py    ← SPAPIAuth (LWA token cache), SPAPIClient (httpx, retry)
│   │       │   │   ├── orders.py    ← get_orders() z auto-paginacją, get_order_items()
│   │       │   │   ├── finances.py  ← Finances v2024-06-19: list_transactions(), parse_transaction_fees(), flatten_breakdowns()
│   │       │   │   ├── inventory.py ← get_inventory_summaries() z paginacją
│   │       │   │   ├── catalog.py   ← Catalog Items API (get_item, search_items, get_items_batch, parse_catalog_item)
│   │       │   │   ├── pricing_api.py ← Competitive pricing, Buy Box stats
│   │       │   │   └── reports.py   ← SP-API Reports API (request/download)
│   │       │   ├── amazon_ads_api/ ← 🆕 Amazon Advertising API connector
│   │       │   │   ├── __init__.py
│   │       │   │   ├── client.py    ← AdsAPIAuth (LWA, osobny od SP-API) + AdsAPIClient (httpx)
│   │       │   │   ├── profiles.py  ← GET /v2/profiles → mapowanie profile_id ↔ marketplace_id
│   │       │   │   ├── campaigns.py ← SP (v3 Accept), SB (v4 + fallback), SD (v3) — AdsCampaignInfo
│   │       │   │   └── reporting.py ← v3 async reports: create/poll/download GZIP JSON
│   │       │   └── mssql/
│   │       │       ├── __init__.py  ← Re-export z mssql_store
│   │       │       ├── netfox.py    ← SCHEMA mapping dict + query functions (legacy)
│   │       │       └── mssql_store.py ← MSSQL facade: orders, inventory, planning, jobs, sync (~1573 linii)
│   │       │
│   │       ├── services/         ════ BUSINESS LOGIC ════
│   │       │   ├── __init__.py
│   │       │   ├── profit_service.py   ← get_exchange_rate(), calculate_order_profit(), recalc_batch()
│   │       │   ├── ai_service.py       ← generate_recommendation() → GPT-5.2 → AIRecommendation
│   │       │   ├── ai_product_matcher.py ← 🆕 AI product matching: run_ai_matching() → GPT-4o → acc_product_match_suggestion (human-in-the-loop)
│   │       │   ├── order_pipeline.py   ← 10-step pipeline: sync→backfill→link→map→stamp→COGS→FX→finances→fees→courier_costs→profit (~1010 linii, raw pyodbc)
│   │       │   ├── sync_service.py     ← Centralny orkiestrator sync (~1869 linii):
│   │       │   │                           sync_orders, sync_product_mapping (5-step EAN/ASIN kaskada),
│   │       │   │                           sync_purchase_prices (3-layer: Holding FIFO + XLSX fallback),
│   │       │   │                           sync_finances, sync_inventory, calc_profit_snapshot,
│   │       │   │                           sync_ecb_exchange_rates (🆕 ECB backup),
│   │       │   │                           GSheet EAN lookup, Baselinker EAN lookup
│   │       │   ├── ads_sync.py         ← 🆕 Amazon Ads sync (profiles → campaigns → daily reports → MERGE Azure SQL)
│   │       │   └── family_mapper/     ← 🆕 Moduł Family Mapper (grupowanie produktów w rodziny DE→marketplace)
│   │       │       ├── __init__.py
│   │       │       ├── de_builder.py    ← Budowanie rodzin DE: parsuj master_key (kolor, rozmiar, materiał), grupuj produkty
│   │       │       ├── marketplace_sync.py ← Sync rodzin na 13 marketplace (SP-API Catalog → dopasowanie wariantów)
│   │       │       ├── matching.py      ← Silnik dopasowania: confidence scoring (SKU/EAN/atrybuty), progi, kary
│   │       │       ├── coverage.py      ← Raport pokrycia marketplace (brakujące warianty, procent kompletności)
│   │       │       └── fix_package.py   ← Pakiety naprawcze: generuj/zatwierdzaj proposed links dla brakujących wariantów
│   │       │
│   │       ├── jobs/             ════ LEGACY CELERY WRAPPERS (nie używane w produkcji) ════
│   │       │   ├── __init__.py
│   │       │   ├── order_pipeline.py    ← Celery wrapper → services/order_pipeline
│   │       │   ├── sync_orders.py      ← Legacy (zastąpiony pipeline step 1)
│   │       │   ├── sync_finances.py    ← Celery wrapper → sync_service
│   │       │   ├── sync_inventory.py   ← Celery wrapper → sync_service
│   │       │   ├── sync_purchase_prices.py ← Celery wrapper → sync_service
│   │       │   └── calc_profit.py      ← Celery wrapper → sync_service
│   │       │
│   │       ├── scheduler.py      ← APScheduler AsyncIOScheduler (in-process, zastąpił Celery beat)
│   │       └── worker.py         ← Legacy Celery app + beat_schedule (nie używany w produkcji)
│   │
│   ├── tests/                    ════ 🆕 TESTY PYTEST (80 testów) ════
│   │   ├── conftest.py           ← Fixtures: auth_headers (JWT), FakeCursor/FakeConnection (mock pyodbc), mock_catalog, test_app
│   │   ├── test_master_key.py    ← 28 testów: priority levels, color normalization (8 języków), size aliases, JSON output
│   │   ├── test_matching.py      ← 10 testów: confidence scoring, SKU/EAN/attr matching, penalties, issue insertion
│   │   ├── test_de_builder.py    ← 21 testów: 7 extraction helpers + rebuild smoke
│   │   └── test_api_families.py  ← 21 testów: all 15 endpoints (empty responses, 404s, auth, trigger mocks)
│   │
│   └── web/                      ════ FRONTEND ════
│       ├── Dockerfile            ← node:20 build → nginx:alpine serve
│       ├── nginx.conf            ← SPA routing + /api/ proxy + /ws/ proxy
│       ├── index.html
│       ├── package.json          ← React 18, TypeScript, Vite, TanStack Query 5, Zustand 5
│       ├── vite.config.ts        ← alias @/ → src/, proxy /api → api:8000
│       ├── tailwind.config.js    ← dark mode class, shadcn vars, amazon kolor #FF9900
│       ├── tsconfig.json
│       ├── tsconfig.node.json
│       ├── postcss.config.js
│       └── src/
│           ├── main.tsx          ← React root + QueryClientProvider
│           ├── App.tsx           ← BrowserRouter, PrivateRoute, 13 tras (9 original + 4 Family Mapper)
│           ├── index.css         ← CSS variables dark mode (bg navy, primary #FF9900)
│           │
│           ├── store/
│           │   └── authStore.ts  ← Zustand persist: accessToken, refreshToken, user, logout()
│           │
│           ├── lib/
│           │   ├── api.ts        ← axios client, interceptory JWT, wszystkie typed API calls
│           │   └── utils.ts      ← cn(), formatPLN(), formatPct(), formatDelta()
│           │
│           ├── components/
│           │   ├── layout/
│           │   │   ├── Layout.tsx   ← Sidebar + TopBar + <Outlet />
│           │   │   ├── Sidebar.tsx  ← 13 pozycji nawigacyjnych, ikony lucide (9 original + 4 Family Mapper)
│           │   │   └── TopBar.tsx   ← Alerty badge (czerwony ≥1 critical), logout
│           │   └── ui/              ← Własne shadcn/ui komponenty (zero zewnętrznej paczki)
│           │       ├── button.tsx
│           │       ├── card.tsx
│           │       ├── badge.tsx    ← Warianty: default, destructive, warning, success, outline
│           │       ├── input.tsx
│           │       ├── label.tsx
│           │       ├── select.tsx
│           │       ├── table.tsx
│           │       ├── progress.tsx
│           │       ├── separator.tsx
│           │       ├── skeleton.tsx
│           │       ├── dialog.tsx
│           │       └── tooltip.tsx
│           │
│           └── pages/
│               ├── Login.tsx            ← Form email/hasło, JWT login, redirect
│               ├── Dashboard.tsx        ← KPI tiles, AreaChart revenue+CM1, tabela mp, panel filtrów (10 presetów dat PL, marketplace dropdown, FBA/FBM)
│               ├── ProfitExplorer.tsx   ← Filtrowana tabela orderów P&L z paginacją
│               ├── Alerts.tsx           ← Karty alertów wg severity, mark-read/resolve
│               ├── Jobs.tsx             ← Trigger + progress bar + historia tasków
│               ├── Pricing.tsx          ← Buy Box stats, chart, tabela ofert
│               ├── Planning.tsx         ← Plan vs Actual chart, miesięczna tabela
│               ├── Inventory.tsx        ← Stan FBA, sugestie zamówień, otwarte PO
│               ├── Ads.tsx              ← KPI ACoS/ROAS, Spend vs Sales chart, kampanie
│               ├── AIRecommendations.tsx← Karty GPT-5.2, akceptuj/odrzuć, generuj
│               ├── FamilyMapper.tsx     ← 🆕 Dashboard rodzin: statystyki, wyszukiwarka, trigger buttons
│               ├── FamilyDetail.tsx     ← 🆕 Split view: DE children + marketplace coverage + links
│               ├── ReviewQueue.tsx      ← 🆕 Kolejka review: filtry (status, confidence), inline approve/reject
│               └── FixPackages.tsx      ← 🆕 Pakiety naprawcze: generuj, przeglądaj, zatwierdź + detail dialog
```

---

## 4. Stack technologiczny

### Backend
| Paczka | Wersja | Dlaczego |
|--------|--------|----------|
| `fastapi` | 0.115 | Async-first, auto-swagger, dependency injection |
| `sqlalchemy` | 2.0 | ORM modeli domenowych + integracje z bazą |
| `alembic` | latest | Legacy (pozostałość po PostgreSQL) |
| `pydantic-settings` | 2.x | Settings z `.env` z walidacją typów |
| `python-jose` | 3.x | JWT encode/decode (HS256) |
| `passlib[bcrypt]` | 1.7 | Hashowanie haseł |
| `celery` | 5.4 | Distributed task queue, beat scheduler |
| `aioredis` | 2.x | Async Redis client (cache + pub/sub) |
| `httpx` | 0.27 | Async HTTP dla SP-API (connection pooling) |
| `pyodbc` | 5.x | Synchroniczny driver MSSQL (ODBC 18) |
| `pandas` | 2.x | Transformacje danych z ERP |
| `openai` | 1.58 | GPT-5.2 z `response_format={type: json_object}` |
| `structlog` | latest | Strukturalne logi JSON (produkcja) |

### Frontend
| Paczka | Wersja | Dlaczego |
|--------|--------|----------|
| `react` | 18 | Concurrent features, Suspense |
| `typescript` | 5.x | Pełne typowanie, critical dla dużego UI |
| `vite` | 5.x | Błyskawiczny HMR i build |
| `@tanstack/react-query` | 5 | Server state, auto-refetch, cache |
| `zustand` | 5 | Minimalistyczny store (tylko auth) |
| `recharts` | 2.x | AreaChart, BarChart — dobre TypeScript types |
| `axios` | 1.x | HTTP client + interceptory JWT |
| `lucide-react` | latest | Ikony SVG o spójnym stylu |
| `class-variance-authority` | 0.7 | Warianty klas Tailwind (button sizes itd.) |
| `tailwindcss` | 3.x | Utility-first CSS |

---

## 5. Instalacja i uruchomienie

### Wymagania
- Docker Desktop i docker-compose (preferowane)
- LUB Python 3.12 + Node 20 lokalnie

### Docker (zalecane)
```bash
cd N:\AmazonCommandCenter

# Krok 1 — infrastruktura
docker-compose up -d redis

# Krok 2 — skonfiguruj MSSQL w .env
# MSSQL_SERVER=192.168.230.120
# MSSQL_PORT=11901
# MSSQL_USER=msobieniowski

# Krok 3 — sprawdź połączenie do MSSQL
docker-compose run --rm api python scripts/discover_mssql_schema.py

# Krok 4 — seed danych
docker-compose run --rm api python scripts/seed_demo.py

# Krok 5 — cały stack
docker-compose up -d

# Krok 6 — sprawdź logi
docker-compose logs -f api
docker-compose logs -f worker
```

### URLs
| Serwis | URL |
|--------|-----|
| Frontend | http://localhost:3010 |
| API Swagger | http://localhost:8000/docs |
| API ReDoc | http://localhost:8000/redoc |
| MSSQL NetfoxAnalityka | 192.168.230.120:11901 (user: msobieniowski) |
| Redis | localhost:6380 |

### Konta testowe (po seed)
| Login | Hasło | Rola |
|-------|-------|------|
| `admin@acc.local` | `Admin1234!` | admin |
| `director@acc.local` | `Director1234!` | director |

### Lokalne uruchamianie (bez Docker)
```bash
# Backend
cd apps/api
pip install -r requirements.txt
# Ustaw MSSQL_*/REDIS_URL w .env.local
uvicorn app.main:app --reload --port 8000

# Worker (osobny terminal)
celery -A app.worker worker --loglevel=info -B

# Frontend
cd apps/web
npm install
npm run dev   # → http://localhost:5173
```

---

## 6. Zmienne środowiskowe

Plik `.env` w root projektu. **Nigdy nie commituj do git.**

```
# ══ APP ══════════════════════════════
APP_ENV=production
SECRET_KEY=ZMIEŃ_NA_32_ZNAKOWY_LOSOWY_STRING   ← KRYTYCZNE w produkcji!
ACCESS_TOKEN_EXPIRE_MINUTES=480                  # 8h
REFRESH_TOKEN_EXPIRE_DAYS=30

# ══ BAZY ══════════════════════════════
MSSQL_SERVER=192.168.230.120
MSSQL_PORT=11901
MSSQL_USER=msobieniowski
MSSQL_PASSWORD=<uzupelnij_lokalnie>
MSSQL_DATABASE=NetfoxAnalityka
REDIS_URL=redis://redis:6379/0

# PostgreSQL (legacy - NIE UZYWAC)
# DATABASE_URL=
# DATABASE_URL_SYNC=

# ══ AMAZON SP-API ═════════════════════
SP_API_CLIENT_ID=amzn1.application-oa2-client.f1bda19edeed409d96f96918d42f65de
SP_API_CLIENT_SECRET=                            ← uzupełnij w Amazon Developer Console
SP_API_REFRESH_TOKEN=                            ← z Amazon Seller Central OAuth flow
SP_API_SELLER_ID=A1O0H08K2DYVHX
SP_API_REGION=eu
SP_API_SANDBOX=False
SP_API_PRIMARY_MARKETPLACE=A1PA6795UKMFR9        # Amazon.de

# ══ MSSQL NetfoxAnalityka ═════════════
# Używaj wartości MSSQL_* z sekcji "BAZY" powyżej.

# ══ OPENAI ════════════════════════════
OPENAI_API_KEY=                                  ← sk-...
OPENAI_MODEL=gpt-5.2

# ══ ERGONODE PIM ══════════════════════
ERGONODE_API_URL=https://api-kadax.ergonode.cloud
ERGONODE_USERNAME=msobieniowski@netfox.pl
ERGONODE_PASSWORD=<uzupelnij_lokalnie>
ERGONODE_API_KEY=<z panelu Ergonode>

# ══ GOOGLE SHEET (EAN fallback) ═══════
GSHEET_ALLEGRO_CSV_URL=https://docs.google.com/spreadsheets/d/.../export?format=csv&gid=481343532

# ══ XLSX CENY ZAKUPU ══════════════════
# Ścieżka do XLSX z cenami zakupu (używane przez push_xlsx_prices.py na lokalnej stacji)
PURCHASE_PRICES_XLSX_PATH=N:\Analityka\00. Oficjalne ceny zakupu dla sprzedaży.xlsx

# ══ CELERY (legacy — nie używane w produkcji) ═══
CELERY_BROKER_URL=redis://redis:6379/1
CELERY_RESULT_BACKEND=redis://redis:6379/2

# ══ MONITORING ════════════════════════
SENTRY_DSN=                                      ← opcjonalnie
```

### Pobieranie brakujących kredencjałów
- **SP_API_CLIENT_SECRET + REFRESH_TOKEN**: Amazon Developer Console → SP API → Applications → autoryzacja Seller Central
- **MSSQL_PASSWORD**: Administrator sieci KADAX (serwer `192.168.230.120`)
- **OPENAI_API_KEY**: platform.openai.com → API Keys

---

## 7. Baza danych — modele i relacje

### Diagram relacji (uproszczony)
```
Marketplace (1) ──────────────────────── (N) Offer
     │                                        │
     │                                        │ sku
     │                                        ▼
     └── (N) AccOrder ──── (N) OrderLine ── Product
              │                                │
              │                                └── (N) InventorySnapshot
              ▼
         FinanceTransaction
              │
              │ (przez marketplace_id)
     AdsCampaign ── (N) AdsCampaignDay
              │
     PlanMonth ── (N) PlanLine (per marketplace)
              │
     AlertRule ── (N) Alert
              │
     AIRecommendation (sku? marketplace?)
              │
     JobRun (task tracking)
     ExchangeRate (date + currency → rate_to_pln)
     User
```

### Kluczowe pola modeli

#### `AccOrder` (centralny model)
```python
amazon_order_id: str        # klucz unikalny z SP-API
marketplace_id:  str        # FK → Marketplace.id
purchase_date:   datetime
status:          str        # Shipped, Delivered, Canceled...
order_total:     Numeric    # w walucie marketplace
currency:        str        # EUR, GBP, SEK...
revenue_pln:     Numeric    # order_total × kurs_FX
cogs_pln:        Numeric    # z NetfoxAnalityka
amazon_fees_pln: Numeric    # FBA + referral
ads_cost_pln:    Numeric    # proporcjonalny udział w ads
contribution_margin_pln: Numeric
cm_percent:      Numeric    # CM1 jako % przychodu
```

#### `InventorySnapshot`
```python
snapshot_date:      date
marketplace_id:     str
sku:                str
qty_fulfillable:    int     # dostępne do sprzedaży
qty_reserved:       int     # zarezerwowane (w transporcie do klienta)
qty_inbound:        int     # w drodze do FBA
qty_unfulfillable:  int     # uszkodzone
days_of_inventory:  int     # DOI = qty_fulfillable / velocity_30d
velocity_30d:       Numeric # śr. sprzedaż dzienna (ostatnie 30 dni)
inventory_value_pln: Numeric
```

#### `AIRecommendation`
```python
rec_type:         str    # pricing | reorder | listing_optimization | ad_budget | risk_flag
title:            str
summary:          str
action_items:     JSON   # lista kroków do wykonania
confidence_score: Numeric # 0.0 - 1.0 (z GPT odpowiedzi)
model_used:       str    # "gpt-5.2"
status:           str    # new | accepted | dismissed
sku:              str?
marketplace_id:   str?
expected_impact_pln: Numeric?
```

### Migracje
```bash
# PostgreSQL/Alembic są legacy.
# Nie uruchamiaj migracji w standardowym setupie.
# Dopuszczalne tylko po jawnym poleceniu architekta.
```

⚠️ Katalog `migrations/` traktuj jako legacy po PostgreSQL.

---

## 8. API — pełna mapa endpointów

Bazowy URL: `http://localhost:8000/api/v1`

### Auth
| Method | Path | Auth | Opis |
|--------|------|------|------|
| POST | `/auth/token` | — | Login → `{access_token, refresh_token}` |
| POST | `/auth/refresh` | — | Odśwież token |
| GET | `/auth/me` | ✓ | Dane zalogowanego usera |
| POST | `/auth/register` | admin | Nowy użytkownik |
| POST | `/auth/change-password` | ✓ | Zmiana hasła |

### KPI
| Method | Path | Query params | Opis |
|--------|------|-------------|------|
| GET | `/kpi/summary` | `date_from, date_to, marketplace_id, fulfillment_channel` | Aggregate KPI (cache Redis 5min) + delta vs prev period |
| GET | `/kpi/chart/revenue` | `date_from, date_to, marketplace_id, fulfillment_channel` | Punkty wykresu przychodu/CM1 |
| GET | `/kpi/marketplaces` | — | Lista marketplace z liczbą zamówień (do dropdownu) |

> `fulfillment_channel` akceptuje: `FBA`, `MFN` (=FBM) lub puste (=wszystkie).
> `/kpi/summary` zwraca również `revenue_delta_pct`, `orders_delta_pct`, `cm_delta_pct` — porównanie z analogicznym wcześniejszym okresem.

### Profit
| Method | Path | Query params | Opis |
|--------|------|-------------|------|
| GET | `/profit/orders` | `sku, mp_id, date_from, date_to, page, page_size` | Paginowana lista orderów z CM1 |
| GET | `/profit/by-sku` | `sku, mp_id, date_from, date_to` | Agregacja CM1 per SKU |

### Pricing
| Method | Path | Opis |
|--------|------|------|
| GET | `/pricing/offers` | Lista ofert z Buy Box statusem (paginacja) |
| POST | `/pricing/offers/update` | Bulk update cen (rola: category_mgr+) |
| GET | `/pricing/buybox-stats` | Buy Box win rate per marketplace |

### Planning
| Method | Path | Opis |
|--------|------|------|
| GET | `/planning/months?year=` | Lista miesięcznych planów |
| POST | `/planning/months` | Nowy plan (rola: director+) |
| PATCH | `/planning/months/{id}/status` | draft→approved→locked |
| GET | `/planning/vs-actual?year=` | Plan vs Rzeczywistość (YTD) |

### Inventory
| Method | Path | Opis |
|--------|------|------|
| GET | `/inventory/` | Stan FBA (paginacja, filter status) |
| GET | `/inventory/open-pos` | Otwarte PO z MSSQL |
| GET | `/inventory/reorder-suggestions` | Auto-sugestie zamówień (DOI < 30d) |

### Ads
| Method | Path | Opis |
|--------|------|------|
| GET | `/ads/campaigns` | Lista kampanii |
| GET | `/ads/summary?days=` | Agregowane KPI reklam |
| GET | `/ads/chart?days=` | Spend vs Sales punkty wykresu |
| GET | `/ads/top-campaigns?days=` | Ranking kampanii wg sprzedaży |

### AI Recommendations
| Method | Path | Opis |
|--------|------|------|
| GET | `/ai/recommendations` | Lista rekomendacji (filter: type, status) |
| GET | `/ai/summary` | Liczniki + top rekomendacja |
| POST | `/ai/generate` | Generuj GPT-5.2 (rola: director+) |
| PATCH | `/ai/recommendations/{id}` | Zmień status (accepted/dismissed) |

### Alerts
| Method | Path | Opis |
|--------|------|------|
| GET | `/alerts` | Lista alertów |
| POST | `/alerts/{id}/read` | Oznacz jako przeczytany |
| POST | `/alerts/{id}/resolve` | Rozwiąż alert |
| GET | `/alerts/rules` | Reguły alertów |
| POST | `/alerts/rules` | Nowa reguła |
| DELETE | `/alerts/rules/{id}` | Usuń regułę |

### Jobs
| Method | Path | Opis |
|--------|------|------|
| POST | `/jobs/run` | Uruchom task `{job_type, marketplace_id}` |
| GET | `/jobs` | Historia tasków |
| GET | `/jobs/{id}` | Status jednego taska |

### Health
| Method | Path | Opis |
|--------|------|------|
| GET | `/health` | Shallow health check → `{"status": "ok"}` (LB probe) |
| GET | `/health/deep` | 🆕 Deep check: Azure SQL (`SELECT 1`) + Redis (`PING`) + SP-API (LWA token) — concurrent. → `{"status": "healthy\|degraded", "elapsed_ms": N, "checks": {...}}` |
| GET | `/` (root) | Health check (lifespan endpoint) → `{"service": "amazon-command-center"}` |

### 🆕 Family Mapper (`/families`)
| Method | Path | Opis | Rola |
|--------|------|------|------|
| GET | `/families` | Lista rodzin (paginacja, filtr marketplace/status) | analyst |
| GET | `/families/marketplaces` | Lista dostępnych marketplace’ów | analyst |
| GET | `/families/review` | Kolejka review (low-confidence linki) | analyst |
| GET | `/families/fix-packages` | Lista pakietów naprawczych | analyst |
| GET | `/families/{family_id}` | Szczegóły rodziny | analyst |
| GET | `/families/{family_id}/children` | Dzieci (warianty DE) rodziny | analyst |
| GET | `/families/{family_id}/links` | Linki marketplace (mapowania na inne kraje) | analyst |
| PATCH | `/families/{family_id}/status` | Zmień status rodziny (draft/active/archived) | ops |
| GET | `/families/{family_id}/coverage` | Raport pokrycia marketplace (brakujące warianty) | analyst |
| GET | `/families/{family_id}/issues` | Lista problemów/ostrzeżeń dla rodziny | analyst |
| POST | `/families/trigger/build-de` | Trigger: zbuduj rodziny DE z master_key | ops |
| POST | `/families/trigger/sync-marketplace` | Trigger: synchronizuj marketplace links | ops |
| POST | `/families/trigger/generate-fix` | Trigger: generuj pakiet naprawczy | ops |
| POST | `/families/fix-packages/{pkg_id}/approve` | Zatwierdź pakiet naprawczy | director |
| GET | `/families/fix-packages/{pkg_id}` | Szczegóły pakietu naprawczego | analyst |

> **⚠️ Route ordering:** Statyczne GET (`/marketplaces`, `/review`, `/fix-packages`) są zdefiniowane PRZED
> parametrycznym `/{family_id}` — inaczej FastAPI próbuje parsować "review" jako int → 422.

---

## 9. WebSocket — real-time

### `WS /ws/jobs/{job_id}`
Streamuje postęp taska. Odpytuje bazę co 2 sekundy.

**Format wiadomości:**
```json
{
  "job_id": 42,
  "status": "running",
  "progress_pct": 65,
  "progress_message": "Processing orders 650/1000",
  "records_processed": 650
}
```
Połączenie zamykane gdy `status` ∈ `{completed, failed, cancelled}`.

### `WS /ws/alerts`
Subskrybuje kanał Redis `acc:alerts`. Klient dostaje push przy każdym nowym alercie.

**Format:**
```json
{
  "id": 99,
  "title": "Buy Box utracony: DE-SKU-001",
  "severity": "critical",
  "marketplace_id": "A1PA6795UKMFR9",
  "triggered_at": "2026-02-25T12:34:56"
}
```

**Jak publishować alert z kodu Python:**
```python
from app.api.ws import publish_alert
await publish_alert(redis, alert_dict)
```

---

## 10. Autentykacja i RBAC

### JWT Flow
```
POST /auth/token {email, password}
    → weryfikacja bcrypt
    → zwraca access_token (8h) + refresh_token (30d)

Każde żądanie:
    Authorization: Bearer <access_token>
    → get_current_user() dekoduje JWT → User

Po wygaśnięciu access:
    POST /auth/refresh {refresh_token}
    → nowy access_token
    (frontend: interceptor w api.ts robi to automatycznie)
```

### Role RBAC
```python
class Role(str, Enum):
    admin        = "admin"         # pełny dostęp + user management
    director     = "director"      # pełny dostęp + planning + generuj AI
    category_mgr = "category_mgr"  # własne kategorie + pricing update
    ops          = "ops"           # orders + inventory + jobs
    analyst      = "analyst"       # read-only wszystko
```

### Jak chronić endpoint
```python
# Minimalna rola — director i wyżej:
@router.post("/planning/months")
async def create_plan(
    current_user: User = Depends(require_role(Role.director))
):
    ...

# Dowolny zalogowany:
async def endpoint(_: User = Depends(get_current_user)):
    ...
```

---

## 11. Business logic — CM1/CM2/NP, FX, zysk

### Przepływ kalkulacji zysku
Plik: `app/services/profit_engine.py` (kanoniczny silnik — ~6400 linii)

**CM1 (marża wkładu I):**
```
CM1 = Revenue_PLN + ShippingCharge_PLN - COGS_PLN - Amazon_Fees_PLN - Logistics_PLN
```
- Revenue = `item_price * quantity * FX` (netto, bez VAT)
- ShippingCharge = pool per marketplace (z `acc_finance_transaction`), dystrybucja wg revenue share
- COGS = `purchase_price_pln * quantity * 1.23` (VAT 23%)
- Amazon Fees = `fba_fee_pln + referral_fee_pln` (z finance bridge)
- Logistics = `logistics_pln` (DHL/GLS courier costs for FBM)

**CM2 (marża wkładu II):**
```
CM2 = CM1 - Ads - Returns_Net
      - FBA_Storage - FBA_Aged - FBA_Removal - FBA_Liquidation
      - Refund_Finance - Shipping_Surcharge - FBA_Inbound
```
- 7 bucketów CM2 ładowanych z `acc_finance_transaction` via `_classify_finance_charge()`
- Alokacja pool→product: AFN units (primary) → revenue (fallback) per marketplace

**NP (zysk netto):**
```
NP = CM2 - Overhead_Allocated
```
- Overhead z DWÓCH źródeł: `acc_profit_overhead_pool` (manual admin) + auto-detect z finance NP-layer charges
- Alokacja wg revenue share per marketplace

> **Legacy note:** `profit_service.py` nadal wykonuje write-back revenue/profit do `acc_order`,
> ale kalkulacje V1 (`mssql_store.py`) NIE SĄ już wywoływane — V1 routes delegują do V2 engine.
> Patrz: `docs/PROFIT_CM2_NP_WIRING_2026-03-07.md`.

### Kursy walut (ExchangeRate)
- Tabela `exchange_rates` z kolumnami: `rate_date, currency, rate_to_pln, source`
- `seed_demo.py` seeduje 90 dni historycznych kursów (hardcoded)
- Docelowo: cronjob NBP API `https://api.nbp.pl/api/exchangerates/rates/a/{currency}/`
- Fallback w `profit_service.py` gdy brak kursu w bazie

### Marketplace configuration
Zdefiniowane w `core/config.py`:
```python
MARKETPLACE_REGISTRY = {
    "A1PA6795UKMFR9": {"code": "DE", "currency": "EUR", ...},
    "A1C3SOZRARQ6R3":  {"code": "PL", "currency": "PLN", ...},
    "A13V1IB3VIYZZH":  {"code": "FR", "currency": "EUR", ...},
    "A33AVAJ2PDY3EV":  {"code": "TR", "currency": "TRY", ...},
    # ... wszystkie 13 marketplace (DE, PL, FR, IT, ES, SE, NL, BE, AT, US, AE, SA, TR)
}
```

---

## 12. Integracja SP-API (Amazon)

Pliki: `app/connectors/amazon_sp_api/`

### Autentykacja LWA (Login with Amazon)
```python
# client.py — SPAPIAuth
class SPAPIAuth:
    _token_cache: dict = {}  # {seller_id: {token, expires_at}}

    async def get_access_token(self) -> str:
        # Sprawdź cache (token ważny 1h)
        # Jeśli wygasł: POST https://api.amazon.com/auth/o2/token
        # z client_id + client_secret + refresh_token
```

### HTTP Client z retry
```python
class SPAPIClient:
    async def get(self, path: str, params: dict) -> dict:
        # Automatyczny retry na 429 (TooManyRequests)
        # Exponential backoff: 1s, 2s, 4s (max 3 próby)
        # Base URL: https://sellingpartnerapi-eu.amazon.com
```

### SP-API Endpoints używane

| Endpoint | Plik | Co pobiera |
|----------|------|-----------|
| `GET /orders/v0/orders` | `orders.py` | Lista zamówień z paginacją (`NextToken`) |
| `GET /orders/v0/orders/{id}/orderItems` | `orders.py` | Pozycje zamówienia |
| `GET /finances/2024-06-19/transactions` | `finances.py` | Transakcje finansowe (v2024-06-19, hierarchical breakdowns) |
| `GET /fba/inventory/v1/summaries` | `inventory.py` | Stany FBA per ASIN/SKU |

### Finances Connector — szczegóły (v2024-06-19)

Plik: `app/connectors/amazon_sp_api/finances.py`

**Kluczowe metody:**

- `list_transactions(posted_after, posted_before, marketplace_id, max_pages=500)` — paginacja NextToken, rate limit 2s/req
- `parse_transaction_fees(txn)` — parsuje transakcję → lista dict'ów z fee rows (ORDER_ID, SKU, charge_type, amount, currency)
- `flatten_breakdowns(breakdowns)` — rekurencyjne spłaszczanie hierarchii breakdownów (tylko liście)
- `extract_order_id(txn)` / `extract_shipment_id(txn)` / `extract_settlement_id(txn)` — statyczne helpery

**Ograniczenia API:**

- Max **180 dni** między `postedAfter` a `postedBefore`
- **48h opóźnienie** na dane najnowszych transakcji
- Rate limit: **0.5 req/s**, burst 10
- `step_sync_finances()` automatycznie chunksuje zakresy >180d

**Mapowanie opłat (bridge_fees):**

```
FBA fees:      FBAPerUnitFulfillmentFee, FBAPerOrderFulfillmentFee,
               FBAWeightBasedFee, FBAPickAndPackFee → fba_fee_pln

Referral fees: Commission, VariableClosingFee,
               FixedClosingFee → referral_fee_pln

Agregacja:     SUM(fba + referral) per order → acc_order.amazon_fees_pln
```

### Dodawanie nowego endpointu SP-API
1. Utwórz nowy plik w `app/connectors/amazon_sp_api/`
2. Użyj `SPAPIClient` z `client.py`
3. Zaimportuj w `connectors/amazon_sp_api/__init__.py`

---

## 13. Integracja MSSQL (NetfoxAnalityka)

Plik: `app/connectors/mssql/netfox.py`

### Konfiguracja połączenia
Driver: `ODBC Driver 18 for SQL Server` (zainstalowany w Dockerfile)
```
SERVER=192.168.230.120,11901
DATABASE=NetfoxAnalityka
UID=Analityka
PWD=<z .env>
TrustServerCertificate=yes
```

### SCHEMA MAPPING — kluczowy concept
Na górze pliku `netfox.py` jest dataclass `_SchemaMap`:
```python
@dataclass
class _SchemaMap:
    tbl_products: str = "dbo.Kartoteki"
    col_sku:      str = "Symbol"
    col_ean:      str = "EAN"
    col_purchase_price: str = "CenaZakupu"
    # ...
```

**Żeby dopasować do rzeczywistego schematu:**
1. Uruchom `python scripts/discover_mssql_schema.py`
2. Znajdź właściwe nazwy tabel i kolumn
3. Edytuj tylko wartości w `_SchemaMap` — SQL się zaktualizuje automatycznie

### Dostępne funkcje
```python
from app.connectors.mssql.netfox import (
    get_product_costs,        # sku, ean, product_name, netto_purchase_price_pln
    get_warehouse_stock,      # sku, product_name, qty_on_hand
    get_open_purchase_orders, # sku, qty_ordered, qty_received, qty_open, expected_delivery
    get_products_with_stock,  # JOIN product_costs + warehouse_stock
    test_connection,          # bool — czy MSSQL dostępny
)
```

⚠️ Połączenie jest **synchroniczne** (pyodbc nie ma async). W FastAPI async handlerze wywołuj w executor:
```python
from asyncio import get_event_loop
df = await get_event_loop().run_in_executor(None, get_product_costs, skus)
```
(Inventory route już to obsługuje przez `test_connection()` guard)

---

## 13.1 Integracja MSSQL (NetfoxDistribution / Subiekt GT)

> **⚠️ KRYTYCZNE: BAZA READ-ONLY! ⚠️**
>
> NetfoxDistribution to **produkcyjna baza ERP Subiekt GT** firmy KADAX.
> **ABSOLUTNY ZAKAZ** wykonywania INSERT, UPDATE, DELETE, ALTER, DROP.
> Dozwolone tylko SELECT. Złamanie tej reguły = uszkodzenie produkcyjnego ERP.
> Każdy przyszły Copilot / developer MUSI przestrzegać tej zasady.

### Połączenie
```
DRIVER={SQL Server}
SERVER=192.168.230.120,11901
DATABASE=NetfoxDistribution
UID=es_netfox
PWD=DlZuRpdZ
```

**Ta sama instancja** co NetfoxAnalityka (`SQLSRV-9\HOSTING_901`, SQL Server 2019).
SQL Browser (UDP 1434) jest zamknięty — używać **wyłącznie portu 11901**.

### Schema: Subiekt GT (InsERT)

Suiekt GT to polski ERP od InsERT. Schemat ma 964 tabel i 374 widoków.
Poniżej kluczowe tabele z perspektywy ACC:

#### Produkty
| Tabela | Opis | Klucz | Ważne kolumny |
|--------|------|-------|---------------|
| `tw__Towar` | Kartoteka produktów (24974) | `tw_Id` | `tw_Symbol` (= ACC internal_sku!), `tw_Nazwa`, `tw_PodstKodKresk` (EAN), `tw_Masa`, `tw_Rodzaj` |
| `tw_Cena` | Aktualne ceny | `tc_IdTowar` → `tw_Id` | `tc_CenaNetto0` (zakup), `tc_CenaNetto1-10` (sprzedaż), `tc_CenaBrutto0-10` |
| `tw_CenaHistoria` | Historia cen | `tch_IdTowar` | Pusta (0 wierszy) |
| `tw_KodKreskowy` | Kody kreskowe | `kk_IdTowar` → `tw_Id` | `kk_Kod` — **15040 kodów MAG_**, z czego 15035 ma EAN |
| `tw_Stan` | Stany magazynowe | `st_TowId` → `tw_Id` | `st_Stan`, `st_StanRez`, `st_MagId` |
| `tw_Komplet` | BOM/Receptury | `kpl_IdKomplet`, `kpl_IdSkladnik` | **Pusta (0 wierszy)** — BOMy nie w Subiekcie |
| `tw_JednMiary` | Jednostki miary | | |
| `tw_ZdjecieTw` | Zdjęcia | | |

#### Dokumenty
| Tabela | Opis | Klucz | Ważne kolumny |
|--------|------|-------|---------------|
| `dok__Dokument` | Wszystkie dokumenty (~6M) | `dok_Id` | `dok_Typ`, `dok_NrPelny`, `dok_DataWyst`, `dok_WartNetto`, `dok_WartBrutto`, `dok_Uwagi`, `dok_OdbiorcaId` |
| `dok_Pozycja` | Pozycje dokumentów | `ob_DokHanId` → `dok_Id` | `ob_TowId`, `ob_Ilosc`, `ob_CenaNetto`, `ob_WartNetto`, `ob_CenaNabycia` (NULL!), `ob_WartBrutto` |

#### Typy dokumentów (`dok_Typ`)
| Typ | Skrót | Nazwa | Ilość |
|-----|-------|-------|-------|
| 1 | FZ | Faktura Zakupu (purchase invoice) | 12,431 |
| 2 | FS | Faktura Sprzedaży (sales invoice) | 1,929,386 |
| 5 | KFZ | Korekta FZ | 3,816 |
| 6 | KFS | Korekta FS (korekta sprzedaży) | 69,521 |
| 9 | MM | Przesunięcie Magazynowe | 28,888 |
| 10 | PZ | Przyjęcie Zewnętrzne (receipt) | 78,939 |
| 11 | WZ | Wydanie Zewnętrzne (issue/shipment) | 1,931,359 |
| 12 | PW | Przyjęcie Wewnętrzne | 392 |
| 13 | RW | Rozchód Wewnętrzny | 897 |
| 15 | ZD | Zamówienie do Dostawcy | 104 |
| 16 | ZK | Zamówienie Klienta (customer order) | 1,927,071 |

#### Kontrahenci i adresy
| Tabela | Opis | Klucz |
|--------|------|-------|
| `kh__Kontrahent` | Kontrahenci (868,293) | `kh_Id` — `kh_Symbol`, `kh_Imie`, `kh_Nazwisko` (często puste!) |
| `adr__Ewid` | Adresy aktualne | `adr_IdObiektu` → `kh_Id` — `adr_Nazwa`, `adr_NazwaPelna`, `adr_NIP` |
| `adr_Historia` | Adresy historyczne | `adrh_Id` — `adrh_Nazwa`, `adrh_NazwaPelna` |

**Link dok → kontrahent:** `dok__Dokument.dok_OdbiorcaId` → `kh__Kontrahent.kh_Id`
**Link dok → adres (nazwa firmy):** `dok__Dokument.dok_OdbiorcaAdreshId` → `adr_Historia.adrh_Id` → `adrh_NazwaPelna`

#### Magazyny (`sl_Magazyn`)
| Symbol | Nazwa | Stan (qty) | Produkty ze stanem > 0 |
|--------|-------|-----------|------------------------|
| MGD | Magazyn Główny Distribution | 6,667,445 | 6,703 |
| GRN | Green Logistics Fulfilment | 172,698 | 174 |
| WYD | Do wyjaśnienia | 7,534 | 201 |
| MDZ | Do Znalezienia | 5,769 | 237 |
| ZRD | Zwroty i Reklamacje | 2,747 | 1,014 |
| MAD | Marketing | 1,583 | 746 |
| TND | Towary uszkodzone | 1,108 | 307 |
| MUK | Magazyn UK | 11 | 1 |
| FKD, KOD, ZDD | Inne (puste) | 0 | 0 |

### SKU Mapping: ACC ↔ Subiekt

**ACC `internal_sku` = Subiekt `tw_Symbol`** — bezpośredni match, 100% (6951/6951).

Kody MAG_ w `tw_KodKreskowy` to dodatkowe kody kreskowe przypisane do produktów.
Format: `MAG_5903699471272` (prefix `MAG_` + EAN/FNSKU).
15040 takich kodów, 15035 ze sparowanym EAN w `tw_PodstKodKresk`.

### COGS: tw_Cena vs ACC — porównanie

| Metryka | Wartość |
|---------|----------|
| ACC aktywne ceny (`valid_to IS NULL`) | 6,951 |
| Subiekt produkty w tw_Cena | 24,974 |
| tw_Cena z `tc_CenaNetto0 > 0` | **32** (!) |
| Matchowane ACC ↔ Subiekt (via tw_Symbol) | 6,951 (100%) |
| Z ceną Subiekt > 0 | **10** |
| Z ceną Subiekt = 0 | **6,941** |

**Wniosek:** `tw_Cena.tc_CenaNetto0` jest praktycznie **pusta** — tylko 32 produkty mają cenę zakupu.
ACC XLSX/Holding pipeline jest jedynym źródłem cen zakupu. Subiekt tej informacji nie utrzymuje.

Prawdziwe ceny zakupu w Subiekcie leżą w **pozycjach FZ** (`dok_Pozycja.ob_CenaNetto` gdzie `dok_Typ = 1`),
ale `ob_CenaNabycia` jest wszędzie NULL. FZ ma 142,043 pozycji z cenami netto.

### Amazon w Subiekcie

Dokumenty Amazon oznaczone w `dok_Uwagi` tagami:
```
|#AmazonFBM_de#|baselinker_id|*order_num*645*blconnect*hash
|#AmazonFBA_de#|...
```

Wszystkie FS/WZ Amazon wystawiane na:
- `kh_Id = 245` → **Aleksy Lisowski Netfox** (kh_Symbol = CHANG) — FBM
- `kh_Id = 569` → **NETFOX TRADE Sp. z o.o.** — inter-company

Netfox Distribution (kh_Id = 1, adr_Id = 1): **NETFOX DISTRIBUTION Sp. z o.o.**, NIP 6812094052, Myślenice.

### EasyStorage (WMS)
System WMS od **9BYTES sp. z o.o.** (easystorage.io, Rzeszów).
Integruje się z Subiektem GT, Baselinker, IdoSell, Comarch.
**Brak publicznego REST API** — zamknięty SaaS z aplikacją mobilną.
Kontakt: wms@easystorage.io / +48 573 568 841.

### Sfera (InsERT API)
Sfera to SDK COM/.NET od InsERT do programistycznej obsługi Subiekta GT.
**Nie jest REST API** — wymaga Windows + COM interop.
**Nie potrzebna** — mamy bezpośredni dostęp SQL do NetfoxDistribution.
API Key (nieużywany): `459f21ffe0999276060e3dbdf4d80189a5c9ac9a`

---

## 14. APScheduler — harmonogram zadań

> **UWAGA:** Celery został zastąpiony przez APScheduler `AsyncIOScheduler` działający in-process w uvicorn.
> Pliki `worker.py` i `jobs/` to **legacy dead code** — nie są używane w produkcji.

Plik: `app/scheduler.py`

### Harmonogram (automatyczne, in-process)

| Job | Harmonogram | Funkcja | Deleguje do |
|-----|-------------|---------|-------------|
| Order Pipeline | co 15 min (IntervalTrigger) | `_order_pipeline()` | `services/order_pipeline.run_order_pipeline()` |
| Sync Purchase Prices | 02:00 (CronTrigger) | `_sync_purchase_prices()` | `services/sync_service.sync_purchase_prices()` |
| 🆕 Sync ECB Exchange Rates | 02:30 (CronTrigger) | `_sync_ecb_exchange_rates()` | `services/sync_service.sync_ecb_exchange_rates(days_back=90)` |
| Sync Finances | 03:00 (CronTrigger) | `_sync_finances()` | `services/sync_service.sync_finances()` |
| Sync Inventory | 04:00 (CronTrigger) | `_sync_inventory()` | `services/sync_service.sync_inventory()` |
| 🆕 Sync Sales & Traffic | 04:30 (CronTrigger) | `_sync_sales_traffic()` | `services/manage_inventory.sync_inventory_sales_traffic(days_back=90)` |
| Calc Profit | 05:00 (CronTrigger) | `_calc_profit()` | `services/sync_service.calc_profit_snapshot()` |
| 🆕 Sync Ads | 07:00 (CronTrigger) | `_sync_ads()` | `services/ads_sync.run_full_ads_sync(days_back=3)` |
| ~~Family Mapper Nightly~~ | ~~01:00 (CronTrigger)~~ | ~~`_family_mapper_pipeline()`~~ | ~~`services/family_mapper/` (build DE → sync mp → coverage)~~ |

> **⚠️ Family Mapper Nightly** jest **WYŁĄCZONY** (zakomentowany w scheduler.py). Uruchamianie wyłącznie manualne
> przez UI triggers (`POST /families/trigger/*`). Odkomentuj w scheduler.py gdy gotowy na automatyzację.

Scheduler startuje w `main.py` → `lifespan()` → `start_scheduler()`.
Każdy job tworzy rekord `acc_al_jobs` (JobRun) dla widoczności w UI.

### Dodatkowo: Scheduled Task na lokalnej stacji

| Trigger | Kiedy | Co robi |
|---------|-------|---------|
| `ACC\PushXlsxPrices` (Scheduled Task) | Codziennie 09:00 | `push_xlsx_prices.py` — XLSX → acc_purchase_price |
| Registry Run (HKCU) | Logon (+60s) | To samo — backup jeśli PC wyłączony o 9:00 |

Logi: `N:\AmazonCommandCenter\logs\push_xlsx_prices.log`

### Order Pipeline — 10 kroków (co 15 min)
Plik: `app/services/order_pipeline.py` (~1010 linii, raw `pyodbc`)

| Step | Funkcja | Co robi | Sync/Async |
|------|---------|---------|-----------|
| 1 | `step_sync_orders(days_back)` | SP-API → upsert `acc_order` + `acc_order_line` (commit co 25 ordersów) | async (SP-API) |
| 2 | `step_backfill_products()` | Tworzy `acc_product` dla SKU/ASIN par z `acc_order_line` bez produktu. **UWAGA:** INSERT musi zawierać `is_parent=0` (kolumna NOT NULL). Bug naprawiony 26.02.2026. | sync (pyodbc) |
| 3 | `step_link_order_lines()` | `UPDATE acc_order_line SET product_id = p.id` gdzie `NULL` a SKU jest w `acc_product` | sync (pyodbc) |
| 4 | `step_map_products()` | 5-krokowa kaskada mapowania (patrz poniżej) via `sync_service` | async |
| 5 | `step_stamp_purchase_prices()` | **2-pass:** Pass 1: acc_product cache, Pass 2: CROSS APPLY acc_purchase_price (8-level priority). Holding/erp_holding ×1.04 multiplier. Pokrycie: **~99%** | sync (pyodbc) |
| 5.5 | COGS audit (`validate_after_stamp`) | Sprawdza jakość COGS po stampowaniu | sync |
| 5.8 | FX sync (`sync_exchange_rates`) | Kursy walut z NBP/ECB | async |
| 5.8b | `step_sync_finances(days_back=3)` | Finances v2024-06-19 → `acc_finance_transaction` (okna 180d, idle 48h) | async (SP-API) |
| 5.9 | `step_bridge_fees()` | Mapuj opłaty z `acc_finance_transaction` → `fba_fee_pln`/`referral_fee_pln` na `acc_order_line` + `amazon_fees_pln` na `acc_order` | sync (pyodbc) |
| 5.95 | `step_sync_courier_costs(days_back=30)` | FBM courier costs (DHL/GLS) → `acc_order.logistics_pln`. **SAFETY:** temp table `#bl_track`, DHL JJD batched (50/query), 120s timeout. Łańcuch: `amazon_order_id` → `ITJK_ZamowieniaBaselinkerAPI` → tracking → `ITJK_CouriersInvoicesDetails` (GLS) / `ITJK_DHL_Costs` (DHL JJD, batched). | sync (pyodbc) |
| 6 | `step_calc_profit(days_back=7)` | Revenue - COGS - Amazon Fees - Ads - Logistics = CM1 | async (pyodbc via mssql_store) |

Orkiestrator `run_order_pipeline()` uruchamia kroki sekwencyjnie z try/except — jeden krok nie blokuje pozostałych.

### SKU Mapping — Step 4 szczegóły (5-krokowa kaskada)

Pipeline: `sync_service.sync_product_mapping()` (~1869 linii)

| Sub-step | Źródło | mapping_source | Co robi |
|----------|--------|---------------|---------|
| 3a | Ergonode PIM | `ergonode` | EAN z acc_product → Ergonode `fetch_ergonode_ean_lookup()` → internal_sku |
| 3a | Google Sheet | `gsheet` | EAN → CSV z GSheet (gid=481343532) → internal_sku |
| 3b | Baselinker MSSQL | `baselinker` | EAN → Baselinker zamówienia w NetfoxAnalityka → internal_sku |
| 3c | Ergonode ASIN | `ergonode_asin` | ASIN (child+parent) z Ergonode → internal_sku |
| 3d | SP-API Catalog | `spapi_ergonode`/`spapi_gsheet`/`spapi_ean_only` | ASIN → `CatalogClient.get_items_batch(identifiers)` → EAN → Ergonode/GSheet → internal_sku |

Wynik aktualny (26.02.2026): ergonode: 989+257, gsheet: 30, baselinker: 17+7, spapi: 47, spapi_ean_only: 36, ergonode_asin: 1.
Łącznie: **1344/1406 produktów z internal_sku** (95.6%). Niezmapowane: 62.

### Ceny zakupu — 3-warstwowa architektura + priorytet źródeł

Pipeline: `sync_service.sync_purchase_prices()` + `order_pipeline.step_stamp_purchase_prices()`

```
8-poziomowy priorytet źródeł cen (w order_pipeline.py Pass 2 CROSS APPLY):
  1. manual         — ręcznie ustawiona cena (najwyższy priorytet)
  2. import_xlsx    — wrzutki od zakupu (pliki z 'cogs from sell/')
  3. xlsx_oficjalne — oficjalny cennik XLSX (~7,605 SKU)
  4. holding        — Netfox Holding FIFO (~4,083 SKU) × 1.04 multiplier
  5. erp_holding    — starszy ERP holding (1 SKU) × 1.04 multiplier
  6. import_csv     — import z CSV (~371 SKU)
  7. cogs_xlsx      — COGS z plików sell (nieliczne)
  8. acc_product    — fallback z cache produktu (najniższy priorytet)

Holding ×1.04: Netfox Holding FIFO ceny są systematycznie ~4% niższe
od oficjalnego cennika (mediana ratio = 0.96, 90% niższe).
Mnożnik stosowany TYLKO przy stampowaniu (Opcja A) — surowa cena w DB bez zmian.

3 warstwy zapisu:
  Layer 1: acc_purchase_price (historia cen, valid_from/valid_to, źródło)
  Layer 2: acc_order_line.purchase_price_pln + cogs_pln (per-line stamp)
  Layer 3: acc_product.netto_purchase_price_pln (cache do szybkiego odczytu)

import_cogs_xlsx.py (ręczny import z 'cogs from sell/'):
  - SOURCE = 'import_xlsx' (priorytet 2)
  - Nadpisuje order lines z KAŻDYM źródłem oprócz 'manual'
  - Pliki od zakupu = najświeższe ceny per fala importu
  - Cap: MAX_PURCHASE_PRICE_PLN = 2000
```

Pokrycie: **~99%** order lines z ceną zakupu.

**Netfox ERP jako źródło xlsx:**
Sprawdzono 5 tabel ITJK_BazaDanychSprzedaz* vs xlsx_oficjalne (~3700 SKU):
- TRADE: 80% match, ceny ~2-5% wyższe (50/50 wyższe/niższe)
- JDG: 77% match, analogicznie
- Holding: 70% match, systematycznie ~4% niższe (90% niższe)
- DIS: 69% match, systematycznie ~4% niższe (92% niższe)
- **Wniosek:** xlsx nie pochodzi z żadnej tabeli ERP — utrzymywany niezależnie

### Manualne uruchamianie

```bash
# Z API (przez Jobs page lub curl)
POST /api/v1/jobs/run {"job_type": "order_pipeline"}
POST /api/v1/jobs/run {"job_type": "sync_purchase_prices"}
POST /api/v1/jobs/run {"job_type": "sync_product_mapping"}
```

### Backfill scripts (na serwerze C:\ACC\)

| Skrypt | Cel | Czas trwania |
| ------ | --- | ------------ |
| `backfill_full.py` | Pełny 2-letni backfill: 9 faz (orders→products→link→map→COGS→FX→finances→bridge_fees→calc_profit) | ~kilka godzin |
| `backfill_gap.py` | Wypełnienie luki (days_back=423, Jan→Sep 2025) | ~30 min |
| `run_backfill_full.bat` | Wrapper `.bat` do uruchamiania przez schtasks | — |
| `run_backfill_gap.bat` | Wrapper `.bat` do uruchamiania przez schtasks | — |

Uruchomienie:

```bash
# Przez schtasks (jednorazowo)
schtasks /Create /TN ACC_FullBackfill /TR "C:\ACC\run_backfill_full.bat" /SC ONCE /ST 00:01 /SD 01/01/2020 /RU copilot-dev /RP MocneHaslo123! /F
schtasks /Run /TN ACC_FullBackfill

# Logi
type C:\ACC\logs\full_backfill_YYYY-MM-DD.log
```

`backfill_full.py` — 9 faz:

1. `step_sync_orders(days_back=730)` — 2 lata zamówień z SP-API (13 marketplace'ów)
2. `step_backfill_products()` — tworzenie acc_product dla nowych SKU/ASIN
3. `step_link_order_lines()` — linkowanie order_line→product
4. `step_map_products()` — 5-krokowa kaskada mapowania (Ergonode/GSheet/Baselinker/ASIN/Catalog)
5. `step_stamp_purchase_prices()` — ceny zakupu (Holding FIFO + XLSX fallback)
6. `sync_exchange_rates(days_back=730)` — kursy walut NBP/ECB
7. `step_sync_finances(days_back=730)` — Finances v2024-06-19 (auto-chunking 180d windows)
8. `step_bridge_fees()` — mapowanie opłat na linie zamówień
9. `recalc_profit_orders(days_back=730)` — kalkulacja marży CM1

### Śledzenie postępu
Każdy task tworzy rekord `acc_al_jobs` (JobRun) w MSSQL z:
- `status`: `pending → running → completed/failed`
- `progress_pct`: 0–100
- `progress_message`: "Processing marketplace DE 650/1000"
- `records_processed`: liczba rekordów
- `duration_seconds`: czas wykonania

Postęp streamowany przez WebSocket `/ws/jobs/{job_id}`.

### Dodawanie nowego taska
```python
# 1. Napisz async funkcję w sync_service.py lub osobnym serwisie
async def my_new_sync(conn, sp_clients):
    job = create_job_run(conn, "my_new_sync")
    # ... logika ...
    update_job_run(conn, job_id, status="completed", progress_pct=100)

# 2. Dodaj wrapper w scheduler.py
async def _my_new_sync():
    conn = pyodbc.connect(MSSQL_CONN_STR, autocommit=True)
    try:
        await my_new_sync(conn, sp_clients)
    finally:
        conn.close()

scheduler.add_job(_my_new_sync, CronTrigger(hour=6, minute=0), id="my_new_sync")

# 3. Dodaj do ALLOWED_JOBS w app/api/v1/jobs.py (manualne uruchamianie z UI)
```

---

## 15. Frontend — strona po stronie

### Wzorzec strony (standard)
```typescript
export default function MyPage() {
  // 1. State lokalny (filtry, paginacja)
  const [filter, setFilter] = useState("");

  // 2. TanStack Query (server state)
  const { data, isLoading } = useQuery({
    queryKey: ["my-data", filter],
    queryFn: () => getMyData({ filter }),
    staleTime: 60_000,  // 1 minuta cache
  });

  // 3. Mutacje
  const { mutate } = useMutation({
    mutationFn: updateSomething,
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["my-data"] }),
  });

  return (
    <div className="space-y-6">
      <h1>Tytuł Strony</h1>
      {isLoading ? <Skeleton /> : <Table data={data} />}
    </div>
  );
}
```

### Strony i ich API calls
| Strona | Główne API calls | Wykres |
|--------|-----------------|--------|
| `Dashboard` | `getKPISummary`, `getRevenueChart`, `getMarketplaces` | AreaChart revenue+CM1 |
| `ProfitExplorer` | `getProfitOrders` | — |
| `Pricing` | `getPricingOffers`, `getBuyBoxStats` | BarChart Buy Box |
| `Planning` | `getPlanMonths`, `getPlanVsActual` | BarChart plan vs actual |
| `Inventory` | `getInventory`, `getReorderSuggestions`, `getOpenPOs` | — |
| `Ads` | `getAdsSummary`, `getAdsChart`, `getTopCampaigns` | AreaChart spend vs sales |
| `AIRecommendations` | `getAIRecommendations`, `getAISummary`, `generateAIRec` | — |
| `Alerts` | `getAlerts`, `markAlertRead`, `resolveAlert` | — |
| `Jobs` | `getJobs`, `runJob` | Progress bar |
| `FamilyMapper` | `getFamilies`, `triggerBuildDE`, `triggerSyncMarketplace` | Stats tiles |
| `FamilyDetail` | `getFamily`, `getFamilyChildren`, `getFamilyLinks`, `getFamilyCoverage` | Coverage grid |
| `ReviewQueue` | `getReviewQueue`, `approveLink`, `rejectLink` | Filterable queue |
| `FixPackages` | `getFixPackages`, `generateFixPackage`, `approveFixPackage` | Detail dialog |

### Auth flow w frontend
```
Login.tsx → login(email, password) → /auth/token
    → authStore.setTokens(access, refresh)
    → localStorage persist (Zustand)

Każdy request → api.ts interceptor → Bearer token
401 → api.ts interceptor → /auth/refresh → nowy token
     → jeśli refresh też 401 → logout() → /login
```

---

## 16. Składowe UI (shadcn/ui)

Własna implementacja w `src/components/ui/` — zero dependencji od `shadcn` CLI.

### Design system
```css
/* Kolory */
--bg-primary: #060d1a        /* najciemniejsze tło */
--bg-secondary: #0f172a      /* karty */
--bg-tertiary: #111827        /* inputs */
--accent: #FF9900            /* Amazon orange */
--text: #ffffff
--text-muted: rgba(255,255,255,0.5)
--border: rgba(255,255,255,0.1)
```

### Używanie komponentów
```typescript
import { Button } from "@/components/ui/button";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { Select, SelectTrigger, SelectContent, SelectItem, SelectValue } from "@/components/ui/select";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
```

### Badge warianty
```typescript
<Badge variant="default">     // #FF9900 — primary action
<Badge variant="success">     // emerald — pozytywny
<Badge variant="warning">     // amber — ostrzeżenie
<Badge variant="destructive"> // red — błąd/krytyczny
<Badge variant="secondary">  // biały/10 — neutralny
<Badge variant="outline">    // border only
```

---

## 17. Znane ograniczenia i pułapki

### Krytyczne — wymagają akcji przed produkcją

1. ~~**`SECRET_KEY` w `.env`**~~ — ✅ DONE. Ustawione w produkcji.

2. ~~**MSSQL kolumny**~~ — ✅ DONE. Schema zweryfikowana, `ensure_v2_schema()` tworzy tabele automatycznie.

2b. ~~**Family Mapper — tabele SQL**~~ — ✅ DONE (28.02.2026). Wymagane 9 tabel:
    `global_family`, `global_family_child`, `marketplace_listing_child`,
    `family_marketplace_link`, `family_mapper_issue`, `family_mapper_run`,
    `fix_package`, `fix_package_item`, `family_mapper_config`.
    Tabele utworzone ręcznie (migracja nie była uruchomiona przez pilota tworzącego kod).
    Bez tych tabel `/api/v1/families` zwracał 500.

3. ~~**SP_API_CLIENT_SECRET + REFRESH_TOKEN**~~ — ✅ DONE. Skonfigurowane dla 13 marketplace.

4. **Kursy NBP** — `seed_demo.py` seeduje hardcoded kursy. Do zrobienia: nowy job w `scheduler.py`: `GET https://api.nbp.pl/api/exchangerates/rates/a/eur/` → `exchange_rates`.

### Architektoniczne

5. **COGS z MSSQL jest synchroniczny** — `pyodbc` nie ma async. Batch kalkulacji zysku (~1000 orderów) może zajmować 5–10 sekund. Uruchamiane jako APScheduler job (in-process) — OK.

5b. **⚠️ SQL Safety (dodane 2026-02-28)**
    - **`SET LOCK_TIMEOUT 30000`** ustawiane na KAŻDYM połączeniu MSSQL (`_db_conn()`, `_connect()`, `_get_conn()`, `_connect()` w cogs_audit). Jeśli query nie dostanie locka w 30s → automatyczny abort zamiast deadlocka.
    - **`QUERY_TIMEOUT_SECONDS = 120`** — connection timeout w `order_pipeline.py`.
    - **`ITJK_DHL_Costs` (13M wierszy, 0 indeksów)** — NIGDY nie rób bezpośredniego JOIN ani CROSS APPLY. Zawsze batch po max 50 JJD numerów.
    - **`ITJK_ZamowieniaBaselinkerAPI` (1.1M wierszy)** — używaj temp table z GROUP BY, nie CROSS APPLY.
    - **Przed każdą ciężką operacją SQL** (full scan, JOIN >1M wierszy, backfill >30d) → brief dla admina (opis, tabele, czas, obciążenie).
    - **WITH (NOLOCK)** na wszystkich SELECT-ach do tabel Netfox (ITJK_*).

6. **Login by nazwa pliku** — route `inventory_routes.py` (nie `inventory.py`) — bo `inventory.py` już istnieje jako model. Przy dodawaniu nowych modułów uważaj na konflikty nazw.

7. **`AccOrder` zamiast `Order`** — model zamówienia nazywa się `AccOrder` żeby uniknąć konfliktu z nazwami zarezerwowanymi SQL.

8. **Alembic/migrations to legacy** — nie uruchamiaj `autogenerate` ani `upgrade` bez jawnej decyzji architekta.

### Frontend

9. **Lint errors w VS Code (Pylance/TS)** — wszystkie `Import "app.core.*" could not be resolved` i `JSX requires react/jsx-runtime` są **oczekiwane** — środowisko jest Docker-only. W Docker wszystko działa. Ignoruj je.

10. **`tsconfig.json` noImplicitAny** — niektóre `(r) => r.data` w `api.ts` mogą dawać TS warning jeśli axios generic type nie działa. Dodaj `: AxiosResponse<T>` lub wyłącz `strict: false` w dev.

---

## 18. Roadmapa — co zostało do zrobienia

### Priorytet 0 — Aktywne prace (Marzec 2026)

> **Backfill zamówień + order lines via SP-API Reports API — IN PROGRESS (2026-03-02)**
>
> - `backfill_via_reports.py` (~765 linii) — bulk download TSV raportów zamówień
> - 30-dniowe okna × 13 marketplace'ów, od najnowszych
> - **Dual MERGE batch upsert:**
>   - `_bulk_upsert_orders()` — 150 rows/batch → `acc_order` (13 kolumn)
>   - `_bulk_upsert_order_lines()` — 120 rows/batch → `acc_order_line` (12 kolumn, JOIN z acc_order na FK)
> - `_collect_order_lines()` — zbiera item-level dane z TSV, syntetyczny `amazon_order_item_id`: `RPT_{order_id}_{sku}` (+ `_N` suffix dla duplikatów SKU w tym samym order)
> - Schema change: `acc_order_line.amazon_order_item_id` NVARCHAR(50) → **NVARCHAR(150)**
> - Cel: ~1M+ orders w acc_order + odpowiadające order_lines (z obecnych ~59K orders, ~65K lines)
> - ETA: ~3-4h od startu
>
> **Mapowanie kosztów kurierów FBM — READY (czeka na backfill)**
>
> - Cache tabel Netfox: invoices (1.7M), packages (3M), bl_orders (1.17M), extras (9.8M)
> - DHL JJD mapping: 197,605 par (z plików JJ z N:\Kurierzy)
> - `courier_cost_mapper.py` gotowy — pokrycie GLS ~90%, DHL ~52%
> - Po backfill: uruchomić na pełnych danych → `logistics_pln` w acc_order

### Priorytet 1 — Produkcja (blokery)

> ✅ **Wszystkie blokery produkcyjne zostały rozwiązane.**
>
> - SECRET_KEY, SP_API credentials — skonfigurowane
> - MSSQL schema — `ensure_v2_schema()` tworzy automatycznie
> - Scheduler działa (APScheduler in-process), synci biegną od lutego 2026
> - 13 marketplace, **1344/1406** zmapowanych produktów, **901** z ceną zakupu
> - COGS pipeline naprawiony — CM1 = 75.5%, pokrycie 75.7% linii zamówień
> - Dashboard z rozbudowanymi filtrami (presety dat PL, marketplace, FBA/FBM)
> - Frontend serwowany przez Vite dev server (port 3010) z proxy do API (port 8000)
> - Scheduled Task `ACC_FRONTEND` — auto-start Vite po restarcie serwera

### Priorytet 2 — Uzupełnienie funkcji

| Funkcja | Gdzie dodać | Status |
|---------|------------|--------|
| ~~**Kursy NBP z prawdziwego API**~~ | `scheduler.py` daily FX sync | ✅ DONE — 424+ kursów, daily o 6:00 |
| **Auto-COGS z Subiekta FZ** | Nowy connector → `ob_CenaNetto` z FZ docs | 🔶 RECON DONE — tw_Cena pusta, FZ ma ceny |
| **Stany magazynowe z Subiekta** | `tw_Stan` → dashboard widget | 🔶 RECON DONE — MGD 6.7M, GRN 173K |
| **Auto-pricing SP-API submission** | `pricing.py` route + APScheduler job | |
| ~~**Amazon Reports API (ACoS historyczny)**~~ | `connectors/amazon_ads_api/reporting.py` | ✅ DONE — v3 async reporting (SP/SB/SD), scheduler 07:00 |
| **Export do Excel** | Każda strona — przycisk + `openpyxl` backend | |
| **Powiadomienia e-mail** | Alert service + `fastapi-mail` | |
| ~~**Porównanie marketplace**~~ | tabela By Marketplace + dropdown filtr | ✅ DONE |
| ~~**FX Rate fix**~~ | `rate_date < purchase_date` (Art. 31a VAT) | ✅ DONE |
| ~~**Netto Revenue + VAT**~~ | `order_total / (1 + vat_rate)`, kolumna `vat_pln` | ✅ DONE |
| **Edycja ceny w Pricing** | Modal + `POST /pricing/offers/update` | |
| **Tworzenie planu przez UI** | Modal w Planning.tsx + `POST /planning/months` | |
| **Historia alertów** | Filtr `is_resolved=true` w Alerts.tsx | |
| **Dokumenty sprzedaży (FV)** | Integracja FS z Subiekta → moduł księgowości | 🔶 RECON — 1.9M FS docs |

### Priorytet 3 — Techniczny dług

| Dług | Rozwiązanie |
|------|------------|
| ~~Brak testów jednostkowych~~ | ✅ DONE — 80 testów pytest (Family Mapper: master_key, matching, de_builder, API endpoints) |
| Brak testów frontend | `vitest` + `@testing-library/react` |
| Brak CI/CD | GitHub Actions: lint + test + docker build |
| ~~Brak Health Check~~ | ✅ DONE — `GET /health/deep` (Azure SQL + Redis + SP-API, concurrent) |
| Brak rate limiting | `slowapi` na `POST /ai/generate` |
| Logi w produkcji | `structlog` → Grafana Loki lub Sentry |
| Brak backup strategii | `pg_dump` cron + object storage |
| Token refresh nie jest thread-safe | Redis lock w SPAPIAuth |

### Priorytet 4 — Nowe moduły

| Moduł | Szkic funkcji |
|-------|--------------|
| **Content Studio** | Edytor tytułów/bulletów → SP-API Listings |
| **Customer Reviews** | Monitoring ocen, trend, odpowiedzi |
| **Competitor Intelligence** | Buybox historia, price history wykresy |
| **Return Analysis** | Zwroty per SKU, powody, trendline |
| **Supplier Portal** | Widok dla dostawców: moje PO, DOI |
| **Mobile PWA** | Responsywny layout + PWA manifest |
| ~~**Family Mapper**~~ | ✅ DONE — Grupowanie produktów w rodziny DE→marketplace, review queue, fix packages |

---

## 19. Konwencje i dobre praktyki

### Python (backend)

```python
# ✅ Zawsze używaj async w FastAPI
async def get_orders(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(AccOrder).limit(100))
    return result.scalars().all()

# ✅ Używaj structlog zamiast print
log = structlog.get_logger(__name__)
log.info("orders.synced", count=len(orders), marketplace=mp_id)

# ✅ Obsługa błędów w connectorach
try:
    df = get_product_costs(skus)
except pyodbc.Error as exc:
    log.warning("mssql.costs_unavailable", error=str(exc))
    df = pd.DataFrame()  # graceful fallback

# ✅ Schematy Pydantic dla wszystkich responses
# Nigdy nie zwracaj raw SQLAlchemy objects z route — użyj schema
```

### TypeScript (frontend)

```typescript
// ✅ Zawsze typuj API responses — dodaj interface do api.ts
export interface NewThing { id: number; name: string; }
export const getNewThing = () =>
  api.get<NewThing[]>("/newthing").then((r) => r.data);

// ✅ staleTime w useQuery — zapobiegaj zbędnym requestom
const { data } = useQuery({
  queryKey: ["newthing"],
  queryFn: getNewThing,
  staleTime: 60_000,  // 1 min minimum
});

// ✅ Skeleton podczas ładowania — nigdy puste ekrany
{isLoading ? <Skeleton className="h-40 w-full" /> : <Content />}

// ✅ Polski UI — strona jest dla polskiego zespołu
<Button>Anuluj</Button>  // nie "Cancel"
```

### Git

```bash
# Konwencja commitów
feat: dodaj moduł X
fix: napraw kalkul CM1 dla EUR
refactor: wydziel service pricing
chore: update requirements.txt

# Branch naming
feature/ai-recommendations
fix/mssql-connection-timeout
hotfix/jwt-expiry-bug
```

---

## 20. Szybki szablon nowego modułu

Poniżej kompletny przepis na dodanie nowego modułu od zera (przykład: `Returns`).

### Krok 1 — Model SQLAlchemy
```python
# app/models/return_model.py
from sqlalchemy import String, Numeric, DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base

class ReturnEvent(Base):
    __tablename__ = "return_events"
    id: Mapped[int] = mapped_column(primary_key=True)
    amazon_order_id: Mapped[str] = mapped_column(String(50), index=True)
    sku: Mapped[str] = mapped_column(String(100))
    reason: Mapped[str] = mapped_column(String(200), nullable=True)
    amount_pln: Mapped[float] = mapped_column(Numeric(12, 2), default=0)
    # ...
```

### Krok 2 — Dodaj do `models/__init__.py`
```python
from app.models.return_model import ReturnEvent
```

### Krok 3 — Schema Pydantic
```python
# app/schemas/returns.py
class ReturnOut(BaseModel):
    id: int
    amazon_order_id: str
    sku: str
    reason: Optional[str]
    amount_pln: float
    class Config:
        from_attributes = True
```

### Krok 4 — Route
```python
# app/api/v1/returns.py
router = APIRouter(prefix="/returns", tags=["returns"])

@router.get("/", response_model=list[ReturnOut])
async def list_returns(db: AsyncSession = Depends(get_db), _: User = Depends(get_current_user)):
    ...
```

### Krok 5 — Zarejestruj w router.py
```python
from app.api.v1.returns import router as returns_router
api_router.include_router(returns_router)
```

### Krok 6 — Zweryfikuj połączenie MSSQL
```bash
docker-compose run --rm api python scripts/discover_mssql_schema.py
```

### Krok 7 — Frontend API types + call
```typescript
// src/lib/api.ts — dodaj na końcu
export interface ReturnEvent { id: number; sku: string; amount_pln: number; }
export const getReturns = () => api.get<ReturnEvent[]>("/returns/").then(r => r.data);
```

### Krok 8 — Strona
```typescript
// src/pages/Returns.tsx — skopiuj istniejącą stronę i zaadaptuj
```

### Krok 9 — Route w App.tsx + link w Sidebar.tsx
```typescript
// App.tsx
<Route path="returns" element={<ReturnsPage />} />

// Sidebar.tsx
{ to: "/returns", icon: RotateCcw, label: "Zwroty" },
```

---

## 21. FBA Ops — status i fallbacki

## 22. Finance Center — status i completeness

- Kanoniczny dashboard backendowy istnieje juz pod:
  - `GET /api/v1/finance/dashboard`
- Frontend `FinanceDashboard.tsx` opiera sie teraz o jeden endpoint, a nie o zestaw niezaleznych query dla gornej sekcji.
- Sekcje dashboardu maja jawny truth-state:
  - `real_data`
  - `partial`
  - `blocked_by_missing_bank_import`
  - `no_data`
- `blocked_by_missing_bank_import` oznacza praktycznie brak danych w `acc_fin_bank_line`; reconciliation pozostaje wtedy technicznie wystawione, ale biznesowo zablokowane.
- Finance import opiera sie produkcyjnie o `financial_event_group_id`.
- `Finances v2024-06-19` pozostaje first-pass, ale realny feed dla tego konta nadal idzie przez:
  - `v0 / financialEventGroups`
  - `v0 / financialEventsByGroupId`
- Background finance jobs:
  - `finance_sync_transactions`
  - `finance_prepare_settlements`
  - `finance_generate_ledger`
  - `finance_reconcile_payouts`
- Stale jobs sa czyszczone automatycznie, a crash w tle ustawia `failure` zamiast zostawiac rekord w `running`.
- Gap diagnostics:
  - `GET /api/v1/finance/sync/gap-diagnostics`
  - `rows_not_attributed_to_marketplace` oznacza, ze group rows istnieja, ale nic nie wpada do marketplace po imporcie
  - `coverage_gap_after_import` oznacza, ze rows sa, ale coverage biznesowy nadal jest za niski
- Stan `30d` po ostatnim backfillu:
  - `DE`: `imported_rows=35426`, `order_coverage_pct=22.8`
  - `IT`: `4980`
  - `FR`: `3952`
  - `ES`: `840`
  - `NL`: `298`
  - `SE`: `247`
  - `BE`: `148`
  - `PL`: `111`
  - `IE`: `91`
- Historyczny problem `rows_not_attributed_to_marketplace` zostal naprawiony dla aktywnych payout groups poza `DE`.
- Nadal krytyczne pozostaje niskie `order_coverage_pct`, wiec po repairze glowny gap reason to juz `coverage_gap_after_import`.
- Event types z niedostatecznym sygnalem marketplace:
  - `AdjustmentEventList`
  - `ProductAdsPaymentEventList`
  - `ServiceFeeEventList`
- Importer fallbackuje teraz marketplace przez:
  - `MarketplaceName` / `StoreName`
  - `amazon_order_id -> acc_order.marketplace_id`
  - marketplace przypisany na poziomie payout group
- Netfox safety:
  - `GET /api/v1/health/netfox-sessions`
  - druga partia `tmp_*.py` ma juz defensywne cleanupy polaczen, zeby nie zostawiac sesji `ACC-Netfox-RO`

- Finance import opiera się dziś produkcyjnie o `financial_event_group_id`.
- `Finances v2024-06-19` jest nadal first-pass, ale dla tego seller context realny feed działa przez:
  - `v0 / financialEventGroups`
  - `v0 / financialEventsByGroupId`
- Ciężkie operacje finance działają już jako background jobs:
  - `finance_sync_transactions`
  - `finance_prepare_settlements`
  - `finance_generate_ledger`
  - `finance_reconcile_payouts`
- `build_settlement_summaries()` i `generate_ledger_from_amazon()` zostały wykonane na pełnym wsadzie:
  - payout groups: `35`
  - ledger rows inserted: `52034`
- Dostępne endpointy operacyjne:
  - `GET /api/v1/finance/sync/completeness`
  - `GET /api/v1/finance/sync/gap-diagnostics`
  - `GET /api/v1/health/netfox-sessions`
- Systemowe alerty finance:
  - `finance_completeness_critical`
  - `finance_completeness_partial`
- Jeśli completeness nie jest `complete`, dashboard i alerty muszą być traktowane jako częściowe źródło prawdy controllingowej/księgowej.

Źródło prawdy dla modułu:
- `apps/api/app/api/v1/fba_ops.py`
- `apps/api/app/schemas/fba_ops.py`
- `apps/api/app/services/fba_ops/service.py`
- `docs/FBA_OPS.md`

### Zakres modułu

Frontend routes:
- `/fba/overview`
- `/fba/inventory`
- `/fba/replenishment`
- `/fba/inbound`
- `/fba/aged-stranded`
- `/fba/bundles`
- `/fba/kpi-scorecard`

---

## 28. Family Restructure — pipeline execute

> **Dodano:** v2.11 (2026-03-06)  
> **Plik główny:** `apps/api/app/services/family_mapper/restructure.py` (1313 linii)  
> **Endpoint:** `POST /api/v1/families/{id}/execute-restructure?marketplace_id=X&dry_run=true`  
> **Dokumentacja:** `docs/FAMILY_RESTRUCTURE_2026-03-06.md`

### Cel

Replikacja kanonicznej struktury rodziny produktów z DE na dowolny marketplace EU.
Pipeline porównuje stan DE (parent, children, variation_theme) z marketplace docelowym
i wykonuje pełen cykl: walidacja → audyt → wzbogacenie z PIM → tłumaczenie → create/reassign.

### Pipeline (7 kroków)

1. **PREFLIGHT** — ładuje DE family + children z SP-API Listings
2. **VALIDATE_THEME** — sprawdza `variation_theme` na target MP via SP-API Product Type Definition
3. **AUDIT_CHILD_ATTRS** — sprawdza color/size na WSZYSTKICH dzieciach (concurrent, semaphore 5, batch 20)
4. **ENRICH_FROM_PIM** — lookup brakujących atrybutów w Ergonode PIM → GPT tłumaczenie → PATCH
5. **CHECK_PARENT / TRANSLATE_PARENT** — sprawdź istniejącego parenta lub przetłumacz atrybuty GPT-5.2
6. **CREATE_PARENT** — PUT listings item na target MP
7. **REASSIGN_CHILD** (×N) — PATCH `child_parent_sku_relationship` per child

### Kluczowe funkcje

| Funkcja | Plik | Opis |
|---------|------|------|
| `execute_restructure()` | `restructure.py` | Główna orkiestracja pipeline |
| `_translate_parent_attributes()` | `restructure.py` | GPT-5.2 tłumaczenie DE→target lang |
| `_validate_variation_theme()` | `restructure.py` | SP-API PTD walidacja theme |
| `_audit_child_attributes()` | `restructure.py` | Pełny audyt ALL children |
| `_enrich_children_from_pim()` | `restructure.py` | PIM lookup + GPT translate + PATCH |
| `get_product_type_definition()` | `listings.py` | SP-API PTD endpoint |
| `fetch_ergonode_variant_lookup()` | `ergonode.py` | PIM variant lookup (color/size/qty) |
| `_build_option_map()` | `ergonode.py` | UUID→text option resolution |

### Ergonode PIM — model atrybutów wariantowych

Ergonode przechowuje wartości wariantowe jako UUID (SELECT/MULTI_SELECT).
Rozwiązanie: `_build_option_map()` pobiera `/attributes/{id}/options` → `{uuid: code_text}`.

| Atrybut PIM | Typ | Przykłady |
|---|---|---|
| `wariant_kolor_tekst` | MULTI_SELECT | grafit, butelkowa zieleń, czerwony, złoty |
| `wariant_text_rozmiar` | SELECT | 10, 12, 14, m, l, s, 16 cm (296 opcji) |
| `wariant_text_ilosc` | SELECT | 10, 20, 50, 100 (34 opcje) |

### Frontend

- `FamilyDetail.tsx` → `StepRow` renderuje 10 typów akcji (VALIDATE_THEME, AUDIT_CHILD_ATTRS, ENRICH_FROM_PIM, TRANSLATE_PARENT, itp.)
- `ExecutionLog` wyświetla summary badges: variation_theme (info), child_attr_audit (green/orange), pim_enrichment (blue)
- `api.ts` → `ExecuteRestructureResult` rozszerzony o `variation_theme`, `child_attr_audit`, `pim_enrichment`

### Wynik testu produkcyjnego (dry-run)

```
Family 1367 (KADAX Pokrywka) / FR / dry_run=true
→ 109 steps, 0 errors
→ VALIDATE_THEME: supported
→ AUDIT: 100/100 checked, color 6 missing, size 19 missing
→ ENRICH_FROM_PIM: 18/19 found in Ergonode, would patch 18
→ TRANSLATE: DE→FR (would translate)
→ CREATE_PARENT + REASSIGN ×100: dry_run
```

### Zabezpieczenia

- `dry_run=True` domyślnie — execute wymaga jawnego `dry_run=false`
- Rate limiting: `asyncio.sleep(0.3)` między SP-API calls
- Semaphore: max 5 audit, max 10 PIM concurrent requests
- JWT: wymaga roli `director` lub `admin`

Backend:
- overview, inventory, inbound, aged, stranded, scorecard
- CRUD: shipment plans, cases, launches, initiatives
- case timeline + comments
- FBA job trigger + diagnostics endpoint

### Integracje Amazon

Co działa:
- Inbound API
- FBA Inventory API summaries
- planning reports dla części marketplace'ów

Co jest niestabilne:
- `GET_STRANDED_INVENTORY_UI_DATA` potrafi wracać `CANCELLED`
- `GET_FBA_INVENTORY_PLANNING_DATA` potrafi wracać trwałe `FATAL` dla wybranych marketplace'ów

### Produkcyjna strategia fallbacków

`sync_fba_inventory` nie zakłada już, że planning report zawsze działa.

Obecny przepływ:
1. reuse recent `DONE` planning report, jeśli istnieje
2. jeśli marketplace jest w cooldown po trwałym `FATAL`, nie twórz nowego planning report request
3. użyj FBA Inventory API jako fallback source
4. dla stranded:
   - spróbuj canonical stranded report
   - jeśli `CANCELLED`, spróbuj ostatniego `DONE`
   - jeśli dalej brak danych, użyj proxy z `unfulfillable_quantity`

To jest ważne dla `IE` i `SE`, gdzie Inventory API działa, ale planning report potrafi kończyć się `FATAL`.

### Diagnostyka

Tabela:
- `dbo.acc_fba_report_diagnostic`

Endpoint:
- `GET /api/v1/fba/diagnostics/report-status`

UI:
- panel diagnostyczny na `FBA Ops Overview`

Diagnoza ma pokazywać:
- planning status / fetch mode
- inventory API fallback status
- stranded status / fallback source
- timestamp i payload diagnostyczny

### Alerty

Alerty FBA są wzbogacone o:
- `detail_json`
- `context_json`
- nazwę produktu z ACC / import / order line / Netfox fallback
- drill-through do inbound lub inventory

Przed zmianami w alert engine najpierw sprawdź `docs/FBA_OPS.md`, bo tam opisany jest aktualny model danych i obecne kompromisy źródłowe.

---

## 23. Amazon Listing Registry — staging i usage

Kanoniczny opis modułu jest w:
- `docs/AMAZON_LISTING_REGISTRY.md`

### Po co to istnieje

Google Sheet z listingami Amazon zawiera krytyczne pola identyfikacyjne produktu:
- `Merchant SKU`
- `Nr art.` / `internal_sku`
- `EAN`
- `ASIN`
- `Parent ASIN`
- `Parent/Child`
- marka, nazwa, czesc kategorii

W runtime nie chcemy opierac krytycznych widokow na live odczycie Google Sheet.
Dlatego arkusz jest synchronizowany do MSSQL jako registry tozsamosci produktu.

### Tabele

- `dbo.acc_amazon_listing_registry`
- `dbo.acc_amazon_listing_registry_sync_state`

### Zasady

- Registry jest zrodlem identyfikacji produktu i relacji listingowych.
- Registry nie jest zrodlem kosztow zakupu.
- Registry nie jest zrodlem fee / finansow.
- Registry moze pomoc znalezc `internal_sku`, ale sama cena dalej pochodzi z:
  - `acc_purchase_price`
  - `00. Oficjalne ceny zakupu dla sprzedazy.xlsx`
  - holding / ERP
  - wpis manualny

### Gdzie jest uzywane

- `Missing COGS` / `Data Quality`
- `AI Product Matcher` jako exact hint i candidate narrowing
- `order_pipeline.py`
  - backfill `acc_product`
  - enrich istniejacych `acc_product`
  - linkowanie brakujacych `acc_order_line.product_id`
- `FBA Ops`
  - inventory title/context
  - inbound line context
  - aged/stranded fallback title
- `Finance Center`
  - ledger enrichment i dodatkowe tagi identyfikacyjne

### Produkcyjna interpretacja

Traktuj to jako `Amazon listing identity registry`, a nie jako kolejny raport operacyjny.
To ma minimalizowac:
- brakujace `product_id`
- brakujace `internal_sku`
- zle cross-mapy `SKU/EAN/ASIN`
- zaleznosc runtime od zewnetrznego arkusza

---

## 24. Manage All Inventory — status i ograniczenia

Kanoniczny opis modułu jest w:
- `docs/MANAGE_ALL_INVENTORY.md`

### Co wdrozone

Frontend:
- `/inventory/overview`
- `/inventory/all`
- `/inventory/families`
- `/inventory/drafts`
- `/inventory/jobs`
- `/inventory/settings`

Backend:
- `apps/api/app/schemas/manage_inventory.py`
- `apps/api/app/services/manage_inventory.py`
- `apps/api/app/api/v1/manage_inventory.py`

Schema bootstrap:
- `dbo.acc_inv_traffic_sku_daily`
- `dbo.acc_inv_traffic_asin_daily`
- `dbo.acc_inv_traffic_rollup`
- `dbo.acc_inv_item_cache`
- `dbo.acc_inv_change_draft`
- `dbo.acc_inv_change_event`
- `dbo.acc_inv_settings`
- `dbo.acc_inv_category_cvr_baseline`

### Obecna warstwa prawdy

- inventory base: `acc_fba_inventory_snapshot`
- listing state: `marketplace_listing_child`
- family context: `global_family_*`
- identity enrichment: `acc_amazon_listing_registry`
- velocity fallback: `acc_order` + `acc_order_line`
- traffic rollups: `acc_inv_traffic_rollup` jesli istnieja

### Czego modul jeszcze nie udaje

To jest produkcyjny shell decyzyjny, ale nie finalna wersja calej specyfikacji.

Jawne ograniczenia:
- `inventory_sync_sales_traffic` jest juz realnym syncem Reports API, ale runtime coverage pozostaje `partial`, dopoki `acc_inv_traffic_rollup` i cache nie zostana pelnie odbudowane na zywej instancji
- jesli traffic rollups sa puste lub niekompletne, UI i API oznaczaja coverage jako `partial`
- draft/apply/rollback pcha juz zmiany przez `JSON_LISTINGS_FEED`, ale auto-build obejmuje tylko `reparent` i `update_theme`; `create_parent` i `detach` wymagaja jawnego payloadu
- `inventory_run_alerts` liczy kandydatow, ale nie zapisuje ich jeszcze do wspolnego silnika alertow
- live backend jest juz na nowym kodzie, a smoke endpointow inventory dla `DE` przeszedl poprawnie
- dla all-market runtime nadal obowiazuje zasada: partial cache scope => fallback do live build
- `Dashboard` nie jest jeszcze naprawde sterowany przez wspolny `Global Filters`; top drivers / leaks reaguja na lokalne filtry dashboardu (`date`, `marketplace`, `fulfillment`, `brand`, `category`)
- `Pricing / Buy Box` respektuje juz `?sku=...` w URL, ale pusty ekran dalej oznacza po prostu brak wsadu w `acc_offer` do czasu `sync_pricing`

### Dlaczego to jest poprawne dla ACC

W ACC wazniejsze od pozornej pelni jest uczciwe komunikowanie kompletności danych.
Ten modul:
- nie zmysla Sessions/CVR
- nie wykonuje ryzykownego live apply bez walidacji i bez jawnego payloadu dla bardziej niebezpiecznych mutacji
- wykorzystuje juz istniejace tabele i job runner ACC zamiast osobnego subsystemu
- nie serwuje partial cache answers dla `/inventory/*`, jesli cache nie pokrywa calego zakresu

---

## 25. Return Tracker — cykl życia zwrotów

### Zakres

Moduł śledzi pełny cykl życia zwrotów Amazon — od refundu finansowego, przez fizyczny zwrot FBA, do rozliczenia COGS w P&L.

### Źródła danych

| Źródło | Co daje | Tabela docelowa |
|--------|---------|-----------------|
| `acc_order.is_refund=1` | Zdarzenie refundu finansowego | `acc_return_item` (seed) |
| SP-API Reports: `GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA` | Fizyczny zwrot FBA (disposition, reason) | `acc_fba_customer_return` |
| Manual override (warehouse team) | Korekta statusu po weryfikacji fizycznej | `acc_return_item` (update) |

### Tabele

- `acc_return_item` — rekord cyklu życia zwrotu (financial_status: pending → sellable_return / damaged_return / lost_in_transit / reimbursed)
- `acc_fba_customer_return` — surowe dane z FBA Customer Returns report
- `acc_return_daily_summary` — dzienne agregaty per marketplace (refundy, sellable, damaged, COGS impact)
- `acc_return_sync_state` — watermark per marketplace dla incremental sync

### Przepływ reconciliation

```
1. Seed: acc_order (is_refund=1) → acc_return_item (status='pending')
   - Proporcjonalna alokacja refund_amount_pln na order lines
2. FBA Report: download → parse TSV → MERGE into acc_fba_customer_return
3. Reconcile:
   - Pending items + FBA report match:
     - SELLABLE → cogs_recovered_pln (COGS odzyskane, re-entry do inventory)
     - DAMAGED/DEFECTIVE/CARRIER_DAMAGED/... → write_off_pln (strata)
   - Pending > 45 days bez matcha → lost_in_transit
4. Rebuild: acc_return_daily_summary (dzienne agregaty dla dashboardu)
```

### Integracja z P&L

```
Revenue:  zawsze pomniejszone o refund_amount_pln
COGS:     odzyskane tylko jeśli item zwrócony jako SELLABLE (re-entry do inventory)
Strata:   COGS write-off jeśli DAMAGED/DEFECTIVE/LOST
```

### API Endpoints (9)

| Endpoint | Metoda | Opis |
|----------|--------|------|
| `/returns/dashboard` | GET | KPIs: refund rate, sellable rate, COGS recovered vs written off, marketplace breakdown, top returned SKUs |
| `/returns/items` | GET | Paginowana lista zwrotów z filtrami (marketplace, status, sku, sorting) |
| `/returns/items/{id}/status` | PUT | Manual override statusu finansowego (warehouse team, nota audytowa) |
| `/returns/seed` | POST | Seed acc_return_item z refundów (idempotentne) |
| `/returns/reconcile` | POST | Match FBA returns z financial items, klasyfikacja disposition |
| `/returns/rebuild-summary` | POST | Odbudowa acc_return_daily_summary |
| `/returns/sync` | POST | Pełny pipeline: download → parse → upsert → seed → reconcile → summary |
| `/returns/backfill` | POST | Historyczny backfill FBA returns (chunked by days) |

### Pliki

| Plik | Opis |
|------|------|
| `apps/api/app/services/return_tracker.py` | Serwis (~1151 linii) — pełna logika biznesowa |
| `apps/api/app/api/v1/returns.py` | Router (9 endpointów) |

### Ważne

- Currency-safe: wyłącznie kolumny `_pln` (`refund_amount_pln`, `cogs_pln`, `cogs_recovered_pln`, `write_off_pln`)
- Disposition map: SELLABLE → sellable_return; DAMAGED/DEFECTIVE/CUSTOMER_DAMAGED/CARRIER_DAMAGED/EXPIRED/DISTRIBUTOR_DAMAGED/WAREHOUSE_DAMAGED → damaged_return
- Frontend: backend API gotowe, strony React jeszcze nie zbudowane

---

## 26. FBA Fee Audit — detekcja anomalii opłat

### Zakres

Analiza opłat FBA pod kątem:
- Anomalii cenowych (nagłe skoki fee per SKU — np. reklasyfikacja wymiarów)
- Nadpłat (overcharge = fee > 1.5× mediana dla danego SKU)
- Porównania z oficjalnym cennikiem Amazon

### API Endpoints (4)

| Endpoint | Metoda | Opis |
|----------|--------|------|
| `/fba/fee-audit/anomalies` | GET | Week-over-week fee jumps per SKU (currency-partitioned LAG) |
| `/fba/fee-audit/timeline/{sku}` | GET | Pełna historia opłat dla SKU (charges, daily agg, anomaly periods) |
| `/fba/fee-audit/overcharges` | GET | Estimated overcharges per SKU (median-based, EUR-normalized) |
| `/fba/fee-audit/reference` | GET | Porównanie z opublikowanymi stawkami z acc_fba_fee_reference |

### Kluczowe decyzje techniczne

1. **LAG() z partycją walutową:** `PARTITION BY sku, currency` zamiast samego `PARTITION BY sku` — eliminuje false positive'y z porównywania np. PLN→SEK (ratio 25x)
2. **EUR normalizacja:** `_load_fx_rates()` pobiera najnowsze kursy z `acc_exchange_rate`, `_to_eur()` przelicza dowolną walutę na EUR — overcharge summary w jednolitej walucie
3. **overcharge_by_currency:** response zawiera rozbicie nadpłat per waluta + łączną sumę w EUR
4. **Median-based threshold:** fee > 1.5× mediana dla SKU = flagowane jako overcharge

### Pliki

| Plik | Opis |
|------|------|
| `apps/api/app/services/fba_ops/fba_fee_audit.py` | Serwis (~752 linii) |
| `apps/api/app/api/v1/fba_ops.py` | Router (4 endpointy zintegrowane w FBA Ops router) |

---

## 27. Currency Mixing Audit — wyniki i fixy

Pełny raport: `docs/CURRENCY_MIXING_AUDIT.md`

### Kontekst

ACC operuje na 9 marketplace'ach EU z walutami: EUR (DE/FR/IT/ES/NL/BE/IE), SEK (SE), PLN (PL), GBP (UK). `item_price` w `acc_order_line` jest w walucie natywnej zamówienia, a kolumny `_pln` (np. `cogs_pln`, `fba_fee_pln`) są już w PLN.

### Znalezione i naprawione bugi

| # | Plik | Funkcja | Severity | Opis | Status |
|---|------|---------|----------|------|--------|
| 1 | `mssql_store.py` | `get_profit_by_sku()` | CRITICAL | `SUM(item_price)` labeled `revenue_pln` — mieszanie walut w SUM | ✅ FIXED |
| 2 | `mssql_store.py` | `sync_profit_snapshot()` | CRITICAL | `item_price` (native) jako `revenue_net` obok `cogs` (PLN) | ✅ FIXED |
| 3 | `finance_center/service.py` | `build_settlement_summaries()` | MEDIUM | `COALESCE(exchange_rate, 1)` traktuje native jako PLN przy braku FX | ✅ FIXED |
| 4 | `profit_engine.py` | `get_data_quality()` missing_cogs | LOW | Cross-currency SUM w rankingu (label `revenue_orig` — uczciwy) | Accepted |
| 5 | `manage_inventory.py` | Traffic rollup `revenue` | LOW | Brak kolumny `currency` (bezpieczne per-marketplace) | Accepted |

### Wzorzec naprawy

Wszystkie fixy używają tego samego pattern co `profit_engine.py`:
```sql
OUTER APPLY (
    SELECT TOP 1 rate_to_pln
    FROM dbo.acc_exchange_rate er WITH (NOLOCK)
    WHERE er.currency = o.currency
      AND er.rate_date <= CAST(o.purchase_date AS DATE)
    ORDER BY er.rate_date DESC
) fx
```
z fallbackiem:
```sql
ISNULL(fx.rate_to_pln,
    CASE o.currency
        WHEN 'EUR' THEN 4.25 WHEN 'GBP' THEN 5.10
        WHEN 'SEK' THEN 0.39 WHEN 'PLN' THEN 1.0 ELSE 4.25
    END)
```

### Poprawnie obsługujące waluty (11+ miejsc)

`profit_engine.py` (table/drilldown/loss/what-if), `order_pipeline.py` (step_bridge_fees), `mssql_store.py` (recalc_profit_orders), `ads_sync.py` (daily+product metrics), `finance_center/service.py` (generate_ledger), `return_tracker.py` (wyłącznie `_pln`), frontend (wyłącznie `formatPLN()` z `_pln` fields).

---

## Kontakty i zasoby

| Zasób | Link/Info |
|-------|-----------|
| Amazon SP-API docs | https://developer-docs.amazon.com/sp-api/ |
| SP-API Rate Limits | https://developer-docs.amazon.com/sp-api/docs/usage-plans-and-rate-limits |
| Amazon Seller Central | https://sellercentral.amazon.de |
| NetfoxAnalityka serwer | `192.168.230.120:11901` — tylko sieć KADAX |
| OpenAI API | https://platform.openai.com |
| NBP kursy walut API | https://api.nbp.pl |
| FastAPI docs | https://fastapi.tiangolo.com |
| TanStack Query | https://tanstack.com/query/v5 |

---

*Ostatnia aktualizacja: 5 Marca 2026 — Amazon Command Center v2.8*
*Zmiany v2.8: Return Tracker — pełny moduł cyklu życia zwrotów (return_tracker.py ~1151 linii, 9 endpointów /returns/*, 4 tabele: acc_return_item + acc_fba_customer_return + acc_return_daily_summary + acc_return_sync_state). Seed z refundów → FBA report sync → reconciliation → COGS recovery/write-off. Manual override z notą audytową. Currency-safe (wyłącznie _pln). Frontend: backend API gotowe, React pages pending.*
*Zmiany v2.7: FBA Fee Audit (fba_fee_audit.py ~752 linii, 4 endpointy /fba/fee-audit/*). Currency Mixing Audit — full codebase scan: 2 CRITICAL + 1 MEDIUM bugów naprawionych (get_profit_by_sku + sync_profit_snapshot: OUTER APPLY acc_exchange_rate; build_settlement_summaries: usunięty fallback COALESCE(exchange_rate,1)). FBA fee audit LAG() PARTITION BY sku,currency. EUR normalizacja overcharge. Finance Dashboard all-marketplace sync (usunięty hardcoded DE-only). docs/CURRENCY_MIXING_AUDIT.md.*
*Zmiany v2.6: Finance Dashboard kanoniczny backend endpoint, truth states (real_data/partial/blocked/no_data), bank import blocker, Pricing/Buy Box ?sku=..., Dashboard local filters.*
*Zmiany v2.5: Manage All Inventory hardening — `acc_inv_item_cache`, Azure SQL runtime optimization, real Sales & Traffic sync z `GET_SALES_AND_TRAFFIC_REPORT`, safe `JSON_LISTINGS_FEED` apply/rollback dla `reparent`/`update_theme`, live smoke `/inventory/overview` i `/inventory/all` dla `DE` po restarcie backendu.*
*Zmiany v2.4: Manage All Inventory shell `/inventory/*`, backend service/router/schema, job types `inventory_*`, honest traffic coverage partials, plus zachowane zmiany v2.3 wokol Amazon Listing Registry.*
*Zmiany v2.3: Amazon Listing Registry (Google Sheet -> MSSQL), job `sync_amazon_listing_registry`, order pipeline enrichment, FBA Ops identity enrichment, Finance Center ledger enrichment, Missing COGS fallback `SKU -> ISK -> Oficjalny XLSX` przez registry.*
*Zmiany v2.1: Price source priority (8-level: manual>import_xlsx>xlsx_oficjalne>holding×1.04>erp_holding×1.04>import_csv>cogs_xlsx>acc_product). RPT_ duplicate fix (19,612 lines removed, MERGE NOT EXISTS guard). Holding ×1.04 multiplier (Opcja A — at stamp time). import_cogs_xlsx respects priority (wrzutki od zakupu: priorytet 2, nadpisuje wszystko oprócz manual). Netfox ERP analysis (no exact xlsx source).*
*Zmiany v2.0: COGS coverage ~99% (2-pass stamping pipeline, XLSX import 3035 SKU, ASIN cross-lookup 76+168 products). Data Quality UI: inline price editing + map-and-price. AI Product Matcher: GPT-4o matching ~122 unmapped bundles, BOM decomposition, human-in-the-loop (acc_product_match_suggestion), 4 nowe endpointy. profit_engine.py ~1560 linii.*
*Zmiany v1.9: FBA Ops module (overview, inventory, inbound, aged/stranded, bundles, scorecard), API router + schemas + service, diagnostics endpoint, report diagnostics table, planning report -> Inventory API fallback for persistent FATAL marketplaces, IE/SE cooldown, richer FBA alerts with structured context and drill-through, dedicated docs/FBA_OPS.md.*
*Zmiany v1.8: Amazon Ads API integration (connector: client+profiles+campaigns+reporting, sync service, scheduler 07:00, DDL migration, 10 profili, 5083 kampanii SP/SB/SD). Security: usunięto hardcoded creds z get_ads_refresh_token.py, .gitignore += ads_tokens/tmp_*.py. Backfill update: 101K+ orders, 116K+ lines.*
*Zmiany v1.7: Order line backfill z TSV (dual MERGE: orders + lines), deep health endpoint (/health/deep — Azure SQL + Redis + SP-API), schema change NVARCHAR(150), syntetyczny klucz RPT_{order_id}_{sku}, backfill v2 restart.*
*Zmiany v1.6: Shipping cost mapping (cache Netfox 15.7M rows, DHL JJD map, GLS/DHL coverage), Reports API bulk backfill (MERGE batch upsert 13x faster), backfill_via_reports.py. Priorytet 0 w roadmapie.*
*Zmiany v1.5: Family Mapper — kompletny moduł (6 serwisów backend, 15 endpointów API, 4 strony React, 80 testów pytest). Nightly job wyłączony (manual-only). Sekcje: repo structure, API map, scheduler, frontend pages, roadmap, testy.*
*Zmiany v1.4: Sekcja 13.1 NetfoxDistribution/Subiekt GT (READ-ONLY!), FX rate fix (Art. 31a), netto revenue + vat_pln, daily FX sync, Top 15 z internal_sku + select SKU/ASIN, job truncation bug fix (NVARCHAR(MAX)), rekon Subiekt COGS/stock/docs.*
*Zmiany v1.3: COGS pipeline fix (is_parent bug), mapping 1344/1406, ceny 901, CM1=75.5%, Dashboard z filtrami, endpoint /kpi/marketplaces, Vite deployment.*
*Autor: GitHub Copilot (Claude Opus 4.6) + Dyrektor E-commerce KADAX*


