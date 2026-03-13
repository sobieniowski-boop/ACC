# Audyt Architektury Backendu ACC
### Data: 2026-03-11
### Zakres: Pełny audyt backendu względem docelowej architektury Amazon Command Center
### Audytor: Backend Architect Agent

---

# 1. Podsumowanie Wykonawcze

1. **ACC to prawdziwy system, nie prototyp.** ~100 tys. linii Pythona, 425+ endpointów API, 50+ tabel w bazie, 9 marketplace'ów EU, podwójne pokrycie Amazon API (SP-API + Ads API). To produkcyjne oprogramowanie obsługujące operację e-commerce.

2. **Warstwa konektorów SP-API jest produkcyjnej jakości.** Wszystkie 29 endpointów SP-API i 12 typów raportów jest pokryte z prawidłową autoryzacją, exponential backoff, telemetrią i parametryzacją marketplace. To najsilniejsza warstwa codebase.

3. **Event Backbone (SQS) istnieje i działa.** Deterministyczny dedup, circuit breaker, rejestr handlerów, replay, adaptacyjne pollowanie. Jednak jest niedostatecznie wykorzystywany — 90%+ przepływu danych nadal idzie przez cronowy batch sync zamiast event-driven.

4. **Silnik finansowy jest kompleksowy, ale kruchy.** Fee taxonomy (70+ typów opłat), 3-warstwowy model zysku (CM1/CM2/NP), FX service z circuit breakerem na stałość kursów, generowanie księgi podwójnego zapisu — to wszystko istnieje. Jednak kalkulacja zysku ma 3 konkurencyjne ścieżki kodu (sync_service.calc_profit, profit_service.recalculate_profit_batch, order_pipeline.step_calc_profit), tworząc ryzyko rozbieżności.

5. **Jest co najmniej 6 God modułów** przekraczających 2500 linii: `profit_engine.py` (6 632), `content_ops.py` (4 906), `mssql_store.py` (4 297), `fba_ops/service.py` (3 921), `order_pipeline.py` (3 023), `family_mapper/restructure.py` (2 662). Wymagają dekompozycji.

6. **Trzy równoległe ścieżki wykonania** istnieją dla tych samych zadań: APScheduler (in-process), Celery Beat i CLI sync_runner. Współdzielą logikę, ale używają różnego okablowania, tworząc obciążenie utrzymaniowe i ryzyko rozbieżności.

7. **Zarządzanie schematem jest chaotyczne.** Istnieje 7 migracji Alembic, ale 14+ modułów serwisowych tworzy tabele inline przez `ensure_*_schema()` DDL zagnieżdżone w logice biznesowej. Dwa światy współistnieją bez strategii migracji.

8. **Brak kanonicznego modelu produktu.** `acc_product` to płaska tabela master (klucz ASIN). `global_family` to osobny system mapowania rodzin DE-kanonicznych. `acc_amazon_listing_registry` to trzeci magazyn produktów synchronizowany z Google Sheets. `acc_offer` to czwarty widok tego samego produktu. Żaden z nich nie tworzy spójnego modelu kanonicznego z transformacjami source→canonical→target.

9. **Logika marketplace jest w 95% sparametryzowana** przez `MARKETPLACE_REGISTRY` — mocny design. 5% zahardkodowanej logiki marketplace (wykrywanie wycieku na PL, zamiana prefiksów MAG_/FBA_, założenie DE-kanonicznej rodziny) jest udokumentowane i celowe.

10. **Root projektu jest zanieczyszczony.** 95 plików `tmp_*`, 30 plików diagnostycznych `_*`, 18 plików logów i 11 skryptów `backfill_*` leży obok kodu produkcyjnego. Brak dyscypliny `.gitignore`.

11. **Pokrycie testami jest minimalne.** 45 plików testowych na ~100 tys. linii kodu. Brak widocznego pipeline'u CI. Testy istnieją, ale nie stanowią siatki bezpieczeństwa, którą powinny być.

12. **Docelowa architektura (30 modułów) jest ~60% pokryta** istniejącym kodem, ale prawie nic nie ma poprawnego kształtu. Większość modułów istnieje jako splątane fragmenty wewnątrz plików God-class, a nie czysto wydzielone komponenty.

13. **Warstwa decyzyjno-inteligentna jest zaskakująco zaawansowana.** Detekcja strategii (20+ typów okazji), pętla zwrotna decision intelligence, silnik sezonowości i analityka executive — to wszystko istnieje. Jednak trzy moduły (strategy_service, executive_service, decision_intelligence_service) operują na tej samej tabeli `growth_opportunity` bez koordynacji.

14. **Podsystem kurierów/logistyki to de facto osobny produkt** osadzony wewnątrz ACC — 10 tabel wysyłkowych, integracje DHL/GLS, estymacja kosztów kurierskich z ML, import rozliczeń, linkowanie do uniwersum zamówień. Dobrze zbudowany, ale stanowi ~15% złożoności codebase.

15. **READ UNCOMMITTED globalnie** — każde zapytanie uruchamia się bez blokad. Akceptowalne dla obciążenia analitycznego read-heavy na Azure SQL, ale niebezpieczne dla gwarancji spójności finansowej. Zapisy finansowe powinny używać co najmniej READ COMMITTED.

---

# 2. Obecny Inwentarz Modułów ACC

| Obecny moduł / Obszar | Funkcja w praktyce | Kluczowe pliki / Foldery | Używane endpointy Amazon | Jakość | Uwagi |
|---|---|---|---|---|---|
| **SP-API Client** | Bazowy klient HTTP z auth, backoff, telemetrią | `connectors/amazon_sp_api/client.py` | LWA token endpoint | ★★★★★ | Najlepsza klasa |
| **Orders Connector** | Pobieranie zamówień + pozycji | `connectors/amazon_sp_api/orders.py` | getOrders, getOrderItems | ★★★★★ | Auto-paginacja |
| **Listings Connector** | CRUD listingów + definicje typów produktów | `connectors/amazon_sp_api/listings.py` | getListingsItem, putListingsItem, patchListingsItem, deleteListingsItem, getDefinitionsProductType | ★★★★★ | Pełny CRUD |
| **Catalog Connector** | Pobieranie elementów katalogu + wyszukiwanie | `connectors/amazon_sp_api/catalog.py` | getCatalogItem, searchCatalogItems | ★★★★★ | Batch + parser |
| **Pricing Connector** | Ceny konkurencyjne, oferty, estymacja prowizji | `connectors/amazon_sp_api/pricing_api.py` | getCompetitivePricing, getItemOffers, getFeesEstimate | ★★★★★ | Kompletny |
| **Inventory Connector** | Podsumowania magazynowe FBA | `connectors/amazon_sp_api/inventory.py` | getInventorySummaries | ★★★★☆ | Minimalny, ale poprawny |
| **Finances Connector** | Transakcje + legacy events | `connectors/amazon_sp_api/finances.py` | v2024-06-19 transactions + v0 events | ★★★★★ | Podwójna wersja |
| **Reports Connector** | Cykl życia raportu (create→poll→download) | `connectors/amazon_sp_api/reports.py` | createReport, getReport, getReportDocument + 12 typów raportów | ★★★★★ | Sprytne ponowne użycie |
| **Feeds Connector** | Pipeline submitowania feedów | `connectors/amazon_sp_api/feeds.py` | createFeedDocument, createFeed, getFeed | ★★★★★ | Potok 3-krokowy |
| **Notifications Connector** | Dystrybucje SQS/EventBridge + subskrypcje | `connectors/amazon_sp_api/notifications.py` | 9 typów notyfikacji, pełny CRUD | ★★★★★ | Przełączanie auth |
| **Brand Analytics** | Raporty wyszukiwań | `connectors/amazon_sp_api/brand_analytics.py` | GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT | ★★★★☆ | Podwójny parser formatów |
| **Inbound Connector** | Śledzenie przesyłek FBA | `connectors/amazon_sp_api/inbound.py` | getShipments, getShipmentItems | ★★★★☆ | Minimalny |
| **Ads API Client** | Bazowy HTTP dla Amazon Ads | `connectors/amazon_ads_api/client.py` | LWA token (Ads) | ★★★★☆ | Brak telemetrii |
| **Ads Profiles** | Mapowanie profil ↔ marketplace | `connectors/amazon_ads_api/profiles.py` | GET /v2/profiles | ★★★★★ | Czyste mapowanie |
| **Ads Campaigns** | Listowanie kampanii SP/SB/SD | `connectors/amazon_ads_api/campaigns.py` | Endpointy listy kampanii SP/SB/SD | ★★★★★ | Design z fallbackiem |
| **Ads Reporting** | Raporty dzienne kampanii + produktów | `connectors/amazon_ads_api/reporting.py` | POST /reporting/reports + 5 typów raportów | ★★★★★ | Implementacja DRY |
| **Order Pipeline** | 6-krokowy sync zamówień + wzbogacanie + zysk | `services/order_pipeline.py` (3023 loc) | Orders API, Finances (pośrednio) | ★★★★☆ | Kanoniczna ścieżka zamówień |
| **Sync Service** | Legacy orkiestrator synca SP-API | `services/sync_service.py` (2567 loc) | Orders, Inventory, Pricing, Catalog, FX | ★★★☆☆ | Częściowo zastąpiony |
| **Profit Engine** | 3-warstwowy model zysku (CM1/CM2/NP) + zapytania + eksport | `services/profit_engine.py` (6632 loc) | Brak (czyta wcześniej zsynchronizowane dane) | ★★★★☆ | God moduł |
| **Profitability Service** | Rollup'y zysku SKU/marketplace + alerty + dashboardy | `services/profitability_service.py` (1637 loc) | Brak | ★★★★☆ | Nakłada się z profit_engine |
| **Finance Center** | Księga, rozliczenia, rekoncyliacja | `services/finance_center/` (2524 loc) | Brak | ★★★★☆ | Dobrze ustrukturyzowany |
| **Event Backbone** | Ingestia + dyspozycja zdarzeń na bazie SQS | `services/event_backbone.py` (1255 loc) | SQS (AWS) | ★★★★★ | Najlepszy moduł |
| **Pricing State** | Historia snapshotów cen + śledzenie BuyBox | `services/pricing_state.py` | Competitive Pricing API | ★★★★☆ | Czysta integracja eventowa |
| **Pricing Rules** | Rekomendacje cenowe oparte na guardrailach | `services/pricing_rules.py` | Brak | ★★★★☆ | Human-in-loop |
| **Listing State** | Kanoniczne śledzenie statusu listingów | `services/listing_state.py` | Listings Items API | ★★★★★ | Event-sourced |
| **Content Ops** | Cykl życia treści + publikacja + compliance | `services/content_ops.py` (4906 loc) | Catalog API, Listings API | ★★★☆☆ | God moduł |
| **FBA Ops** | Magazyn FBA + uzupełnianie + inbound + audyt prowizji | `services/fba_ops/` (3921 loc) | Reports API, Inventory API | ★★★☆☆ | Monolityczny |
| **Family Mapper** | Mapowanie rodzin produktów między marketplace'ami | `services/family_mapper/` (8 plików) | Catalog API | ★★★★☆ | Dobrze ustrukturyzowany pakiet |
| **Ads Sync** | Synchronizacja danych reklamowych | `services/ads_sync.py` | Wszystkie endpointy Ads API | ★★★★☆ | Czyste MERGE'e |
| **Strategy Service** | Detekcja 20+ typów okazji | `services/strategy_service.py` (1288 loc) | Brak | ★★★★☆ | Nakłada się z executive |
| **Decision Intelligence** | Pętla zwrotna wykonania | `services/decision_intelligence_service.py` (833 loc) | Brak | ★★★★☆ | Czysty design |
| **Executive Service** | KPI poziomu CEO + scoring zdrowia | `services/executive_service.py` (843 loc) | Brak | ★★★★☆ | Nakłada się ze strategią |
| **Seasonality** | Profilowanie sezonowości popytu | `services/seasonality_service.py` + `seasonality_opportunity_engine.py` | Brak | ★★★★☆ | Czysty |
| **Tax Compliance** | EU VAT, OSS, dowody, raportowanie | `services/tax_compliance/` (11 plików) | Brak | ★★★★★ | Najlepsza architektura |
| **Return Tracker** | Cykl życia zwrotów + korekta COGS | `services/return_tracker.py` | FBA Returns Report | ★★★★☆ | Dobry model finansowy |
| **Guardrails** | 23+ kontrole zdrowia runtime | `services/guardrails.py` + `guardrails_backbone.py` | Brak | ★★★★★ | Doskonała obserwowalność |
| **Kurier/Logistyka** | Integracja DHL + GLS + estymacja kosztów | 12+ plików serwisów kurierskich | DHL24 SOAP, GLS REST | ★★★★☆ | Oddzielny podsystem |
| **Scheduler** | ~35 zaplanowanych zadań z elekcją lidera | `scheduler.py` (1675 loc) | SQS (pośrednio) | ★★★★☆ | Megaplik |
| **MSSQL Store** | Centralny helper DB z 80+ dyspozycjami zadań | `connectors/mssql/mssql_store.py` (4297 loc) | Brak | ★★★☆☆ | God moduł |
| **Config** | Ustawienia Pydantic z flagami featurowymi | `core/config.py` | N/D | ★★★★★ | Dobrze ustrukturyzowany |
| **Security** | JWT, RBAC, tokeny z zakresem marketplace | `core/security.py` | N/D | ★★★★☆ | Solidny |
| **Fee Taxonomy** | Klasyfikacja 70+ typów opłat Amazon | `core/fee_taxonomy.py` | N/D | ★★★★★ | Mózg księgowy |

---

# 3. Mapowanie Modułów Docelowych (1–30)

| # | Moduł docelowy | Status | Obecna lokalizacja kodu | Główne problemy | Rekomendacja | Priorytet |
|---|---|---|---|---|---|---|
| 1 | **Account Hub** | PARTIALLY_EXISTS | `core/config.py` (dane uwierzytelniające), `models/marketplace.py`, `seller_registry.py` | Brak zunifikowanej abstrakcji konta. Założone jedno konto sprzedawcy. Rejestr marketplace to statyczna tabela, nie dynamiczny hub. Brak wsparcia multi-seller. | EXTRACT_TO_MODULE — Utworzyć `app/platform/account_hub.py` z modelem Seller, skarbcem poświadczeń, rejestrem marketplace i cyklem życia tokenów SP-API. | P1 |
| 2 | **SP-API Gateway** | EXISTS_GOOD | `connectors/amazon_sp_api/` (12 plików) | Brak connection pooling (nowy klient httpx na każde żądanie). Ręczne `asyncio.sleep()` w niektórych modułach zamiast scentralizowanego zarządcy rate'ów. | REFACTOR_IN_PLACE — Dodać connection pooling i wyekstrahować sleep per-endpoint do konfiguracji rate governor. | P2 |
| 3 | **Ads API Gateway** | EXISTS_GOOD | `connectors/amazon_ads_api/` (4 pliki) | Brak telemetrii (w przeciwieństwie do SP-API). Brak retry na 5xx. | REFACTOR_IN_PLACE — Dodać telemetrię w stylu SP-API i retry na 5xx do bazowego klienta Ads. | P2 |
| 4 | **Rate Limit Manager** | EXISTS_BUT_WEAK | `core/rate_limit.py` (tylko login), backoff klienta SP-API, ręczne sleepy | Rate limiting istnieje per-connector, ale brak scentralizowanego menedżera limitów dla wszystkich wywołań Amazon API. Nagłówki rate SP-API (`x-amzn-RateLimit-Limit`) są odczytywane, ale nie używane do adaptacyjnego throttlingu. | EXTRACT_TO_MODULE — Zbudować zunifikowany `platform/rate_governor.py` śledzący limity per-endpoint we wszystkich wywołaniach SP-API i Ads. | P1 |
| 5 | **Event Bus Orchestrator** | EXISTS_BUT_WRONG_SHAPE | `services/event_backbone.py` | Doskonała ingestia + dyspozycja, ale: (a) obsługuje tylko notyfikacje SP-API, nie wewnętrzne eventy domenowe; (b) batch synce nie emitują eventów; (c) brak pub/sub do komunikacji między modułami; (d) używa synchronicznego pyodbc, nie async. Powinien być centralnym systemem nerwowym, a jest odbiornikiem notyfikacji. | REBUILD_FROM_SCRATCH — Zachować SQS poller + dedup + circuit breaker, ale opakować w właściwy bus eventów domenowych obsługujący zarówno zewnętrzne (SQS), jak i wewnętrzne (in-process) eventy. Wszystkie batch synce powinny emitować eventy po zakończeniu. | P1 |
| 6 | **Job Scheduler & Async Runner** | EXISTS_BUT_WRONG_SHAPE | `scheduler.py`, `worker.py`, `sync_runner.py`, `mssql_store.py` (funkcje zadań), `jobs/*.py` | Trzy równoległe ścieżki wykonania. `scheduler.py` to 1675 linii mieszających wszystkie domeny. `mssql_store.run_job_type()` dispatchuje 80+ typów zadań w jednym switch-case. Taski Celery używają przestarzałego `asyncio.get_event_loop()`. | REFACTOR_IN_PLACE — (1) Podzielić scheduler na moduły domenowe. (2) Usunąć schedule Celery Beat (zachować APScheduler jako kanoniczny). (3) Naprawić przestarzałe wzorce async. (4) Dodać retry na poziomie schedulera z backoff. | P1 |
| 7 | **Kanoniczny Model Produktu** | MISSING | `models/product.py` to płaska tabela master; `global_family` to rodziny DE-kanoniczne; `acc_amazon_listing_registry` to import z GSheet; `acc_offer` to stan marketplace | Brak kanonicznego modelu reprezentującego: jeden produkt → wiele reprezentacji marketplace → z transformacją source/canonical/target. Product, Offer, Listing i Family to cztery rozłączone tabele. Brak cyklu życia produktu (draft → active → discontinued). | REBUILD_FROM_SCRATCH — Zaprojektować właściwy kanoniczny model produktu: `CanonicalProduct` (produkt brand ownera), `MarketplacePresence` (listing per-rynek), `OfferSnapshot` (stan ceny/buybox), `FamilyAssignment` (hierarchia wariantów). To najważniejsza luka architektoniczna. | P0 |
| 8 | **Marketplace Mapping Engine** | PARTIALLY_EXISTS | `family_mapper/matching.py`, `sync_service._find_or_create_product`, `order_pipeline.step_map_products` | Matching istnieje w family_mapper dla rodzin wariantów. Mapowanie SKU istnieje w order_pipeline z kaskadą 4 źródeł (Ergonode → GSheet → Baselinker → ASIN). Ale brak zunifikowanego silnika mapowania — każdy moduł ma własną strategię wyszukiwania. | EXTRACT_TO_MODULE — Zunifikować całą logikę mapowania w `platform/marketplace_mapping.py`. Jeden punkt wejścia: dane (sku, marketplace) → (canonical_product, internal_sku, ean, family_id). | P1 |
| 9 | **Listing Snapshot Store** | EXISTS_GOOD | `services/listing_state.py` | Dobrze zaprojektowany model event-sourced z historią. Śledzenie statusu z raportów, eventów SP-API i odświeżania na żądanie. Jedyna luka: brak snapshotu treści (tylko status/problemy). | KEEP_AS_IS — Rozszerzyć o snapshoty treści (tytuł, bullets, zdjęcia) obok statusu. | P2 |
| 10 | **Offer & Price Snapshot Store** | EXISTS_GOOD | `services/pricing_state.py`, `models/offer.py` | `pricing_state` ma właściwą historię snapshotów z archiwizacją. `acc_offer` przechwytuje bieżący stan. Śledzenie BuyBox istnieje. Brakuje: przechowywanie krajobrazu konkurencyjnego (wszyscy sprzedawcy, nie tylko własna oferta). | REFACTOR_IN_PLACE — Dodać snapshoty ofert konkurencji obok śledzenia własnej oferty. | P2 |
| 11 | **Inventory & Supply Snapshot Store** | EXISTS_BUT_WEAK | `models/inventory.py`, `jobs/sync_inventory.py`, `services/fba_ops/service.py`, `services/manage_inventory.py` | Trzy nakładające się implementacje magazynowe: (a) job `sync_inventory.py` z logiką biznesową, (b) `fba_ops.sync_inventory_cache` z danymi raportowymi, (c) `manage_inventory` z ruchem/DOI. Brak jednego źródła prawdy o magazynie. Brak ochrony upsert w jobie Celery. | MERGE_WITH_ANOTHER_MODULE — Skonsolidować w `data/inventory_store.py`. Jedna ścieżka ingestii: SP-API Inventory API + FBA Reports → normalizacja → upsert do `acc_inventory_snapshot` z właściwym dedupem. | P1 |
| 12 | **Finance & Margin Ledger** | EXISTS_BUT_WRONG_SHAPE | `services/finance_center/`, `models/finance.py`, `core/fee_taxonomy.py`, `services/profit_engine.py`, `services/profitability_service.py`, `order_pipeline.step_bridge_fees` | Fee taxonomy jest doskonała. Finance Center generuje wpisy księgowe. ALE: kalkulacja zysku ma 3 ścieżki kodu. Fee bridging (transakcja finansowa → linia zamówienia) zakopany w order_pipeline (400+ linii surowego SQL). Profitability rollup'y duplikują zapytania profit_engine. `acc_finance_transaction` nie ma constraintu deduplikacji. | REFACTOR_IN_PLACE — (1) Wyeliminować legacy ścieżkę profit_service.py. (2) Wyekstrahować fee bridging z order_pipeline do finance_center. (3) Dodać unique constraint na transakcjach finansowych. (4) Scalić zapytania profitability_service do profit_engine jako jedynego interfejsu zapytań. | P0 |
| 13 | **Orders Ingestion Module** | EXISTS_GOOD | `services/order_pipeline.py` kroki 1-4, `connectors/amazon_sp_api/orders.py` | Kanoniczna ścieżka. Stan synca per-marketplace. Wykrywanie luk. Detekcja zmian oparta na hashach. Pipeline 6-krokowy jest dobrze zaprojektowany. | KEEP_AS_IS | P3 |
| 14 | **Listings Ingestion Module** | EXISTS_BUT_WEAK | `services/sync_service.py` (raportowy), `services/listing_state.py` (eventowy), `services/amazon_listing_registry.py` (GSheet) | Trzy ścieżki ingestii danych o listingach, żadna nie produkuje zunifikowanego snapshotu listingu. `sync_service` pobiera raporty listingowe, `listing_state` obsługuje eventy SP-API, a listing_registry importuje z Google Sheets. | EXTRACT_TO_MODULE — Utworzyć `ingestion/listings_ingestion.py` unifikujący: raportowy bulk sync + eventy real-time + mapowanie rejestru w jeden stan listingu per (sku, marketplace). | P1 |
| 15 | **Catalog Intelligence Module** | PARTIALLY_EXISTS | `connectors/amazon_sp_api/catalog.py`, `services/family_mapper/de_builder.py`, `services/ptd_cache.py`, `services/ptd_validator.py` | Dane katalogowe są pobierane, ale nie systematycznie przechowywane. Definicje typów produktów są cache'owane. Mapowanie rodzin używa danych katalogowych do dopasowywania między rynkami. Brak stałego magazynu snapshotów katalogu. | EXTRACT_TO_MODULE — Zbudować `ingestion/catalog_intelligence.py` z: snapshotami elementów katalogowych, historią BSR, cache typów produktów, drzewem kategorii. Zasilać kanoniczny model produktu. | P1 |
| 16 | **Pricing Intelligence Module** | EXISTS_GOOD | `services/pricing_state.py`, `services/pricing_rules.py`, `connectors/amazon_sp_api/pricing_api.py` | Czyste rozdzielenie: przechwytywanie (pricing_state) → ewaluacja (pricing_rules) → rekomendacja. Integracja event backbone dla notyfikacji real-time. Brakuje: batch przez `getCompetitiveSummary` nowe API. | KEEP_AS_IS — Dodać wsparcie batch API `getCompetitiveSummary`. | P2 |
| 17 | **Inventory Ingestion Module** | EXISTS_BUT_WEAK | `jobs/sync_inventory.py`, `services/fba_ops/service.py` | Patrz #11 — zduplikowana ingestia bez dedupu. | MERGE_WITH_ANOTHER_MODULE (patrz #11) | P1 |
| 18 | **Reports Ingestion Module** | EXISTS_GOOD | `connectors/amazon_sp_api/reports.py` | 12 typów raportów obsługiwanych. Sprytne ponowne użycie. Właściwy cykl życia. Używany przez 5+ serwisów. | KEEP_AS_IS | P3 |
| 19 | **Notifications Ingestion Module** | EXISTS_GOOD | `connectors/amazon_sp_api/notifications.py`, `services/event_backbone.py` | Pełny cykl życia: destynacje → subskrypcje → pollowanie SQS → ingestia → dyspozycja. 9 typów notyfikacji. Dedup + circuit breaker. | KEEP_AS_IS — Zwiększyć częstotliwość pollowania SQS z 2 min do 30 sek dla eventów krytycznych czasowo. | P2 |
| 20 | **Ads Reporting Ingestion Module** | EXISTS_GOOD | `services/ads_sync.py`, `connectors/amazon_ads_api/reporting.py` | Profile → kampanie → metryki dzienne → metryki produktów. Pokrycie SP/SB/SD. Konwersja do PLN. Upserty oparte na MERGE. | KEEP_AS_IS | P3 |
| 21 | **Catalog Health Monitor** | PARTIALLY_EXISTS | `services/guardrails.py` (check_unknown_fee_types, check_order_vs_finance_totals), `services/listing_state.py` (health_summary) | Zdrowie listingów istnieje przez `get_listing_health_summary()`. Guardrails sprawdzają świeżość pipeline'u. Brak dedykowanego monitora zdrowia katalogu ze śledzeniem supresji, scoringiem kompletności treści, analizą jakości obrazów. | EXTRACT_TO_MODULE — Zbudować `ops_intelligence/catalog_health.py` ciągnący z listing_state + content_ops + family_mapper do produkcji zunifikowanej karty scoringowej zdrowia katalogu. | P2 |
| 22 | **Listing Diff & Anomaly Detector** | PARTIALLY_EXISTS | `services/listing_state.py` (historia), `services/content_ops.py` (get_content_diff) | Diffing treści istnieje w content_ops. Historia listing state przechwytuje tranzycje. Brak automatycznej detekcji anomalii (np. nieoczekiwane zmiany tytułu, przejęte listingi, podmiana zdjęć). | DEFER_TO_PHASE_2 | P2 |
| 23 | **Buy Box & Offer Radar** | PARTIALLY_EXISTS | `services/pricing_state.py` (buybox_overview), `models/offer.py` (has_buybox) | Posiadanie BuyBox śledzone per snapshot. Brak śledzenia konkurentów, trendów win-rate, alertów na utrzymującą się utratę BuyBox. | DEFER_TO_PHASE_2 | P2 |
| 24 | **Inventory Risk Engine** | PARTIALLY_EXISTS | `services/fba_ops/service.py` (uzupełnianie), `services/manage_inventory.py` (DOI), `services/strategy_service.py` (okazje magazynowe) | Kalkulacja DOI istnieje. Sugestie uzupełnień istnieją. Strategia wykrywa okazje niskostanowe. Brak zunifikowanego scoringu ryzyka (prawdopodobieństwo braku towaru, koszt nadmiernego stanu, ryzyko odpisów na starzenie). | EXTRACT_TO_MODULE | P2 |
| 25 | **Profit Engine** | EXISTS_BUT_WRONG_SHAPE | `services/profit_engine.py`, `services/profit_service.py`, `services/profitability_service.py`, `order_pipeline.step_calc_profit` | Trzy ścieżki kodu kalkulacji zysku. God moduł o 6632 liniach mieszający kalkulację, zapytania, eksport i zarządzanie modelem kosztowym. Profitability_service duplikuje wzorce zapytań. | REFACTOR_IN_PLACE — (1) Usunąć legacy profit_service.py. (2) Podzielić profit_engine na: `profit_calculator.py` (logika CM1/CM2/NP), `profit_query.py` (zapytania API), `profit_export.py` (XLSX), `cost_model.py` (konfiguracja). (3) Scalić funkcje zapytań profitability_service do profit_query. | P0 |
| 26 | **Refund / Fee Anomaly Engine** | PARTIALLY_EXISTS | `services/return_tracker.py`, `services/fba_ops/fba_fee_audit.py`, `services/guardrails.py` | Śledzenie zwrotów jest dobre. Audyt prowizji istnieje w FBA ops. Guardrails sprawdzają nieznane prowizje. Brak zunifikowanej detekcji anomalii zwrotów (nagłe skoki wskaźnika zwrotów, seryjni zwracający, automatyzacja reklamacji reimbursement). | DEFER_TO_PHASE_2 | P2 |
| 27 | **Repricing Decision Engine** | PARTIALLY_EXISTS | `services/pricing_rules.py` (guardrails + rekomendacje), `services/strategy_service.py` (okazje cenowe) | Reguły cenowe ewaluują guardrails i generują rekomendacje. Strategia wykrywa okazje cenowe. Brak automatycznej realizacji repricingu — zawsze human-in-loop. Brak dynamicznego algorytmu repricingu (strategie min/max, śledzenie konkurencji, uwzględnienie marży). | DEFER_TO_PHASE_3 | P3 |
| 28 | **Content Optimization Engine** | PARTIALLY_EXISTS | `services/content_ops.py` (pełny cykl życia), `services/ai_service.py` (rekomendacje) | Zarządzanie taskami treści, kontrola wersji, pipeline publikacji, walidacja polityk — to wszystko istnieje. AI generuje rekomendacje. Brak automatycznego scoringu treści, analizy SEO, benchmarkingu treści konkurencji. | REFACTOR_IN_PLACE — Podzielić content_ops.py (4906 loc) na: zarządzanie taskami, zarządzanie wersjami, pipeline publikacji, silnik polityk, kolejka compliance. | P1 |
| 29 | **Feed / Listings Action Center** | EXISTS_BUT_WEAK | `connectors/amazon_sp_api/feeds.py`, `services/content_ops.py` (publish push), `services/family_mapper/restructure.py` | Submitowanie feedów działa. Publikacja treści używa circuit breakera. Restrukturyzacja rodzin wykonuje feedy. Brak zunifikowanego centrum akcji: rozproszone między content_ops (publikacja), family_mapper (restrukturyzacja) i bezpośrednimi wywołaniami SP-API. | EXTRACT_TO_MODULE — Utworzyć `execution/action_center.py` jako jedyny punkt wejścia dla wszystkich operacji zapisu do Amazon (zmiany cen, aktualizacje treści, restrukturyzacje rodzin, korekty magazynowe). Centralny ślad audytowy, circuit breaker i rate limiting. | P1 |
| 30 | **Alerting / Cases / Operator Console** | PARTIALLY_EXISTS | `services/guardrails.py` (health checks), `models/alert.py`, `mssql_store.py` (alert CRUD), `services/fba_ops/service.py` (cases), `services/courier_alerts.py` | Alerty istnieją jako proste sprawdzenia progowe. Cases istnieją w FBA ops. Guardrails zapewniają zdrowie systemu. Brak zunifikowanego backendu konsoli operatora — alerty, cases, akcje i zatwierdzenia rozproszone między modułami. | EXTRACT_TO_MODULE — Utworzyć pakiet `ops_console/`: zunifikowany kanał alertów, zarządzanie sprawami, kolejka akcji, workflow zatwierdzania, API dashboardu operatora. | P2 |

---

# 4. Zapachy Architektury i Ryzyka Strukturalne

## Sprzężenia (Coupling)

| Problem | Waga | Lokalizacja |
|---|---|---|
| `scheduler.py` importuje 20+ modułów serwisowych, tworząc fan-out zależności | WYSOKA | `app/scheduler.py` |
| `mssql_store.run_job_type()` dispatchuje 80+ typów zadań — uniwersalny punkt sprzężenia | WYSOKA | `connectors/mssql/mssql_store.py` |
| 3 serwisy współdzielą tabelę `growth_opportunity` bez warstwy koordynacji | ŚREDNIA | `strategy_service`, `executive_service`, `decision_intelligence_service` |
| `content_ops.py` bezpośrednio wywołuje SP-API z wewnątrz CRUD zarządzania treścią | ŚREDNIA | `services/content_ops.py` |
| `order_pipeline.step_bridge_fees` zagnieżdża 400+ linii logiki finansowej | ŚREDNIA | `services/order_pipeline.py` |
| `main.py` lifespan importuje ~18 modułów serwisowych do tworzenia schematów | NISKA | `app/main.py` |

## Async / Kolejki

| Problem | Waga | Lokalizacja |
|---|---|---|
| Event backbone obsługuje tylko notyfikacje SP-API, nie wewnętrzne eventy domenowe — 90% przepływu danych go omija | WYSOKA | `services/event_backbone.py` |
| 4 joby Celery używają przestarzałego `asyncio.get_event_loop().run_until_complete()` — zepsuje się na Python 3.12+ | WYSOKA | `jobs/calc_profit.py`, `sync_finances.py`, `sync_inventory.py`, `sync_purchase_prices.py` |
| Trzy równoległe ścieżki wykonania (APScheduler, Celery Beat, CLI) dla tych samych operacji | ŚREDNIA | `scheduler.py`, `worker.py`, `sync_runner.py` |
| Pollowanie SQS co 2 minuty — eventy krytyczne czasowo (zmiany ofert, status zamówień) mogą być nieaktualne | ŚREDNIA | `scheduler.py._poll_sqs_notifications` |
| Zależności zadań sprzężone czasowo (ceny o 02:00 → finanse o 03:00 → zysk o 05:00) bez jawnych sprawdzeń zależności | ŚREDNIA | `scheduler.py` |
| Brak obsługi dead-letter dla nieudanych zaplanowanych zadań — trzeba czekać do następnego nocnego uruchomienia | ŚREDNIA | `scheduler.py` |

## Persystencja

| Problem | Waga | Lokalizacja |
|---|---|---|
| 14+ serwisów tworzy tabele inline przez `ensure_*_schema()` DDL — podwójny świat z Alembic | WYSOKA | Prawie wszystkie serwisy |
| `acc_finance_transaction` nie ma unique constraint — możliwe duplikaty insertów | WYSOKA | `models/finance.py` |
| `InventorySnapshot` nie ma upsert — job `sync_inventory.py` zawsze robi `db.add()` tworząc duplikaty | WYSOKA | `jobs/sync_inventory.py`, `models/inventory.py` |
| `Alert.marketplace_id` i `JobRun.marketplace_id` to zwykłe stringi, nie FK — brak integralności referencyjnej | ŚREDNIA | `models/alert.py`, `models/job.py` |
| Tabele rodzin nie mają prefiksu konwencji `acc_` | NISKA | `models/family.py` |
| `READ UNCOMMITTED` globalnie — brudne odczyty danych finansowych | ŚREDNIA | `core/database.py` |
| JSON przechowywany jako kolumny `Text` w 5+ modelach — utrata możliwości zapytań | NISKA | Wiele modeli |

## Integracja API

| Problem | Waga | Lokalizacja |
|---|---|---|
| Brak connection pooling w klientach SP-API/Ads (nowy klient httpx na każde żądanie) | ŚREDNIA | `connectors/amazon_sp_api/client.py`, `connectors/amazon_ads_api/client.py` |
| Ręczny `asyncio.sleep()` rate limiting w 4 modułach konektorów — niescentralizowany | NISKA | `orders.py`, `catalog.py`, `pricing_api.py`, `finances.py` |
| Klient Amazon Ads nie ma retry na 5xx ani telemetrii (SP-API ma oba) | NISKA | `connectors/amazon_ads_api/client.py` |
| `getCompetitiveSummary` (batch pricing) endpoint nieużywany — chaotyczne wywołania per-ASIN | NISKA | `connectors/amazon_sp_api/pricing_api.py` |

## Modelowanie Domenowe

| Problem | Waga | Lokalizacja |
|---|---|---|
| **Brak kanonicznego modelu produktu** — Product, Offer, Listing, Family, Registry to 5 rozłącznych reprezentacji | KRYTYCZNA | Wiele modeli |
| Mapowanie SKU ma 4 konkurencyjne strategie wyszukiwania bez zunifikowanego interfejsu | WYSOKA | `order_pipeline`, `sync_service`, `amazon_listing_registry`, `family_mapper` |
| Zysk ma 3 ścieżki kalkulacji (`calc_profit`, `recalculate_profit_batch`, `step_calc_profit`) | WYSOKA | 3 pliki |
| Warstwa decyzyjna używa nazw tabel bez prefiksu (`growth_opportunity`, `executive_daily_metrics`) | NISKA | `strategy_service`, `executive_service`, `decision_intelligence_service` |

## Obserwowalność

| Problem | Waga | Lokalizacja |
|---|---|---|
| System guardrails jest doskonały (23+ sprawdzeń), ale wyniki nie są eksponowane przez alerty/PagerDuty | NISKA | `services/guardrails.py` |
| Telemetria SP-API rejestruje każde wywołanie — dobrze. Ads API nie ma telemetrii | NISKA | Konektory |
| Brak distributed tracing (correlation IDs istnieją tylko w event backbone) | ŚREDNIA | Cały system |

## Konfiguracja / Sekrety

| Problem | Waga | Lokalizacja |
|---|---|---|
| URL Google Sheets z konkretnym GID zahardkodowany w konfiguracji — krucha zewnętrzna zależność | ŚREDNIA | `core/config.py`, `services/amazon_listing_registry.py` |
| Ścieżka XLSX cen zakupu to udział sieciowy (`N:\Analityka\...`) — specyficzna dla środowiska | NISKA | `core/config.py` |
| Wszystkie poświadczenia w zmiennych środowiskowych — prawidłowy wzorzec | OK | `core/config.py` |

## Wydajność

| Problem | Waga | Lokalizacja |
|---|---|---|
| Nowy klient httpx na każde żądanie SP-API/Ads — narzut ustanawiania połączenia | ŚREDNIA | Konektory |
| `profit_engine.py` ładuje wszystkie dane do Pythona do przetwarzania — brak agregacji na poziomie DB dla dużych zakresów dat | ŚREDNIA | `services/profit_engine.py` |
| `seller_registry` globalny cache bez TTL — nieaktualny do restartu | NISKA | `services/seller_registry.py` |
| Wzorzec cache in-memory reimplementowany 5+ razy — brak współdzielonego narzędzia | NISKA | Wiele serwisów |

## Testowalność

| Problem | Waga | Lokalizacja |
|---|---|---|
| 45 plików testowych na ~100 tys. linii kodu — minimalne pokrycie | WYSOKA | `tests/` |
| Serwisy używają surowych połączeń pyodbc — trudne do mockowania w testach jednostkowych | ŚREDNIA | Wszystkie serwisy używające `connect_acc()` |
| Brak dependency injection — serwisy importują singletony bezpośrednio | ŚREDNIA | Cały system |
| `profit_service.py` utrzymywany tylko dla testów — martwy kod na ścieżce produkcyjnej | NISKA | `services/profit_service.py` |

---

# 5. Brakujące Kluczowe Bloki Budulcowe

| Priorytet | Brakujący blok | Dlaczego blokuje Command Center | Wpływ |
|---|---|---|---|
| **P0** | **Kanoniczny Model Produktu** | Każdy moduł ma własną reprezentację produktu. Brak transformacji source → canonical → target. Nie można zbudować wiarygodnej inteligencji cross-market bez zunifikowanej tożsamości produktu. | Blokuje moduły 7, 8, 15, 21, 24, 25, 27, 28 |
| **P0** | **Zunifikowany Kalkulator Zysku** (jedna ścieżka) | Trzy konkurencyjne ścieżki kalkulacji zysku tworzą ryzyko rozbieżności i uniemożliwiają audyt. Nikt nie jest w stanie odpowiedzieć "która liczba zysku jest prawidłowa?" | Blokuje zaufanie do danych finansowych |
| **P1** | **Wewnętrzny Bus Eventów Domenowych** | Event backbone obsługuje tylko notyfikacje SP-API. Batch synce nie emitują eventów. Brak pub/sub między modułami. System nadal w 90% sterowany cronem, nie eventami. | Blokuje architekturę reaktywną |
| **P1** | **Scentralizowane Zarządzanie Migracjami Schematów** | 14+ serwisów zagnieżdża DDL. Brak sposobu na poznanie aktualnego stanu schematu. Brak możliwości rollbacku. | Blokuje pewność deployu |
| **P1** | **Action Center** (zunifikowana brama zapisu) | Zapisy do Amazon (ceny, treści, feedy, restrukturyzacje rodzin) rozproszone między 4+ modułami bez centralnego śladu audytowego czy zarządzania rate. | Blokuje bezpieczeństwo operacyjne |
| **P1** | **Współdzielona Biblioteka Narzędziowa** | `_f()`, `_fetchall_dict()`, `_mkt_code()`, `_connect()` zduplikowane w 8+ plikach. Cache in-memory reimplementowany 5 razy. | Blokuje utrzymywalność |
| **P2** | **Scentralizowany Zarządca Rate** | Limity rate per-endpoint obsługiwane ad-hoc (ręczne sleepy). Brak adaptacyjnego throttlingu na podstawie nagłówków odpowiedzi. | Blokuje efektywność API |
| **P2** | **Magazyn Inteligencji Konkurencyjnej** | Śledzona tylko własna oferta. Brak stałego przechowywania cen konkurencji, trendów liczby sprzedawców, ani historii win-rate BuyBox. | Blokuje moduły 23, 27 |
| **P2** | **Suite Testów Integracyjnych** | 45 plików testowych na 100 tys. LOC. Brak pipeline'u CI. Nie można bezpiecznie refaktoryzować. | Blokuje cały refaktoring |

---

# 6. Matryca Przebudowa vs Refaktoring

| Komponent | Obecny stan | Refaktoring? | Przebudowa? | Dlaczego? | Ryzyko zostawienia jak jest |
|---|---|---|---|---|---|
| Konektory SP-API | Produkcyjna jakość | Drobny | Nie | Już dobre. Dodać connection pooling. | Niskie |
| Konektory Ads API | Produkcyjna jakość | Drobny | Nie | Dodać telemetrię + retry na 5xx. | Niskie |
| Event Backbone | Doskonały, ale wąski | Tak | Częściowo | Zachować dedup/CB/dispatch. Dodać wewnętrzne eventy domenowe. | Średnie — system zostaje sprzężony z crona |
| Kanoniczny Model Produktu | Brakujący | N/D | **Tak** | Nie ma co refaktoryzować — fundamentalna luka | **Krytyczne** — ciągła niespójność danych |
| Profit Engine | God moduł | **Tak** | Nie | Podzielić na kalkulator + zapytania + eksport + model kosztowy | Wysokie — niekonserwowalny przy 6632 liniach |
| Content Ops | God moduł | **Tak** | Nie | Podzielić na 5 sub-modułów | Wysokie — niekonserwowalny przy 4906 liniach |
| FBA Ops | Monolit | **Tak** | Nie | Wyekstrahować audyt prowizji, cases, inbound, uzupełnianie | Średnie |
| Order Pipeline | Dobry, ale mieszany | Drobny | Nie | Wyekstrahować fee bridging do finance_center | Średnie |
| Finance Center | Dobrze ustrukturyzowany | Nie | Nie | Zachować. Dodać constraint dedupu. | Niskie |
| Tax Compliance | Najlepsza struktura | Nie | Nie | Referencyjna architektura dla innych modułów | Niskie |
| Scheduler | Megaplik | **Tak** | Nie | Podzielić na moduły domenowe | Średnie — obciążenie utrzymaniowe |
| MSSQL Store | God moduł | **Tak** | Nie | Wyekstrahować kolejkę zadań do własnego modułu | Średnie |
| Profit Service | Legacy/martwy | **Usunąć** | Nie | Używany tylko przez testy — migrować testy do silnika V2 | Niskie, ale mylące |
| Sync Service | Częściowo zastąpiony | Wygaszać | Nie | Ścieżka zamówień zastąpiona. Zachować ścieżki magazynowe/cenowe do zbudowania modułu ingestii. | Średnie — podwójne ścieżki |
| Inventory Sync (Job) | Zepsuty (brak upsert) | N/D | **Tak** | Logika biznesowa w warstwie joba, brak dedupu, przestarzały async | **Wysokie** — tworzy zduplikowane snapshoty |
| Joby Celery (4 pliki) | Przestarzały async | **Naprawić** | Nie | Zamienić `get_event_loop()` na `asyncio.run()` | **Wysokie** — zepsuje się na Python 3.12+ |

---

# 7. Rekomendowana Docelowa Struktura Backendu

```
app/
├── platform/                          # Infrastruktura przekrojowa
│   ├── account_hub.py                 # Konta sprzedawców, poświadczenia, rejestr marketplace'ów
│   ├── rate_governor.py               # Scentralizowane śledzenie limitów per-endpoint
│   ├── event_bus.py                   # Bus eventów domenowych (SQS external + internal pub/sub)
│   ├── job_scheduler.py               # Zunifikowany scheduler (z scheduler.py + worker.py)
│   ├── action_center.py               # Zunifikowana brama zapisu do Amazon (feedy, listingi, ceny)
│   └── shared/                        # Współdzielone narzędzia
│       ├── db.py                      # _connect, _fetchall_dict, _f, _i, _mkt_code
│       ├── cache.py                   # Generyczny cache in-memory z TTL
│       └── fx.py                      # Serwis FX (z core/fx_service.py)
│
├── connectors/                        # Klienci zewnętrznych API (ZACHOWAĆ JAK JEST)
│   ├── amazon_sp_api/                 # 12 plików — produkcyjna jakość
│   ├── amazon_ads_api/                # 4 pliki — dodać telemetrię
│   ├── dhl24_api/
│   ├── gls_api/
│   ├── mssql/
│   └── ergonode.py, ecb.py, nbp.py
│
├── domain/                            # Bazowe modele domenowe (NOWE)
│   ├── canonical_product.py           # Kanoniczny produkt + obecność na marketplace + rodzina
│   ├── marketplace_mapping.py         # Zunifikowane mapowanie SKU/ASIN/EAN/InternalSKU
│   └── fee_taxonomy.py               # Przenieść z core/ — to logika domenowa
│
├── ingestion/                         # Moduły ingestii danych (WYEKSTRAHOWAĆ Z services/)
│   ├── orders.py                      # Z order_pipeline kroki 1-4
│   ├── listings.py                    # Z sync_service + listing_state + registry
│   ├── catalog.py                     # Z catalog connector + ptd_cache
│   ├── pricing.py                     # Z pricing_state.capture_*
│   ├── inventory.py                   # Z sync_inventory + fba_ops.sync_cache
│   ├── finance.py                     # Z order_pipeline.step_sync_finances
│   ├── ads.py                         # Z ads_sync.py
│   ├── returns.py                     # Z return_tracker.py funkcje synca
│   └── notifications.py              # Poller SQS (z event_backbone.poll_sqs)
│
├── warehouse/                         # Znormalizowane magazyny danych (WYEKSTRAHOWAĆ Z models/)
│   ├── orders_store.py                # AccOrder, OrderLine — interfejs zapytań
│   ├── listing_store.py               # ListingState + historia — interfejs zapytań
│   ├── pricing_store.py               # PricingSnapshot + archiwum — interfejs zapytań
│   ├── inventory_store.py             # InventorySnapshot — interfejs zapytań
│   ├── finance_store.py               # FinanceTransaction + Ledger — interfejs zapytań
│   ├── ads_store.py                   # AdsCampaign + metryki dzienne — interfejs zapytań
│   └── catalog_store.py              # CatalogSnapshot + ProductType — interfejs zapytań
│
├── intelligence/                      # Logika biznesowa / analityka (REFAKTORYZACJA Z services/)
│   ├── profit/
│   │   ├── calculator.py              # Obliczanie CM1/CM2/NP (z jądra profit_engine)
│   │   ├── query.py                   # Tabele zysków, drilldowny, KPI
│   │   ├── export.py                  # Eksport XLSX
│   │   └── cost_model.py             # Konfiguracja modelu kosztowego
│   ├── catalog_health.py             # Z guardrails + listing_state + zdrowie treści
│   ├── inventory_risk.py             # Z fba_ops + manage_inventory + strategy
│   ├── pricing_intelligence.py       # Z pricing_rules + detekcja cenowa strategy
│   ├── seasonality.py                # Z seasonality_service + opportunity_engine
│   ├── strategy.py                   # Z strategy_service (detekcja okazji)
│   ├── decision_feedback.py          # Z decision_intelligence_service
│   └── executive.py                  # Z executive_service (scoring zdrowia)
│
├── execution/                         # Operacje zapisu (WYEKSTRAHOWAĆ)
│   ├── content_ops/
│   │   ├── tasks.py                   # CRUD tasków
│   │   ├── versions.py               # CRUD wersji
│   │   ├── publish.py                # Pipeline publikacji
│   │   ├── policy.py                 # Walidacja polityk
│   │   └── compliance.py             # Kolejka compliance
│   ├── family_mapper/                # ZACHOWAĆ JAK JEST — dobrze ustrukturyzowany
│   ├── fba_ops/
│   │   ├── overview.py
│   │   ├── replenishment.py
│   │   ├── inbound.py
│   │   ├── cases.py
│   │   └── fee_audit.py
│   ├── pricing_actions.py            # Realizacja zmian cenowych przez action_center
│   └── returns.py                    # Akcje zwrotowe / korekty COGS
│
├── compliance/                        # ZACHOWAĆ JAK JEST — najlepiej ustrukturyzowany moduł
│   └── tax_compliance/               # 11 plików — referencyjna architektura
│
├── logistics/                         # Podsystem kurierski (ZACHOWAĆ JAK JEST)
│   ├── dhl/
│   ├── gls/
│   ├── cost_estimation.py
│   ├── billing_import.py
│   └── order_linking.py
│
├── api/                              # Routy API (ZACHOWAĆ, drobne porządki)
│   ├── v1/
│   └── ws.py
│
├── core/                             # Infrastruktura frameworka (ZACHOWAĆ)
│   ├── config.py
│   ├── database.py
│   ├── db_connection.py
│   ├── security.py
│   ├── redis_client.py
│   ├── circuit_breaker.py
│   └── scheduler_lock.py
│
├── models/                           # Modele ORM SQLAlchemy (ZACHOWAĆ, dodać kanoniczny)
│
└── migrations/                       # WSZYSTKIE zmiany schematu idą tutaj (SKONSOLIDOWAĆ)
    └── versions/
```

### Granice Własności
- **Zespół platformy** zarządza: platform/, core/, connectors/, models/, migrations/
- **Zespół danych** zarządza: ingestion/, warehouse/
- **Zespół analityczny** zarządza: intelligence/
- **Zespół operacyjny** zarządza: execution/, compliance/, logistics/
- **Zespół produktowy** zarządza: api/

### Granice Serwisów (przy ewentualnym podziale)
1. **Serwis Ingestii Danych** — ingestion/ + connectors/ + warehouse/ (ścieżka zapisu)
2. **Serwis Inteligencji** — intelligence/ + warehouse/ (ścieżka odczytu)
3. **Serwis Konsoli Operatora** — api/ + execution/ (UI + akcje)
4. **Serwis Compliance** — compliance/ (podatkowy + regulacyjny)

### Granice Przechowywania
- **Główna DB** (Azure SQL) — wszystkie tabele `acc_*`
- **Redis** — blokada schedulera, circuit breaker, rate limiter, broker Celery, cache odpowiedzi
- **SQS** — notyfikacje SP-API, wewnętrzne eventy domenowe (przyszłość)
- **Blob Storage** — raporty, eksporty, paczki audytowe (przyszłość)

---

# 8. Proponowana Topologia SQS

### Kolejki

| Nazwa Kolejki | Przeznaczenie | Producenci | Konsumenci | Współbieżność |
|---|---|---|---|---|
| `acc-spapi-notifications` | Eventy SP-API (istniejąca) | Amazon SQS | event_bus.poll_sqs | 1 (adaptacyjna) |
| `acc-ingestion-complete` | Eventy domenowe "Sync X zakończony" | Wszystkie moduły ingestii | intelligence/, execution/ | 3 |
| `acc-actions` | Operacje zapisu do Amazon | Moduły execution/ | worker action_center | 1 (rate-limited) |
| `acc-alerts` | Triggery alertów | intelligence/, guardrails | worker ops_console | 2 |

### Strategia DLQ

| Kolejka | DLQ | Maks. Prób | Retencja |
|---|---|---|---|
| `acc-spapi-notifications` | `acc-spapi-notifications-dlq` | 3 | 14 dni |
| `acc-ingestion-complete` | `acc-ingestion-complete-dlq` | 5 | 7 dni |
| `acc-actions` | `acc-actions-dlq` | 3 | 14 dni |
| `acc-alerts` | `acc-alerts-dlq` | 5 | 7 dni |

### Strategia Idempotencji
- **Event backbone** (istniejący): deterministyczny fingerprint SHA-256 — ZACHOWAĆ
- **Eventy ingestion-complete**: Uwzględnić `{source}:{marketplace}:{sync_window_end}` jako klucz dedupu
- **Wiadomości akcji**: Uwzględnić `{action_type}:{sku}:{marketplace}:{timestamp}` jako klucz idempotencji z TTL 24h w Redis

### Strategia Ponawiania
- **Exponential backoff** z jitterem w visibility timeout SQS: 30s → 60s → 120s
- **DLQ po 3-5 niepowodzeniach** w zależności od kolejki
- **Ręczny replay** przez `event_bus.replay_events()` (istniejąca zdolność)

### Strategia Replay
- Zachować istniejące `replay_events()` z event_backbone
- Rozszerzyć na wszystkie kolejki: funkcja `replay_dlq(queue_name, filter)`
- Wszystkie eventy persistowane w `acc_event_log` przed przetworzeniem — replay z DB jeśli stan SQS utracony

---

# 9. Proponowany Model Danych / Domeny Przechowywania

### Konta
| Tabela | Przeznaczenie |
|---|---|
| `acc_marketplace` | Rejestr marketplace'ów (ISTNIEJE — ★★★★★) |
| `acc_seller_account` | NOWA — Konto sprzedawcy + poświadczenia + status |
| `acc_user` | Użytkownicy + RBAC (ISTNIEJE) |

### Katalog / Kanoniczny Produkt
| Tabela | Przeznaczenie |
|---|---|
| `acc_canonical_product` | NOWA — Tożsamość produktu brand ownera (internal_sku PK, EAN, marka, kategoria) |
| `acc_marketplace_presence` | NOWA — Obecność na rynku per-marketplace (canonical_product FK, marketplace FK, SKU, ASIN, status) |
| `acc_product` | ISTNIEJE — Zachować jako most migracyjny, stopniowo zastępować |
| `acc_catalog_snapshot` | NOWA — Dane katalogowe point-in-time z SP-API |
| `acc_product_type_cache` | ISTNIEJE (implicit) — Cache definicji typów produktów |

### Listingi
| Tabela | Przeznaczenie |
|---|---|
| `acc_listing_state` | ISTNIEJE — Kanoniczny status listingu (★★★★★) |
| `acc_listing_state_history` | ISTNIEJE — Tranzycje statusów |
| `acc_amazon_listing_registry` | ISTNIEJE — Mapowanie master z GSheet |

### Oferty / Ceny
| Tabela | Przeznaczenie |
|---|---|
| `acc_offer` | ISTNIEJE — Bieżący stan oferty |
| `acc_pricing_snapshot` | ISTNIEJE — Historia obserwacji cenowych |
| `acc_pricing_snapshot_archive` | ISTNIEJE — Archiwalne snapshoty |
| `acc_pricing_rule` | ISTNIEJE — Guardrails cenowe |
| `acc_pricing_recommendation` | ISTNIEJE — Sugestie zatwierdzone przez człowieka |

### Magazyn
| Tabela | Przeznaczenie |
|---|---|
| `acc_inventory_snapshot` | ISTNIEJE — Dzienny stan magazynowy (wymaga naprawy UPSERT) |
| `acc_fba_inventory_snapshot` | ISTNIEJE — Szczegóły FBA (z raportów) |
| `acc_inv_traffic_sku_daily` | ISTNIEJE — Prędkość sprzedaży |

### Finanse
| Tabela | Przeznaczenie |
|---|---|
| `acc_finance_transaction` | ISTNIEJE — Surowe zdarzenia finansowe Amazon (wymaga UNIQUE constraint) |
| `acc_finance_ledger` | ISTNIEJE — Księga podwójnego zapisu |
| `acc_finance_account` | ISTNIEJE — Plan kont |
| `acc_finance_settlement` | ISTNIEJE — Podsumowania rozliczeń |
| `acc_finance_payout_reconciliation` | ISTNIEJE — Rekoncyliacja bankowa |

### Zamówienia
| Tabela | Przeznaczenie |
|---|---|
| `acc_order` | ISTNIEJE — Nagłówek zamówienia z kaskadą zysków (★★★★★) |
| `acc_order_line` | ISTNIEJE — Pozycje zamówienia z rozkładem kosztów |
| `acc_order_sync_state` | ISTNIEJE — Watermark synca per-marketplace |

### Reklamy
| Tabela | Przeznaczenie |
|---|---|
| `acc_ads_profile` | ISTNIEJE |
| `acc_ads_campaign` | ISTNIEJE |
| `acc_ads_campaign_day` | ISTNIEJE — Dzienne metryki kampanii |
| `acc_ads_product_day` | ISTNIEJE — Dzienne metryki poziomu ASIN |

### Raporty
| Tabela | Przeznaczenie |
|---|---|
| `acc_report_request` | NOWA — Centralne śledzenie wszystkich żądań raportów |

### Feedy
| Tabela | Przeznaczenie |
|---|---|
| `acc_feed_submission` | NOWA — Centralne śledzenie wszystkich submisji feedów |

### Notyfikacje / Eventy
| Tabela | Przeznaczenie |
|---|---|
| `acc_event_log` | ISTNIEJE — Wszystkie eventy (★★★★★) |
| `acc_event_processing_log` | ISTNIEJE — Audyt wykonania handlerów |
| `acc_event_handler_health` | ISTNIEJE — Stan circuit breakera |
| `acc_event_destination` | ISTNIEJE — Destynacje SQS |
| `acc_event_subscription` | ISTNIEJE — Subskrypcje notyfikacji |

### Alerty / Sprawy
| Tabela | Przeznaczenie |
|---|---|
| `acc_al_rule` | ISTNIEJE — Definicje reguł alertów |
| `acc_al_alert` | ISTNIEJE — Wyzwolone alerty |
| `acc_system_alert` | ISTNIEJE — Alerty systemowe |
| `acc_fba_case` | ISTNIEJE — Sprawy wsparcia FBA |
| `acc_guardrail_results` | ISTNIEJE — Wyniki kontroli zdrowia |

### Zadania
| Tabela | Przeznaczenie |
|---|---|
| `acc_job` | ISTNIEJE — Uruchomienia zadań + status + retry |
| `acc_job_run` | ISTNIEJE (ORM) — Śledzenie zadań (na bazie ORM) |

---

# 10. Mapa Drogowa Wdrożenia

## Faza 1: Fundament (Konieczne) — 4-6 tygodni

### Cele
- Wyeliminować ryzyka uszkodzenia danych
- Ustanowić jedno źródło prawdy dla zysku
- Stworzyć współdzieloną infrastrukturę
- Naprawić kompatybilność z Python 3.12+

### Zadania

| # | Zadanie | Pracochłonność | Zależności | Ryzyko |
|---|---|---|---|---|
| 1.1 | **Dodać UNIQUE constraint na `acc_finance_transaction`** (`posted_date, amazon_order_id, sku, charge_type, amount`) | 1 dzień | Najpierw zdeduplikować istniejące dane | Migracja schematu na żywej DB |
| 1.2 | **Naprawić upsert synca magazynowego** — zamienić `db.add()` na MERGE w `sync_inventory.py` | 1 dzień | Brak | Niskie |
| 1.3 | **Naprawić przestarzałe wzorce async** — zamienić `asyncio.get_event_loop().run_until_complete()` na `asyncio.run()` w 4 jobach Celery | 0.5 dnia | Brak | Przetestować wykonanie tasków Celery |
| 1.4 | **Wyekstrahować współdzielone narzędzia** — `_f()`, `_i()`, `_mkt_code()`, `_fetchall_dict()`, `_connect()`, cache in-memory → `platform/shared/` | 3 dni | Dotyka 8+ plików | Ryzyko regresji w serwisach |
| 1.5 | **Wyeliminować legacy ścieżkę zysku** — usunąć `profit_service.py`, migrować testy na silnik V2 | 2 dni | Zweryfikować brak wywołań produkcyjnych | Niskie |
| 1.6 | **Podzielić `scheduler.py`** na moduły domenowe (orders, finance, inventory, ads, profit, content, logistics, strategy) | 3 dni | Brak | Nie wolno zmienić zachowania harmonogramu |
| 1.7 | **Skonsolidować zarządzanie schematami** — wyekstrahować wszystkie DDL `ensure_*_schema()` do skryptów migracyjnych, uruchamiać przez jedno `ensure_all_schemas()` | 5 dni | Audyt wszystkich 14+ plików schematów | Musi być idempotentne |
| 1.8 | **Naprawić `Alert.marketplace_id` i `JobRun.marketplace_id`** — dodać constrainty FK | 1 dzień | Oczyszczanie danych jeśli istnieją sieroty | Niskie |
| 1.9 | **Oczyścić root projektu** — przenieść pliki `tmp_*`, `_*`, `*.log` do `_archive/` lub usunąć | 0.5 dnia | Potwierdzić, że nic nie jest referowane | Bardzo niskie |
| 1.10 | **Dodać brakujące UNIQUE constrainty** — `acc_inventory_snapshot(product_id, marketplace_id, snapshot_date)`, `acc_offer(sku, marketplace_id)` | 1 dzień | Zdeduplikować istniejące dane | Migracja schematu |

### Faza 1 Razem: ~18 dni

---

## Faza 2: Architektura (Wysokie przełożenie) — 6-10 tygodni

### Cele
- Ustanowić kanoniczny model produktu
- Zbudować wewnętrzny bus eventów domenowych
- Zdekomponować God moduły
- Stworzyć zunifikowane centrum akcji

### Zadania

| # | Zadanie | Pracochłonność | Zależności | Ryzyko |
|---|---|---|---|---|
| 2.1 | **Zaprojektować i wdrożyć Kanoniczny Model Produktu** — tabele `acc_canonical_product` + `acc_marketplace_presence`, migracja z `acc_product` + `acc_offer` | 10 dni | Faza 1 ukończona | Wysokie — dotyka wszystkiego |
| 2.2 | **Zbudować zunifikowany Marketplace Mapping Engine** — jedno wyszukiwanie: `(sku, marketplace) → canonical_product` | 5 dni | 2.1 | Średnie |
| 2.3 | **Rozszerzyć Event Backbone o wewnętrzne eventy domenowe** — moduły ingestii emitują eventy `{domain}.{action}` po zakończeniu batch; moduły intelligence subskrybują | 5 dni | Współdzielone narzędzia z Fazy 1 | Średnie |
| 2.4 | **Podzielić `profit_engine.py`** na calculator + query + export + cost_model (4 pliki) | 5 dni | 1.5 (usunięty legacy profit) | Średnie — refaktoryzacja 6632 linii |
| 2.5 | **Podzielić `content_ops.py`** na tasks + versions + publish + policy + compliance (5 plików) | 5 dni | Brak | Średnie |
| 2.6 | **Podzielić `fba_ops/service.py`** na overview + replenishment + inbound + cases + launches (5 plików) | 3 dni | Brak | Niskie |
| 2.7 | **Zbudować Action Center** — zunifikowana brama zapisu ze śladem audytowym, circuit breakerem, rate limitingiem | 5 dni | 2.3 (event bus) | Średnie |
| 2.8 | **Zbudować `ingestion/listings.py`** — zunifikować ingestię listingów: raportowy bulk sync + eventy real-time + mapowanie rejestru | 5 dni | 2.1 (kanoniczny produkt) | Średnie |
| 2.9 | **Zbudować `ingestion/inventory.py`** — jedna ścieżka z SP-API + Reports → znormalizowany magazyn z upsert | 3 dni | 1.2, 2.1 | Niskie |
| 2.10 | **Scalić funkcje zapytań `profitability_service` do modułu profit query** | 2 dni | 2.4 | Niskie |

### Faza 2 Razem: ~48 dni

---

## Faza 3: Zaawansowane / Optymalizacja — 8-12 tygodni

### Cele
- Pełna architektura event-driven
- Automatyczna realizacja decyzji
- Inteligencja konkurencyjna
- Utwardzenie produkcyjne

### Zadania

| # | Zadanie | Pracochłonność | Zależności | Ryzyko |
|---|---|---|---|---|
| 3.1 | **Repricing Decision Engine** — dynamiczne strategie repricingu, śledzenie konkurencji, algorytmy uwzględniające marżę | 15 dni | Faza 2 kanoniczny produkt + pricing | Wysokie — bezpośredni wpływ na P&L |
| 3.2 | **Competitor Intelligence Store** — śledzenie ofert wszystkich sprzedawców, trendy win-rate BuyBox, krajobraz konkurencyjny | 10 dni | Faza 2 pricing intelligence | Średnie |
| 3.3 | **Inventory Risk Engine** — prawdopodobieństwo braków, koszt nadmiernego stanu, modelowanie odpisów na starzenie | 8 dni | Faza 2 kanoniczny produkt + magazyn | Średnie |
| 3.4 | **Content Optimization Engine** — scoring treści, analiza SEO, benchmarking treści konkurencji | 10 dni | Faza 2 podział content ops | Średnie |
| 3.5 | **Implementacja topologii SQS** — kolejki ingestion-complete, actions, alerts z DLQ | 5 dni | Faza 2 event bus | Średnie |
| 3.6 | **Zbudować kompletny suite testów** — cel 60%+ pokrycia na krytycznych ścieżkach (zysk, finanse, zamówienia) | 15 dni | Faza 2 ukończona | Niskie |
| 3.7 | **Connection pooling** dla klientów SP-API/Ads — współdzielony httpx.AsyncClient per connector | 3 dni | Brak | Niskie |
| 3.8 | **Refund / Fee Anomaly Engine** — detekcja skoków, identyfikacja seryjnych zwrotów, reklamacje reimbursement | 8 dni | Faza 2 ukończona | Średnie |
| 3.9 | **Backend konsoli operatora** — zunifikowany kanał alertów, zarządzanie sprawami, workflow zatwierdzania | 10 dni | Faza 2 action center | Średnie |
| 3.10 | **Buy Box Radar** — trendy win-rate, alerty konkurencyjne, śledzenie pozycji | 5 dni | 3.2 (sklep konkurencyjny) | Niskie |

### Faza 3 Razem: ~89 dni

---

# 11. Szybkie Wygrane (Najbliższe 7 dni)

| # | Zmiana | Wpływ | Pracochłonność | Gdzie |
|---|---|---|---|---|
| 1 | **Naprawić 4 przestarzałe wywołania `asyncio.get_event_loop()`** → `asyncio.run()` | Zapobiega awarii na Python 3.12+ | 2 godziny | `jobs/calc_profit.py`, `sync_finances.py`, `sync_inventory.py`, `sync_purchase_prices.py` |
| 2 | **Dodać `UNIQUE` constraint na `acc_finance_transaction`** | Zapobiega duplikatom danych finansowych | 4 godziny (dedup + migracja) | `models/finance.py` + skrypt migracyjny |
| 3 | **Naprawić `sync_inventory.py` na MERGE/upsert** zamiast `db.add()` | Zapobiega zduplikowanym snapshotom magazynowym | 3 godziny | `jobs/sync_inventory.py` |
| 4 | **Usunąć `profit_service.py`** (najpierw zweryfikować brak produkcyjnych wywołań) | Eliminuje zamieszanie o kanoniczną ścieżkę zysku | 2 godziny | `services/profit_service.py` |
| 5 | **Oczyścić root projektu** — przenieść 95 `tmp_*` + 30 `_*` + 18 `*.log` do `_archive/` | Zmniejsza obciążenie poznawcze, wyjaśnia codebase | 1 godzina | Root projektu |
| 6 | **Wyekstrahować `_f`, `_i`, `_mkt_code`, `_fetchall_dict`, `_connect`** do `app/platform/shared/db.py` | Eliminuje 8-krotną duplikację | 1 dzień | 8+ plików serwisowych |
| 7 | **Zmniejszyć interwał pollowania SQS** z 2 min do 30 sek | Szybsza reakcja na zmiany ofert/zamówień | 30 min | `scheduler.py` |

---

# 12. Brutalna Prawda

**ACC to imponujące narzędzie operacyjne, które przerosło swoją architekturę.**

Codebase ma ~100 tys. linii Pythona, 425 endpointów API, 50+ tabel i prawdziwą głębię domenową obejmującą zamówienia, finanse, ceny, treści, logistykę, rodziny, reklamy, compliance podatkowy i strategię. To nie jest zabawka. Obsługuje 9 marketplace'ów EU Amazon z prawdziwą księgowością finansową denominowaną w PLN.

**Ale to jeszcze nie jest Command Center.**

To **kolekcja pionowych wycinków** — każdy wycinek (zamówienia, finanse, treści, FBA, ceny, logistyka, strategia) został zbudowany od początku do końca z własnym modelem danych, własną logiką synca, własnymi wzorcami zapytań i własnym cache in-memory. Katalog `services/` ma 80+ plików, wiele przekraczających 1000 linii, ze zduplikowanymi funkcjami pomocniczymi i zagnieżdżonym DDL. Nie ma horyzontalnej architektury łączącej te wycinki.

**Najwyższy pojedynczy dług to brak kanonicznego modelu produktu.** Pięć różnych tabel (`acc_product`, `acc_offer`, `acc_listing_state`, `global_family`, `acc_amazon_listing_registry`) reprezentuje "produkt" z różnych perspektyw i żaden serwis ich nie unifikuje. Dopóki to nie zostanie naprawione, każdy moduł będzie nadal budował własne wyszukiwanie produktu, a inteligencja cross-market pozostanie krucha.

**Drugi najwyższy dług to kalkulacja zysku.** Trzy ścieżki kodu obliczają zysk i nikt nie jest w stanie definitywnie odpowiedzieć, która jest autorytatywna. `profit_engine.py` o 6632 liniach musi zostać podzielony, a legacy ścieżki muszą zostać usunięte.

**Trzeci najwyższy dług to fakt, że event backbone — najlepiej zaprojektowany moduł w codebase — jest prawie nieużywany.** 90% przepływu danych idzie przez crony sprzężone czasowo (`02:00 → 03:00 → 04:00 → 05:00`). System ma SQS, ma handlery, ma dedup, ma circuit breakery — a potem przepuszcza prawie wszystko przez `scheduler.py` w stałych interwałach czasowych. Przyszłość event-driven jest architektonicznie obecna, ale operacyjnie uśpiona.

**Co musi się zmienić najpierw:**
1. Kanoniczny model produktu — bez niego nic innego nie złoży się poprawnie.
2. Jedna ścieżka zysku — zaufanie finansowe jest nienegocjowalne.
3. Wewnętrzne eventy domenowe — backbone istnieje; przepuścić przez niego wszystko.
4. Dekompozycja God modułów — `profit_engine`, `content_ops`, `fba_ops` i `mssql_store` muszą zostać podzielone zanim jakikolwiek zespół będzie mógł pracować równolegle bez konfliktów merge.

**Moduł tax_compliance to wzór, do którego powinien dążyć cały codebase:** 11 plików, każdy z jasną ograniczoną odpowiedzialnością, czystymi interfejsami, właściwym rozdzieleniem domen. Użyjcie go jako referencyjnej architektury do refaktoryzacji wszystkiego innego.
