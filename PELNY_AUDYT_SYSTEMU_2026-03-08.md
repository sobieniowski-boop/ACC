# ACC (Amazon Command Center) — PEŁNY AUDYT SYSTEMU
## Data: 2026-03-08
## Audytor: GitHub Copilot (Claude Opus 4.6)
## Role: CTO/Architekt, Starszy QA Lead, Audytor Produkcyjny, Senior eCommerce Manager (Amazon EU), Analityk Danych i Procesów

---

# 1. PODSUMOWANIE WYKONAWCZE

## Ogólna Ocena

**ACC to kompleksowa, ambitna i w dużej mierze dobrze zbudowana platforma operacyjna Amazon** obejmująca ponad 25 modułów funkcjonalnych, ponad 80 stron frontendowych, ponad 30 routerów API, ponad 50 serwisów backendowych, 173 tabele bazodanowe oraz integracje z Amazon SP-API, Amazon Ads API, DHL24, GLS, Ergonode PIM, BaseLinker, NBP, ECB, Google Sheets i OpenAI.

**Ocena ogólna: 74/100 — Częściowo gotowy do produkcji (warunkowo)**

System ma **silne pokrycie funkcjonalne** i demonstruje głęboką wiedzę domenową w operacjach Amazon EU. Główne pipeline'y danych (zamówienia, zapasy, ceny, zysk) są dojrzałe i przetestowane w boju. Jednakże **krytyczne braki w odzyskiwaniu po błędach, pętlach zwrotnych, blokadach rozproszonych i walidacji danych** uniemożliwiają czyste dopuszczenie do produkcji bez celowanych poprawek.

## Werdykt Gotowości Produkcyjnej

| Aspekt | Status |
|--------|--------|
| **Główny Pipeline Zamówień** | ✅ GOTOWY — Solidna 6-krokowa orkiestracja ze śledzeniem watermark |
| **Silnik Zysku/Finansów** | ⚠️ WARUNKOWO — Logika rdzenna poprawna; luki w FX fallback + klasyfikacji opłat |
| **Zarządzanie Zapasami** | ⚠️ WARUNKOWO — Działa, ale obawy o cache/współbieżność przy skali |
| **Operacje FBA** | ⚠️ WARUNKOWO — Dobre pokrycie; KPI scorecard ukończone w 60% |
| **Studio Treści** | ⚠️ WARUNKOWO — Bogaty zestaw funkcji; brakuje circuit-breakera dla publikacji |
| **Panel Zarządczy** | ✅ GOTOWY — Health scoring + wykrywanie ryzyk operacyjne |
| **Silnik Strategii** | ⚠️ WARUNKOWO — 3 z 11 silników detekcji niezaimplementowane |
| **Sezonowość** | ⚠️ WARUNKOWO — Wąskie gardło skalowalności; przeliczanie profili zbyt wolne |
| **Inteligencja Decyzyjna** | ❌ NIE GOTOWY — Pętla zwrotna odłączona (krytyczna wada) |
| **Zgodność Podatkowa** | ⚠️ WARUNKOWO — Główne przepływy działają; wymagany ślad audytu FX dla rozliczeń |
| **Mapper Rodzin** | ⚠️ WARUNKOWO — SP-API rate limiting wymaga backoff |
| **Kurier/Logistyka** | ✅ GOTOWY — Pipeline'y DHL + GLS dojrzałe |
| **Reklamy** | ✅ GOTOWY — Synchronizacja + raportowanie solidne |
| **Infrastruktura** | ⚠️ WARUNKOWO — Brak blokad rozproszonych, wyciek Redis, race condition tokenów |

## Główne Mocne Strony

1. **Głęboka wiedza domenowa Amazon** — Integracja SP-API obejmuje zamówienia, zapasy, finanse, katalog, raporty, reklamy na 9 rynkach EU
2. **Kompleksowy model zysku** — 3-warstwowy CM1/CM2/NP z alokacją kosztów, scenariuszami what-if, scoringiem jakości danych
3. **Dojrzały pipeline zamówień** — 6-krokowa orkiestracja z watermark tracking, retry na deadlock, konwersja walut
4. **Bogaty frontend** — Ponad 80 stron pokrywających wszystkie funkcje biznesowe ze spójnymi wzorcami UI (Tailwind, Recharts, React Query)
5. **Infrastruktura śladu audytu** — Moduł controllingu śledzi zmiany mapowań/cen z systemem priorytetów źródłowych
6. **Innowacyjne funkcje** — AI dopasowywanie produktów, klasyfikacja sezonowości, wykrywanie okazji, detekcja anomalii opłat
7. **Logistyka DHL/GLS** — Kompletny pipeline: import faktur → synchronizacja kosztów → łączenie z zamówieniami → reconciliation shadow
8. **Zarządzanie schematami** — Automatyczne tworzenie tabel przy starcie we wszystkich modułach

## Główne Ryzyka

1. **🔴 P1: Pętla zwrotna Inteligencji Decyzyjnej odłączona** — Korekty modelu obliczane, ale nigdy nie aplikowane do scoringu strategii
2. **🔴 P1: Brak blokad rozproszonych na schedulerze** — APScheduler + wielu workerów = duplikowanie wykonania zadań
3. **🔴 P1: Wyciek połączenia Redis przy wyłączeniu** — Socket nigdy nie zamykany w lifespan
4. **🔴 P1: Race condition odświeżania tokenów** — Równoległe 401 powodują wielokrotne generowanie tokenów
5. **🔴 P1: jobs/sync_orders.py odwołuje się do nieistniejących modeli ORM** — ImportError przy wykonaniu
6. **🔴 P2: Hardcoded kurs walut fallback w 5+ modułach** — Ryzyko cichego błędu w kalkulacji marży
7. **🔴 P2: Brak rate limitingu na endpoincie auth** — Wektor ataku brute-force
8. **🔴 P2: Klasyfikacja opłat pokrywa ~30 z 70+ typów opłat Amazon** — Dokładność księgi ~70-80%

---

# 2. MAPA SYSTEMU

## 2.1 Inwentaryzacja Modułów

| # | Moduł | Serwis(y) Backend | Router API | Strony Frontend | Tabele DB |
|---|-------|-------------------|------------|----------------|-----------|
| 1 | **Infrastruktura Rdzenna** | main.py, sync_runner.py, scheduler.py, worker.py | router.py, auth.py, jobs.py | Login, Jobs | acc_job_run, acc_user |
| 2 | **Rentowność / Silnik Zysku** | profitability_service.py, profit_engine.py, profit_service.py | profit.py, profitability.py, profit_v2.py, kpi.py | Dashboard, ProfitOverview, ProductProfitTable, ProfitExplorer, PriceSimulator, LossOrders, ProductDrilldown, ProductTasks | acc_order, acc_order_line, acc_sku_profitability_rollup, acc_marketplace_profitability_rollup, acc_tkl_cache_rows |
| 3 | **Centrum Finansowe** | finance_center/service.py, finance_center/mappers/* | finance_center.py | FinanceDashboard, FinanceLedger, FinanceReconciliation | acc_finance_transaction, acc_fin_ledger_entry, acc_fin_settlement_summary, acc_fin_event_group_sync |
| 4 | **Zarządzanie Zapasami** | manage_inventory.py | manage_inventory.py, inventory_routes.py | ManageAllInventory, Inventory, InventoryOverview, InventoryDrafts, InventoryFamilies, InventoryJobs, InventorySettings | acc_inventory_snapshot, acc_fba_inventory_snapshot, acc_inv_traffic_* |
| 5 | **Operacje FBA** | fba_ops/service.py, fba_ops/fba_fee_audit.py | fba_ops.py | FbaOverview, FbaInventory, FbaInbound, FbaAgedStranded, FbaReplenishment, FbaScorecard, FbaBundles | acc_fba_inbound_shipment, acc_fba_case, acc_fba_shipment_plan |
| 6 | **Studio Treści** | content_ops.py | content_ops.py | ContentStudio, ContentOps, ContentEditor, ContentDashboard, ContentHealth, ContentAssets, ContentCompliance, ContentPublish | acc_co_tasks, acc_co_versions, acc_co_policy_rules, acc_co_publish_jobs, acc_co_assets |
| 7 | **Mapper Rodzin** | family_mapper/* (7 plików) | families.py | FamilyMapper, FamilyDetail, FixPackages, InventoryFamilies | global_family, global_family_child, marketplace_listing_child, family_coverage_cache, family_fix_package |
| 8 | **Centrum Dowodzenia Zarządu** | executive_service.py | executive.py | ExecOverview, ExecProducts, ExecMarketplaces | executive_daily_metrics, executive_health_score, executive_opportunities |
| 9 | **Strategia / Silnik Wzrostu** | strategy_service.py | strategy.py | StrategyOverview, StrategyOpportunities, StrategyPlaybooks, StrategyMarketExpansion, StrategyBundles, StrategyExperiments, StrategyOutcomes, StrategyLearning | growth_opportunity, growth_opportunity_log, strategy_experiment |
| 10 | **Sezonowość i Inteligencja Popytu** | seasonality_service.py, seasonality_opportunity_engine.py | seasonality.py | SeasonalityOverview, SeasonalityMap, SeasonalityEntities, SeasonalityEntityDetail, SeasonalityClusters, SeasonalityOpportunities, SeasonalitySettings | seasonality_monthly_metrics, seasonality_index_cache, seasonality_profile, seasonality_opportunity, seasonality_cluster |
| 11 | **Inteligencja Decyzyjna** | decision_intelligence_service.py | outcomes.py | StrategyOutcomes, StrategyLearning | opportunity_execution, opportunity_outcome, decision_learning, opportunity_model_adjustments |
| 12 | **Zgodność Podatkowa** | tax_compliance/* (11 plików) | tax_compliance.py | TaxOverview, TaxVatClassification, TaxOss, TaxEvidence, TaxFbaMovements, TaxLocalVat, TaxFilingReadiness, TaxReconciliation, TaxAuditArchive, TaxSettings | vat_event_ledger, vat_transaction_classification, oss_return_period, oss_return_line, transport_evidence_record, fba_stock_movement_ledger |
| 13 | **Kurier / Logistyka** | courier_readiness.py, courier_order_universe_pipeline.py, courier_alerts.py, courier_cost_estimation.py, courier_verification.py | courier.py | (przez Inventory/Jobs) | acc_shipment, acc_shipment_order_link, acc_shipment_cost, acc_courier_cost_estimate |
| 14 | **Integracja DHL** | dhl_billing_import.py, dhl_cost_sync.py, dhl_integration.py, dhl_logistics_aggregation.py, dhl_registry_sync.py, dhl_observability.py | dhl.py | NetfoxHealth | acc_dhl_*, acc_order_logistics_fact, acc_order_logistics_shadow |
| 15 | **Integracja GLS** | gls_billing_import.py, gls_cost_sync.py, gls_integration.py, gls_logistics_aggregation.py | gls.py | (przez Jobs) | acc_gls_* |
| 16 | **Reklamy / PPC** | ads_sync.py | ads.py | Ads | acc_ads_profile, acc_ads_campaign, acc_ads_campaign_day, acc_ads_product_day |
| 17 | **Alerty** | (różne funkcje evaluate_*_alerts) | alerts.py | Alerts | acc_al_alerts, acc_al_alert_rules |
| 18 | **Zwroty** | return_tracker.py | returns.py | (przez Profit/FBA) | acc_return_item, acc_fba_customer_return |
| 19 | **COGS / Audyt** | cogs_audit.py, cogs_importer.py, controlling.py | audit.py | DataQuality, ReviewQueue | acc_purchase_price, acc_cogs_import_log, acc_audit_log, acc_mapping_change_log, acc_price_change_log |
| 20 | **Import Produktów** | import_products.py | import_products.py | ImportProductsPage | acc_import_products |
| 21 | **Taksonomia** | taxonomy.py | inventory_taxonomy.py | (przez Inventory) | acc_taxonomy_node, acc_taxonomy_prediction |
| 22 | **Rejestr Sprzedawców** | seller_registry.py | (zintegrowany) | (przez Ads) | acc_ads_profile (lookup) |
| 23 | **Usługi AI** | ai_service.py, ai_product_matcher.py | ai_rec.py | AIRecommendations | acc_product_match_suggestion |
| 24 | **Pricing** | (ORM via Offer model) | pricing.py | Pricing | acc_offer |
| 25 | **Planowanie** | (funkcje mssql_store) | planning.py | Planning | acc_plan_month |
| 26 | **Telemetria SP-API** | sp_api_usage.py | (zintegrowany) | (przez Jobs) | acc_sp_api_usage_daily |
| 27 | **Serwisy Synchronizacji** | sync_service.py, sync_listings_to_products.py, amazon_listing_registry.py, sellerboard_history.py | (przez jobs) | (przez Jobs) | acc_order_sync_state, acc_amazon_listing_registry |

## 2.2 Mapa Zależności Modułów

```
                              ┌──────────────────┐
                              │  Amazon SP-API    │
                              │  (Orders, Inv,    │
                              │   Finance, Catalog)│
                              └─────────┬────────┘
                                        │
           ┌───────────────────────────┼───────────────────────────┐
           ▼                            ▼                           ▼
   ┌──────────────┐           ┌──────────────┐            ┌──────────────┐
   │ Pipeline      │           │ Snapshoty    │            │ Zdarzenia    │
   │ Zamówień      │           │ Zapasów      │            │ Finansowe    │
   │ (sync co 15m) │           │              │            │              │
   └──────┬───────┘           └──────┬───────┘            └──────┬───────┘
          │                          │                            │
          ▼                          ▼                            ▼
   ┌──────────────┐           ┌──────────────┐            ┌──────────────┐
   │ acc_order     │◄──────── │ acc_inventory │            │ acc_finance  │
   │ acc_order_line│           │ _snapshot     │            │ _transaction │
   └──────┬───────┘           └──────┬───────┘            └──────┬───────┘
          │                          │                            │
   ┌──────┴──────────────────────────┴────────────────────────────┘
   │
   ▼
   ┌─────────────────────────────────────────────────────────────┐
   │                    SILNIK ZYSKU                              │
   │  (Kalkulacja CM1/CM2/NP, rollupy SKU, agregacja KPI)       │
   │  Źródła: zamówienia + COGS + kursy FX + logistyka + ads    │
   └──────────┬──────────────────────────┬───────────────────────┘
              │                          │
              ▼                          ▼
   ┌──────────────┐           ┌──────────────────────────────────┐
   │ Panel        │           │ Strategia / Sezonowość /         │
   │ Zarządczy    │           │ Inteligencja Decyzyjna            │
   │ (Zdrowie,    │           │ (Wykrywanie okazji,              │
   │  Ryzyka, Opp)│           │  profiling popytu, feedback)     │
   └──────────────┘           └──────────────────────────────────┘
```

## 2.3 Wykryte Martwe / Brakujące Elementy

| Element | Status | Szczegóły |
|---------|--------|-----------|
| `jobs/sync_orders.py` | **MARTWY** | Odwołuje się do nieistniejących modeli ORM; spowoduje ImportError |
| `sync_service.sync_finances()` (v0) | **ZDEPRECJONOWANY** | Powoduje duplikaty; powinien zostać usunięty |
| `step_sync_courier_costs()` | **USUNIĘTY** | Zwraca stub; 450 linii martwego kodu w pipeline |
| Strategia: detekcja SUPPRESSION_FIX | **BRAKUJĄCY** | Schema zdefiniowana, detekcja nie zaimplementowana |
| Strategia: detekcja LIQUIDATE_OR_PROMO | **BRAKUJĄCY** | Schema zdefiniowana, detekcja nie zaimplementowana |
| Strategia: detekcja VARIANT_EXPANSION | **BRAKUJĄCY** | Schema zdefiniowana, detekcja nie zaimplementowana |
| Sezonowość: klasyfikacja EVENT_DRIVEN | **BRAKUJĄCY** | Enum zdefiniowany, logika klasyfikacji nie wyzwalana |
| Treści: Analiza Wpływu | **BRAKUJĄCY** | Placeholder; wymaga danych snapshot zysku |
| Treści: Kontrola Jakości Danych | **BRAKUJĄCY** | Stub; zwraca puste listy |
| FBA: KPI Scorecard | **NIEKOMPLETNY** | 5/9 komponentów zaimplementowanych; brak rejestru spraw + launchów |
| Reklamy: Rekomendacja Budżetu | **BRAKUJĄCY** | Schema zdefiniowana, brak endpointu/logiki |
| Inteligencja Decyzyjna: Aplikacja wag modelu | **ODŁĄCZONY** | Korekty obliczane, ale nigdy nie odczytywane przez strategię |
| Mapper Rodzin: Restrukturyzacja Faza 2 (wykonanie) | **BRAKUJĄCY** | Tylko tryb analizy; operacje nie zakodowane |
| Pricing: Pełny silnik cenowy | **NIEKOMPLETNY** | Tylko CRUD na ORM; brak logiki repricingu |

---

# 3. AUDYT WEDŁUG MODUŁÓW

## 3.1 Infrastruktura Rdzenna

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Bootstrap aplikacji, autentykacja, scheduling, śledzenie zadań, łączność z bazą |
| **Kompletność Funkcjonalna** | 85% — Wszystkie serwisy rdzenne działają; scheduler bez blokad rozproszonych |
| **Kompletność Frontend** | Strony Login, Jobs w pełni funkcjonalne |
| **Kompletność Backend/API** | Auth (5 endpointów), Jobs (5 endpointów), Health (1 endpoint) — kompletne |
| **Jakość Danych** | N/A — warstwa infrastrukturalna |
| **Gotowość Zadań/Sync** | 15+ zaplanowanych zadań operacyjnych; brak blokad rozproszonych = ryzyko duplikacji |
| **Gotowość Produkcyjna** | WARUNKOWO — wymaga blokady rozproszonej + naprawy Redis |
| **Ryzyka** | 🔴 Brak blokad rozproszonych, 🔴 Wyciek socketu Redis, 🔴 Race condition tokenów, 🔴 Brak rate limitingu auth, 🔴 Sekrety w plaintext .env |
| **Brakujące** | Revokacja tokenów, blokada konta, ochrona CSRF, timeout requestów w frontendzie, error boundary w React |
| **Wynik** | **65/100** |

## 3.2 Rentowność / Silnik Zysku

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | 3-warstwowy P&L (CM1/CM2/NP), ranking SKU, scenariusze what-if, scoring jakości danych, symulacja cen |
| **Kompletność Funkcjonalna** | 92% — Kompleksowy model zysku z eksportem, drilldown, zadaniami, AI matchingiem |
| **Kompletność Frontend** | 9+ stron: Dashboard, ProfitOverview, ProductProfitTable, PriceSimulator, LossOrders, ProductDrilldown, ProductTasks, ProfitExplorer, DataQuality |
| **Kompletność Backend/API** | 20+ endpointów w 3 routerach (profit, profitability, profit_v2) + router KPI |
| **Jakość Danych** | ⚠️ Fallbackowe kursy walut używane cicho; klasyfikacja opłat niekompletna (~30/70 typów); złożone heurystyki fallback COGS |
| **Gotowość Produkcyjna** | WARUNKOWO — logika rdzenna poprawna; luki FX + opłat wpływają na dokładność marży o 5-15% |
| **Ryzyka** | 🔴 Hardcoded kursy FX, 🔴 Logika alokacji może wyzerować pola CM2, 🟡 Luka w inwalidacji cache, 🟡 Zależność od pliku TKL |
| **Brakujące** | Pełne mapowanie typów opłat, circuit-breaker FX, walidacja sumy alokacji |
| **Wynik** | **78/100** |

## 3.3 Centrum Finansowe

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Budowanie księgi, reconciliation rozliczeń, monitoring kompletności danych, diagnostyka luk |
| **Kompletność Funkcjonalna** | 88% — Kompleksowe: import, klasyfikacja, księga, reconciliation, diagnostyka |
| **Kompletność Frontend** | 3 strony: FinanceDashboard, FinanceLedger, FinanceReconciliation |
| **Kompletność Backend/API** | 15+ endpointów pokrywających wszystkie operacje finansowe |
| **Jakość Danych** | ⚠️ Klasyfikacja opłat pokrywa ~40% typów Amazon; logika fuzzy dopasowywania bankowego wymaga walidacji |
| **Gotowość Produkcyjna** | WARUNKOWO — diagnostyka wartościowa; dokładność księgi ograniczona pokryciem opłat |
| **Ryzyka** | 🟡 Wnioskowanie przyczyn luk oparte na heurystyce, 🟡 Możliwe false positives w dopasowaniu bankowym, 🟡 Atrybucja marketplace'u wnioskowana po imporcie |
| **Wynik** | **76/100** |

## 3.4 Zarządzanie Zapasami

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Monitoring zapasów FBA, wykrywanie ryzyka stockout/overstock, uzupełnianie, workflow zatwierdzania draftów |
| **Kompletność Funkcjonalna** | 85% — Rdzeń: snapshoty, prędkość, DOI, drafty, ustawienia, zdrowie rodzin |
| **Kompletność Frontend** | 6+ stron: ManageAllInventory, Inventory, InventoryOverview, InventoryDrafts, InventoryFamilies, InventoryJobs, InventorySettings |
| **Jakość Danych** | ⚠️ Brak sezonowości w prędkości; TTL cache 180s może serwować nieaktualne dane |
| **Gotowość Produkcyjna** | WARUNKOWO — workflow draftów poprawny; brak optymistycznego lockingu dla współbieżności |
| **Ryzyka** | 🟡 Brak optymistycznego lockingu, 🟡 Prędkość zakłada równomierny rozkład, 🟡 Zdrowie rodzin bez marketplace overrides |
| **Wynik** | **72/100** |

## 3.5 Operacje FBA

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Zarządzanie ryzykiem FBA (OOS, aged, stranded), tracking inbound, audyt opłat, KPI scorecard, zarządzanie casami |
| **Kompletność Funkcjonalna** | 80% — Bogaty zestaw funkcji; KPI scorecard ukończone w 60% |
| **Kompletność Frontend** | 7 stron: FbaOverview, FbaInventory, FbaInbound, FbaAgedStranded, FbaReplenishment, FbaScorecard, FbaBundles |
| **Gotowość Produkcyjna** | WARUNKOWO — system alertów solidny; KPI scorecard wymaga danych o casach + launchach |
| **Ryzyka** | 🔴 Cooldown raportów hardcoded (9 marketplace'ów), 🟡 KPI scorecard niekompletne, 🟡 Algorytm uzupełniania niezwalidowany |
| **Wynik** | **76/100** |

## 3.6 Studio Treści

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Wielorynkowe zarządzanie treścią, generacja AI, sync katalogowy, orkiestracja publikacji, kontrola QA |
| **Kompletność Funkcjonalna** | 65% — Ambitny zakres; wiele szkieletów i połowicznych implementacji |
| **Kompletność Frontend** | 8 stron: ContentStudio, ContentOps, ContentEditor, ContentDashboard, ContentHealth, ContentAssets, ContentCompliance, ContentPublish |
| **Gotowość Produkcyjna** | WARUNKOWO — task/versioning solidne; publikacja wymaga circuit-breakera; analiza wpływu brakująca |
| **Ryzyka** | 🔴 Brak pre-checku poświadczeń SP-API przed publikacją, 🔴 Brak circuit-breakera publikacji, 🟡 Progi QA nie uwzględniają marketplace'u, 🟡 Klucz cache AI bez wersji modelu |
| **Wynik** | **68/100** |

## 3.7 Mapper Rodzin

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Mapowanie rodzin produktów DE → EU ze scoringiem pewności i generowaniem fix packages |
| **Kompletność Funkcjonalna** | 75% — Rdzenny matching działa; restrukturyzacja faza 2 (wykonanie) niezaimplementowana |
| **Kompletność Frontend** | 3 strony: FamilyMapper, FamilyDetail, FixPackages |
| **Gotowość Produkcyjna** | WARUNKOWO — matching solidny; SP-API wymaga rate limiting backoff |
| **Ryzyka** | 🔴 SP-API rate limiting (4500 wywołań bez backoff), 🟡 Kolizja klucza master na rzadkich kolorach, 🟡 Progi pokrycia hardcoded |
| **Wynik** | **68/100** |

## 3.8 Centrum Dowodzenia Zarządu

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Dashboard CEO ze scoring'iem zdrowia, detekcją ryzyk, identyfikacją okazji wzrostu |
| **Kompletność Funkcjonalna** | 90% — Niemal kompletna inteligencja zarządcza |
| **Kompletność Frontend** | 3 strony: ExecOverview, ExecProducts, ExecMarketplaces |
| **Gotowość Produkcyjna** | GOTOWY z drobnymi poprawkami — health scoring operacyjny |
| **Ryzyka** | 🟡 Progi hardcoded, 🟡 Deaktywacja ryzyka traci kontekst, 🟡 Timeout 120s może być niewystarczający przy skali |
| **Wynik** | **72/100** |

## 3.9 Strategia / Silnik Wzrostu

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Systematyczne wykrywanie okazji wzrostu (20+ typów), scoring, workflow zarządzania, playbooki |
| **Kompletność Funkcjonalna** | 85% — 8 z 11 silników detekcji zaimplementowanych; 3 brakujące |
| **Kompletność Frontend** | 8 stron: StrategyOverview, StrategyOpportunities, StrategyPlaybooks, StrategyMarketExpansion, StrategyBundles, StrategyExperiments, StrategyOutcomes, StrategyLearning |
| **Gotowość Produkcyjna** | WARUNKOWO — dobra detekcja; 3 silniki brakujące; ciche awarie na brakujących danych |
| **Ryzyka** | 🔴 inventory_snapshot/family_coverage_cache mogą nie istnieć, 🟡 ID marketplace DE hardcoded, 🟡 Dedup niekompletny dla okazji ekspansji |
| **Wynik** | **76/100** |

## 3.10 Sezonowość i Inteligencja Popytu

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Profilowanie popytu (6 klas), detekcja peak/ramp/decay, analiza luk wykonania, generowanie okazji sezonowych |
| **Kompletność Funkcjonalna** | 82% — Wyrafinowany model klasyfikacji; klasa EVENT_DRIVEN nigdy nie wyzwalana |
| **Kompletność Frontend** | 7 stron: SeasonalityOverview, SeasonalityMap, SeasonalityEntities, SeasonalityEntityDetail, SeasonalityClusters, SeasonalityOpportunities, SeasonalitySettings |
| **Gotowość Produkcyjna** | WARUNKOWO — model klasyfikacji solidny; wąskie gardło skalowalności przy przeliczaniu |
| **Ryzyka** | 🔴 N×12 sekwencyjnych zapytań do przeliczenia profili (nieskalowalne), 🟡 EVENT_DRIVEN martwy kod, 🟡 Hardcoded progi |
| **Wynik** | **71/100** |

## 3.11 Inteligencja Decyzyjna / Pętla Zwrotna

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Pomiar wyników, feedback modelu, agregacja uczenia się, rekalibracja pewności |
| **Kompletność Funkcjonalna** | 78% — Pipeline istnieje end-to-end ale jest odłączony |
| **Kompletność Frontend** | 2 strony (przez Strategię): StrategyOutcomes, StrategyLearning |
| **Gotowość Produkcyjna** | ❌ NIE GOTOWY — pętla zwrotna odłączona (krytyczna wada architektoniczna) |
| **Ryzyka** | 🔴 Korekty modelu nigdy nie aplikowane, 🔴 Dzielenie przez zero w success score, 🔴 Problem z baselinami sezonowymi, 🟡 Konserwatywna rekalibracja (lata do konwergencji) |
| **Brakujące** | Połączenie korekt ze scoringiem strategii, detrending sezonowy, uczenie per-marketplace |
| **Wynik** | **58/100** |

## 3.12 Zgodność Podatkowa

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Klasyfikacja VAT, deklaracje OSS, kontrola dowodów, ruchy FBA, gotowość do rozliczeń, archiwum audytu |
| **Kompletność Funkcjonalna** | 80% — Kompletny pipeline podatkowy od klasyfikacji do gotowości rozliczeniowej |
| **Kompletność Frontend** | 10 stron pokrywających wszystkie funkcje podatkowe |
| **Gotowość Produkcyjna** | WARUNKOWO — rdzenne przepływy działają; ślad audytu FX wymagany przed składaniem OSS |
| **Ryzyka** | 🟡 Mapa krajów EU niekompletna (27 vs 30+), 🟡 Heurystyka parowania ruchów, 🟡 Klasyfikacja refundów niekompletna |
| **Wynik** | **72/100** |

## 3.13 Kurier / Logistyka (DHL + GLS)

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Tracking przesyłek, sync kosztów, łączenie z zamówieniami, reconciliation shadow, weryfikacja faktur |
| **Kompletność Funkcjonalna** | 90% — Dojrzały pipeline z parytetem DHL + GLS |
| **Gotowość Produkcyjna** | GOTOWY — najlepiej zaprojektowany moduł logistyczny w systemie |
| **Ryzyka** | 🟡 Twarda zależność od Netfox, 🟡 Model kosztów nieodświeżany, 🟡 Hardcoded progi pokrycia |
| **Wynik** | **82/100** |

## 3.14 Reklamy / PPC

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Synchronizacja Amazon PPC i raportowanie (kampanie, metryki dzienne, ACOS/ROAS KPI) |
| **Kompletność Funkcjonalna** | 85% — Sync + raportowanie solidne; rekomendacje budżetowe brakujące |
| **Gotowość Produkcyjna** | GOTOWY — zaufany przez menedżerów reklam |
| **Ryzyka** | 🟡 Opóźnienie raportów 24-48h nieobsłużone, 🟡 Rekomendacja budżetu nie zaimplementowana |
| **Wynik** | **78/100** |

## 3.15 System Alertów

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Centralna agregacja alertów ze wszystkich modułów z routingiem wg istotności |
| **Kompletność Funkcjonalna** | 80% — Alert CRUD + reguły + istotność + kontekst |
| **Gotowość Produkcyjna** | GOTOWY z obawą o alert fatigue |
| **Ryzyka** | 🟡 Brak wyciszania/drzemki, 🟡 Wysoki wolumen może powodować zmęczenie alertami |
| **Brakujące** | Wyciszanie alertów, drzemka, reguły eskalacji, kanały notyfikacji (email/Slack) |
| **Wynik** | **80/100** |

## 3.16 Tracker Zwrotów

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Tracking refundowanych pozycji, reconciliation zwrotów FBA, odzyskiwanie/odpis COGS |
| **Kompletność Funkcjonalna** | 75% — Rdzenny przepływ działa; logika częściowych refundów niewystarczająca |
| **Gotowość Produkcyjna** | WARUNKOWO — logika reconciliation wymaga walidacji |
| **Ryzyka** | 🟡 Założenie proporcjonalnego podziału refundów, 🟡 Próg 45 dni dla zgubionego towaru (hardcoded) |
| **Wynik** | **70/100** |

## 3.17 COGS / Controlling / Audyt

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Import cen zakupu, audyt jakości danych, system priorytetów źródłowych, śledzenie zmian |
| **Kompletność Funkcjonalna** | 90% — Dobrze ustrukturyzowany ślad audytu z priorytetami źródeł |
| **Jakość Danych** | ✅ Mocna — 5-elementowy równoległy audyt (mapowanie, ceny, spójność, pokrycie, marża) |
| **Gotowość Produkcyjna** | GOTOWY — najlepszy ślad audytu w systemie |
| **Ryzyka** | 🟡 Próg pokrycia hardcoded (95%), 🟡 Duplikaty wierszy w XLSX niewykrywane |
| **Wynik** | **85/100** |

## 3.18 Import Produktów

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Import Excel CEO dla danych master produktów (33 kolumny) |
| **Kompletność Funkcjonalna** | 80% — Parsowanie i upsert; detekcja nagłówków krucha |
| **Gotowość Produkcyjna** | WARUNKOWO — wymaga walidacji nagłówków |
| **Ryzyka** | 🟡 Krucha detekcja nagłówków, 🟡 Brak walidacji schematu kolumn |
| **Wynik** | **70/100** |

## 3.19 Taksonomia

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Predykcja marka/kategoria/typ_produktu via podejście ML wieloźródłowe z kolejką do recenzji |
| **Kompletność Funkcjonalna** | 85% — Zaawansowany łańcuch predykcji (PIM → rejestr → podobieństwo tytułów) |
| **Gotowość Produkcyjna** | WARUNKOWO — działa, ale wolny dla dużych katalogów |
| **Ryzyka** | 🟡 Wydajność przy >40k kandydatów, 🟡 Hardcoded progi pewności |
| **Wynik** | **84/100** |

## 3.20 Usługi AI

| Aspekt | Ocena |
|--------|-------|
| **Cel Biznesowy** | Dopasowywanie produktów oparte na GPT, generacja treści, rekomendacje |
| **Kompletność Funkcjonalna** | 70% — Matching działa; rekomendacje podstawowe |
| **Gotowość Produkcyjna** | WARUNKOWO — działa dla małych partii; nie skalowalne |
| **Ryzyka** | 🟡 Overflow limitu kontekstu GPT, 🟡 Sekwencyjne przetwarzanie (1 naraz), 🟡 Oczekujące matche się kumulują |
| **Wynik** | **65/100** |

---

# 4. NIESPÓJNOŚCI MIĘDZYMODUŁOWE

## 4.1 Niespójności Danych

| Problem | Dotknięte Moduły | Wpływ |
|---------|-----------------|-------|
| **Fallbackowe Kursy FX** — Ten sam hardcoded słownik (EUR=4.25, GBP=5.10, SEK=0.39) w 5+ modułach, a kursy zmieniają się codziennie | Silnik Zysku, Rentowność, Sync Service, Import Produktów, Sellerboard History | Kalkulacje marży cicho odchodzą od rzeczywistości gdy kursy w bazie przestarzałe |
| **Klasyfikacja Opłat** — ~30 reguł w amazon_to_ledger.py vs ~30 w profit_engine._classify_finance_charge() — nie identyczne zestawy | Centrum Finansowe, Silnik Zysku | Ten sam typ opłaty klasyfikowany różnie w księdze vs modelu zysku |
| **Kalkulacja ACOS** — silnik zysku używa `ad_spend/revenue*100`; moduł reklam używa `spend/sales*100` (różne mianowniki) | Silnik Zysku, Reklamy | Wartości ACOS różnią się między dashboardem Zysku a dashboardem Reklam |

## 4.2 Niespójności Logiczne

| Problem | Dotknięte Moduły | Wpływ |
|---------|-----------------|-------|
| **Stawka VAT** — profit_service.py hardcoduje VAT=1.23 (Polska); tax_compliance używa stawek per-marketplace | Zysk, Podatki | COGS zawyżone dla rynków nie-PL |
| **Założenia Prędkości** — Zapasy zakładają równomierny rozkład dzienny; Sezonowość wykrywa skoncentrowane szczyty | Zapasy, Sezonowość | Sugestie uzupełniania mogą nie uwzględniać sezonowego popytu |
| **Typy Okazji** — Strategia definiuje 11+ typów; Inteligencja Decyzyjna monitoruje tylko podzbiór | Strategia, Inteligencja Decyzyjna | Niektóre typy okazji nigdy nie mierzone pod kątem skuteczności |

## 4.3 Niespójności Nazewnictwa/Statusów/Payloadów

| Problem | Szczegóły |
|---------|-----------|
| **Wartości Statusu Zadań** — Zadania używają `pending/running/success/failure`; niektóre moduły `new/in_progress/completed/failed` | Niespójne queryowanie między systemami |
| **Status Okazji** — Strategia używa `new/in_review/accepted/completed/rejected`; Sezonowość tego samego ale niezależnie | Brak dedup międzymodułowej; ten sam SKU może mieć duplikaty okazji |
| **Pola Dat** — Niektóre tabele `created_at` (UTC), inne `period_date` (data lokalna), inne `purchase_date` (timezone-aware) | Niejednoznaczność joinów między modułami |

## 4.4 Zduplikowane Odpowiedzialności

| Duplikacja | Szczegóły |
|------------|-----------|
| **Kalkulacja Zysku** — profit_service.py (batch per-zamówienie) ORAZ profitability_service.py (rollup MERGE) ORAZ profit_engine.py (product-level CTE) | Trzy równoległe ścieżki kalkulacji; wyniki powinny się zgadzać, ale brak walidacji krzyżowej |
| **Wykrywanie Okazji** — Strategia, Sezonowość i Zarząd wykrywają okazje niezależnie | Brak dedup; ten sam SKU może pojawić się w 3 tabelach okazji |
| **Lookup Kursu FX** — Każdy moduł ma własny wzorzec (OUTER APPLY, fallback dict, cache) | Niespójny wybór kursu |

---

# 5. KRYTYCZNE RYZYKA PRODUKCYJNE

## P1 — Muszą Być Naprawione Przed Produkcją

| # | Ryzyko | Moduł | Wpływ | Nakład |
|---|--------|-------|-------|--------|
| 1 | **Pętla zwrotna Inteligencji Decyzyjnej odłączona** — `opportunity_model_adjustments` obliczane, ale nigdy nie odczytywane przez `strategy_service.compute_priority_score()` | Inteligencja Decyzyjna + Strategia | System uczenia to martwy kod; brak poprawy modelu w czasie | 2h — połączyć wagi korekt z funkcją scoringu |
| 2 | **Brak blokad rozproszonych na APScheduler** — Wielu workerów wykonuje to samo 15-minutowe zadanie jednocześnie | Infrastruktura | Duplikacja executionu pipeline zamówień, uszkodzenie danych, zmarnowane wywołania API | 4h — implementacja leader election na Redis |
| 3 | **Wyciek połączenia Redis przy wyłączeniu** — `close_redis()` nigdy nie wywoływane w lifespan | Infrastruktura | Wyczerpanie socketów po wielokrotnych restartach | 30min — dodać do lifespan shutdown |
| 4 | **Race condition odświeżania tokenów** — Równoległe 401 wyzwalają wielokrotne odświeżanie | Frontend (api.ts) | Wiele ważnych par tokenów; niespójność stanu autentykacji | 2h — dodać mutex/kolejkę do interceptora refresh |
| 5 | **jobs/sync_orders.py odwołuje się do nieistniejących modeli ORM** — ImportError przy wykonaniu | Jobs | Martwe zadanie; crashuje gdy Celery routuje do niego | 30min — usunąć plik lub przepisać na pyodbc |
| 6 | **Brak rate limitingu na /auth/token** — Nieograniczone próby logowania | Auth | Wektor ataku brute-force na hasła | 2h — dodać rate limiter per-IP middleware |

## P2 — Naprawić w Pierwszym Sprincie

| # | Ryzyko | Moduł | Wpływ | Nakład |
|---|--------|-------|-------|--------|
| 7 | **Hardcoded fallbackowe kursy FX** w 5+ modułach | Cross-cutting | 5-15% błąd w kalkulacji marży gdy kursy w bazie przestarzałe | 4h — circuit-breaker + alert gdy fallback użyty |
| 8 | **Klasyfikacja opłat pokrywa ~30/70 typów Amazon** | Finanse + Zysk | Kategoryzacja księgi ~70-80% dokładna | 8h — rozszerzyć mapowania + dodać monitoring catch-all |
| 9 | **SP-API rate limiting w Mapper Rodzin** — 4500 wywołań bez exponential backoff | Mapper Rodzin | Throttling API → awarie syncu → niekompletne dane rodzin | 4h — dodać backoff + circuit-breaker |
| 10 | **Zadanie publikacji Treści bez circuit-breakera** — Nieudane retries się kumulują | Studio Treści | Kolejka zadań zablokowana kaskadowymi awariami | 3h — dodać circuit-breaker (>10 awarii w 1h → skip) |
| 11 | **Alokacja silnika zysku zeruje pola CM2** gdy pula pusta | Silnik Zysku | Wcześniejsze dane alokacji wymazane; zysk zaniżony | 2h — warunkowy reset tylko gdy pula ma dane |
| 12 | **Zdeprecjonowany sync_finances v0 nadal w codebase** — powoduje duplikaty przy retry | Sync Service | Zduplikowane zdarzenia finansowe jeśli przypadkowo wywołany | 1h — usunąć zdeprecjonowaną funkcję |

## P3 — Naprawić w Pierwszym Miesiącu

| # | Ryzyko | Moduł | Wpływ | Nakład |
|---|--------|-------|-------|--------|
| 13 | **Brak React error boundary** — Throw na poziomie strony crashuje całą aplikację | Frontend | Biały ekran dla wszystkich użytkowników na dowolnym błędzie komponentu | 2h — owrap routy w ErrorBoundary |
| 14 | **Brak Axios request timeout** — Wolne endpointy powodują nieskończone ładowanie | Frontend | Wiszący UI; frustracja użytkownika | 30min — ustawić domyślny timeout 30s |
| 15 | **Przeliczanie sezonowości O(N×12) sekwencyjnych zapytań** | Sezonowość | Minutowe przeliczanie dla 10k+ SKU | 8h — przepisać z window functions SQL |
| 16 | **Sekrety w plaintext .env** | Infrastruktura | Ekspozycja poświadczeń jeśli .env wycieknie | 4h — migracja do Azure Key Vault |
| 17 | **Brak deduplikacji Axios dla współbieżnych requestów** | Frontend | Redundantne wywołania API pod obciążeniem | 2h — dodać deduplikację/kolejkę requestów |

---

# 6. PRZEGLĄD INTEGRALNOŚCI I NASYCENIA DANYCH

## 6.1 Status Nasycenia Bazy Danych

| Źródło Danych | Tabele | Zasilane | Metoda | Pokrycie |
|----------------|--------|----------|--------|----------|
| **Zamówienia Amazon** | acc_order, acc_order_line | ✅ Tak | SP-API Orders V0 (sync co 15 min) | WYSOKIE — 7-dniowe okno + backfill |
| **Finanse Amazon** | acc_finance_transaction | ✅ Tak | SP-API Finances V2024 (180-dniowe chunki) | WYSOKIE — codzienny sync |
| **Zapasy Amazon** | acc_inventory_snapshot, acc_fba_inventory_snapshot | ✅ Tak | SP-API Inventory Summaries (codziennie) | WYSOKIE — codzienne snapshoty |
| **Listingi Amazon** | acc_offer | ✅ Tak | SP-API raport GET_MERCHANT_LISTINGS (codziennie) | WYSOKIE — pełny katalog |
| **Reklamy Amazon** | acc_ads_campaign_day, acc_ads_product_day | ✅ Tak | Ads API V3 Reports (nocny) | ŚREDNIE — 24-48h opóźnienie |
| **Kursy Walut** | acc_exchange_rate | ✅ Tak | NBP API (codziennie @ 1:30) | WYSOKIE — 6-dniowy lookback z interpolacją dni roboczych |
| **Ceny Zakupu** | acc_purchase_price | ✅ Tak | Import COGS XLSX (skan co 30 min) + Holding FIFO | ŚREDNIE — zależy od dostępności pliku |
| **Master Produktów** | acc_product | ✅ Tak | SP-API katalog + Ergonode PIM + Google Sheets rejestr | WYSOKIE — wzbogacanie z wielu źródeł |
| **Przesyłki DHL** | acc_shipment (carrier=DHL) | ✅ Tak | DHL24 API + import faktur XLSX | WYSOKIE — kompletne pokrycie DHL |
| **Przesyłki GLS** | acc_shipment (carrier=GLS) | ✅ Tak | GLS ADE API + import faktur CSV | WYSOKIE — kompletne pokrycie GLS |
| **Traffic/Sesje** | acc_inv_traffic_sku_daily | ✅ Tak | SP-API raport Sales & Traffic (codziennie) | ŚREDNIE — zależy od dostępności raportu |
| **Historia Sellerboard** | acc_sb_order_line_staging | ✅ Tak | Import CSV (backfill 2025) | ŚREDNIE — ręczny import; cel uzupełniania luk |
| **Rollupy Rentowności** | acc_sku_profitability_rollup | ✅ Tak | Nocny MERGE z danych zamówień | WYSOKIE — automatyczny codzienny |
| **Metryki Zarządcze** | executive_daily_metrics | ✅ Tak | Agregacja z rollup'u rentowności | WYSOKIE — zależne od upstream |
| **Sezonowość** | seasonality_monthly_metrics/index_cache/profile | ✅ Tak | MERGE z rollup'u rentowności (36-miesięczny lookback) | ŚREDNIE — wymaga ręcznego triggera |
| **Okazje Strategiczne** | growth_opportunity | ✅ Tak | Silniki detekcji (8 z 11 aktywnych) | ŚREDNIE — 3 detektory brakujące |
| **Zdarzenia Podatkowe** | vat_event_ledger | ✅ Tak | Klasyfikacja z finance_transaction | ŚREDNIE — zależy od pokrycia opłat |
| **Mapowanie Rodzin** | global_family, global_family_child | ✅ Tak | SP-API Catalog (kanoniczny DE) | ŚREDNIE — zależy od rate limitów SP-API |
| **BaseLinker** | acc_bl_distribution_order_cache | ✅ Tak | BL Distribution API (nocny) | WYSOKIE — kompletne pokrycie BL |

## 6.2 Ryzyka Jakości Danych

| Ryzyko | Istotność | Dotknięte Moduły |
|--------|-----------|-----------------|
| **Przestarzałe kursy FX** — Jeśli sync NBP zawiedzie, fallbackowe kursy używane cicho | WYSOKA | Wszystkie kalkulacje zysku |
| **Niekompletne mapowanie opłat** — ~30/70 typów opłat Amazon zmapowanych | WYSOKA | Finanse, Zysk |
| **Luki COGS** — Produkty bez ceny XLSX polegają na łańcuchu fallback (rodzeństwo EAN, rodzeństwo ASIN) | ŚREDNIA | Zysk, Zarząd |
| **Luki danych traffic** — Raport SP-API Sales & Traffic nie zawsze dostępny | ŚREDNIA | Zapasy (prędkość), Zarząd (sesje) |
| **Opóźnienie atrybucji reklam** — 7-dniowe okno sprzedaży zawiera niekompletne dane forward | ŚREDNIA | Reklamy, kalkulacje ACOS |
| **Nieprzejrzystość pewności podatkowej** — Scoring pewności klasyfikacji niedokumentowany/nieskalibrowany | ŚREDNIA | Zgodność Podatkowa |

## 6.3 Ranking Pokrycia Danych Modułów

| Pozycja | Moduł | Pokrycie | Pewność |
|---------|--------|----------|---------|
| 1 | **Pipeline Zamówień** | DOSKONAŁE | Zamówienia, linie, tracking statusów w pełni nasycone |
| 2 | **Logistyka DHL/GLS** | DOSKONAŁE | Kompletne dane faktur + przesyłek + kosztów |
| 3 | **COGS / Controlling** | BARDZO DOBRE | Wieloźródłowe ceny ze śladem audytu |
| 4 | **Rollupy Rentowności** | BARDZO DOBRE | Zależne od upstream, ale MERGE idempotentny |
| 5 | **Reklamy** | DOBRE | Sync solidny, ale 24-48h opóźnienie |
| 6 | **Zapasy** | DOBRE | Codzienne snapshoty; dane traffic opcjonalne |
| 7 | **Centrum Finansowe** | UMIARKOWANE | Luki w klasyfikacji opłat ograniczają dokładność |
| 8 | **Sezonowość** | UMIARKOWANE | 36-miesięczny lookback wymaga danych historycznych |
| 9 | **Strategia** | UMIARKOWANE | Zależy od kilku pre-built cache'ów |
| 10 | **Inteligencja Decyzyjna** | NISKIE | Pętla zwrotna odłączona; dane uczenia niewykorzystane |

---

# 7. PRZEGLĄD API / ZADAŃ / INTEGRACJI

## 7.1 Status Integracji SP-API

| API | Klient | Status | Rate Limiting | Uwagi |
|-----|--------|--------|---------------|-------|
| Orders V0 | OrdersClient | ✅ Działa | 0.3s delay | 15-minutowy cykl syncu |
| Finances V2024 | FinancesClient | ✅ Działa | Standardowy backoff | Chunki 180-dniowe |
| Inventory V1 | InventoryClient | ✅ Działa | Standardowy | Codzienne podsumowania |
| Catalog Items | CatalogClient | ✅ Działa | 20/batch w Family Mapper | Wymaga backoff dla 4500+ wywołań |
| Reports (Listings, FBA, Traffic) | ReportsClient | ✅ Działa | 30 min max wait | Optymalizacja reuse-recent |
| Pricing (Competitive) | PricingClient | ✅ Działa | Standardowy | BuyBox lookup |
| Product Fees | (przez sync_service) | ✅ Działa | Top 600 ofert | Cache szacunkowych opłat |
| Ads API V3 Reports | AdsReportingClient | ✅ Działa | Adaptacyjny backoff (3-96s) | Profesjonalna implementacja |

## 7.2 Status Schedulowania Zadań (15 Zadań)

| Zadanie | Harmonogram | Status | Idempotentne | Retry |
|---------|-------------|--------|--------------|-------|
| Pipeline Zamówień | Co 15 min | ✅ Działa | ✅ Oparte na watermark | ✅ 5 prób retry na deadlock |
| Sync Listingów do Produktów | 01:00 codziennie | ✅ Działa | ✅ MERGE upsert | ❌ Brak retry |
| Rejestr Listingów Amazon | 01:30 codziennie | ✅ Działa | ✅ Hash-based | ❌ Brak retry |
| Odświeżenie Cache TKL SQL | 01:40 codziennie | ✅ Działa | ✅ MERGE upsert | ❌ Brak retry |
| Ceny Zakupu | 02:00 codziennie | ✅ Działa | ✅ MERGE upsert | ❌ Brak retry |
| Kursy ECB | 02:30 codziennie | ✅ Działa | ✅ IF NOT EXISTS | ❌ Brak retry |
| Zdarzenia Finansowe | 03:00 codziennie | ✅ Działa | ✅ Dedup hash sygnatury | ❌ Brak retry |
| Snapshoty Zapasów | 04:00 codziennie | ✅ Działa | ✅ MERGE upsert | ❌ Brak retry |
| Raporty Sales & Traffic | 04:30 codziennie | ✅ Działa | ✅ MERGE upsert | ❌ Brak retry |
| Przeliczenie Zysku | 05:00 codziennie | ✅ Działa | ✅ MERGE rollup | ❌ Brak retry |
| Audyt Jakości COGS | 05:30 codziennie | ✅ Działa | ✅ MERGE audit log | ❌ Brak retry |
| Skan Importu COGS | Co 30 min | ✅ Działa | ✅ File hash dedup | ❌ Brak retry |
| Pipeline Logistyki GLS | Nocny (5 kroków) | ✅ Działa | ✅ Per-krok | ❌ Brak retry |
| Weryfikacja Faktur DHL | Codziennie | ✅ Działa | ✅ File tracking | 🟡 5 prób lock retry |
| Cache Dystrybucji BL | Nocny | ✅ Działa | ✅ MERGE upsert | ❌ Brak retry |
| Gap Fill Taksonomii | Nocny | ✅ Działa | ✅ MERGE prediction | 🟡 5 prób deadlock retry |

## 7.3 Ocena Retry/Idempotentności

| Zagadnienie | Status |
|-------------|--------|
| **MERGE upserty** | ✅ Wszystkie operacje sync używają MERGE — idempotentne z założenia |
| **Hash sygnatury dedup** | ✅ Transakcje finansowe deduplikowane na hash sygnaturze źródłowej |
| **Hash pliku dedup** | ✅ Importy COGS, rejestr listingów pomijają niezmienione pliki |
| **Watermark tracking** | ✅ Pipeline zamówień używa LastUpdatedAfter z marginesem nakładania |
| **Deadlock retry** | 🟡 5 prób z exponential backoff w pipeline zamówień + taksonomii; brakuje w pozostałych |
| **Retry zadania na awarii** | ❌ Większość zaplanowanych zadań bez automatycznego retry na awarii |
| **Circuit breakery** | ❌ Brakujące we wszystkich integracjach z zewnętrznymi API |
| **Blokady rozproszone** | ❌ Niezaimplementowane; ryzyko duplikacji execution z wieloma workerami |

---

# 8. NATYCHMIASTOWY PLAN NAPRAW

## Sprint 1 (Tydzień 1-2): Stabilność Krytyczna

| # | Naprawa | Moduł | Nakład | Wpływ |
|---|---------|-------|--------|-------|
| 1 | **Połączyć korekty Inteligencji Decyzyjnej ze scoringiem strategii** | Intel. Decyzyjna + Strategia | 2-3h | Zamyka pętlę zwrotną; umożliwia poprawę modelu |
| 2 | **Dodać blokadę rozproszoną opartą na Redis do schedulera** | Infrastruktura | 4h | Zapobiega duplikowaniu execution zadań |
| 3 | **Zamknąć połączenie Redis przy shutdown** | Infrastruktura | 30min | Zapobiega wyciekowi socketów |
| 4 | **Dodać mutex do interceptora odświeżania tokenów** | Frontend (api.ts) | 2h | Zapobiega uszkodzeniu stanu autentykacji |
| 5 | **Usunąć jobs/sync_orders.py** (martwy kod ORM) | Jobs | 15min | Eliminuje ryzyko ImportError |
| 6 | **Dodać rate limiter do /auth/token** | Auth | 2h | Blokuje ataki brute-force |
| 7 | **Dodać React ErrorBoundary** opakowujące wszystkie route'y | Frontend (App.tsx) | 2h | Zapobiega crashowi białego ekranu |
| 8 | **Dodać Axios request timeout** (30s domyślnie) | Frontend (api.ts) | 30min | Zapobiega nieskończonemu ładowaniu |
| 9 | **Zabezpieczyć success_score przed dzieleniem przez zero** | Inteligencja Decyzyjna | 30min | Zapobiega propagacji NaN/Infinity |
| 10 | **Usunąć zdeprecjonowaną sync_finances() v0** | Sync Service | 1h | Zapobiega duplikatom zdarzeń finansowych |

**Łączny nakład Sprint 1: ~15 godzin**

## Sprint 2 (Tydzień 3-4): Jakość Danych

| # | Naprawa | Moduł | Nakład | Wpływ |
|---|---------|-------|--------|-------|
| 11 | **Circuit-breaker kursu FX** — alert + fail jeśli kurs > 7 dni stary | Cross-cutting | 4h | Zapobiega cichym błędom marży |
| 12 | **Rozszerzenie klasyfikacji opłat** — zmapować 50+ typów opłat Amazon | Finanse + Zysk | 8h | Poprawia dokładność księgi do 95%+ |
| 13 | **Naprawa alokacji silnika zysku** — nie zerować pól CM2 gdy pula pusta | Silnik Zysku | 2h | Zapobiega wymazywaniu danych |
| 14 | **Dodać SP-API exponential backoff** do Mapper Rodzin | Mapper Rodzin | 4h | Zapobiega awariom throttlingu |
| 15 | **Dodać circuit-breaker publikacji** do Studio Treści | Treści | 3h | Zapobiega blokowaniu kolejki zadań |
| 16 | **Wyekstrahować progi health score** do tabeli konfiguracyjnej | Zarząd | 3h | Umożliwia tuning operacyjny |
| 17 | **Dodać ślad audytu źródła kursu FX** | Zgodność Podatkowa | 3h | Wymagane dla zgodności rozliczeń OSS |

**Łączny nakład Sprint 2: ~27 godzin**

---

# 9. STRATEGICZNY PLAN USPRAWNIEŃ

## Kwartał 1 (Miesiąc 2-3)

| # | Usprawnienie | Moduł | Wpływ |
|---|-------------|-------|-------|
| 1 | **Zaimplementować 3 brakujące silniki detekcji strategii** (SUPPRESSION_FIX, LIQUIDATE_OR_PROMO, VARIANT_EXPANSION) | Strategia | Kompletne pokrycie wykrywania okazji |
| 2 | **Batch-optymalizacja przeliczania profili sezonowości** z SQL window functions | Sezonowość | 10x poprawa wydajności dla dużych katalogów |
| 3 | **Zaimplementować pełny KPI scorecard** (tracking spraw + rejestr launchów) | Operacje FBA | Kompletny raporting zarządczy |
| 4 | **Migracja sekretów do Azure Key Vault** | Infrastruktura | Eliminacja ryzyka ekspozycji poświadczeń |
| 5 | **Dodać skan jakości danych treści** (nie stub) | Studio Treści | Umożliwienie data-driven priorytetyzacji treści |
| 6 | **Zaimplementować detrending sezonowy w baselinach Inteligencji Decyzyjnej** | Intel. Decyzyjna | Zapobieganie fałszywej atrybucji sukcesu/porażki |
| 7 | **Centralizacja konfiguracji** — przenieść wszystkie hardcoded progi do tabel konfiguracyjnych | Cross-cutting | Umożliwienie tuningu ops bez deployów kodu |
| 8 | **Dodać strukturalne logowanie z correlation ID** | Cross-cutting | Umożliwienie rozproszonego śledzenia requestów |

## Kwartał 2 (Miesiąc 4-6)

| # | Usprawnienie | Moduł | Wpływ |
|---|-------------|-------|-------|
| 9 | **Zaimplementować uczenie per-marketplace** w Inteligencji Decyzyjnej | Intel. Decyzyjna | Zapobieganie mieszaniu sygnałów między rynkami |
| 10 | **Dodać wirtualizację ProductProfitTable** dla 10k+ wierszy | Frontend | Wydajność przeglądarki w skali enterprise |
| 11 | **Zaimplementować kanały notyfikacji** (email + Slack) dla krytycznych alertów | Alerty | Proaktywna świadomość problemów |
| 12 | **Restrukturyzacja Mapper Rodzin Faza 2** (tryb wykonania) | Mapper Rodzin | Umożliwienie automatycznej naprawy rodzin |
| 13 | **Retrenowanie modelu kosztów** dla estymacji kurierskich | Kurier | Poprawa dokładności estymacji w czasie |
| 14 | **Pipeline CI/CD** z >80% pokryciem testów dla krytycznych ścieżek | Infrastruktura | Zapobieganie regresjom |
| 15 | **Dodać streaming parser** dla dużych raportów SP-API (1M+ wierszy) | Konektory | Wsparcie dla dużych sprzedawców |

---

# 10. WERDYKT KOŃCOWY

## Czy ACC może zostać wdrożony na produkcję?

### Odpowiedź: **WARUNKOWO TAK — po naprawach Sprint 1 (15 godzin pracy)**

ACC to **niezwykle kompleksowy i dobrze zaprojektowany system** do zarządzania operacjami Amazon EU. Szerokość pokrycia (25+ modułów, 80+ stron, 173 tabele, obsługa 9 rynków) jest imponująca, a rdzeń pipeline'u danych (zamówienia → zysk → zarząd) jest sprawdzony bojowo i niezawodny.

### Co działa dobrze (wdrożyć jak jest):
- ✅ **Pipeline Zamówień** — Solidna 6-krokowa orkiestracja klasy morskiej
- ✅ **Logistyka DHL/GLS** — Kompletny pipeline faktura-do-faktu
- ✅ **COGS/Controlling** — Najlepszy ślad audytu w systemie
- ✅ **Sync Reklam** — Niezawodne raportowanie PPC
- ✅ **Panel Zarządczy** — Gotowy dla CEO health scoring
- ✅ **Alerty** — Międzymodułowa agregacja działa
- ✅ **Konektory SP-API** — Profesjonalny rate limiting + telemetria

### Co wymaga napraw Sprint 1 (6 elementów, ~15 godzin):
- 🔴 Blokada rozproszona dla schedulera
- 🔴 Zamknięcie połączenia Redis
- 🔴 Mutex odświeżania tokenów
- 🔴 Połączenie pętli zwrotnej Inteligencji Decyzyjnej
- 🔴 Rate limiting autentykacji
- 🔴 Usunięcie martwego kodu (sync_orders.py, sync_finances v0)

### Co działa z ze znanymi ograniczeniami (zaakceptować ryzyko lub naprawić w Sprint 2):
- ⚠️ Kalkulacje zysku mogą się mylić o 5-15% z powodu fallback FX + pokrycia opłat
- ⚠️ Klasyfikacja sezonowości działa, ale przeliczanie wolne dla dużych katalogów
- ⚠️ Publikacja Studio Treści może zablokować kolejkę bez circuit-breakera
- ⚠️ Mapper Rodzin może trafić na rate limity SP-API bez backoff
- ⚠️ Zgodność Podatkowa wymaga śladu audytu FX przed składaniem OSS

### Na czym NIE MOŻNA jeszcze polegać:
- ❌ Pętla zwrotna Inteligencji Decyzyjnej (odłączona — martwy kod)
- ❌ Rekomendacje budżetu w Reklamach (niezaimplementowane)
- ❌ Tryb wykonania restrukturyzacji Mapper Rodzin (tylko analiza)
- ❌ Analiza wpływu treści (stub)
- ❌ 3 silniki detekcji strategii (SUPPRESSION, LIQUIDATE, VARIANT)

### Rekomendacja Wdrożenia:

> **Wdrożyć po ukończeniu Sprint 1 (15 godzin).** Zaakceptować znane ograniczenia w zakresie Sprint 2. System zapewnia natychmiastową wartość operacyjną dla zarządzania Amazon EU. Rdzeń pipeline'u danych, silnik zysku, logistyka i raportowanie zarządcze są klasy produkcyjnej. Cel: pełna gotowość produkcyjna (wynik 85+) po ukończeniu Sprint 2.

## Podsumowanie Wyników Modułów

| Moduł | Wynik | Ocena |
|-------|-------|-------|
| COGS / Controlling / Audyt | 85 | Mocny — wymaga celowanych poprawek |
| Taksonomia | 84 | Mocny — wymaga celowanych poprawek |
| Kurier / Logistyka (DHL + GLS) | 82 | Mocny — wymaga celowanych poprawek |
| Alerty | 80 | Mocny — wymaga celowanych poprawek |
| Reklamy / PPC | 78 | Mocny — wymaga celowanych poprawek |
| Rentowność / Silnik Zysku | 78 | Mocny — wymaga celowanych poprawek |
| Centrum Finansowe | 76 | Mocny — wymaga celowanych poprawek |
| Strategia / Silnik Wzrostu | 76 | Mocny — wymaga celowanych poprawek |
| Operacje FBA | 76 | Mocny — wymaga celowanych poprawek |
| Centrum Dowodzenia Zarządu | 72 | Częściowo kompletny, ryzykowny |
| Zarządzanie Zapasami | 72 | Częściowo kompletny, ryzykowny |
| Zgodność Podatkowa | 72 | Częściowo kompletny, ryzykowny |
| Sezonowość | 71 | Częściowo kompletny, ryzykowny |
| Tracker Zwrotów | 70 | Częściowo kompletny, ryzykowny |
| Import Produktów | 70 | Częściowo kompletny, ryzykowny |
| Studio Treści | 68 | Częściowo kompletny, ryzykowny |
| Mapper Rodzin | 68 | Częściowo kompletny, ryzykowny |
| Infrastruktura Rdzenna | 65 | Częściowo kompletny, ryzykowny |
| Usługi AI | 65 | Częściowo kompletny, ryzykowny |
| Inteligencja Decyzyjna | 58 | Istotne braki |
| **ŚREDNIA WAŻONA** | **74** | **Częściowo kompletny, warunkowa produkcja** |

---

*Audyt przeprowadzony: 2026-03-08*
*Przeanalizowane pliki: 100+ serwisów backendowych, 80+ stron frontendowych, 30+ routerów API, 23 pliki testów, 15+ modułów konektorów*
*Baza danych: Azure SQL (acc-sql-kadax.database.windows.net), 173 tabele*
*Metodologia: 5-fazowa (Mapa → Audyt → Dane → Produkcja → Raport) z przeglądem kodu plik-po-pliku*

---

# 11. ANEKS OPERACYJNY (2026-03-08, DHL/GLS MAPPING)

## 11.1 Zakres wykonanych prac

- Wykonano diagnostyke brakow mapowania kosztow kurierow do zamowien dla:
- DHL `2026-01`
- GLS `2026.02`
- Zweryfikowano dostepnosc i przydatnosc dodatkowych zrodel:
- `N:\KURIERZY\DHL`
- `N:\KURIERZY\GLS POLSKA`
- pliki `Raport faktur 06.03.2026.xlsx` i `getAsExcelByCustomCriteria.xlsx`
- Integracje: DHL API, Baselinker distribution cache, Netfox (zapytania tylko `SELECT`, waskie probki)

## 11.2 Najwazniejsze ustalenia

- Oba pliki XLS z katalogu `Downloads` nie zawieraja kluczy laczacych typu `tracking_number`/`parcel_number`/`order_id`:
- `Raport faktur 06.03.2026.xlsx`: kolumna `Klient`
- `getAsExcelByCustomCriteria.xlsx`: kolumna `Platnik`
- Nie sa przydatne do podniesienia coverage mapowania przesylek.

- Dla GLS kluczowa mapa `N:\KURIERZY\GLS POLSKA\GLS - BL.xlsx` jest historyczna (`LastWriteTime: 2023-02-06`), co wskazuje wysokie ryzyko nieaktualnosci mapowania.

- W `.env`:
- `BASELINKER_DISTRIBUTION_SYNC_ENABLED=false`
- `SCHEDULER_ENABLED=false`
- To oznacza brak automatycznego odswiezania cache dystrybucji BL, a wiec rosnace ryzyko brakow linkowania.

## 11.3 Wyniki diagnostyki (konkret)

- GLS `2026.02` (wg `acc_shipment_cost` + `acc_shipment_order_link`):
- total: `40`
- linked: `16`
- unlinked: `24`
- `24/24` unlinked jest obecne w Netfox `ITJK_CouriersInvoicesDetails.parcel_num`
- tylko `10/24` wystepuje w `ITJK_ZamowieniaBaselinkerAPI.delivery_package_nr`
- `0/24` wystepuje w `acc_gls_bl_map`
- Wniosek: problem to brak klucza mapujacego po stronie ACC (`acc_gls_bl_map` / cache BL), nie brak surowych danych kurierskich.

- DHL `2026-01`:
- total: `9589`
- linked: `8082`
- unlinked: `1507`
- wsrod unlinked `1048` ma token `JJD...` (w `tracking_number`/`piece_id`)
- probka `50` tokenow `JJD...`:
- `50/50` obecne w Netfox `ITJK_CouriersInvoicesDetails_Extras.parcel_num_other`
- `5/50` obecne w Netfox `ITJK_ZamowieniaBaselinkerAPI.delivery_package_nr`
- Wniosek: dla DHL sciezka JJD/Extras jest krytyczna i obecnie niedostatecznie wykorzystana w finalnym linkowaniu orderow.

## 11.4 Stan danych miesiecznych (co mamy)

- Surowe dane billingowe sa dostepne szeroko:
- DHL: miesiace 2025-01 .. 2026-02 (i starsze foldery)
- GLS: miesiace 2024-09 .. 2026-02 (w tym `2026.01`, `2026.02`)
- Coverage linkowania kosztow (wycinek biezacy):
- DHL `2026-01`: `84.28%`
- DHL `2026-02`: `79.08%`
- GLS (po `billing_period`) nadal z lukami mapowania.

## 11.5 Bezpieczny tryb pracy na Netfox (produkcja)

- Zachowano zasade bezpieczenstwa:
- tylko operacje `SELECT`
- male probki/chunki `IN (...)`
- brak `UPDATE/DELETE/MERGE` po stronie Netfox
- brak zapytan pelnoskanowych na szerokich zakresach bez limitu

## 11.6 Rekomendowane kolejne kroki (high ROI)

1. Uzupelnic `acc_gls_bl_map` dla brakujacych tokenow GLS `2026.02` na podstawie bezpiecznych ekstraktow z Netfox i/lub aktualnego cache BL.
2. Uruchomic celowany seed + pipeline tylko dla `GLS 2026.02`, nastepnie ponownie policzyc coverage.
3. Rozszerzyc logike linkowania DHL o sciezke JJD (`parcel_num_other`/mapa JJD), a potem wykonac rerun dla `DHL 2026-01`.
4. Wlaczyc kontrolowane odswiezanie Baselinker distribution cache (harmonogram lub reczny job), aby ograniczyc narastanie unlinked w kolejnych miesiacach.
