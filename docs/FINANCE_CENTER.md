# Finance Center

Status na `2026-03-04`.

## Scope

`Finance Center` jest aktywnym modułem ACC dla:
- importu Amazon finance feed,
- budowy payout groups (`financial_event_group_id`),
- generowania ledgera,
- payout reconciliation,
- kontroli kompletności feedu finance.

Backend:
- `apps/api/app/api/v1/finance_center.py`
- `apps/api/app/services/finance_center/service.py`
- `apps/api/app/services/finance_center/mappers/amazon_to_ledger.py`

Frontend:
- `apps/web/src/pages/FinanceDashboard.tsx`
- `apps/web/src/pages/FinanceLedger.tsx`
- `apps/web/src/pages/FinanceReconciliation.tsx`

## Current production behavior

### 0. Dashboard truth model

Kanoniczny dashboard backendowy istnieje pod:
- `GET /api/v1/finance/dashboard`

Frontend `FinanceDashboard.tsx` korzysta teraz z jednego endpointu i dostaje gotowe sekcje zamiast skladac gorne kafle z wielu niezaleznych query.

Sekcje dashboardu sa jawnie oznaczane jako:
- `real_data`
- `partial`
- `blocked_by_missing_bank_import`
- `no_data`

Najwazniejsze znaczenie operacyjne:
- `blocked_by_missing_bank_import` oznacza, ze reconciliation nie ma jeszcze zrodla w `acc_fin_bank_line`
- zera na dashboardzie nie powinny byc interpretowane jako brak ruchu, jesli sekcja nie ma statusu `real_data`

### 1. Finance import path

Produkcyjny import finansów działa dziś tak:
- najpierw próba `Finances v2024-06-19 / listTransactions`
- jeśli feed jest pusty dla seller context, fallback do:
  - `v0 / financialEventGroups`
  - `v0 / financialEventsByGroupId`

Kluczowym identyfikatorem payout group jest:
- `financial_event_group_id`

Nie należy opierać reconciliation na samym `settlement_id`, bo nie jest to stabilne źródło prawdy dla tego konta.

### 2. Background jobs

Ciężkie operacje finance są uruchamiane w tle z job runnera ACC:
- `finance_sync_transactions`
- `finance_prepare_settlements`
- `finance_generate_ledger`
- `finance_reconcile_payouts`

`queue_finance_job(...)` zwraca job od razu, a wykonanie leci w tle w daemon thread procesu API.

To oznacza:
- UI nie powinno już wisieć na długich operacjach,
- postęp trzeba obserwować przez `acc_al_jobs` / ekran `Jobs`.

### 3. Progress reporting

Finance jobs raportują postęp:
- `finance_sync_transactions`: `groups_synced / groups_skipped / current_group`
- `finance_prepare_settlements`: liczba zbudowanych settlement summaries
- `finance_generate_ledger`: postęp po liczbie przerobionych rows

### 4. Completeness alerts

System health dla finance completeness działa automatycznie:
- `finance_completeness_critical`
- `finance_completeness_partial`

Alerty są aktualizowane przez:
- ręczne importy finance
- scheduler `evaluate_alerts`

Jeśli feed jest niekompletny, dashboard i alerty mają to komunikować jawnie jako `partial` / `critical`.

### 4b. Bank import dependency

Payout reconciliation nie jest pelna bez importu banku:
- `POST /api/v1/finance/import/bank/csv`
- `POST /api/v1/finance/reconcile/payouts/auto-match`

Jesli `acc_fin_bank_line = 0`, dashboard powinien pokazywac status:
- `blocked_by_missing_bank_import`

To jest stan oczekiwany i uczciwy; payout widgets nie powinny byc wtedy traktowane jako gotowe KPI zarzadcze.

### 5. Gap diagnostics

Dostępny jest endpoint:
- `GET /api/v1/finance/sync/gap-diagnostics`

Pokazuje per marketplace:
- ile payout groups śledzimy,
- ile grup ma rows,
- ile rows i orders faktycznie zaimportowano,
- `event_type_counts`,
- `day_coverage_pct`,
- `order_coverage_pct`,
- `gap_reason`

Ważne ograniczenie:
- `financialEventGroups` w `v0` jest account-wide,
- marketplace przypisujemy inferencyjnie po eventach grupy,
- to nie jest natywny marketplace field z endpointu group list.

### 6. Current data trust

Na dziś feed finance nadal nie jest kompletny produkcyjnie dla wszystkich marketplace'ów.

Interpretacja:
- ledger i payout groups istnieją,
- ale completeness może być nadal `critical`,
- decyzje controllingowe/księgowe muszą uwzględniać coverage status.

### 7. Current executed state

Na pełnym wsadzie zostało już wykonane:
- `build_settlement_summaries()` → `35` payout groups
- `generate_ledger_from_amazon(180)` → `52034` inserted rows

## Netfox operational safety

Dodatkowy health endpoint:
- `GET /api/v1/health/netfox-sessions`

Pokazuje aktywne sesje oznaczone jako:
- `APP=ACC-Netfox-RO`

## 2026-03-03 hardening update

### Finance gap diagnostics

`GET /api/v1/finance/sync/gap-diagnostics` rozroznia teraz dodatkowo:
- `rows_not_attributed_to_marketplace` - payout groups maja rows, ale po imporcie zero wierszy zostalo przypisanych do marketplace
- `coverage_gap_after_import` - rows sa zaimportowane, ale coverage wzgledem orderow nadal jest za niski

Stan `30d` po attribution repair:
- `DE`: `tracked_groups=3`, `imported_rows=35426`, `imported_orders=7773`, `order_coverage_pct=22.8`, `gap_reason=coverage_gap_after_import`
- `IT`: `tracked_groups=3`, `imported_rows=4980`, `imported_orders=948`
- `FR`: `tracked_groups=3`, `imported_rows=3952`, `imported_orders=724`
- `ES`: `tracked_groups=3`, `imported_rows=840`, `imported_orders=149`
- `NL`: `tracked_groups=2`, `imported_rows=298`, `imported_orders=58`
- `SE`: `tracked_groups=2`, `imported_rows=247`, `imported_orders=37`
- `BE`: `tracked_groups=2`, `imported_rows=148`, `imported_orders=31`
- `PL`: `tracked_groups=1`, `imported_rows=111`, `imported_orders=29`
- `IE`: `tracked_groups=2`, `imported_rows=91`, `imported_orders=16`

Wniosek:
- problem `rows_not_attributed_to_marketplace` nie jest juz glownym blokerem dla aktywnych marketplace
- wszystkie aktywne marketplace maja juz imported finance rows
- obecna luka to glownie `coverage_gap_after_import`, czyli zaimportowane rows istnieja, ale biznesowe pokrycie orderow nadal jest zbyt niskie

### Background jobs

- stale finance jobs sa czyszczone przez `_cleanup_stale_finance_jobs(...)`
- background finance thread oznacza job jako `failure`, jesli wywali sie przed `set_job_success(...)`
- swiezy test `finance_prepare_settlements` domyka sie poprawnie w tle

### Netfox operational cleanup

- druga partia `tmp_*.py` dostala defensywne cleanupy polaczen Netfoxa przez `atexit` / jawne close
- celem jest ograniczenie wiszacych sesji ERP po recznych skryptach diagnostycznych

## 2026-03-03 attribution repair update

Naprawa historycznych payout groups poza `DE` zostala wykonana:
- odtworzone grupy: `17`
- odtworzone rows: `9539`

Stan `30d` po repairze:
- `DE`: `35426` rows
- `IT`: `4980`
- `FR`: `3952`
- `ES`: `840`
- `NL`: `298`
- `SE`: `247`
- `BE`: `148`
- `PL`: `111`
- `IE`: `91`

Wniosek:
- problem `rows_not_attributed_to_marketplace` byl w praktyce historycznym brakiem rows dla payout groups poza `DE`
- po repairze wszystkie aktywne marketplace maja juz rows w `acc_finance_transaction`
- nadal pozostaje problem coverage biznesowego, bo `order_coverage_pct` jest nadal zbyt niskie i gap reason przechodzi na `coverage_gap_after_import`

## 2026-03-04 dashboard clarification update

Finance Dashboard nie jest juz tylko zlepkiem frontowych query. Kanoniczna odpowiedz pochodzi z:
- `GET /api/v1/finance/dashboard`

Operacyjna interpretacja sekcji:
- `real_data` - sekcja ma wiarygodny, bezposredni wsad
- `partial` - dane istnieja, ale coverage albo agregacja nie sa jeszcze kompletne
- `blocked_by_missing_bank_import` - sekcja zalezy od banku, a `acc_fin_bank_line` nadal jest puste
- `no_data` - brak wsadu zrodlowego dla tej sekcji

Najwazniejsza praktyczna konsekwencja:
- `Ledger` i `finance sync diagnostics` sa juz realnie uzywalne
- `Payout reconciliation` pozostaje biznesowo ograniczone do czasu importu banku

### Event types and marketplace signal

Na zywych payloadach marketplace signal wyglada tak:
- `ShipmentEventList` - niesie marketplace signal
- `RefundEventList` - niesie marketplace signal
- `RetrochargeEventList` - niesie marketplace signal
- `AdjustmentEventList` - nie niesie marketplace signal
- `ProductAdsPaymentEventList` - nie niesie marketplace signal
- `ServiceFeeEventList` - nie niesie marketplace signal

Dlatego importer uzywa teraz fallbacku:
1. `MarketplaceName` / `StoreName`
2. `amazon_order_id -> acc_order.marketplace_id`
3. marketplace przypisany na poziomie payout group

## 2026-03-03 coverage gap analysis update

Dashboard i endpoint gap diagnostics pokazuja juz nie tylko status, ale tez skale feedu:
- `imported_rows`
- `tracked_groups`
- `imported_orders`
- `missing_order_rows`
- `missing_order_distinct_orders`
- `unmapped_rows`
- `likely_gap_driver`
- breakdown po wieku zamowien (`0_6d`, `7_13d`, `14_29d`)
- breakdown po kanale (`AFN`, `MFN`)
- breakdown `missing_orders_in_acc` po wieku i typie eventu

Najczestsze aktualne `likely_gap_driver`:
- `missing_orders_in_acc` - finance rows istnieja, ale czesc orderow nie jest dobrze spinana z `acc_order`
- `general_coverage_gap` - feed istnieje, ale pokrycie orderow pozostaje niskie
- `unmapped_finance_rows` - pozostale przypadki, gdzie imported rows nadal nie tlumacza pokrycia orderow

Aktualny wniosek z `missing_orders_in_acc`:
- to nie jest juz problem marketplace attribution
- dominujacy wzorzec poza `DE` to swieze `ShipmentEventList` z `0_6d`, czyli prawdopodobny lag order syncu / opoznienie pojawienia sie orderow w `acc_order`
- `DE` ma mieszany profil: brakujace refund rows + nadal istotne `unmapped_rows`

Interpretacja produkcyjna:
- controlling ma teraz widoczne: status, skale feedu i prawdopodobna przyczyne luki bez wchodzenia w surowa diagnostyke
- nadal nie wolno traktowac finance coverage jako kompletnego source of truth dla month close bez weryfikacji completeness

## 2026-03-03 order sync hardening update

Przyczyna `missing_orders_in_acc` zostala doprecyzowana:
- to nie byl glownie marketplace mismatch
- dominowaly swieze finance rows (`ShipmentEventList`, `0_6d`), dla ktorych orderow brakowalo fizycznie w `acc_order`
- glowny problem byl architektoniczny: `sync_orders` opieral sie o ruchome okno `now - 30 min`
- przy przerwach schedulera >30 min czesc orderow mogla wypasc na stale, jesli Amazon nie zmienil juz ich `LastUpdateDate`

Wdrozone zabezpieczenia:
- trwały watermark per marketplace w `acc_order_sync_state`
- overlap `15 min` i safety lag `2 min`
- recovery po downtime: kolejne runy startuja od ostatniego udanego `window_to`, a nie od biezacego czasu
- health endpoint `GET /api/v1/health/order-sync`
- alert systemowy `order_sync_gap`
- banner ryzyka order sync gap w Finance Dashboard

Stan po wdrozeniu:
- `order-sync health = healthy`
- wszystkie marketplace maja swiezy `last_successful_window_to`
- swieze luki `0_6d` zostaly zredukowane, ale nie zniknely jeszcze calkowicie

### Historical order catch-up 2026-01-01 -> 2026-03-03

Zakres historyczny od `2026-01-01` do `2026-03-03` zostal dociagniety dwiema sciezkami:
- bulk catch-up historyczny
- live incremental catch-up po watermarku

Stan praktyczny po domknieciu:
- `DE`: orders od `2026-01-01 00:05:39` do `2026-03-03 11:28:48`
- `FR`: od `2026-01-01 00:05:06` do `2026-03-03 11:24:24`
- `IT`: od `2026-01-01 06:20:33` do `2026-03-03 11:14:56`
- `ES`: od `2026-01-01 00:23:26` do `2026-03-03 11:00:34`
- `NL`: od `2026-01-01 10:36:25` do `2026-03-03 11:01:06`
- `PL`: od `2026-01-01 10:06:02` do `2026-03-03 10:42:51`
- `SE`: od `2026-01-01 10:57:20` do `2026-03-03 10:54:19`
- `BE`: od `2026-01-01 08:03:10` do `2026-03-03 10:54:51`
- `IE`: od `2026-01-01 10:16:11` do `2026-03-03 10:36:47`

### Bulk order report caveat

Praktyczne zastrzezenie produkcyjne:
- `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL` zachowuje sie jak seller-account-wide bulk feed
- nie nalezy traktowac `acc_backfill_report_progress.orders_upserted` jako wiarygodnego per-market wolumenu
- report backfill jest bezpieczny jako bulk catch-up do `acc_order` / `acc_order_line`
- nie jest bez dodatkowej atrybucji kanonicznym zrodlem do per-market diagnostyki coverage

Wniosek:
- do incremental production syncu source of truth pozostaje Orders API + watermark
- Reports API sluzy jako narzedzie catch-up / recovery historycznego, nie jako per-market telemetry feed

To służy do szybkiego sprawdzenia, czy ACC nie zostawia zbędnych połączeń do ERP.
## 2026-03-04 fee gap diagnostics + watch update

Dla brakow FBA/referral fee zostala dodana osobna sciezka diagnostyczna i watchdog:

- `GET /api/v1/profit/v2/fee-gap-diagnostics`
- `POST /api/v1/profit/v2/fee-gap-watch/seed`
- `POST /api/v1/profit/v2/fee-gap-watch/recheck`

Nowe tabele pomocnicze:
- `acc_fee_gap_watch` - stan brakow na poziomie orderu (`open`, `resolved`, `amazon_events_available`)
- `acc_fee_gap_recheck_run` - historia uruchomien rechecka

Co daje diagnostyka:
- rozbicie gapow per `marketplace` + `gap_type` (`fba` / `referral`) + `gap_reason`
- osobna lista `DE` dla przypadku `finance_exists_no_fba_charge_type`
- listy orderow:
  - `likely_amazon_missing` (`no_finance_rows`)
  - `likely_internal_fixable` (`sku_mismatch_or_unallocated`, brak charge type, itp.)

Co robi recheck:
- bierze otwarte case'y z watchlisty,
- odpytuje Amazon v0 per order (`financialEvents by order id`),
- aktualizuje status:
  - `resolved` - lokalnie brak juz luki,
  - `amazon_events_available` - Amazon zaczal zwracac eventy, trzeba rerun bridge fee,
  - `open` - Amazon nadal zwraca 0 eventow.

Automatyzacja:
- scheduler ma codzienny job `03:20`:
  - seed watchlisty z ostatnich 30 dni
  - recheck (batch limit 100)
- job type: `fee_gap_watch_recheck` (widoczny w `acc_al_jobs`)

Artefakty z biezacego uruchomienia (`2026-03-04`, zakres `2026-02-03 -> 2026-03-04`):
- `docs/fee_gap_reasons_2026-03-04.csv`
- `docs/fee_gap_de_finance_exists_no_fba_charge_2026-03-04.csv`
- `docs/fee_gap_amazon_missing_2026-03-04.csv`
- `docs/fee_gap_internal_fixable_2026-03-04.csv`
- `docs/FEE_GAP_DIAGNOSTICS_2026-03-04.md`

## 2026-03-05 cross-module note (Profit v2)

Powiazany raport zmian dla warstwy profit (ASIN-first, parent rollups, API/UI grouping):
- `docs/PROFIT_V2_UPDATE_2026-03-05.md`

Kontekst:
- Finance feed i fee diagnostics sa traktowane jako upstream source dla kosztow `profit/v2`,
- kolejne przepiecie modelu CM1/CM2/NP ma bazowac na tych danych i zachowac jawny status coverage.

Status update:
- hard switch CM semantics (CM1/CM2/NP) jest juz wdrozony w `profit/v2` (backend + UI),
- szczegoly implementacyjne i walidacja build/compile: `docs/PROFIT_V2_UPDATE_2026-03-05.md` (sekcja post-refactor).

## 2026-03-06 finance -> profit integration update

Dopieta zostala kolejna warstwa integracji finance feedu do `profit/v2`:

- `ShippingCharge` z `acc_finance_transaction` jest doliczany do realized revenue w `profit/v2/products`,
  ale tylko dla `MFN/FBM` (AFN/FBA bez sztucznego doliczania).
- Alokacja `ShippingCharge` na linie zamowienia:
  - priorytet: proporcja po `item_price`,
  - fallback: proporcja po `quantity_ordered`.

Nowe metryki jakosci dopiete do `ProductProfitItem`:
- `shipping_match_pct` (ile zamowien ma shipping charge eventy),
- `finance_match_pct` (ile zamowien ma jakiekolwiek finance rows),
- `return_rate`, `tacos`, `days_of_cover`.

Znaczenie operacyjne:
- Finance feed jest teraz widoczny nie tylko przez coverage dashboard, ale rowniez bezposrednio w unit economics na tabeli produktowej,
- latwiej rozdzielic przypadki:
  - brak finance rows (upstream Amazon / feed lag),
  - rows sa, ale bez konkretnych charge type,
  - rows sa i poprawnie wchodza do revenue/cost.
