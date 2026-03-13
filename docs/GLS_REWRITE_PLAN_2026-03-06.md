# GLS Rewrite Plan - 2026-03-06

## Cel

Przepisac warstwe GLS w ACC tak, aby:

- koszty i raporty byly zasilane z realnych plikow `N:\KURIERZY\GLS POLSKA`
- model byl shipment-centric, nie order-centric
- `acc_order.logistics_pln` przestalo byc recznym polem z legacy mappera
- API GLS zostalo zepchniete do roli opcjonalnej warstwy operacyjnej, nie finansowej

To nie jest "dopinka jeszcze jednego klienta API". To jest wymiana modelu danych i zrodla prawdy dla GLS Poland.

## Najwazniejszy wniosek

Dla GLS Poland zrodlem prawdy kosztowej nie jest API.

W praktyce mamy:

- miesieczne pliki `GLS_*.csv` z numerem paczki, kosztem, statusem i `note1`
- pomocniczy plik `GLS - BL.xlsx` z mapowaniem `tracking_number -> order_id`
- osobny obszar `Korekty kosztowe\...` z korektami i dodatkowymi specyfikacjami

To wystarcza, zeby zbudowac produkcyjny, audytowalny flow kosztowy bez uzalezniania ACC od publicznego API GLS.

## Co wiemy po przegladzie danych

### 1. Miesieczne CSV sa bardzo dobre

Przykladowy plik `N:\KURIERZY\GLS POLSKA\2026.02\GLS_6501031953.csv` zawiera m.in.:

- `invoice_num`
- `date`
- `delivery_date_x`
- `parcel_num`
- `netto`
- `toll`
- `fuel_surcharge`
- `note1`
- `parcel_status`
- `srv`

To oznacza, ze jeden plik daje jednoczesnie:

- numer przesylki GLS
- koszt przesylki
- status przesylki
- fallback do `BL order_id` przez `note1`

### 2. Jest dodatkowe mapowanie GLS -> BL

Plik `N:\KURIERZY\GLS POLSKA\GLS - BL.xlsx` zawiera:

- `tracking_number`
- `order_id`
- `custom_1`

To jest dodatkowa warstwa kontroli i fallback dla matchingu.

### 3. Obecny direct matching juz dziala dobrze

Na obecnych cache:

- `260282` MFN orders ma GLS package
- `253258` ma direct cost po `parcel_num = tracking`
- to daje ok. `97.3%` coverage order-level

Po unikalnym trackingu:

- `986946` distinct GLS tracking
- `904058` distinct tracking z invoice
- ok. `91.6%` coverage tracking-level

Wniosek:

- bez zadnego nowego API direct path juz jest mocny
- brakujace kilka procent trzeba domknac przez `note1`, korekty kosztowe i opoznione faktury

## Dlaczego obecny stan trzeba zmienic

### 1. GLS jest logicznie rozszczepiony

W repo sa dwa swiaty:

- warstwa kosztowa oparta o cache invoice
- warstwa API oparta o mix `GLS Group` i `GLS Poland`

Te swiaty nie skladaja sie w jeden kanoniczny model danych.

### 2. Nowy mapper GLS jest ubozszy niz legacy

`apps/api/app/services/courier_cost_mapper.py`

- umie tylko `parcel_num = tracking`
- czyta `acc_cache_invoices`
- nie ma fallbacku `note1`

`apps/api/app/services/order_pipeline.py`

- ma `GLS direct`
- ma tez `GLS note1 fallback`

Efekt:

- nowy cache-based flow GLS jest prostszy, ale traci czesc coverage legacy

### 3. Koszt dalej jest finalnie order-centric

Nawet jesli dopasowanie GLS jest dobre, system dalej:

- zapisuje wynik do `acc_order.logistics_pln`
- albo do `acc_shipping_cost`
- i nie ma kanonicznej prawdy per przesylka

### 4. API GLS nie rozwiazuje finansow

W repo sa:

- `connectors/gls_api/client.py`
- `connectors/gls_api/ade_client.py`
- `connectors/gls_api/cost_center.py`

Ale:

- `GLSADEClient` to sensowna warstwa operacyjna
- `GLSClient` dla `track-and-trace-v1` jest dla Polski niepewny
- `GLSCostCenterClient` nie jest API do kosztow paczek ani raportow billingowych

## Decyzja architektoniczna

### Zasady

1. Koszt GLS Poland ma pochodzic z plikow billingowych, nie z API.
2. Kanoniczny model danych ma byc wspolny z DHL:
   - `acc_shipment`
   - `acc_shipment_order_link`
   - `acc_shipment_cost`
   - `acc_order_logistics_fact`
3. Dla GLS potrzebujemy osobnych tabel stagingowych, ale nie osobnego kanonicznego modelu shipment.
4. API GLS ma byc opcjonalnym dodatkiem operacyjnym, nie krytycznym elementem kosztow.
5. Legacy `acc_order.logistics_pln` ma byc tylko agregatem pochodnym.

## Co zostawic, co zamrozic, co przepisac

### Zostawic

- `apps/api/app/connectors/gls_api/ade_client.py`
  - ma sens do operacji GLS Poland: tracking, POD, labels, preparing box, pickup
- czesc ADE routera w `apps/api/app/api/v1/gls.py`
  - endpointy `ade/*` sa sensowne jako warstwa diagnostyczna/operacyjna
- wspolny model shipment-centric z DHL
  - `acc_shipment*`
  - `acc_order_logistics_fact`

### Zamrozic

- `apps/api/app/connectors/gls_api/client.py`
  - traktowac jako eksperymentalne / niekrytyczne dla GLS Poland
- `apps/api/app/connectors/gls_api/auth.py`
  - tylko jesli ten klient zostaje jako opcjonalne T&T
- `apps/api/app/connectors/gls_api/cost_center.py`
  - nie uzywac do kosztow GLS w ACC
- endpointy `gls/health`, `gls/track/*`, `gls/cost-center/*`
  - nie rozwijac ich jako glownej sciezki biznesowej

Zamrozenie nie oznacza natychmiastowego usuniecia. Oznacza:

- bez nowych ficzerow
- bez przepinania na to profitu i KPI
- tylko diagnostyka, jesli juz istnieje

### Przepisac od zera

- warstwe kosztowa GLS
- staging import plikow GLS
- matching do orderow
- agregacje do `acc_order_logistics_fact`
- shadow vs legacy

## Docelowy model danych dla GLS

### Staging

Dodac:

#### `acc_gls_import_file`

- `source_kind`
- `file_path`
- `file_name`
- `document_number`
- `file_size_bytes`
- `file_mtime_utc`
- `status`
- `rows_imported`
- `error_message`
- `last_imported_at`
- `updated_at`

#### `acc_gls_billing_document`

- `document_number`
- `invoice_date`
- `source_file`
- `rows_count`
- `last_imported_at`

#### `acc_gls_billing_line`

- `document_number`
- `invoice_date`
- `delivery_date`
- `parcel_number`
- `recipient_name`
- `recipient_postal_code`
- `recipient_city`
- `recipient_country`
- `weight`
- `declared_weight`
- `billing_weight`
- `net_amount`
- `toll_amount`
- `fuel_amount`
- `surcharge_amount`
- `storewarehouse_amount`
- `billing_type`
- `note1`
- `parcel_status`
- `service_code`
- `source_file`
- `source_row_no`
- `source_hash`

#### `acc_gls_bl_map`

Zasilane z `GLS - BL.xlsx`.

- `tracking_number`
- `bl_order_id`
- `map_source`
- `source_file`
- `source_row_no`
- `source_hash`

#### `acc_gls_billing_correction`

Zasilane z `Korekty kosztowe\...`.

- `document_number`
- `parcel_number`
- `bl_order_id`
- `correction_type`
- `delta_net_amount`
- `delta_toll_amount`
- `delta_fuel_amount`
- `delta_total_amount`
- `source_file`
- `source_row_no`

### Kanoniczny model

Nie tworzymy osobnego `gls_shipment`.

Uzywamy:

- `acc_shipment`
  - `carrier = 'GLS'`
  - `source_system = 'gls_billing_files'` albo `gls_ade`
- `acc_shipment_order_link`
- `acc_shipment_cost`
  - `cost_source = 'gls_billing_files'`
- `acc_order_logistics_fact`

## Matching

### Kolejnosc dopasowania

1. `parcel_num -> tracking`
   - `acc_gls_billing_line.parcel_number`
   - `acc_cache_packages.courier_package_nr`

2. `note1 -> BL order_id`
   - `acc_gls_billing_line.note1`
   - `acc_cache_packages.order_id`
   - potem do `amazon_order_id`

3. `GLS - BL.xlsx`
   - `tracking_number -> bl_order_id`

4. fallback przez `courier_inner_number`
   - tylko jesli praktyka danych pokaze, ze to cos daje

### Zasady linkowania

- `tracking` ma pierwszenstwo nad `note1`
- `note1` ma byc fallbackiem, nie glownym kluczem
- trzeba zapisywac:
  - `link_method`
  - `link_confidence`
  - `is_primary`

## Kolejnosc wdrozenia

### Faza 1 - importer billing files

Dodac:

- `apps/api/app/services/gls_billing_import.py`

Zakres:

- import `N:\KURIERZY\GLS POLSKA\YYYY.MM\GLS_*.csv`
- import `N:\KURIERZY\GLS POLSKA\GLS - BL.xlsx`
- zapis do `acc_gls_import_file`, `acc_gls_billing_document`, `acc_gls_billing_line`, `acc_gls_bl_map`
- idempotencja po `(source_kind, file_path, file_size, file_mtime)`

### Faza 2 - shipment seed

Z `acc_gls_billing_line` zasilic:

- `acc_shipment`
- `acc_shipment_order_link`

Minimalne pola:

- `shipment_number = parcel_number`
- `tracking_number = parcel_number`
- `status_label = parcel_status`
- `delivered_at = delivery_date`
- `is_delivered = parcel_status like 'Dor%'`

### Faza 3 - cost sync

Z `acc_gls_billing_line` i korekt zasilic:

- `acc_shipment_cost`

Zasady:

- jedna paczka moze miec kilka linii z jednego dokumentu lub korekt
- finalny koszt shipmentu ma byc suma linii podstawowych i korekt
- `is_estimated = 0`

### Faza 4 - aggregate

Agregowac do:

- `acc_order_logistics_fact`

Tak samo jak przy DHL:

- koszt orderu jest suma kosztow shipmentow
- nie wolno wracac do modelu `one order -> one guessed number`

### Faza 5 - shadow

Porownac:

- legacy `acc_order.logistics_pln`
- nowy agregat GLS z `acc_order_logistics_fact`

Tylko dla kohorty GLS.

### Faza 6 - profit / KPI / UI

Po przejsciu shadow:

- przepiac profit
- przepiac KPI
- przepiac UI i drilldown

### Faza 7 - wygaszenie legacy

Na koncu:

- nie rozwijac dalej GLS w `order_pipeline.step_sync_courier_costs()`
- nie rozwijac starego GLS path w `courier_cost_mapper.py`
- `acc_order.logistics_pln` zostawic tylko jako pole pochodne / kompatybilnosciowe

## Observability

Od poczatku dodac warstwe audytowa analogiczna do DHL:

- `GET /api/v1/gls/cost-trace`
- `GET /api/v1/gls/unmatched-shipments`
- `GET /api/v1/gls/shadow-diff`

Trace ma pokazywac:

- z jakiego pliku przyszla linia GLS
- jaki byl `document_number`
- czy match poszedl po `tracking`, `note1`, czy `GLS - BL.xlsx`
- jakie korekty weszly do finalnego kosztu

## Ryzyka

### 1. Korekty kosztowe

Folder `Korekty kosztowe` to osobny strumien danych, nie detal implementacyjny.
Jesli go pominiemy, finalny koszt GLS nie bedzie pelna prawda.

### 2. Duplikaty i ponowne eksporty

W folderach sa wielokrotne eksporty i kopie plikow.
Importer musi byc idempotentny i rozroznia:

- ten sam dokument
- ten sam `parcel_number`
- rozne pliki z ta sama trescia

### 3. Opoznienia billingowe

Nie kazda swieza paczka ma od razu koszt w miesiecznym CSV.
Shadow i KPI musza to uwzgledniac, inaczej beda sztuczne "missing cost".

## Minimalny backlog implementacyjny

1. `config.py`
   - `GLS_BILLING_ROOT_PATH`
   - `GLS_BILLING_BL_MAP_PATH`
   - `GLS_BILLING_CORRECTIONS_PATH`

2. `gls_integration.py`
   - bootstrap tabel `acc_gls_*`

3. `gls_billing_import.py`
   - parser CSV
   - parser `GLS - BL.xlsx`
   - parser korekt

4. `gls_cost_sync.py`
   - sync do `acc_shipment_cost`

5. `gls_logistics_aggregation.py`
   - aggregate + shadow

6. `api/v1/gls.py`
   - endpointy billing/observability

7. testy
   - parser CSV
   - match `tracking`
   - match `note1`
   - aggregate
   - shadow

## Rekomendacja wykonawcza

Nie rozwijac dalej GLS jako "hybrydy API + stary mapper + cache hack".

Wlasciwa sciezka jest taka:

1. importer z `N:\KURIERZY\GLS POLSKA`
2. shipment-centric koszt
3. agregat do `acc_order_logistics_fact`
4. shadow przeciw legacy
5. dopiero potem przepiecie profitu

To jest prostsze niz DHL, bo GLS juz w plikach daje:

- `parcel_num`
- `note1`
- status
- i realny koszt

Czyli technicznie GLS powinno byc nastepnym, szybszym do uporzadkowania kurierem po DHL.
