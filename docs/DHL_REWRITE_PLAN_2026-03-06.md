# DHL Rewrite Plan - 2026-03-06

## Cel

Zastapic obecne, niedokladne i rozproszone rozwiazania kurierskie pelna, produkcyjna integracja DHL oparta o realne dane z WebAPI2. Docelowo ACC ma miec jeden kanoniczny model przesylki, kosztu i statusu doreczenia, a `acc_order.logistics_pln` ma byc tylko agregatem pochodnym, nie recznie mapowanym placeholderem.

To nie jest "dopinka DHL". To jest wymiana modelu danych i zrodla prawdy dla kosztow FBM.

## Dlaczego obecny stan trzeba usunac

### 1. Dane kosztowe sa order-level, nie package-level

- `apps/api/app/services/order_pipeline.py`
  - `step_sync_courier_costs()` zapisuje koszt bezposrednio do `acc_order.logistics_pln`
  - jeden order dostaje jedna liczbe, bez prawdy o wielu paczkach
- `apps/api/app/services/courier_cost_mapper.py`
  - zapisuje do `acc_shipping_cost`
  - tabela ma unikalnosc po `amazon_order_id`
  - mapper deduplikuje wyniki jako `first match wins`

Efekt:
- split shipmenty sa z natury zle opisane
- nie da sie audytowac kosztu konkretnej paczki
- nie da sie poprawnie policzyc dostawy, doreczenia i reklamacji per przesylka

### 2. Sa dwa konkurencyjne zrodla prawdy

- stary pipeline:
  - `acc_order.logistics_pln`
- nowy mapper:
  - `acc_shipping_cost`

Profit, KPI i wiekszosc UI czyta nadal `acc_order.logistics_pln`, wiec nawet jesli `acc_shipping_cost` ma lepsze dane, system dalej liczy na starym fundamencie.

### 3. DHL opiera sie na plikach i obejsciach

- `apps/api/app/services/courier_cost_mapper.py`
  - korzysta z `acc_dhl_jjd_map` zasilanego z plikow `N:\Kurierzy\DHL\JJ\...`
- `apps/api/oneoff_prime_delivery_bl_dhl_audit.py`
  - audyt delivery bazuje na XLSX i jednorazowym skrypcie

Efekt:
- brak ciaglego syncu
- brak pewnosci danych
- brak audytowalnosci
- duza zaleznosc od recznych operacji

### 4. Profit i raporty sa zasilane heurystykami

- `apps/api/app/services/profit_engine.py`
  - realized profit bierze `o.logistics_pln`
  - model what-if ma fallbacki typu `tkl_low_sample`
  - koszty logistyczne sa czesciowo estymowane, nie wynikaja z pelnej prawdy przesylkowej

### 5. Brak produkcyjnej integracji DHL w architekturze aplikacji

W repo istnieje wzorzec integracji GLS:
- `apps/api/app/connectors/gls_api/*`
- `apps/api/app/api/v1/gls.py`
- `apps/api/app/core/config.py`

Analogicznego modulu DHL nie ma.

## Co uznajemy za docelowy stan

### Zasady

1. DHL API jest zrodlem prawdy dla tozsamosci przesylki, tracking events, scanow i proof-of-delivery.
2. Koszt ma byc przypisany do przesylki lub piece, a koszt zamowienia ma byc agregatem pochodnym.
3. Legacy pole `acc_order.logistics_pln` zostaje na potrzeby zgodnosci wstecznej, ale jest tylko wynikiem agregacji nowego modelu.
4. Zadna logika DHL nie moze zalezec od plikow na dysku sieciowym ani od one-off skryptow.
5. Integracja ma byc idempotentna, audytowalna i bezpieczna dla produkcji.
6. Pierwszy etap jest read-only. Bez tworzenia przesylek i bez generowania etykiet w produkcji.

## Zakres funkcjonalny integracji DHL

### Etap 1 - obowiazkowy

- pobieranie realnych danych przesylki DHL
- pobieranie trackingu i scanow
- pobieranie proof of delivery / ePOD
- budowa kanonicznego rejestru przesylek DHL
- przypisanie przesylki do:
  - `amazon_order_id`
  - `bl_order_id`
  - tracking number / JJD / piece id
- zapis realnego kosztu przesylki lub kosztu dostepnego do rozliczenia
- agregacja kosztu do poziomu orderu
- alerty operacyjne dla rozjazdow dostawy i kosztu

### Etap 2 - opcjonalny po ustabilizowaniu read-only

- tworzenie przesylek
- etykiety
- pickup
- parcelshop flows

To powinno byc osobnym projektem i osobnym feature flagiem. Nie wolno tego mieszac z pierwszym przepieciem kosztow i trackingu.

## Docelowy model danych

Obecny `acc_shipping_cost` nie nadaje sie na kanoniczna tabele, bo jest order-centric i ma unikalnosc po `amazon_order_id`.

Proponowany model:

### `acc_shipment`

Jeden rekord na przesylke logistyczna.

Pole minimalne:
- `id`
- `carrier` = `DHL`
- `carrier_account`
- `shipment_number`
- `piece_id`
- `tracking_number`
- `service_code`
- `ship_date`
- `status_code`
- `status_label`
- `is_delivered`
- `delivered_at`
- `recipient_country`
- `source_system` = `dhl_webapi2`
- `source_payload_hash`
- `first_seen_at`
- `last_seen_at`
- `last_sync_at`

### `acc_shipment_order_link`

Laczenie przesylki z naszym swiatem orderowym.

Pole minimalne:
- `shipment_id`
- `amazon_order_id`
- `acc_order_id`
- `bl_order_id`
- `link_method`
- `link_confidence`
- `is_primary`

### `acc_shipment_event`

Historia zdarzen trackingowych i scanow.

Pole minimalne:
- `shipment_id`
- `event_code`
- `event_label`
- `event_at`
- `location_city`
- `location_country`
- `raw_payload`
- unikalnosc po `(shipment_id, event_code, event_at, location_city)`

### `acc_shipment_pod`

Proof-of-delivery / ePOD.

Pole minimalne:
- `shipment_id`
- `pod_type`
- `available`
- `document_ref`
- `downloaded_at`
- `raw_payload`

### `acc_shipment_cost`

Koszt na poziomie przesylki.

Pole minimalne:
- `shipment_id`
- `cost_source`
- `currency`
- `net_amount`
- `fuel_amount`
- `toll_amount`
- `gross_amount`
- `invoice_number`
- `invoice_date`
- `billing_period`
- `is_estimated`
- `raw_payload`

### `acc_order_logistics_fact`

Tabela pochodna, agregowana z przesylek do poziomu orderu.

Pole minimalne:
- `amazon_order_id`
- `acc_order_id`
- `shipments_count`
- `total_logistics_pln`
- `delivered_shipments_count`
- `last_delivery_at`
- `calc_version`
- `calculated_at`

To ta tabela powinna zasilac `acc_order.logistics_pln` i warstwe raportowa.

## Warstwa integracji w kodzie

### Nowe komponenty backendu

Dodac:

- `apps/api/app/connectors/dhl24_api/client.py`
  - cienki klient HTTP/SOAP zalezne od metody
  - retry, timeout, logowanie, redakcja sekretow
- `apps/api/app/connectors/dhl24_api/models.py`
  - parse payloadow DHL do wewnetrznych struktur
- `apps/api/app/connectors/dhl24_api/errors.py`
- `apps/api/app/services/dhl_registry_sync.py`
  - sync przesylek i mapowan order <-> shipment
- `apps/api/app/services/dhl_tracking_sync.py`
  - sync eventow i delivered state
- `apps/api/app/services/dhl_cost_sync.py`
  - sync kosztow i rozliczen
- `apps/api/app/services/dhl_reconciliation.py`
  - agregacja do order-level i rozjazdy
- `apps/api/app/api/v1/dhl.py`
  - read-only endpointy diagnostyczne i manualne joby

### Konfiguracja

Dodac do `apps/api/app/core/config.py`:

- `DHL24_API_USERNAME`
- `DHL24_API_PASSWORD`
- `DHL24_API_BASE_URL`
- `DHL24_PARCELSHOP_USERNAME`
- `DHL24_PARCELSHOP_PASSWORD`
- `DHL24_TIMEOUT_SEC`
- `DHL24_ENABLED`
- `DHL24_WRITE_ENABLED`

`DHL24_WRITE_ENABLED` ma domyslnie byc `False`.

### Router i endpointy

Dodac router analogiczny do GLS:

- `GET /dhl/health`
- `GET /dhl/shipments/{tracking_number}`
- `GET /dhl/shipments/{tracking_number}/events`
- `GET /dhl/shipments/{tracking_number}/pod`
- `POST /dhl/sync/shipments`
- `POST /dhl/sync/tracking`
- `POST /dhl/sync/costs`
- `POST /dhl/reconcile/orders`

W produkcji pierwsza wersja ma byc read-only, z jobami manualnymi i cronowymi.

## Jak powinna wygladac logika danych

### 1. Rejestr przesylek

Punktem startowym nie powinno byc:
- XLSX
- `acc_dhl_jjd_map`
- jednorazowe skrypty

Punktem startowym powinny byc:
- zamowienia MFN z `acc_order`
- paczki z BaseLinker / cache paczek
- identyfikatory tracking / JJD, ktore juz znamy
- odpowiedzi DHL API zwracajace shipment identity

### 2. Tracking i delivered truth

Status przesylki ma byc wyliczany z eventow DHL, nie z:
- pustych pol w BL
- recznych arkuszy
- heurystyk statusowych

`oneoff_prime_delivery_bl_dhl_audit.py` powinien zostac zastapiony cyklicznym syncem trackingowym i alertami.

### 3. Koszt

Docelowy koszt ma byc zapisany na przesylce. Order-level koszt powstaje wtornie.

Jesli DHL API udostepnia realny koszt rozliczony:
- zapisujemy go w `acc_shipment_cost`

Jesli DHL API udostepnia tylko quote / cene taryfowa:
- zapisujemy quote jako `is_estimated = 1`
- finalny billed amount musi przyjsc z innego zrodla rozliczeniowego
- ale nadal trzymamy shipment registry i tracking w DHL jako source of truth

To jest jedyny punkt wymagajacy potwierdzenia na realnych odpowiedziach API.

### 4. Agregacja do orderu

Nie wolno juz bezposrednio pisac do `acc_order.logistics_pln` z pipeline DHL.

Nowy przeplyw:

1. sync shipmentow
2. sync eventow
3. sync kosztow
4. `acc_order_logistics_fact` = agregacja po linkach shipment-order
5. dopiero na koncu aktualizacja `acc_order.logistics_pln` jako mirror zgodnosci wstecznej

## Co trzeba usunac lub wygasic

### Do natychmiastowego wygaszenia po wdrozeniu shadow mode

- `apps/api/app/services/order_pipeline.py`
  - `step_sync_courier_costs()`
- scheduler odpalajacy legacy courier cost mapping do `acc_order.logistics_pln`
- plikowe zasilanie `acc_dhl_jjd_map`
- `apps/api/oneoff_prime_delivery_bl_dhl_audit.py`
  - jako proces operacyjny, niekoniecznie od razu do usuniecia z repo

### Do zastapienia lub przebudowy

- `apps/api/app/services/courier_cost_mapper.py`
  - albo usunac
  - albo przebudowac do carrier-agnostic reconciler korzystajacego z `acc_shipment*`
- `acc_shipping_cost`
  - nie rozwijac dalej jako glownej tabeli
  - ewentualnie migrowac dane historyczne i zamknac temat

### Do przepiecia na nowe zrodlo

- `apps/api/app/services/profit_engine.py`
- `apps/api/app/services/profit_service.py`
- `apps/api/app/connectors/mssql/mssql_store.py`
- `apps/api/app/api/v1/kpi.py`
- warstwa web korzystajaca z `logistics_pln`

## Fazy wdrozenia

### Faza 0 - bezpieczenstwo i freeze

- nie uzywac produkcyjnych sekretow w commitach, docs i logach
- dodac env vars i redakcje sekretow
- ustawic read-only mode dla wszystkich jobow DHL
- nie dotykac create shipment / labels / pickup

### Faza 1 - schema i konektor

- dodac klienta DHL
- dodac nowe tabele `acc_shipment*`
- dodac parse warstwy i surowy zapis payloadow
- dodac healthcheck i manualny test pojedynczego tracking number

Kryterium wyjscia:
- dla pojedynczej paczki umiemy pobrac i zapisac:
  - identity
  - events
  - delivered state
  - POD availability

### Faza 2 - registry sync i linkowanie orderow

- zbudowac job backfillu dla ostatnich 90-180 dni MFN
- zbudowac linkowanie:
  - tracking -> BL package
  - BL package -> BL order
  - BL order -> amazon_order_id
- zapisac confidence i metode dopasowania

Kryterium wyjscia:
- dla zdecydowanej wiekszosci przesylek mamy stabilny link do orderu

### Faza 3 - cost sync

- sprawdzic, czy API daje billed amount czy quote
- zapisac koszt do `acc_shipment_cost`
- przeliczyc PLN wedlug spójnej polityki FX
- zbudowac agregat `acc_order_logistics_fact`

Kryterium wyjscia:
- jedna przesylka ma jeden lub wiele rekordow kosztowych, a order dostaje kontrolowany agregat

### Faza 4 - shadow mode

- rownolegle liczyc:
  - legacy `acc_order.logistics_pln`
  - nowy agregat z `acc_shipment_cost`
- porownywac rozjazdy:
  - brak kosztu
  - rozne kwoty
  - brak delivered state
  - order z wieloma paczkami i tylko jednym kosztem legacy

Kryterium wyjscia:
- znamy wszystkie rozjazdy i mamy swiadoma decyzje migracyjna

### Faza 5 - przepiecie profitu i KPI

- `profit_engine.py` czyta `acc_order_logistics_fact`
- KPI i dashboardy licza CM po nowym agregacie
- `acc_order.logistics_pln` jest tylko mirror column dla starego UI i eksportow

### Faza 6 - wyciecie legacy

- usunac `step_sync_courier_costs()`
- usunac cron/job legacy
- zatrzymac plikowy JJD map
- zdeprecjonowac `acc_shipping_cost`
- zaktualizowac docs i runbooki

## Testy, ktore musza powstac

### Unit

- parser odpowiedzi DHL
- mapowanie eventow na status wewnetrzny
- linkowanie shipment -> order
- agregacja kosztow shipment -> order

### Integracyjne

- sync pojedynczej paczki
- sync wielu paczek z paginacja / batching
- retry po timeout / 5xx
- idempotentny re-run tego samego syncu

### Reconciliation

- order z jedna paczka
- order z wieloma paczkami
- order z brakiem kosztu, ale z delivered event
- order z quote, ale bez final invoice amount

## Alerty operacyjne

Po wdrozeniu trzeba dodac alerty:

- DHL delivered, ale BL/Amazon nie ma delivered date
- shipment istnieje, ale brak linku do orderu
- order MFN ma paczki, ale zero kosztu po X dniach
- order ma wiecej niz jedna paczke, a legacy koszt byl pojedynczy
- przesylka ma POD, ale brak zamkniecia reklamacji / audytu

## Otwarta kwestia krytyczna

Musimy potwierdzic na realnych odpowiedziach produkcyjnych:

1. czy DHL WebAPI2 zwraca realny koszt rozliczony, czy tylko quote
2. jaki identyfikator jest najlepszym kluczem technicznym:
   - tracking number
   - JJD
   - piece id
3. czy wszystkie potrzebne shipmenty sa wyszukiwalne po okresie i loginie konta

Bez tej odpowiedzi nie wolno finalnie projektowac tylko jednej tabeli kosztowej.

## Rekomendowana kolejnosc prac

1. Zrobic read-only klienta DHL i test jednej paczki.
2. Postawic `acc_shipment`, `acc_shipment_event`, `acc_shipment_order_link`.
3. Uruchomic backfill shipment registry.
4. Dodac sync eventow i POD.
5. Potwierdzic model kosztu DHL.
6. Zbudowac `acc_shipment_cost` i `acc_order_logistics_fact`.
7. Przepiac profit i KPI.
8. Wylaczyc legacy.

## Decyzja architektoniczna

Nie rozwijamy dalej:
- `step_sync_courier_costs()`
- `acc_shipping_cost`
- mapowania opartego o pliki JJD

Budujemy od nowa:
- shipment-centric model danych
- read-only DHL integration layer
- cost and tracking reconciliation layer
- order-level aggregate derived from shipments

To jest jedyna droga, zeby koszt byl:
- realny
- audytowalny
- paczka-po-paczce
- uzywalny w profit, reklamacji i operacjach
