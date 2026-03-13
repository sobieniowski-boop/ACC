# Family Restructure — Raport techniczny (2026-03-06)

> **Moduł:** Family Mapper → execute_restructure v3  
> **Plik:** `apps/api/app/services/family_mapper/restructure.py` (1313 linii)  
> **Status:** ✅ Produkcja (dry-run przetestowany), gotowy do execute  
> **Endpoint:** `POST /api/v1/families/{id}/execute-restructure?marketplace_id=X&dry_run=true`

---

## 1. Cel modułu

Family Restructure replikuje kanoniczną strukturę rodziny produktów z DE na dowolny marketplace EU.
Pipeline analizuje stan DE (parent, children, variation_theme) i porównuje z marketplace docelowym,
a następnie wykonuje: walidację theme, audyt atrybutów dzieci, wzbogacenie z PIM, tłumaczenie GPT,
tworzenie parenta i reassignment children.

---

## 2. Pipeline — 7 kroków

| # | Krok | Opis | Status |
|---|------|------|--------|
| 1 | `PREFLIGHT_DE_CHILD` | Załaduj kanoniczne dane DE family + children z SP-API | ok |
| 2 | `VALIDATE_THEME` | Sprawdź czy `variation_theme` jest wspierany na target MP przez SP-API PTD | ok/warning |
| 3 | `AUDIT_CHILD_ATTRS` | Audyt atrybutów (color/size) WSZYSTKICH dzieci na target MP | warning jeśli braki |
| 4 | `ENRICH_FROM_PIM` | Lookup brakujących color/size w Ergonode PIM + GPT tłumaczenie + PATCH | ok/dry_run |
| 5 | `CHECK_PARENT` / `TRANSLATE_PARENT` | Sprawdź/utwórz parent, przetłumacz atrybuty GPT-5.2 | ok/dry_run |
| 6 | `CREATE_PARENT` | PUT listings item na target MP (parent SKU z DE) | ACCEPTED/dry_run |
| 7 | `REASSIGN_CHILD` (×N) | PATCH child_parent_sku_relationship per child | ACCEPTED/dry_run |

Dodatkowe kroki informacyjne:
- `FOREIGN_PARENT_INFO` — informacja o obcych parentach wykrytych na target MP
- `TRANSLATE_PARENT` — raport z tłumaczenia GPT (co przetłumaczono, ile tokenów)

---

## 3. Kluczowe funkcje

### `execute_restructure(family_id, target_mp, dry_run=True)` — główna orkiestracja
- Ładuje DE family z DB + SP-API
- Uruchamia pipeline kroków 1-7
- Zwraca `ExecuteRestructureResult` z `steps[]`, `errors`, `status`, summary objects

### `_translate_parent_attributes(attrs, target_mp, product_type)` — GPT-5.2
- Tłumaczenie `item_name`, `bullet_point`, `product_description`, `generic_keyword`
- Model: GPT-5.2 (env `OPENAI_MODEL`)
- Prompt: specjalizowany per product_type, zachowuje SEO + technical terms
- Skips jeśli target_lang == German

### `_validate_variation_theme(target_mp, product_type, desired_theme)` — SP-API PTD
- Pobiera Product Type Definition z SP-API (`/definitions/2020-09-01/productTypes/{pt}`)
- Sprawdza czy `variation_theme` jest w `propertyNames` definicji
- Zwraca `{supported: bool, message: str}`

### `_audit_child_attributes(target_mp, child_skus)` — pełny audyt
- **Sprawdza WSZYSTKIE dzieci** (nie sample!)
- Concurrent: `asyncio.Semaphore(5)`, batch po 20
- Pobiera `getListingsItem` per child z target MP
- Sprawdza obecność `color_name`/`color_map` i `size_name`/`size_map`
- Zwraca per-child status: `{asin, sku, has_color, has_size, missing: [...]}`

### `_enrich_children_from_pim(target_mp, audit_children, actionable, product_type, dry_run)`
- Zbiera ASINy z brakującymi atrybutami
- Wywołuje `fetch_ergonode_variant_lookup(asins)` → color/size z PIM
- Tłumaczy wartości PIM (polskie) → język target MP via GPT-5.2
- W trybie execute: PATCH atrybuty na target MP via SP-API
- Zwraca `{total_missing, pim_found, patched/dry_run, details[]}`

---

## 4. Integracja Ergonode PIM

### Plik: `apps/api/app/connectors/ergonode.py`

#### Nowe funkcje (dodane 2026-03-06):

**`_build_option_map(client, attr_code)`**
- Pobiera ID atrybutu z paginowanej listy `/api/v1/en_GB/attributes`
- Następnie GET `/api/v1/en_GB/attributes/{id}/options` → lista `[{id, code, label}]`
- Zwraca mapę `{uuid: code_text}` do rozwiązywania wartości SELECT/MULTI_SELECT

**`fetch_ergonode_variant_lookup(target_asins)`**
- Input: `set[str]` ASINów do sprawdzenia
- Buduje 3 option mapy (concurrent): `wariant_kolor_tekst`, `wariant_text_rozmiar`, `wariant_text_ilosc`
- Iteruje produkty PIM z filtrem po `asin_child`
- Pobiera szczegóły produktu: `/api/v1/en_GB/products/{id}`
- Rozwiązuje UUID wartości → tekst via option mapy
- Output: `{asin: {color, size, quantity, sku_ergonode, ergonode_id}}`

#### Atrybuty wariantowe w Ergonode:
| Atrybut PIM | Typ | Przykłady wartości |
|---|---|---|
| `wariant_kolor_tekst` | MULTI_SELECT (UUID) | grafit, butelkowa zieleń, czerwony, złoty |
| `wariant_text_rozmiar` | SELECT (UUID) | 10, 12, 14, m, l, s, 16 cm, 21 cm (296 opcji) |
| `wariant_text_ilosc` | SELECT (UUID) | 10, 20, 50, 100, 200 (34 opcje) |

---

## 5. Integracja SP-API Listings

### Plik: `apps/api/app/connectors/amazon_sp_api/listings.py`

**`get_product_type_definition(product_type, requirements, locale)`**
- Endpoint: `GET /definitions/2020-09-01/productTypes/{productType}`
- Params: `requirements` (LISTING/LISTING_OFFER_ONLY), `locale`
- Używane przez `_validate_variation_theme()` do sprawdzenia czy PTD wspiera dany theme

---

## 6. Frontend

### `apps/web/src/pages/FamilyDetail.tsx`
- **StepRow**: 10 akcji z dedykowanym renderowaniem:
  - `VALIDATE_THEME` — badge supported/unsupported + komunikat
  - `TRANSLATE_PARENT` — lista przetłumaczonych pól
  - `AUDIT_CHILD_ATTRS` — tabela per-child z ✓/✗ dla color/size
  - `ENRICH_FROM_PIM` — summary: total_missing, pim_found, dry_run/patched, target_language
- **ExecutionLog**: Summary sekcje:
  - variation_theme (info badge)
  - child_attr_audit (green/orange badge)
  - pim_enrichment (blue badge jeśli found > 0)

### `apps/web/src/lib/api.ts`
- `ExecuteRestructureStep` — rozszerzony o `dry_run?`, `total_missing?`, `pim_found?`, `patched?`
- `ExecuteRestructureResult` — rozszerzony o:
  - `variation_theme?: {theme, supported, message}`
  - `child_attr_audit?: {total, checked, color_missing, size_missing, children}`
  - `pim_enrichment?: {total_missing, pim_found, patched} | null`

---

## 7. Wyniki testów — Family 1367 / FR / dry_run=true

```
Family: KADAX Pokrywka do słoika (ID=1367)
Parent ASIN: B0G7GL3QNB  |  SKU: 7P-HO4I-IM4E
Product Type: CONTAINER_LID
Variation Theme: color/size
Target: FR (A13V1IB3VIYZZH)
Mode: dry_run=true

Status: completed | Steps: 109 | Errors: 0

1. PREFLIGHT_DE_CHILD     → ok
2. VALIDATE_THEME          → supported (variation_theme supported for CONTAINER_LID on FR)
3. AUDIT_CHILD_ATTRS       → warning | Checked 100/100: color 6 missing, size 19 missing
4. ENRICH_FROM_PIM         → ok | PIM lookup: 18/19 found in Ergonode, would patch 18 (dry_run)
5. TRANSLATE_PARENT        → dry_run | Would translate from German to French
6. CREATE_PARENT           → dry_run | Would create parent 7P-HO4I-IM4E on FR
7. REASSIGN_CHILD ×100     → dry_run
   + FOREIGN_PARENT_INFO ×3 → info (obce parenty na FR)
```

---

## 8. Konfiguracja

### Wymagane zmienne `.env`:
- `OPENAI_API_KEY` — klucz API OpenAI (GPT-5.2 tłumaczenia)
- `OPENAI_MODEL` — model (default: `gpt-5.2`)
- `OPENAI_MAX_TOKENS` — max tokenów (default: 4096)
- `ERGONODE_API_URL` — URL PIM (`https://api-kadax.ergonode.cloud`)
- `ERGONODE_USERNAME`, `ERGONODE_PASSWORD`, `ERGONODE_API_KEY` — credentials PIM
- `SP_API_*` — credentials SP-API (seller, client_id, client_secret, refresh_token)

### Marketplace'y wspierane:
Wszystkie 13 EU marketplace'ów w `MARKETPLACE_LANGUAGE` / `MARKETPLACE_LOCALE`.
Źródło kanoniczne: zawsze DE (`A1PA6795UKMFR9`).

---

## 9. Bezpieczeństwo i limity

- **Dry-run domyślny** — `dry_run=True`, execute wymaga jawnego `dry_run=false`
- **Rate limiting** — `asyncio.sleep(0.3)` między wywołaniami SP-API
- **Semaphore** — max 5 równoległych audit requests, max 10 PIM requests
- **Batching** — audyt w batch po 20, PIM lookup w batch po 50
- **No hardcoded credentials** — wszystko z `.env`
- **Endpoint chroniony JWT** — wymaga roli `director` lub `admin`

---

## 10. Znane ograniczenia

1. **PIM coverage** — nie wszystkie produkty mają atrybuty wariantowe w Ergonode (18/19 = 95% hit rate)
2. **GPT translation quality** — tłumaczenia SEO mogą wymagać review (szczególnie bullet points)
3. **SP-API throttling** — przy dużych rodzinach (100+ children) audyt trwa ~25s
4. **Execute mode** — przetestowany tylko dry-run; pierwszy execute powinien być na małej rodzinie
5. **MULTI_SELECT kolor** — Ergonode zwraca listę UUID; bierzemy pierwszy pasujący
