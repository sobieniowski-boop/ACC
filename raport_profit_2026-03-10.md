# Raport naprawy ekranów Profit — 10 marca 2026

**Projekt:** Amazon Command Center (ACC)  
**Autor:** Copilot / Miłosz Sobieniowski  
**Data:** 2026-03-10

---

## 1. Naprawione błędy

### 1.1 Brakujące kolumny w Azure SQL
Tabela `acc_order` nie posiadała trzech kolumn wymaganych przez endpoint `/profit/v2/products`, co powodowało crash z błędem *"Invalid column name"*.

**Dodane kolumny** (typ `DECIMAL(18,4) NULL`):
- `shipping_surcharge_pln` — dopłata wysyłkowa
- `promo_order_fee_pln` — opłata za zamówienie promocyjne
- `refund_commission_pln` — prowizja od zwrotu

**Zabezpieczenie:** Funkcja `_ensure_order_sync_state_schema()` w `order_pipeline.py` została zaktualizowana — automatycznie tworzy te 3 kolumny przy starcie serwera, jeśli nie istnieją.

### 1.2 Frontend — ProfitOverview.tsx
Usunięto 5 sztywno zakodowanych presetów dat (7d/30d/90d/bieżący miesiąc/poprzedni miesiąc). Komponent teraz korzysta ze wspólnego hooka `usePageFilters()` oraz komponentu `PageFilterBar`, co umożliwia **dowolny zakres dat** (np. styczeń 2026).

### 1.3 Frontend — ProductProfitTable.tsx
Naprawiono podwójny stan błędu/pustej listy — dodano warunek `!isError`, aby komunikat „Brak produktów" nie wyświetlał się jednocześnie z banerem błędu.

---

## 2. Wyniki weryfikacji

| Test | Wynik |
|------|-------|
| TypeScript (kompilacja) | **0 błędów** w obu zmodyfikowanych plikach |
| Python — `test_bridge_fees` + `test_fee_taxonomy` | **161 passed** ✅ |
| Python — `test_p1_financial_fixes` + `test_phase0_revenue_fix` | **26 passed** ✅ |
| Smoke test — `get_product_profit_table()` (styczeń 2026) | **5 843 wierszy** ✅ |

**Top 3 SKU wg przychodu (styczeń 2026):**

| SKU | Przychód (PLN) |
|-----|---------------:|
| MAG_5903699471982 | 161 816 |
| MAG_5903699457177 | 86 430 |
| MAG_5903699471999 | 70 352 |

---

## 3. Jakość danych — stan bieżący

### ✅ Co działa
- **Przychody** — dane obecne: 101K+ zamówień, 116K+ pozycji zamówień (źródło: Reports API TSV)
- **Katalog produktów** — 5 843 unikalnych produktów za sam styczeń 2026

### ⚠️ CM1 = 0 dla wszystkich produktów
Jest to **oczekiwane zachowanie** na obecnym etapie:
1. Trzy nowe kolumny zawierają wartości `NULL` (obsługiwane przez `ISNULL(..., 0)` w SQL)
2. Bridge fee backfill (`step_bridge_order_fees`) nie został jeszcze uruchomiony — nie przepisał danych z `acc_finance_transaction` do nowych kolumn
3. Backfill finansowy pokrywa okresy **gru 2024 – mar 2025** oraz **mar 2026**, ale istnieje **luka: kwi 2025 – lut 2026**

---

## 4. Pozostałe prace

| Priorytet | Zadanie | Status |
|:---------:|---------|--------|
| 🔴 | **Backfill finansowy** — uzupełnienie okresu kwi 2025 – lut 2026 | W trakcie |
| 🔴 | **Bridge fee computation** — uruchomienie `step_bridge_order_fees` w celu wypełnienia 3 nowych kolumn z danych `acc_finance_transaction` | Do wykonania |
| 🟡 | **Amazon Ads — raporty dzienne** — kampanie zsynchronizowane (5 083), brak danych kosztowych | Do wykonania |

---

*Po uzupełnieniu backfillu finansowego i uruchomieniu bridge fee computation wartości CM1/CM2 powinny pojawić się na dashboardzie Profit.*
