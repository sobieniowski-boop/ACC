# Profit v2 What-if: Plan / Observed / Decision Logistics Model

## Cel
W `GET /api/v1/profit/v2/what-if` koszt kuriera dla FBM jest liczony produkcyjnie jako 3 warstwy:

- `Plan` — koszt standardowy z TKL (`plan_logistics_pln`)
- `Observed` — koszt historyczny z realnych wysyłek single-SKU (`observed_logistics_pln`)
- `Decision` — koszt używany do CM2 (`decision_logistics_pln`, alias: `estimated_logistics_pln`)

Dodatkowo API zwraca:

- `logistics_gap_pct` (Observed vs Plan)
- `logistics_decision_rule`
- `logistics_observed_samples`
- `execution_drift` (flaga trwałego pogorszenia wykonania vs plan)

## Reguły decyzji (FBM)
Per SKU + marketplace + scenariusz qty:

1. **Low sample** (`samples < 5`) -> `Decision = Plan`
2. **High and stable sample** (`samples >= 15`, `P75 <= 1.35 * median`) ->
   blend `0.60 * Plan + 0.40 * Observed`, ale z bezpiecznym floor:
   `Decision = max(blend, max(Plan, P75))`
3. **Pozostałe przypadki z Plan + history** -> `Decision = max(Plan, P75)`
4. **Brak TKL, jest history** -> `Decision = P75 history` (lub median gdy brak P75)
5. **Brak obu źródeł** -> `Decision = 0`, flaga braków

## Execution Drift
`execution_drift = true`, gdy:

- `Plan > 0`
- `samples >= 10`
- `observed_median >= 1.10 * Plan`
- `observed_p75 >= 1.20 * Plan`

To sygnalizuje trwałe odchylenie wykonania magazyn/kurier od standardu TKL.

## UI
Na widoku what-if są kolumny:

- `Plan (TKL)`
- `Observed (hist.)`
- `Decision`
- `Gap %`

CM2 korzysta z `Decision`, nie z samego `Plan` ani samego `Observed`.

Od 2026-03-05 CM2/NP sa liczone konsekwentnie takze dla what-if:
- `CM1 = revenue - cogs - fees - decision_logistics`
- `CM2 = CM1 - ads - returns_net - fba_storage - fba_aged - fba_removal - fba_liquidation`
- `NP = CM2 - overhead_allocated`

## Powiazane update'y

- Szczegolowy raport zmian po ostatniej aktualizacji i przed przepieciem modelu CM:
  - `docs/PROFIT_V2_UPDATE_2026-03-05.md`

## Update 2026-03-06 (alignment with realized)

Po stronie `Realized` (nie tylko what-if) dopieto:
- doliczanie `ShippingCharge` do revenue dla `MFN/FBM` na bazie finance transactions,
- metryki `return_rate`, `tacos`, `days_of_cover`, `shipping_match_pct`, `finance_match_pct`,
- rozwijanie `parent -> child` rowniez w tabeli realized.

W praktyce:
- porownanie `Realized vs What-if` jest teraz bardziej spojne semantycznie,
- what-if nadal pozostaje warstwa estymacyjna (Plan/Observed/Decision),
- realized pokazuje faktyczny efekt eventow finance na revenue.
