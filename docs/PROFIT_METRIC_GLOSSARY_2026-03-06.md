## Profit Metric Glossary

Canonical definitions for ACC, effective from 2026-03-07:

- `CM1`
  - Direct contribution margin.
  - Formula: `revenue - cogs - amazon_fees - logistics`
  - Revenue includes: netto item revenue (item_price - item_tax - promotion_discount) × FX **+ ShippingCharge** (customer-paid shipping, net of ShippingTax, distributed by revenue share per marketplace).
  - Meaning: direct, order-matchable profitability before ads, returns, storage and overhead.

- `CM2`
  - Post-operational contribution margin.
  - Formula: `CM1 - ads - returns_net - fba_storage - fba_aged - fba_removal - fba_liquidation - refund_finance - shipping_surcharge - fba_inbound`
  - Meaning: margin after direct selling costs plus operating costs tied to selling and returns.
  - New buckets added 2026-03-07:
    - `refund_finance` — ReturnPostage, ReturnShipping, RestockingFee (from acc_finance_transaction)
    - `shipping_surcharge` — ShippingHB, ShippingChargeback (Amazon shipping penalties)
    - `fba_inbound` — FBAInventoryPlacementServiceFee (inbound placement costs)

- `NP`
  - Net profit after overhead allocation.
  - Formula: `CM2 - overhead_allocated`
  - Overhead pools can come from:
    - `acc_profit_overhead_pool` (manually configured admin pools)
    - `acc_finance_transaction` NP-layer charges: ServiceFee, Adjustment, other_overhead (auto-detected via `_classify_finance_charge()`)
  - Meaning: fully loaded profitability after allocated overhead.

Non-canonical metrics that must not be labelled as `CM1`:

- `CM`
  - Order-level contribution margin used by legacy Profit Explorer.
  - Formula: `revenue - cogs - amazon_fees - ads - logistics`
  - This is not canonical `CM1`, because ads are already deducted.

- `Content Impact Margin`
  - Snapshot metric used by Content Dashboard impact view.
  - Formula: `revenue - cogs - transport`
  - This is a content-ops proxy from `acc_al_profit_snapshot`, not canonical `CM1`, `CM2` or `NP`.

Rollout rules:

- Use `CM1`, `CM2`, `NP` labels only when the backend follows the canonical formulas above.
- If a screen uses a narrower or wider formula, rename the label instead of overloading canonical names.
- If a screen is page-scoped, the summary must say so explicitly. Otherwise summary values must be full-scope.
