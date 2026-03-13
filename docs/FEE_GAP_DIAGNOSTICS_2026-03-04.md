# Fee Gap Diagnostics (2026-03-04)

Zakres: **2026-02-03 -> 2026-03-04**

- Total gap orders: **5149**
- Amazon-missing candidates (`no_finance_rows`): **4723**
- Internal-fixable candidates: **426**
- DE (`finance exists but no FBA charge type`): **132**

## Missing orders by marketplace (fba + referral)

- BE: **531** (fba: 103, referral: 428)
- DE: **3327** (fba: 1155, referral: 2172)
- ES: **33** (fba: 20, referral: 13)
- FR: **130** (fba: 21, referral: 109)
- IE: **2** (fba: 0, referral: 2)
- IT: **68** (fba: 34, referral: 34)
- NL: **476** (fba: 72, referral: 404)
- PL: **282** (fba: 46, referral: 236)
- SE: **300** (fba: 102, referral: 198)

## Top gap reasons

- DE referral `no_finance_rows`: **2067** orders
- DE fba `no_finance_rows`: **974** orders
- BE referral `no_finance_rows`: **426** orders
- NL referral `no_finance_rows`: **397** orders
- PL referral `no_finance_rows`: **236** orders
- DE fba `finance_exists_no_fba_charge_type`: **132** orders

Pliki:
- `fee_gap_reasons_2026-03-04.csv`
- `fee_gap_de_finance_exists_no_fba_charge_2026-03-04.csv`
- `fee_gap_amazon_missing_2026-03-04.csv`
- `fee_gap_internal_fixable_2026-03-04.csv`
