# Prime Delivery - Escalation to Amazon Prime Support

Date: 2026-03-04

Context:
- Scope analyzed: 317 Prime delivery report rows with blank `Actual Delivery Date`
- Supporting DHL/BL audit files:
  - `C:\ACC\prime_delivery_bl_dhl_audit_20260304_174902.csv`
  - `C:\ACC\prime_delivery_bl_dhl_audit_20260304_174902_slim.csv`

Key findings:
- In 265 out of 317 cases, DHL file data contains a real delivery timestamp (`DORECZENIE_DATACZAS_BORSOFT`).
- Despite this, Amazon Prime report still has blank `Actual Delivery Date`.
- In the same 265 cases:
  - `Delivery Scan/Attempt = No` in Amazon report
  - `Any Physical Scan From Carrier = Yes` in 263 cases
- Geography:
  - 264 of these 265 delivered cases are Austria (`AT`)
  - 1 case is Germany (`DE`)
- Main service patterns:
  - `Pri Intl` = 209
  - `Std DE Intl_1` = 55
- Carrier patterns:
  - `DHL` = 264
  - `DHL eCommerce` = 1

Operational interpretation:
- Carrier-side data indicates actual delivery happened.
- Amazon sees carrier physical scans, but does not recognize a final delivery or delivery-attempt event for these shipments.
- This strongly suggests an issue in recognition of final carrier events for DE -> AT Prime shipments.

Requested Amazon review:
1. Confirm why `Actual Delivery Date` remains blank when carrier data contains a confirmed delivery timestamp.
2. Confirm whether final delivery scans for:
   - `DHL`
   - `DHL eCommerce`
   - `Pri Intl`
   - `Std DE Intl_1`
   are fully recognized for DE -> AT Prime shipments.
3. Confirm whether there is a known issue with cross-border DE -> AT final event recognition.
4. Confirm whether Amazon expects a more specific carrier/service mapping or a different shipment confirmation payload for these shipments.

Example evidence:
- 137 cases: DHL delivered, but BaseLinker has no package at all
- 73 cases: DHL delivered, BaseLinker package exists, but BL has no tracking status date
- Even with these BL inconsistencies, DHL delivery timestamps exist for the majority of affected shipments, while Amazon still leaves `Actual Delivery Date` blank

Requested outcome:
- Explain whether Amazon is rejecting or not recognizing valid final delivery events.
- Confirm what exact carrier/service confirmation data is required so these shipments count correctly in Prime delivery reporting.
