# Prime Delivery - Escalation to BaseLinker / Operations

Date: 2026-03-04

Context:
- Source workbook: Prime delivery report
- Scope analyzed: 317 orders with blank `Actual Delivery Date`
- Supporting files:
  - `C:\ACC\prime_delivery_bl_dhl_delivered_bl_no_package_137.csv`
  - `C:\ACC\prime_delivery_bl_dhl_delivered_bl_no_status_date_73.csv`

Key findings:
- DHL file audit shows a real `delivery_date` for 265 out of 317 cases.
- 137 cases have a real DHL delivery date, but BaseLinker has no package at all.
- 73 cases have a real DHL delivery date, BaseLinker has a package, but `BL tracking_status_date` is empty.
- Both subsets are 100% Austria deliveries (`buyer_country = AT`).
- Dominant services:
  - `Pri Intl`
  - `Std DE Intl_1`
- Dominant carriers in Amazon report:
  - `DHL`
  - `DHL eCommerce`

Case A: DHL delivered, BL has no package
- Volume: 137
- File: `C:\ACC\prime_delivery_bl_dhl_delivered_bl_no_package_137.csv`
- Pattern:
  - DHL has `DORECZENIE_DATACZAS_BORSOFT`
  - Amazon has tracking
  - BaseLinker has no package row / no package number

Case B: DHL delivered, BL has package, but BL has no tracking status date
- Volume: 73
- File: `C:\ACC\prime_delivery_bl_dhl_delivered_bl_no_status_date_73.csv`
- Pattern:
  - DHL has `DORECZENIE_DATACZAS_BORSOFT`
  - BaseLinker package exists
  - `BL tracking_status_date` is empty
  - In 72/73 rows `bl_courier_other_name = dhl`
  - In 1/73 row `bl_courier_other_name = gls`

What needs to be checked in BaseLinker / ops flow:
1. Which system writes shipment confirmation and tracking back to Amazon for MFN Prime orders.
2. Why BaseLinker package data is missing for 137 orders even though Amazon already has tracking and DHL has final delivery.
3. Why package rows exist for 73 orders, but BL does not store or update `tracking_status_date`.
4. Whether carrier/service mapping for:
   - `DHL`
   - `DHL eCommerce`
   - `Pri Intl`
   - `Std DE Intl_1`
   is stable and consistent in BL.
5. Whether any cross-border DE -> AT shipments are handed off in a way that prevents BL from updating final delivery status.

Requested outcome:
- Explain why BL package data is absent or incomplete for the attached order lists.
- Confirm which integration is responsible for updating Amazon shipment confirmation.
- Confirm whether a fix is needed in BL package creation, tracking sync, or carrier mapping.
