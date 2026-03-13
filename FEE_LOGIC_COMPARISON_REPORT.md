# Raport porównawczy: Logika opłat — Narzędzie zewnętrzne vs ACC

**Data analizy**: 2026-03-09  
**Ostatnia aktualizacja**: 2026-03-09 (po implementacji poprawek shipping + DSF)  
**Zakres danych zewnętrznych**: 01.01.2026 – 09.03.2026 (111 505 zamówień Shipped, 9 marketplace'ów)  
**Zakres danych ACC**: ~209K transakcji finance od 31.12.2024

### 📋 Zmiany wdrożone (2026-03-09)
1. **Revenue = item_price − tax − promo + buyer-paid shipping** — dodano ShippingCharge + ShippingTax − ShippingDiscount do przychodu (WSZYSTKIE kanały: AFN + MFN)
2. **DigitalServicesFee → CM2** — przeniesione z CM1/referral (profit_layer=None, LOST) do CM2/amazon_other_fee (SERVICE_FEE)
3. **ShippingDiscount** — uwzględnione w shipping CTE (pomniejsza shipping revenue)
4. **Shipping w alokacjach** — dodane do ratio_by_marketplace (CM2), ads allocation (ASIN-level), loss_lines detection

---

## 1. Architektura — fundamentalna różnica

| Aspekt | Narzędzie zewnętrzne | ACC |
|--------|---------------------|-----|
| **Granulacja** | 1 wiersz = 1 zamówienie (order-level) | 3 warstwy: order_line (CM1) → finance_transaction → profitability_rollup |
| **Źródło opłat** | Mieszane: Orders API + Finance API (spłaszczone do jednego wiersza) | Finance API v2024-06-19 → `acc_finance_transaction` (70+ typów), bridgowane do `acc_order_line` |
| **Taksonomia** | ~10 kolumn stałych | 70+ `charge_type` w `FEE_REGISTRY` → 18 kategorii → 13 bucketów P&L → 3 warstwy (CM1/CM2/NP) |
| **Waluta** | Oryginalna (EUR) per order | PLN (przeliczenie FX przy imporcie + FX na rollupie) |
| **Identyfikacja SKU** | Brak (kolumna Products = ASIN) | Per-SKU via finance `sku` + fallback przez `internal_sku` z rejestru listingów |

---

## 2. Mapowanie kolumn: External → ACC

### 2.1 `OrderTotalAmount` → ACC: `item_price` (order_line)

| | Zewnętrzne | ACC |
|---|---|---|
| **Źródło API** | Orders API → `OrderTotal.Amount` | Orders API → `OrderItem.ItemPrice.Amount` |
| **Poziom** | Per-order (gross) | Per-order-line (per-item) |
| **Zawiera** | ItemPrice + ewentualnie ShippingPrice (zależy od marketplace) | Tylko item price (shipping osobno) |
| **Uwagi** | Dla FBA: OrderTotal = cena produktu (shipping=0 bo Prime). Dla MFN: OrderTotal zawiera cenę produktu, shipping podany osobno. | `item_price` to zawsze cena produktu. Shipping jest oddzielnie w `acc_finance_transaction` jako `ShippingCharge`. |

**Kluczowa różnica**: Zewnętrzne łączy OrderTotal + Shipping na poziomie zamówienia. ACC rozdziela item_price (Orders API) od ShippingCharge (Finance API).

---

### 2.2 `Shipping` (Revenue) → ACC: `ShippingCharge` + `ShippingTax` − `ShippingDiscount` (finance_transaction → revenue)

| | Zewnętrzne | ACC (po poprawce) |
|---|---|---|
| **Źródło API** | Orders API → shipping charge z OrderItem | Finance API → `ShippingCharge` + `ShippingTax` + `ShippingDiscount` |
| **Wartość** | Dodatnia (przychód od kupującego) | Kwota netto (ShippingCharge + ShippingTax − ShippingDiscount) konwertowana do PLN |
| **Rola w profit** | Bezpośrednio dodawane do przychodu zamówienia | ✅ Dodawane do revenue (proportional allocation per order line) — WSZYSTKIE kanały (AFN + MFN) |
| **Kwota globalna (ext.)** | +129 846 EUR | ACC: netto shipping revenue (proporcjonalnie alokowane per SKU) |

**✅ NAPRAWIONE**: Revenue ACC teraz = `(item_price − item_tax − promotion_discount) × FX + shipping_netto_pln × line_share`. Shipping jest alokowane proporcjonalnie do udziału item_price danej linii w zamówieniu. Dotyczy WSZYSTKICH kanałów (AFN i MFN).

**Implementacja**: Finance transaction → CTE `shipping_per_order` agregujące `ShippingCharge + ShippingTax + ShippingDiscount` → OUTER APPLY → proporcionalna alokacja per order_line → dodawane do revenue_pln w warstwie Python (`rev = line_revenue + shipping_charge_revenue`).

---

### 2.3 `Tax` → ACC: `item_tax` (order_line) + `Tax` (finance_transaction, REVENUE)

| | Zewnętrzne | ACC |
|---|---|---|
| **Źródło API** | Finance API lub Orders API → zagregowane per order | Dual: (1) `item_tax` z Orders API per line, (2) `Tax` z Finance API |
| **Wartość** | Ujemna (−524 504 EUR) = potrącenie | `item_tax` dodatnie (sub-total tax). Finance `Tax` = kwota VAT. |
| **Rola w profit** | Odejmowane od gross = net revenue | ACC: `revenue_pln = (item_price − item_tax − promo) × FX` → tax jest odjęte na poziomie CM1 |
| **Pokrywane typy** | Jeden zbiorczy "Tax" | ACC rozróżnia: `Tax`, `ShippingTax`, `GiftWrapTax`, `PrincipalTax`, `ShippingChargeTax`, `MarketplaceFacilitatorVAT-Principal/Shipping/Giftwrap`, `LowValueGoodsTax-Principal/Shipping`, `MarketplaceWithheldTax`, `TCSxGSTFee` |

**Logika identyczna** — obie strony odejmują tax od gross revenue. ACC przechowuje więcej wariantów typu podatku w `acc_finance_transaction` (11 subtypów: `Tax`, `ShippingTax`, `GiftWrapTax`, `PrincipalTax`, `ShippingChargeTax`, `MarketplaceFacilitatorVAT-*`, `LowValueGoodsTax-*`, `MarketplaceWithheldTax`, `TCSxGSTFee`), co pozwala na dokładniejsze raportowanie podatkowe per marketplace. W obliczeniach profit obie strony dochodzą do tego samego wyniku — netto revenue po odliczeniu podatku.

---

### 2.4 `Comission` → ACC: `Commission` / `ReferralFee` (finance_transaction → order_line.referral_fee_pln)

| | Zewnętrzne | ACC (po poprawce) |
|---|---|---|
| **Źródło API** | Finance API → `Commission` | Finance API → `breakdownType` ∈ {`Commission`, `ReferralFee`, `VariableClosingFee`, `FixedClosingFee`} |
| **Wartość** | Ujemna (−479 791 EUR) per order | Kwota w oryginalnej walucie, konwertowana do PLN |
| **Mapowanie** | 1 kolumna "Comission" | Kategoria `REFERRAL_FEE` (4 charge_types) → bridgowana do `acc_order_line.referral_fee_pln` |
| **Bridge do SKU** | Brak (order-level) | `step_bridge_fees()`: (1) Direct SKU match, (2) internal_sku via registry, (3) residual allocation by price weight |
| **Rollup** | — | `acc_sku_profitability_rollup.amazon_fees_pln` = SUM(referral_fee_pln) z order_line |

**Logika prowizji (CM1)** — ACC bridguje **4 typy** do `referral_fee_pln`:
- `Commission` — standardowa prowizja referral
- `ReferralFee` — alternatywna nazwa (Amazon używa zamiennie)
- `VariableClosingFee` — media closing fee (książki, DVD)
- `FixedClosingFee` — stała opłata zamknięcia (media)

**✅ DigitalServicesFee — przeniesione do CM2**: DSF to opłata za usługi cyfrowe (głównie FR, częściowo DE) — NIE jest prowizją referral. Przeniesione z `REFERRAL_FEE` (profit_layer=None → LOST!) do `SERVICE_FEE` (profit_layer="cm2", bucket="amazon_other_fee"). Analogicznie `DigitalServicesFeeFBA`. Teraz prawidłowo widoczne w P&L jako osobna opłata CM2.

**Brak podwójnego naliczenia**: `step_bridge_fees()` bridguje TYLKO `Commission` i FBA fee types. DSF nie był bridgowany, więc przeniesienie do CM2 nie powoduje duplikatu. Weryfikacja: DSF miał wcześniej profit_layer=None → nigdzie nie był liczony (LOST). Teraz jest w CM2 → amazon_other_fee_pln.

---

### 2.5 `FBAPerUnitFulfillmentFee` → ACC: `FBA_FEE` category → order_line.fba_fee_pln

| | Zewnętrzne | ACC |
|---|---|---|
| **Źródło API** | Finance API → `FBAPerUnitFulfillmentFee` | Finance API → `breakdownType` ∈ {`FBAPerUnitFulfillmentFee`, `FBAPerOrderFulfillmentFee`, `FBAWeightBasedFee`, `FBAPickAndPackFee`, `FBAWeightHandlingFee`, `FBAOrderHandlingFee`, `FBAPerUnitFulfillment`, `FBADeliveryServicesFee`} |
| **Wartość** | Ujemna (−161 588 EUR) per order | Kwota per-SKU, konwertowana do PLN |
| **Mapowanie** | 1 kolumna | Kategoria `FBA_FEE` (8 charge_types) → bridgowana do `acc_order_line.fba_fee_pln` |
| **Bridge** | Order-level | `step_bridge_fees()`: agregacja FBA fee → proporcjonalny podział na line items wg qty |
| **Rollup** | — | `acc_sku_profitability_rollup.fba_fees_pln` = SUM(fba_fee_pln) |

**⚠️ RÓŻNICA**: Zewnętrzne liczy **tylko** `FBAPerUnitFulfillmentFee`. ACC liczy **8 wariantów FBA fulfillment**, w tym:
- `FBAPerOrderFulfillmentFee` — per-order (nie per-unit)
- `FBAWeightBasedFee` — bazowane na wadze
- `FBADeliveryServicesFee` — dostawa

ACC jest dokładniejsze — uwzględnia wszystkie warianty opłaty fulfillment.

---

### 2.6 `Item promotion` → ACC: `promotion_discount` (order_line) + `PromotionDiscount` (finance_transaction)

| | Zewnętrzne | ACC |
|---|---|---|
| **Źródło API** | Orders API / Finance API | Orders API → `OrderItem.PromotionDiscount.Amount` → `acc_order_line.promotion_discount` |
| **Wartość** | Ujemna (−12 807 EUR) | Dodatnia na order_line (kwota rabatu), odejmowana w revenue: `revenue = item_price − item_tax − promotion_discount` |
| **Rola w profit** | Odejmowane od gross | Odejmowane w revenue (CM1) |
| **Finance API** | — | `PromotionDiscount` w finance_transaction (kategoria `REVENUE`, profit_layer=None) — **nie dodawane ponownie** do kosztu |

**Logika identyczna** — obie strony odejmują rabat od przychodu. ACC dodatkowo przechowuje `PromotionDiscount` z Finance API ale z `profit_layer=None` (skip), żeby nie policzyć podwójnie.

---

### 2.7 `Ship Promotion` → ACC: `ShippingDiscount` (finance_transaction → w shipping CTE)

| | Zewnętrzne | ACC (po poprawce) |
|---|---|---|
| **Źródło API** | Orders API / Finance API | Finance API → `breakdownType = "ShippingDiscount"` |
| **Wartość** | Ujemna (−5 639 EUR) | Kategoria `REVENUE`, sign=−1, amount ujemne w DB |
| **Rola w profit** | Odejmowane od shipping revenue | ✅ Uwzględnione w shipping CTE — pomniejsza shipping_charge_pln |

**✅ NAPRAWIONE**: `ShippingDiscount` jest teraz w IN clause shipping CTE: `charge_type IN ('ShippingCharge','ShippingTax','ShippingDiscount')`. Kwota ShippingDiscount w DB jest ujemna (rabat), więc naturalne dodanie do SUM pomniejsza netto shipping revenue. Logika identyczna z zewnętrznym.

---

### 2.8 `Gift wrap` → ACC: `GiftWrap` (finance_transaction, REVENUE)

| | Zewnętrzne | ACC |
|---|---|---|
| **Źródło API** | Orders API / Finance API | Finance API → `breakdownType` ∈ {`GiftWrap`, `GiftWrapTax`, `GiftWrapCharge`} |
| **Wartość** | Ujemna w ext. (−14.84 EUR total) — dziwne, powinno być revenue | ACC: REVENUE category, profit_layer=None |
| **Rola w profit** | Wliczane (ujemne = odejmowane??) | Pominięte w kalkulacji profit (profit_layer=None) |

**⚠️ ANOMALIA W ZEWNĘTRZNYM**: Gift wrap powinno być przychodem (kupujący płaci), ale w CSV jest ujemne (−14.84). To sugeruje, że jest to GiftWrapTax lub gift wrap chargeback (GiftwrapChargeback), nie revenue. Narzędzie zewnętrzne może mieć tu błąd w interpretacji znaku.

ACC traktuje GiftWrap jako revenue component i pomija w profit (tak samo jak shipping).

---

### 2.9 `Coupons` → ACC: brak bezpośredniego odpowiednika (tekst, nie kwota)

| | Zewnętrzne | ACC |
|---|---|---|
| **Typ danych** | **Tekst** (np. "Percentage Off 2025/12/16 13-34-11-655") | N/A |
| **Wartość pieniężna** | Brak — to opis kuponu, nie kwota | Coupon impact = wliczony w `promotion_discount` (Orders API) |
| **ACC fee types** | — | `CouponRedemptionFee`, `CouponParticipationFee`, `CouponPerformanceFee` (opłaty za udział w programie kuponowym) → kategoria `PROMO_FEE`, bucket `promo` (CM2) |

**Wniosek**: Zewnętrzne **nie liczy kwoty kuponów** — kolumna Coupons to tylko label. Faktyczny rabat z kuponu jest w `Item promotion`. ACC natomiast wyłapuje **opłaty pobierane przez Amazon za program kuponowy** (CouponRedemptionFee itp.) — to dodatkowy koszt, którego zewnętrzne nie widzi.

---

### 2.10 `ShippingCost` → ACC: `logistics_pln` (rollup)

| | Zewnętrzne | ACC |
|---|---|---|
| **Źródło** | Nieznane (prawdopodobnie estymacja lub BuyShippingAPI) | ERP Netfox (koszty wysyłki GLS/DHL/InPost) lub prorating z acc_order |
| **Wartość** | Ujemna (−576 485 EUR) — koszt wysyłki MFN | `logistics_pln` na rollupie |
| **Dokładność** | **User mówi: "jest zły"** | Oparty na rzeczywistych fakturach logistycznych z ERP |

**User-acknowledged**: Obie strony mogą mieć tu niedokładności. ACC czerpie z ERP (lepsze źródło), zewnętrzne prawdopodobnie szacuje.

---

## 3. Opłaty które ACC widzi, a zewnętrzne NIE

### 3.1 Opłaty CM2 (Finance Transaction → bucket allocation)

| ACC charge_type | Kategoria | ACC bucket | Kwota (2026 Q1) | Obecne w extern? |
|---|---|---|---|---|
| `FBAStorageFee` / `StorageFee` | FBA_STORAGE | fba_storage → `storage_fee_pln` | −12 265 EUR | ✅ (screeny P&L) |
| `FBALongTermStorageFee` / `FBAAgedInventorySurcharge` | FBA_STORAGE | fba_aged → `storage_fee_pln` | (w fba_storage) | ✅ (screeny P&L) |
| `FBARemovalFee` / `FBADisposalFee` | FBA_REMOVAL | fba_removal → `other_fees_pln` | −973 EUR | ✅ (screeny P&L) |
| `ShippingHB` (Heavy/Bulky) | SHIPPING_SURCHARGE | shipping_surcharge → `other_fees_pln` | −1 902 EUR | ✅ (screeny P&L: "Shipping hold-back") |
| `ShippingChargeback` | SHIPPING_SURCHARGE | shipping_surcharge | −634 EUR | ❌ |
| `ReturnPostageBilling_Postage/VAT` | REFUND | refund_cost → `refund_pln` | −604 EUR | ❌ |
| `CustomerReturnHRRUnitFee` | REFUND | refund_cost | −432 EUR | ✅ (screeny P&L: "Hrr non apparel rollup") |
| `DigitalServicesFee` / `DigitalServicesFeeFBA` | SERVICE_FEE | amazon_other_fee → `other_fees_pln` | −389 EUR (2748 trans.) | ✅ (screeny P&L: "Digital services fee") |
| `WAREHOUSE_DAMAGE` / `WAREHOUSE_LOST` | WAREHOUSE_LOSS | warehouse_loss | +51 EUR (reimbursement) | ✅ (screeny P&L: "Warehouse damage/lost") |
| `REVERSAL_REIMBURSEMENT` | WAREHOUSE_LOSS | warehouse_loss | +119 EUR | ✅ (screeny P&L: "Reversal reimbursement") |
| `AmazonForAllFee` | OTHER_FEE | amazon_other_fee | −36 EUR | ✅ (screeny P&L: "Amazon for all fee") |
| `RefundCommission` | REFERRAL_FEE | refund_cost (CM2) | −388 EUR | ✅ (screeny P&L: "Refunded referral fee") |
| `GiftwrapChargeback` | REFUND | refund_cost | −6 EUR | ❌ |

### 3.2 Opłaty NP (Net Profit layer)

| ACC charge_type | Kategoria | ACC bucket | Kwota (2026) | Obecne w extern? |
|---|---|---|---|---|
| `Subscription` | SERVICE_FEE | service_fee | −48 EUR | ✅ (screeny P&L: "Subscription") |
| `VineFee` | PROMO_FEE | promo | −209 EUR | ✅ (screeny P&L: "Vine enrollment fee") |

### 3.3 Cash flow entries (tracked but excluded from P&L)

| ACC charge_type | Kwota | P&L impact |
|---|---|---|
| `ReserveDebit` / `ReserveCredit` | ±44.9K EUR | Zero (profit_layer=None, sign=0) |

---

## 4. Opłaty z zewnętrznego raportu bez odpowiednika w ACC

| External column | Opis | Obecne w ACC? |
|---|---|---|
| `IsPremiumOrder` | Flag boolean | ❌ Nie śledzimy (nie jest opłatą) |
| `ShippedByAmazonTFM` | Flag (The Fulfilled by Merchant) | ❌ ACC ma fulfillment_channel (AFN/MFN) |
| `IsBusinessOrder` | Flag B2B | ❌ Nie śledzimy |

**Brak brakujących opłat** — wszystkie kolumny pieniężne z zewnętrznego mają odpowiednik w ACC.

---

## 5. Podsumowanie różnic w logice liczenia

### 5.1 Revenue

| Element | Zewnętrzne | ACC (po poprawce) | Status |
|---|---|---|---|
| **Formuła** | `OrderTotal + Shipping + GiftWrap + Tax + ItemPromo + ShipPromo` | `(item_price − item_tax − promotion_discount) × FX + (ShippingCharge + ShippingTax + ShippingDiscount) × FX × line_share` | ✅ Zbieżne |
| **Shipping** | W revenue | ✅ W revenue (AFN + MFN) | ✅ |
| **ShippingDiscount** | Odejmowane od shipping | ✅ Odejmowane (ujemne amount w CTE) | ✅ |
| **GiftWrap** | Obecne (ujemne — anomalia) | Pominięte (profit_layer=None) | ⚠️ Marginalny (−15 EUR) |

### 5.2 Commission / Referral Fee

| Element | Zewnętrzne | ACC (po poprawce) | Status |
|---|---|---|---|
| **CM1 zakres** | `Commission` (1 typ) | `Commission` + `ReferralFee` + `VariableClosingFee` + `FixedClosingFee` (4 typy) | ✅ ACC pełniejsze |
| **DigitalServicesFee** | CM2 osobna linia: "Digital services fee" | ✅ CM2/amazon_other_fee (SERVICE_FEE) | ✅ Zbieżne |
| **RefundCommission** | CM2 osobna linia: "Refunded referral fee" | ✅ CM2/refund_cost | ✅ Zbieżne |

### 5.3 FBA Fulfillment Fee

| Element | Zewnętrzne | ACC | Różnica |
|---|---|---|---|
| **Zakres** | Tylko `FBAPerUnitFulfillmentFee` | 8 wariantów FBA fee | ACC jest dokładniejsze |
| **ShippingHB** | ✅ Osobna linia "Shipping hold-back" | ✅ CM2/shipping_surcharge | ✅ Zbieżne |

### 5.4 Opłaty poza fulfillmentem i prowizją

| Element | Zewnętrzne (screeny P&L) | ACC |
|---|---|---|
| **FBA Storage** | ✅ Osobna linia | ✅ CM2/fba_storage |
| **FBA Removal** | ✅ Osobna linia | ✅ CM2/fba_removal |
| **Return costs** | ✅ Sekcja "Refund cost" z wieloma subliniami | ✅ CM2/refund_cost |
| **Warehouse loss** | ✅ "Warehouse damage" + "Warehouse lost" | ✅ CM2/warehouse_loss |
| **Vine** | ✅ "Vine enrollment fee" | ✅ CM2/promo |
| **Coupon fees** | ✅ "Coupon redemption/performance/participation fee" | ✅ CM2/promo |
| **Paid services fee** | ✅ "Paid services fee" (−5K/mies.) | ⚠️ Do weryfikacji mapowania |
| **Liquidation fees** | ✅ "Liquidations brokerage fee" | ✅ CM2/fba_liquidation |
| **SAFE-T reimbursement** | ✅ "Safet reimbursement charge" | ✅ CM2/warehouse_loss |
| **Deal fees** | ✅ "Deal participation fee rollup" | ✅ CM2/promo |

---

## 6. Diagram przepływu danych — ACC

```
SP-API Finance v2024-06-19
     │
     ▼
parse_transaction_fees()          ← breakdownType → charge_type
     │
     ▼
acc_finance_transaction           ← 70+ charge_types, amount w oryginalnej walucie + PLN
     │
     ├──▶ classify_fee()          ← FEE_REGISTRY (exact match / fuzzy / txn-type override)
     │         │
     │         ▼
     │    FeeEntry { category, profit_layer, profit_bucket, sign }
     │         │
     │    ┌────┴────────────────────────────┐
     │    │                                 │
     │    ▼                                 ▼
     │  profit_layer = None               profit_layer = "cm2" / "np"
     │  (CM1 fees, CASH_FLOW,             (CM2/NP costs)
     │   GiftWrap — skip)                 │
     │                                     ▼
     │  ShippingCharge/Tax/Discount  → Pool allocation by mkt/bucket
     │  → shipping_per_order CTE          │
     │  → proportional allocation →       ▼
     │    REVENUE (dodawane do rev)  acc_sku_profitability_rollup
     │                                     .refund_pln
     ▼                                     .storage_fee_pln
step_bridge_fees()                         .other_fees_pln ← DSF in CM2!
     │                                     .ad_spend_pln
     ▼
acc_order_line
  .referral_fee_pln
  .fba_fee_pln
     │
     ▼
MERGE → rollup
  .amazon_fees_pln ← referral
  .fba_fees_pln ← fba
  .revenue_pln ← item_price-tax-promo + shipping_netto
  .cogs_pln ← ERP
  .logistics_pln ← ERP/order
```

---

## 7. Wnioski i rekomendacje

### ✅ Gdzie ACC jest lepszy:
1. **70+ charge_types vs ~50 linii P&L** — ACC ma pełną taksonomię z FEE_REGISTRY + automatyczną klasyfikacją + profit_layer routing
2. **Per-SKU granulacja** — ACC bridguje opłaty do poziomu SKU, zewnętrzne ma per-ASIN (≈order)
3. **3-layer profit model** — CM1 (order-line), CM2 (finance pool), NP (overhead) vs płaski P&L
4. **FX handling** — ACC konwertuje po kursie z dnia zamówienia
5. **Unknown fee detection** — nieklasyfikowane charge_types → sign=0 + warning log

### ✅ Co naprawione (2026-03-09):
1. **Revenue = item + shipping** — ShippingCharge + ShippingTax + ShippingDiscount teraz w revenue (wszystkie kanały)
2. **DigitalServicesFee → CM2** — przeniesione z CM1/REFERRAL (profit_layer=None → LOST) do CM2/amazon_other_fee
3. **Shipping w alokacjach** — ratio_by_marketplace, ads allocation, loss_lines detection
4. **Brak podwójnego naliczenia** — zweryfikowane: ShippingCharge ma profit_layer=None (nie w cost pool), DSF nie był bridgowany do order_line

### ⚠️ Architektura P&L — narzędzie zewnętrzne vs ACC:
Narzędzie zewnętrzne prezentuje **~50 indywidualnych linii opłat** w P&L (np. "Digital services fee: −686 EUR", "FBA storage fee: −2,156 EUR", "Paid services fee: −5,000 EUR" itd.). To podejście jest transparentne i łatwe do audytu.

ACC "bundluje" opłaty do ~5 kolumn rollupa (`amazon_fees_pln`, `fba_fees_pln`, `storage_fee_pln`, `refund_pln`, `other_fees_pln`). To utrudnia weryfikację — w bundlu mogą ukrywać się duplikaty lub braki.

**Rekomendacja**: Rozbudować rollup o granularne kolumny per charge_type (lub dodać drill-down endpoint) żeby użytkownik widział rozkład identyczny jak w narzędziu zewnętrznym.

### 🔴 Do weryfikacji:
1. **GiftWrap revenue** — zewnętrzne raportuje jako ujemne (anomalia), ACC pomija → marginalny wpływ (−15 EUR), ale do weryfikacji
2. **Paid services fee** — zewnętrzne: −5K EUR/mies., do sprawdzenia czy ACC ma odpowiadający charge_type
3. **Finance data coverage** — starsze zamówienia (Dec 2025) mogą nie mieć finance transactions z powodu 180-dniowego okna API

---

## 8. Podsumowanie mapowania — tabela finalna

| # | Opłata (extern) | → ACC charge_type(s) | ACC źródło | ACC warstwa | ACC rollup column | Zgodność |
|---|---|---|---|---|---|---|
| 1 | OrderTotalAmount | item_price | Orders API | CM1 order_line | revenue_pln (po odj. tax+promo) | ✅ Logika zbieżna |
| 2 | Shipping (buyer) | ShippingCharge + ShippingTax + ShippingDiscount | Finance API | ✅ revenue (via CTE + proportion) | revenue_pln (dodawane) | ✅ Zbieżne |
| 3 | Tax | Tax, item_tax + 11 subwariantów | Orders+Finance API | CM1 (via item_tax) | w revenue_pln (odjęte) | ✅ Zbieżne |
| 4 | Comission | Commission, ReferralFee, VariableClosingFee, FixedClosingFee | Finance API | CM1 → order_line.referral_fee_pln | amazon_fees_pln | ✅ ACC pełniejsze |
| 4b | Digital services fee | DigitalServicesFee, DigitalServicesFeeFBA | Finance API | CM2 → amazon_other_fee | other_fees_pln | ✅ Zbieżne |
| 5 | FBAPerUnitFulfillmentFee | FBAPerUnit/PerOrder/Weight/Pick/WeightHandling/OrderHandling/Delivery | Finance API | CM1 → order_line.fba_fee_pln | fba_fees_pln | ✅+ ACC pełniejsze |
| 6 | Item promotion | promotion_discount + PromotionDiscount | Orders API | CM1 (via promo_discount) | w revenue_pln (odjęte) | ✅ Zbieżne |
| 7 | Ship Promotion | ShippingDiscount | Finance API | ✅ w shipping CTE (pomniejsza revenue) | w revenue_pln | ✅ Zbieżne |
| 8 | Gift wrap | GiftWrap, GiftWrapTax, GiftWrapCharge | Finance API | REVENUE (skip) | — | ⚠️ ACC pomija (marginalny) |
| 9 | Coupons | (tekst, nie kwota) | — | — | — | N/A (nie jest fee) |
| 10 | ShippingCost | logistics_pln (z ERP) | ERP Netfox | CM1 rollup | logistics_pln | ⚠️ Różne źródła |
| 11 | FBA storage fee | FBAStorageFee + warianty | Finance API | CM2 | storage_fee_pln | ✅ Zbieżne |
| 12 | FBA removal/disposal | FBARemovalFee, FBADisposalFee | Finance API | CM2 | other_fees_pln | ✅ Zbieżne |
| 13 | Shipping hold-back | ShippingHB, ShippingChargeback | Finance API | CM2 | other_fees_pln | ✅ Zbieżne |
| 14 | Refund cost | ReturnPostage*, CustomerReturnHRR*, Goodwill, Concession | Finance API | CM2 | refund_pln | ✅ Zbieżne |
| 15 | Warehouse damage/lost | WAREHOUSE_DAMAGE/LOST, SAFE-T* | Finance API | CM2 | other_fees_pln | ✅ Zbieżne |
| 16 | Vine/Deal/Coupon fees | VineFee, DealFees, CouponFees | Finance API | CM2 | other_fees_pln | ✅ Zbieżne |
| 17 | Subscription | Subscription, EPR*, ServiceFees | Finance API | NP | (overhead) | ✅ Zbieżne |

**Legenda**: ✅ = zbieżne, ✅+ = ACC pełniejsze, ⚠️ = różnica, ❌ = brak w jednym systemie
