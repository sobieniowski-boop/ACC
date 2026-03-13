import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Calculator, DollarSign, TrendingUp, Target, ChevronDown } from "lucide-react";
import { simulatePrice } from "@/lib/api";
import type { PriceSimulatorResult } from "@/lib/api";
import { formatPct, cn } from "@/lib/utils";

const CURRENCIES = [
  { value: "EUR", label: "€ EUR" },
  { value: "GBP", label: "£ GBP" },
  { value: "SEK", label: "kr SEK" },
  { value: "PLN", label: "zł PLN" },
];

function InputField({
  label, value, onChange, placeholder, step = "0.01", min = "0",
}: {
  label: string; value: string; onChange: (v: string) => void; placeholder?: string; step?: string; min?: string;
}) {
  return (
    <label className="space-y-1">
      <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{label}</span>
      <input
        type="number"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        step={step}
        min={min}
        className="block w-full rounded-lg border border-border bg-card px-3 py-2 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-amazon/50"
      />
    </label>
  );
}

function ResultCard({
  label, value, suffix, color, icon: Icon,
}: {
  label: string; value: string; suffix?: string; color?: string; icon: React.ElementType;
}) {
  return (
    <div className="rounded-xl border border-border bg-card p-5">
      <div className="mb-3 flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">{label}</span>
        <div className="rounded-lg bg-muted p-1.5">
          <Icon className={cn("h-4 w-4", color)} />
        </div>
      </div>
      <div className={cn("text-2xl font-bold tabular-nums", color)}>
        {value}{suffix && <span className="text-sm font-normal text-muted-foreground ml-1">{suffix}</span>}
      </div>
    </div>
  );
}

export default function PriceSimulatorPage() {
  const [salePrice, setSalePrice] = useState("29.99");
  const [purchaseCost, setPurchaseCost] = useState("8.50");
  const [shippingCost, setShippingCost] = useState("2.00");
  const [amazonFeePct, setAmazonFeePct] = useState("15");
  const [fbaFee, setFbaFee] = useState("3.50");
  const [adCost, setAdCost] = useState("1.50");
  const [currency, setCurrency] = useState("EUR");
  const [fxRate, setFxRate] = useState("");
  const [validationError, setValidationError] = useState<string | null>(null);

  const mutation = useMutation({
    mutationFn: () =>
      simulatePrice({
        sale_price: parseFloat(salePrice) || 0,
        purchase_cost: parseFloat(purchaseCost) || 0,
        shipping_cost: parseFloat(shippingCost) || 0,
        amazon_fee_pct: parseFloat(amazonFeePct) || 15,
        fba_fee: parseFloat(fbaFee) || 0,
        ad_cost: parseFloat(adCost) || 0,
        currency,
        ...(fxRate ? { fx_rate: parseFloat(fxRate) } : {}),
      }),
  });

  const result: PriceSimulatorResult | undefined = mutation.data;

  const handleCalculate = () => {
    const price = parseFloat(salePrice);
    if (!price || price <= 0) {
      setValidationError("Sale price must be greater than 0.");
      return;
    }
    setValidationError(null);
    mutation.mutate();
  };

  const currSymbol = CURRENCIES.find((c) => c.value === currency)?.label.charAt(0) ?? "€";

  return (
    <div className="space-y-6 p-6 max-w-4xl">
      <div className="flex items-center gap-3">
        <div className="rounded-lg bg-amazon/10 p-2">
          <Calculator className="h-6 w-6 text-amazon" />
        </div>
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Price Simulator</h1>
          <p className="text-sm text-muted-foreground">Calculate profit, margin, and breakeven price for an Amazon product.</p>
        </div>
      </div>

      {/* Input form */}
      <div className="rounded-xl border border-border bg-card p-6 space-y-5">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          <InputField label="Sale Price" value={salePrice} onChange={setSalePrice} placeholder="29.99" />
          <InputField label="Purchase Cost" value={purchaseCost} onChange={setPurchaseCost} placeholder="8.50" />
          <InputField label="Shipping Cost" value={shippingCost} onChange={setShippingCost} placeholder="2.00" />
          <InputField label="Amazon Fee %" value={amazonFeePct} onChange={setAmazonFeePct} placeholder="15" />
          <InputField label="FBA Fee (per unit)" value={fbaFee} onChange={setFbaFee} placeholder="3.50" />
          <InputField label="Ad Cost (per unit)" value={adCost} onChange={setAdCost} placeholder="1.50" />
        </div>

        <div className="flex flex-wrap items-end gap-4">
          <label className="space-y-1">
            <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Currency</span>
            <div className="relative">
              <select
                value={currency}
                onChange={(e) => setCurrency(e.target.value)}
                className="appearance-none rounded-lg border border-border bg-card pl-3 pr-8 py-2 text-sm font-medium focus:outline-none focus:ring-2 focus:ring-amazon/50"
              >
                {CURRENCIES.map((c) => (
                  <option key={c.value} value={c.value}>{c.label}</option>
                ))}
              </select>
              <ChevronDown className="pointer-events-none absolute right-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            </div>
          </label>
          <InputField label="FX Rate (optional, auto if empty)" value={fxRate} onChange={setFxRate} placeholder="auto" />
          <button
            onClick={handleCalculate}
            disabled={mutation.isPending}
            className="self-end rounded-lg bg-amazon px-6 py-2 text-sm font-medium text-white hover:bg-amazon/90 disabled:opacity-50 transition-colors"
          >
            {mutation.isPending ? "Calculating…" : "Calculate"}
          </button>
        </div>
        {validationError && (
          <p className="text-sm text-destructive">{validationError}</p>
        )}
      </div>

      {/* Results */}
      {result && (
        <div className="space-y-4">
          <h2 className="text-lg font-semibold">Results</h2>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <ResultCard
              label="Profit per unit"
              value={`${currSymbol}${result.profit.toFixed(2)}`}
              icon={DollarSign}
              color={result.profit >= 0 ? "text-green-500" : "text-destructive"}
            />
            <ResultCard
              label="Margin"
              value={formatPct(result.margin_pct)}
              icon={TrendingUp}
              color={result.margin_pct >= 10 ? "text-green-500" : result.margin_pct >= 0 ? "text-yellow-500" : "text-destructive"}
            />
            <ResultCard
              label="Breakeven Price"
              value={`${currSymbol}${result.breakeven_price.toFixed(2)}`}
              icon={Target}
              color="text-amazon"
            />
            <ResultCard
              label="Total Cost"
              value={`${currSymbol}${result.total_cost.toFixed(2)}`}
              icon={Calculator}
            />
          </div>

          {/* Cost breakdown */}
          <div className="rounded-xl border border-border bg-card p-5">
            <h3 className="text-sm font-semibold mb-3">Cost Breakdown</h3>
            <div className="space-y-2">
              {[
                { label: "Purchase Cost", value: result.purchase_cost },
                { label: "Shipping Cost", value: result.shipping_cost },
                { label: "Amazon Fee", value: result.amazon_fee },
                { label: "FBA Fee", value: result.fba_fee },
                { label: "Ad Cost", value: result.ad_cost },
              ].map((row) => (
                <div key={row.label} className="flex items-center justify-between text-sm">
                  <span className="text-muted-foreground">{row.label}</span>
                  <span className="tabular-nums font-medium">
                    {currSymbol}{row.value.toFixed(2)}
                  </span>
                </div>
              ))}
              <div className="border-t border-border pt-2 flex items-center justify-between text-sm font-semibold">
                <span>Total Cost</span>
                <span className="tabular-nums">{currSymbol}{result.total_cost.toFixed(2)}</span>
              </div>
              <div className="flex items-center justify-between text-sm font-semibold">
                <span>Sale Price</span>
                <span className="tabular-nums">{currSymbol}{result.sale_price.toFixed(2)}</span>
              </div>
              <div className={cn("flex items-center justify-between text-sm font-bold",
                result.profit >= 0 ? "text-green-500" : "text-destructive")}>
                <span>Profit</span>
                <span className="tabular-nums">{currSymbol}{result.profit.toFixed(2)}</span>
              </div>
            </div>
            {result.fx_rate !== 1 && (
              <div className="mt-3 text-xs text-muted-foreground">
                FX rate: 1 {result.currency} = {result.fx_rate.toFixed(4)} PLN
              </div>
            )}
          </div>
        </div>
      )}

      {mutation.isError && (
        <div className="rounded-xl border border-destructive/50 bg-destructive/10 p-4 text-sm text-destructive">
          Error: {(mutation.error as Error).message}
        </div>
      )}
    </div>
  );
}
