import { create } from "zustand";
import { persist } from "zustand/middleware";

export type ProfitMode = "cm1" | "cm2" | "np";
export type CurrencyView = "base" | "original";
export type RowDensity = "compact" | "comfortable";

interface UserPreferencesState {
  currencyView: CurrencyView;
  profitMode: ProfitMode;
  rowDensity: RowDensity;
  setCurrencyView: (v: CurrencyView) => void;
  setProfitMode: (v: ProfitMode) => void;
  setRowDensity: (v: RowDensity) => void;
}

export const useUserPreferences = create<UserPreferencesState>()(
  persist(
    (set) => ({
      currencyView: "base",
      profitMode: "cm1",
      rowDensity: "comfortable",
      setCurrencyView: (currencyView) => set({ currencyView }),
      setProfitMode: (profitMode) => set({ profitMode }),
      setRowDensity: (rowDensity) => set({ rowDensity }),
    }),
    { name: "acc-user-preferences" },
  ),
);
