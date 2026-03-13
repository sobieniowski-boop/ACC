import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Building2,
  Key,
  Shield,
  CalendarClock,
  Plus,
  CheckCircle2,
  XCircle,
  Eye,
  EyeOff,
  Trash2,
  RefreshCw,
} from "lucide-react";
import {
  getAccountHubDashboard,
  getSellerAccounts,
  createSellerAccount,
  updateSellerAccount,
  getSellerCredentials,
  storeSellerCredential,
  revokeSellerCredential,
  validateSellerCredentials,
  getSellerPermissions,
  grantSellerPermission,
  revokeSellerPermission,
  getSellerSchedulerStatus,
} from "@/lib/api";
import type {
  SellerAccount,
  SellerCredentialMeta,
  SellerPermission,
  AccountHubDashboard,
  CredentialValidation,
  SchedulerStatus,
} from "@/lib/api";
import { cn } from "@/lib/utils";

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

const SELLER_STATUS_STYLES: Record<string, string> = {
  active: "bg-green-500/10 text-green-400",
  inactive: "bg-zinc-500/10 text-zinc-400",
  suspended: "bg-red-500/10 text-red-400",
  onboarding: "bg-amber-500/10 text-amber-400",
};

const PERM_STYLES: Record<string, string> = {
  admin: "bg-red-500/10 text-red-400",
  full: "bg-blue-500/10 text-blue-400",
  read_only: "bg-green-500/10 text-green-400",
};

/* ------------------------------------------------------------------ */
/*  Dashboard KPIs                                                     */
/* ------------------------------------------------------------------ */

function DashboardKPIs({ data }: { data: AccountHubDashboard | undefined }) {
  const cards = [
    { label: "Aktywni sprzedawcy", value: data?.sellers?.active ?? 0, color: "text-green-400", icon: Building2 },
    { label: "Lacznie sprzedawcy", value: data?.sellers?.total ?? 0, color: "text-blue-400", icon: Building2 },
    { label: "Unikalni uzytkownicy", value: data?.users_with_access ?? 0, color: "text-purple-400", icon: Shield },
    { label: "Powiadczenia", value: data?.valid_credentials ?? 0, color: "text-amber-400", icon: Key },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      {cards.map((c) => (
        <div key={c.label} className="rounded-xl border border-border bg-card p-4">
          <div className="flex items-center gap-2 text-sm text-muted-foreground mb-1">
            <c.icon className={cn("h-4 w-4", c.color)} />
            {c.label}
          </div>
          <p className="text-2xl font-bold">{c.value}</p>
        </div>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Sellers Tab                                                        */
/* ------------------------------------------------------------------ */

function SellersPanel() {
  const [showCreate, setShowCreate] = useState(false);
  const queryClient = useQueryClient();

  const { data: sellers, isLoading } = useQuery({
    queryKey: ["seller-accounts"],
    queryFn: () => getSellerAccounts(),
  });

  const createMut = useMutation({
    mutationFn: (d: { seller_id: string; name: string; marketplace_ids: string[] }) =>
      createSellerAccount(d),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["seller-accounts"] });
      queryClient.invalidateQueries({ queryKey: ["account-hub-dashboard"] });
      setShowCreate(false);
    },
  });

  const updateMut = useMutation({
    mutationFn: ({ id, ...rest }: { id: number; status?: string }) =>
      updateSellerAccount(id, rest),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["seller-accounts"] });
      queryClient.invalidateQueries({ queryKey: ["account-hub-dashboard"] });
    },
  });

  const [fSellerId, setFSellerId] = useState("");
  const [fSellerName, setFSellerName] = useState("");
  const [fMarketplaces, setFMarketplaces] = useState("");

  return (
    <div className="space-y-4">
      <div className="flex justify-end">
        <button onClick={() => setShowCreate(!showCreate)}
          className="flex items-center gap-1 rounded-md bg-primary px-3 py-1.5 text-sm text-primary-foreground">
          <Plus className="h-4 w-4" /> Dodaj sprzedawce
        </button>
      </div>

      {showCreate && (
        <div className="rounded-lg border border-border p-4 space-y-3 bg-card">
          <input value={fSellerId} onChange={(e) => setFSellerId(e.target.value)}
            placeholder="Seller ID (np. A1O0H08K2DYVHX)" className="w-full rounded border border-border bg-background px-3 py-1.5 text-sm" />
          <input value={fSellerName} onChange={(e) => setFSellerName(e.target.value)}
            placeholder="Nazwa sprzedawcy" className="w-full rounded border border-border bg-background px-3 py-1.5 text-sm" />
          <input value={fMarketplaces} onChange={(e) => setFMarketplaces(e.target.value)}
            placeholder="Marketplace IDs (oddzielone przecinkami)" className="w-full rounded border border-border bg-background px-3 py-1.5 text-sm" />
          <div className="flex gap-2">
            <button onClick={() => { if (fSellerId.trim() && fSellerName.trim()) createMut.mutate({ seller_id: fSellerId.trim(), name: fSellerName.trim(), marketplace_ids: fMarketplaces.split(",").map(s => s.trim()).filter(Boolean) }); }}
              disabled={!fSellerId.trim() || !fSellerName.trim()} className="rounded bg-primary px-4 py-1.5 text-sm text-primary-foreground disabled:opacity-50">Utworz</button>
            <button onClick={() => setShowCreate(false)} className="rounded border px-4 py-1.5 text-sm">Anuluj</button>
          </div>
        </div>
      )}

      {isLoading && <p className="text-sm text-muted-foreground">Ladowanie...</p>}

      <div className="space-y-2">
        {sellers?.items?.map((s: SellerAccount) => (
          <div key={s.id} className="flex items-center gap-3 rounded-lg border border-border p-3">
            <Building2 className="h-5 w-5 text-muted-foreground flex-shrink-0" />
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium truncate">{s.name}</p>
              <div className="flex gap-2 mt-0.5 text-xs text-muted-foreground">
                <span className="font-mono">{s.seller_id}</span>
                {s.marketplace_ids && <span>{s.marketplace_ids.join(", ")}</span>}
              </div>
            </div>
            <span className={cn("rounded px-2 py-0.5 text-xs", SELLER_STATUS_STYLES[s.status] || "bg-muted text-muted-foreground")}>
              {s.status}
            </span>
            {s.status === "onboarding" && (
              <button onClick={() => updateMut.mutate({ id: s.id, status: "active" })}
                className="rounded border border-green-500/30 px-2 py-1 text-xs text-green-400 hover:bg-green-500/10" title="Aktywuj">
                <CheckCircle2 className="h-3 w-3" />
              </button>
            )}
            {s.status === "active" && (
              <button onClick={() => updateMut.mutate({ id: s.id, status: "inactive" })}
                className="rounded border border-zinc-500/30 px-2 py-1 text-xs text-zinc-400 hover:bg-zinc-500/10" title="Deaktywuj">
                <XCircle className="h-3 w-3" />
              </button>
            )}
          </div>
        ))}
        {sellers && sellers.items.length === 0 && (
          <p className="text-sm text-muted-foreground text-center py-8">Brak sprzedawcow</p>
        )}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Credentials Tab                                                    */
/* ------------------------------------------------------------------ */

function CredentialsPanel() {
  const queryClient = useQueryClient();
  const [selectedSeller, setSelectedSeller] = useState<number | null>(null);
  const [showStore, setShowStore] = useState(false);
  const [credType, setCredType] = useState("sp_api");
  const [credKey, setCredKey] = useState("");
  const [credValue, setCredValue] = useState("");

  const { data: sellers } = useQuery({
    queryKey: ["seller-accounts"],
    queryFn: () => getSellerAccounts(),
  });

  const { data: creds, isLoading: credsLoading } = useQuery({
    queryKey: ["seller-creds", selectedSeller],
    queryFn: () => (selectedSeller ? getSellerCredentials(selectedSeller) : Promise.resolve([])),
    enabled: !!selectedSeller,
  });

  const { data: validation, refetch: refetchValidation } = useQuery({
    queryKey: ["seller-creds-validate", selectedSeller],
    queryFn: () => (selectedSeller ? validateSellerCredentials(selectedSeller) : Promise.resolve(null)),
    enabled: false,
  });

  const storeMut = useMutation({
    mutationFn: () =>
      storeSellerCredential(selectedSeller!, {
        credential_type: credType,
        credential_key: credKey,
        plaintext_value: credValue,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["seller-creds", selectedSeller] });
      setShowStore(false);
      setCredKey("");
      setCredValue("");
    },
  });

  const revokeMut = useMutation({
    mutationFn: (credId: number) => revokeSellerCredential(credId),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["seller-creds", selectedSeller] }),
  });

  return (
    <div className="space-y-4">
      <div className="flex gap-2 items-center flex-wrap">
        <select value={selectedSeller ?? ""} onChange={(e) => setSelectedSeller(e.target.value ? Number(e.target.value) : null)}
          className="rounded-md border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">Wybierz sprzedawce</option>
          {sellers?.items?.map((s: SellerAccount) => (
            <option key={s.id} value={s.id}>{s.name} ({s.seller_id})</option>
          ))}
        </select>
        {selectedSeller && (
          <>
            <button onClick={() => setShowStore(!showStore)}
              className="flex items-center gap-1 rounded-md bg-primary px-3 py-1.5 text-sm text-primary-foreground">
              <Plus className="h-4 w-4" /> Dodaj klucz
            </button>
            <button onClick={() => refetchValidation()}
              className="flex items-center gap-1 rounded-md border px-3 py-1.5 text-sm hover:bg-muted">
              <RefreshCw className="h-4 w-4" /> Waliduj
            </button>
          </>
        )}
      </div>

      {validation && (
        <div className={cn("rounded-lg border p-3 text-sm", validation.valid ? "border-green-500/30 bg-green-500/5" : "border-amber-500/30 bg-amber-500/5")}>
          <p className="font-medium">{validation.valid ? "Wszystkie wymagane klucze obecne" : "Brakujace klucze"}</p>
          {validation.missing_keys && validation.missing_keys.length > 0 && (
            <ul className="mt-1 text-xs text-muted-foreground list-disc list-inside">
              {validation.missing_keys.map((k: string) => <li key={k}>{k}</li>)}
            </ul>
          )}
        </div>
      )}

      {showStore && selectedSeller && (
        <div className="rounded-lg border border-border p-4 space-y-3 bg-card">
          <div className="flex gap-2">
            <select value={credType} onChange={(e) => setCredType(e.target.value)}
              className="rounded border border-border bg-background px-3 py-1.5 text-sm">
              <option value="sp_api">SP-API</option>
              <option value="ads_api">Ads API</option>
              <option value="lwa">LWA</option>
            </select>
            <input value={credKey} onChange={(e) => setCredKey(e.target.value)}
              placeholder="Klucz (np. refresh_token)" className="flex-1 rounded border border-border bg-background px-3 py-1.5 text-sm" />
          </div>
          <input value={credValue} onChange={(e) => setCredValue(e.target.value)}
            type="password" placeholder="Wartosc (zaszyfrowana po zapisie)"
            className="w-full rounded border border-border bg-background px-3 py-1.5 text-sm" />
          <div className="flex gap-2">
            <button onClick={() => { if (credKey.trim() && credValue.trim()) storeMut.mutate(); }}
              disabled={!credKey.trim() || !credValue.trim()} className="rounded bg-primary px-4 py-1.5 text-sm text-primary-foreground disabled:opacity-50">Zapisz</button>
            <button onClick={() => setShowStore(false)} className="rounded border px-4 py-1.5 text-sm">Anuluj</button>
          </div>
        </div>
      )}

      {credsLoading && <p className="text-sm text-muted-foreground">Ladowanie...</p>}

      {!selectedSeller && (
        <p className="text-sm text-muted-foreground text-center py-8">Wybierz sprzedawce aby zobaczyc klucze</p>
      )}

      <div className="space-y-2">
        {creds?.map((c: SellerCredentialMeta) => (
          <div key={c.id} className="flex items-center gap-3 rounded-lg border border-border p-3">
            <Key className="h-4 w-4 text-muted-foreground" />
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium">{c.credential_key}</p>
              <div className="flex gap-2 mt-0.5 text-xs text-muted-foreground">
                <span className="rounded bg-muted px-1.5 py-0.5">{c.credential_type}</span>
                <span>{new Date(c.updated_at).toLocaleDateString("pl-PL")}</span>
              </div>
            </div>
            <span className={cn("rounded px-2 py-0.5 text-xs", c.is_valid ? "bg-green-500/10 text-green-400" : "bg-red-500/10 text-red-400")}>
              {c.is_valid ? "Aktywny" : "Uniewazniony"}
            </span>
            {c.is_valid && (
              <button onClick={() => revokeMut.mutate(c.id)}
                className="rounded border border-red-500/30 px-2 py-1 text-xs text-red-400 hover:bg-red-500/10" title="Uniewaznij">
                <Trash2 className="h-3 w-3" />
              </button>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Permissions Tab                                                    */
/* ------------------------------------------------------------------ */

function PermissionsPanel() {
  const queryClient = useQueryClient();
  const [selectedSeller, setSelectedSeller] = useState<number | null>(null);
  const [showGrant, setShowGrant] = useState(false);
  const [grantEmail, setGrantEmail] = useState("");
  const [grantLevel, setGrantLevel] = useState("read_only");

  const { data: sellers } = useQuery({
    queryKey: ["seller-accounts"],
    queryFn: () => getSellerAccounts(),
  });

  const { data: perms, isLoading } = useQuery({
    queryKey: ["seller-perms", selectedSeller],
    queryFn: () => (selectedSeller ? getSellerPermissions(selectedSeller) : Promise.resolve([])),
    enabled: !!selectedSeller,
  });

  const grantMut = useMutation({
    mutationFn: () =>
      grantSellerPermission(selectedSeller!, { user_email: grantEmail, permission_level: grantLevel, granted_by: "ui" }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["seller-perms", selectedSeller] });
      setShowGrant(false);
      setGrantEmail("");
    },
  });

  const revokeMut = useMutation({
    mutationFn: ({ sellerId, email }: { sellerId: number; email: string }) =>
      revokeSellerPermission(sellerId, { user_email: email }),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["seller-perms", selectedSeller] }),
  });

  return (
    <div className="space-y-4">
      <div className="flex gap-2 items-center flex-wrap">
        <select value={selectedSeller ?? ""} onChange={(e) => setSelectedSeller(e.target.value ? Number(e.target.value) : null)}
          className="rounded-md border border-border bg-background px-3 py-1.5 text-sm">
          <option value="">Wybierz sprzedawce</option>
          {sellers?.items?.map((s: SellerAccount) => (
            <option key={s.id} value={s.id}>{s.name} ({s.seller_id})</option>
          ))}
        </select>
        {selectedSeller && (
          <button onClick={() => setShowGrant(!showGrant)}
            className="flex items-center gap-1 rounded-md bg-primary px-3 py-1.5 text-sm text-primary-foreground">
            <Plus className="h-4 w-4" /> Nadaj uprawnienie
          </button>
        )}
      </div>

      {showGrant && selectedSeller && (
        <div className="rounded-lg border border-border p-4 space-y-3 bg-card">
          <input value={grantEmail} onChange={(e) => setGrantEmail(e.target.value)}
            placeholder="Email uzytkownika" type="email" className="w-full rounded border border-border bg-background px-3 py-1.5 text-sm" />
          <select value={grantLevel} onChange={(e) => setGrantLevel(e.target.value)}
            className="rounded border border-border bg-background px-3 py-1.5 text-sm">
            <option value="admin">Admin</option>
            <option value="full">Pelny</option>
            <option value="read_only">Tylko odczyt</option>
          </select>
          <div className="flex gap-2">
            <button onClick={() => { if (grantEmail.trim()) grantMut.mutate(); }}
              disabled={!grantEmail.trim()} className="rounded bg-primary px-4 py-1.5 text-sm text-primary-foreground disabled:opacity-50">Nadaj</button>
            <button onClick={() => setShowGrant(false)} className="rounded border px-4 py-1.5 text-sm">Anuluj</button>
          </div>
        </div>
      )}

      {!selectedSeller && (
        <p className="text-sm text-muted-foreground text-center py-8">Wybierz sprzedawce aby zobaczyc uprawnienia</p>
      )}

      {isLoading && <p className="text-sm text-muted-foreground">Ladowanie...</p>}

      <div className="space-y-2">
        {perms?.map((p: SellerPermission) => (
          <div key={p.id} className="flex items-center gap-3 rounded-lg border border-border p-3">
            <Shield className="h-4 w-4 text-muted-foreground" />
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium">{p.user_email}</p>
              <p className="text-xs text-muted-foreground mt-0.5">
                od {new Date(p.granted_at).toLocaleDateString("pl-PL")}
              </p>
            </div>
            <span className={cn("rounded px-2 py-0.5 text-xs", PERM_STYLES[p.permission_level] || "bg-muted text-muted-foreground")}>
              {p.permission_level}
            </span>
            <button onClick={() => revokeMut.mutate({ sellerId: selectedSeller!, email: p.user_email })}
              className="rounded border border-red-500/30 px-2 py-1 text-xs text-red-400 hover:bg-red-500/10" title="Uniewaznij">
              <XCircle className="h-3 w-3" />
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Scheduler Tab                                                      */
/* ------------------------------------------------------------------ */

function SchedulerPanel() {
  const { data, isLoading } = useQuery({
    queryKey: ["seller-scheduler"],
    queryFn: getSellerSchedulerStatus,
  });

  return (
    <div className="space-y-4">
      {isLoading && <p className="text-sm text-muted-foreground">Ladowanie...</p>}
      <div className="space-y-2">
        {data?.map((s: SchedulerStatus) => (
          <div key={s.seller_id} className="flex items-center gap-3 rounded-lg border border-border p-3">
            <CalendarClock className="h-5 w-5 text-muted-foreground" />
            <div className="flex-1 min-w-0">
              <p className="text-sm font-medium">{s.name}</p>
              <p className="text-xs text-muted-foreground mt-0.5 font-mono">{s.seller_id}</p>
            </div>
            <div className="flex gap-3 text-xs text-muted-foreground">
              <span>{s.jobs_last_24h} zadań (24h)</span>
              <span>{s.last_job_at ? new Date(s.last_job_at).toLocaleString("pl-PL") : "brak"}</span>
            </div>
            <span className={cn("rounded px-2 py-0.5 text-xs", s.status === "active" ? "bg-green-500/10 text-green-400" : "bg-zinc-500/10 text-zinc-400")}>
              {s.status}
            </span>
          </div>
        ))}
        {data && data.length === 0 && (
          <p className="text-sm text-muted-foreground text-center py-8">Brak danych schedulera</p>
        )}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/*  Page                                                               */
/* ------------------------------------------------------------------ */

const TABS = [
  { key: "sellers", label: "Sprzedawcy", icon: Building2 },
  { key: "credentials", label: "Klucze", icon: Key },
  { key: "permissions", label: "Uprawnienia", icon: Shield },
  { key: "scheduler", label: "Scheduler", icon: CalendarClock },
] as const;

type TabKey = (typeof TABS)[number]["key"];

export default function AccountHub() {
  const [tab, setTab] = useState<TabKey>("sellers");

  const { data: dashboard } = useQuery({
    queryKey: ["account-hub-dashboard"],
    queryFn: getAccountHubDashboard,
    refetchInterval: 30_000,
  });

  return (
    <div className="mx-auto max-w-7xl space-y-6 p-6">
      <h1 className="text-2xl font-bold">Account Hub</h1>

      <DashboardKPIs data={dashboard} />

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-border">
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            className={cn(
              "flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors",
              tab === t.key
                ? "border-primary text-primary"
                : "border-transparent text-muted-foreground hover:text-foreground"
            )}
          >
            <t.icon className="h-4 w-4" />
            {t.label}
          </button>
        ))}
      </div>

      {/* Tab content */}
      {tab === "sellers" && <SellersPanel />}
      {tab === "credentials" && <CredentialsPanel />}
      {tab === "permissions" && <PermissionsPanel />}
      {tab === "scheduler" && <SchedulerPanel />}
    </div>
  );
}
