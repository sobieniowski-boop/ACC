import React from "react";
import ReactDOM from "react-dom/client";
import * as Sentry from "@sentry/react";
import { QueryClient, QueryClientProvider, MutationCache } from "@tanstack/react-query";
import type { AxiosError } from "axios";
import App from "./App";
import "./index.css";
import "./css/design-system.css";
import "./css/layout.css";
import "./css/components.css";

const SENTRY_DSN = import.meta.env.VITE_SENTRY_DSN as string | undefined;

if (SENTRY_DSN) {
  Sentry.init({
    dsn: SENTRY_DSN,
    environment: import.meta.env.VITE_APP_ENV || "development",
    integrations: [
      Sentry.browserTracingIntegration(),
      Sentry.replayIntegration({ maskAllText: true, blockAllMedia: true }),
    ],
    tracesSampleRate: import.meta.env.PROD ? 0.1 : 0.0,
    replaysSessionSampleRate: 0,
    replaysOnErrorSampleRate: 1.0,
    sendDefaultPii: false,
  });
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 60_000,
      gcTime: 15 * 60_000,
      retry: 1,
      refetchOnWindowFocus: false,
      refetchOnReconnect: false,
      refetchIntervalInBackground: false,
    },
  },
  mutationCache: new MutationCache({
    onError: (error) => {
      const axErr = error as AxiosError<{ detail?: string }>;
      const msg = axErr.response?.data?.detail || axErr.message || "Operacja nie powiodła się";
      console.error("[Mutation]", axErr.response?.status, msg);
      Sentry.captureException(error);
    },
  }),
});

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <Sentry.ErrorBoundary
      fallback={({ error }) => (
        <div style={{ padding: "2rem", textAlign: "center" }}>
          <h1>Wystąpił nieoczekiwany błąd</h1>
          <p>{(error as Error)?.message}</p>
          <button onClick={() => window.location.reload()}>
            Odśwież stronę
          </button>
        </div>
      )}
    >
      <QueryClientProvider client={queryClient}>
        <App />
      </QueryClientProvider>
    </Sentry.ErrorBoundary>
  </React.StrictMode>
);
