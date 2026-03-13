import { Outlet } from "react-router-dom";
import { Component, type ErrorInfo, type ReactNode } from "react";
import * as Sentry from "@sentry/react";
import Sidebar from "./Sidebar";
import TopBar from "./TopBar";

class ErrorBoundary extends Component<
  { children: ReactNode },
  { error: Error | null }
> {
  state: { error: Error | null } = { error: null };

  static getDerivedStateFromError(error: Error) {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error("[ErrorBoundary]", error, info.componentStack);
    Sentry.captureException(error, { contexts: { react: { componentStack: info.componentStack ?? "" } } });
  }

  render() {
    if (this.state.error) {
      return (
        <div className="flex flex-1 items-center justify-center p-8">
          <div className="max-w-lg rounded-lg border border-red-500/30 bg-red-500/10 p-6">
            <h2 className="mb-2 text-lg font-bold text-red-400">
              Wystąpił błąd na stronie
            </h2>
            <p className="text-sm text-red-300">
              {this.state.error.message}
            </p>
            <button
              onClick={() => this.setState({ error: null })}
              className="mt-4 rounded bg-red-500/20 px-3 py-1 text-sm text-red-300 hover:bg-red-500/30"
            >
              Spróbuj ponownie
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

export default function Layout() {
  return (
    <div className="flex h-screen overflow-hidden bg-background">
      <Sidebar />
      <div className="flex flex-1 flex-col overflow-hidden">
        <TopBar />
        <main className="flex-1 overflow-y-auto p-6">
          <ErrorBoundary>
            <Outlet />
          </ErrorBoundary>
        </main>
      </div>
    </div>
  );
}
