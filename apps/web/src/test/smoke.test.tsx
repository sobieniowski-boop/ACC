import { renderWithProviders } from "../test/test-utils";

// Mock the API module
vi.mock("@/lib/api");

// Mock auth store
vi.mock("@/store/authStore", () => ({
  useAuthStore: Object.assign(
    vi.fn(() => ({
      accessToken: "test-token",
      user: { email: "test@test.com", role: "admin" },
      setAccessToken: vi.fn(),
      setUser: vi.fn(),
      logout: vi.fn(),
    })),
    {
      getState: () => ({
        accessToken: "test-token",
        user: { email: "test@test.com", role: "admin" },
        setAccessToken: vi.fn(),
        setUser: vi.fn(),
        logout: vi.fn(),
      }),
    }
  ),
}));

import React from "react";
import LoginPage from "@/pages/Login";
import { formatPLN, formatPct, cn } from "@/lib/utils";
import * as api from "@/lib/api";

// ── Test 1: Utility functions ──
describe("formatPLN", () => {
  it("formats number as PLN currency string", () => {
    const result = formatPLN(1234.56);
    expect(result).toContain("1");
    expect(typeof result).toBe("string");
  });

  it("handles zero", () => {
    const result = formatPLN(0);
    expect(result).toBeDefined();
  });
});

describe("formatPct", () => {
  it("formats percentage", () => {
    const result = formatPct(12.34);
    expect(result).toContain("12");
  });
});

describe("cn utility", () => {
  it("merges class names", () => {
    expect(cn("foo", "bar")).toBe("foo bar");
  });

  it("handles conditional classes", () => {
    expect(cn("foo", false && "bar", "baz")).toBe("foo baz");
  });
});

// ── Test 2: API module exports ──
describe("API module exports", () => {
  it("exports essential functions", () => {
    expect(typeof api.login).toBe("function");
    expect(typeof api.getMe).toBe("function");
  });
});

// ── Test 3: Login page renders ──
describe("LoginPage", () => {
  it("renders input fields", () => {
    const { container } = renderWithProviders(<LoginPage />);
    const inputs = container.querySelectorAll("input");
    expect(inputs.length).toBeGreaterThanOrEqual(2);
  });

  it("renders a submit button", () => {
    const { container } = renderWithProviders(<LoginPage />);
    const button = container.querySelector("button[type='submit'], button");
    expect(button).toBeTruthy();
  });
});
