import { describe, it, expect, beforeEach } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderWithProviders } from "@/test/test-utils";
import { ThemeToggle } from "../theme-toggle";
import { useThemeStore } from "@/store/themeStore";

describe("ThemeToggle", () => {
  beforeEach(() => {
    useThemeStore.setState({ theme: "system", resolvedTheme: "dark" });
    document.documentElement.classList.remove("dark");
  });

  it("renders with system icon by default", () => {
    renderWithProviders(<ThemeToggle />);
    const btn = screen.getByRole("button", { name: "Motyw systemowy" });
    expect(btn).toBeInTheDocument();
  });

  it("cycles system → light → dark → system", async () => {
    const user = userEvent.setup();
    renderWithProviders(<ThemeToggle />);

    const btn = screen.getByRole("button");

    // system → light
    await user.click(btn);
    expect(useThemeStore.getState().theme).toBe("light");

    // light → dark
    await user.click(btn);
    expect(useThemeStore.getState().theme).toBe("dark");

    // dark → system
    await user.click(btn);
    expect(useThemeStore.getState().theme).toBe("system");
  });
});
