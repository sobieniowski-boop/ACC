import { describe, it, expect, vi } from "vitest";
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { renderWithProviders } from "@/test/test-utils";
import { Button } from "../button";

describe("Button", () => {
  it("renders with default variant", () => {
    renderWithProviders(<Button>Kliknij</Button>);
    const btn = screen.getByRole("button", { name: "Kliknij" });
    expect(btn).toBeInTheDocument();
    expect(btn).toHaveClass("bg-primary");
  });

  it("renders destructive variant", () => {
    renderWithProviders(<Button variant="destructive">Usuń</Button>);
    const btn = screen.getByRole("button", { name: "Usuń" });
    expect(btn).toHaveClass("bg-destructive");
  });

  it("renders ghost variant", () => {
    renderWithProviders(<Button variant="ghost">Ghost</Button>);
    const btn = screen.getByRole("button", { name: "Ghost" });
    expect(btn).toHaveClass("hover:bg-accent");
  });

  it("handles click events", async () => {
    const user = userEvent.setup();
    const handleClick = vi.fn();
    renderWithProviders(<Button onClick={handleClick}>Click me</Button>);
    await user.click(screen.getByRole("button", { name: "Click me" }));
    expect(handleClick).toHaveBeenCalledOnce();
  });

  it("forwards ref", () => {
    const ref = vi.fn();
    renderWithProviders(<Button ref={ref}>Ref</Button>);
    expect(ref).toHaveBeenCalledWith(expect.any(HTMLButtonElement));
  });

  it("is disabled when disabled prop is set", () => {
    renderWithProviders(<Button disabled>Disabled</Button>);
    expect(screen.getByRole("button", { name: "Disabled" })).toBeDisabled();
  });
});
