import { describe, it, expect } from "vitest";
import { screen } from "@testing-library/react";
import { renderWithProviders } from "@/test/test-utils";
import { Alert, AlertTitle, AlertDescription } from "../alert";

describe("Alert", () => {
  it("renders default variant", () => {
    renderWithProviders(
      <Alert>
        <AlertTitle>Tytuł</AlertTitle>
        <AlertDescription>Opis alertu</AlertDescription>
      </Alert>,
    );
    expect(screen.getByRole("alert")).toBeInTheDocument();
    expect(screen.getByText("Tytuł")).toBeInTheDocument();
    expect(screen.getByText("Opis alertu")).toBeInTheDocument();
  });

  it("renders destructive variant", () => {
    renderWithProviders(
      <Alert variant="destructive">
        <AlertTitle>Błąd</AlertTitle>
      </Alert>,
    );
    const alert = screen.getByRole("alert");
    expect(alert).toHaveClass("text-destructive");
  });

  it("renders success variant", () => {
    renderWithProviders(
      <Alert variant="success">
        <AlertTitle>Sukces</AlertTitle>
      </Alert>,
    );
    const alert = screen.getByRole("alert");
    expect(alert).toHaveClass("text-emerald-600");
  });

  it("renders warning variant", () => {
    renderWithProviders(
      <Alert variant="warning">
        <AlertTitle>Uwaga</AlertTitle>
      </Alert>,
    );
    const alert = screen.getByRole("alert");
    expect(alert).toHaveClass("text-amber-600");
  });
});
