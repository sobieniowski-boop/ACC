import { Sun, Moon, Monitor } from "lucide-react";
import { Button } from "@/components/ui/button";
import { useThemeStore, type Theme } from "@/store/themeStore";

const cycle: Record<Theme, Theme> = {
  system: "light",
  light: "dark",
  dark: "system",
};

const icons: Record<Theme, React.ReactNode> = {
  system: <Monitor className="h-4 w-4" />,
  light: <Sun className="h-4 w-4" />,
  dark: <Moon className="h-4 w-4" />,
};

const labels: Record<Theme, string> = {
  system: "Motyw systemowy",
  light: "Motyw jasny",
  dark: "Motyw ciemny",
};

export function ThemeToggle() {
  const theme = useThemeStore((s) => s.theme);
  const setTheme = useThemeStore((s) => s.setTheme);

  return (
    <Button
      variant="ghost"
      size="icon"
      onClick={() => setTheme(cycle[theme])}
      title={labels[theme]}
      aria-label={labels[theme]}
    >
      {icons[theme]}
    </Button>
  );
}
