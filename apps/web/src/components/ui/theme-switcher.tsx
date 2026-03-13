import { Sun, Moon, Monitor, Check } from "lucide-react";
import * as DropdownMenu from "@radix-ui/react-dropdown-menu";
import { useThemeStore, type Theme } from "@/store/themeStore";

const options: { value: Theme; label: string; icon: React.ReactNode }[] = [
  { value: "system", label: "Systemowy", icon: <Monitor className="h-4 w-4" /> },
  { value: "light", label: "Jasny", icon: <Sun className="h-4 w-4" /> },
  { value: "dark", label: "Ciemny", icon: <Moon className="h-4 w-4" /> },
];

const triggerIcons: Record<Theme, React.ReactNode> = {
  system: <Monitor className="h-4 w-4" />,
  light: <Sun className="h-4 w-4" />,
  dark: <Moon className="h-4 w-4" />,
};

export function ThemeSwitcher() {
  const theme = useThemeStore((s) => s.theme);
  const setTheme = useThemeStore((s) => s.setTheme);

  return (
    <DropdownMenu.Root>
      <DropdownMenu.Trigger asChild>
        <button
          className="inline-flex h-9 w-9 items-center justify-center rounded-md border border-input bg-background text-sm font-medium hover:bg-accent hover:text-accent-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          aria-label="Zmień motyw"
        >
          {triggerIcons[theme]}
        </button>
      </DropdownMenu.Trigger>

      <DropdownMenu.Portal>
        <DropdownMenu.Content
          className="z-50 min-w-[160px] overflow-hidden rounded-md border bg-popover p-1 text-popover-foreground shadow-md animate-in fade-in-0 zoom-in-95"
          sideOffset={5}
          align="end"
        >
          {options.map((opt) => (
            <DropdownMenu.Item
              key={opt.value}
              className="relative flex cursor-pointer select-none items-center gap-2 rounded-sm px-2 py-1.5 text-sm outline-none hover:bg-accent hover:text-accent-foreground focus:bg-accent focus:text-accent-foreground"
              onSelect={() => setTheme(opt.value)}
            >
              {opt.icon}
              <span>{opt.label}</span>
              {theme === opt.value && (
                <Check className="ml-auto h-4 w-4 text-primary" />
              )}
            </DropdownMenu.Item>
          ))}
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu.Root>
  );
}
