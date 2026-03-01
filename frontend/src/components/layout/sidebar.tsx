"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  Database,
  FileCode,
  FolderOpen,
  BarChart3,
  Settings,
  ChevronRight,
} from "lucide-react";
import { clsx } from "clsx";

const NAV_ITEMS = [
  { label: "Projects", href: "/projects", icon: FolderOpen },
  { label: "Schema", href: "/schema", icon: FileCode },
  { label: "Migrations", href: "/migrations", icon: Database },
  { label: "Analytics", href: "/analytics", icon: BarChart3 },
  { label: "Settings", href: "/settings", icon: Settings },
];

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-60 border-r bg-card flex flex-col shrink-0">
      {/* Logo */}
      <div className="px-6 py-5 border-b">
        <span className="text-xl font-bold">
          Cobol<span className="text-primary">Shift</span>
        </span>
      </div>

      {/* Navigation */}
      <nav className="flex-1 px-3 py-4 space-y-1">
        {NAV_ITEMS.map(({ label, href, icon: Icon }) => {
          const active =
            href === "/" ? pathname === "/" : pathname.startsWith(href);
          return (
            <Link
              key={href}
              href={href}
              className={clsx(
                "flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition-colors",
                active
                  ? "bg-primary/10 text-primary"
                  : "text-muted-foreground hover:bg-muted hover:text-foreground"
              )}
            >
              <Icon className="w-4 h-4 shrink-0" />
              {label}
              {active && <ChevronRight className="w-3 h-3 ml-auto" />}
            </Link>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-6 py-4 border-t text-xs text-muted-foreground">
        CobolShift v0.1.0
      </div>
    </aside>
  );
}
