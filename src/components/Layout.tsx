import { cn } from "@/lib/utils"
import type { Page } from "@/types"
import { ClipboardList, Music, BarChart3, History, Moon } from "lucide-react"

interface NavProps {
  currentPage: Page
  onNavigate: (page: Page) => void
}

const navItems: { page: Page; label: string; icon: React.ReactNode; short: string }[] = [
  { page: "list", label: "Events", short: "Events", icon: <ClipboardList className="h-5 w-5" /> },
  { page: "event-record", label: "Event Record", short: "Record", icon: <Music className="h-5 w-5" /> },
  { page: "night-of-actuals", label: "Night Of", short: "Night Of", icon: <Moon className="h-5 w-5" /> },
  { page: "post-event-review", label: "Post-Event", short: "Review", icon: <BarChart3 className="h-5 w-5" /> },
  { page: "historical", label: "Historical", short: "History", icon: <History className="h-5 w-5" /> },
]

export function Layout({ currentPage, onNavigate, children }: NavProps & { children: React.ReactNode }) {
  return (
    <div className="min-h-screen flex flex-col bg-background">
      {/* Top header */}
      <header className="sticky top-0 z-50 border-b border-border bg-card/95 backdrop-blur supports-[backdrop-filter]:bg-card/80">
        <div className="flex items-center justify-between px-4 h-14 max-w-2xl mx-auto w-full">
          <div className="flex items-center gap-2">
            <div className="h-7 w-7 rounded-md bg-primary flex items-center justify-center">
              <Music className="h-4 w-4 text-primary-foreground" />
            </div>
            <span className="font-semibold text-sm tracking-wide text-foreground">VenueOS</span>
          </div>
          <span className="text-xs text-muted-foreground font-medium uppercase tracking-widest">
            Event Record
          </span>
        </div>
      </header>

      {/* Main content */}
      <main className="flex-1 overflow-auto pb-20">
        <div className="max-w-2xl mx-auto w-full">
          {children}
        </div>
      </main>

      {/* Bottom navigation */}
      <nav className="fixed bottom-0 left-0 right-0 z-50 border-t border-border bg-card/95 backdrop-blur supports-[backdrop-filter]:bg-card/80">
        <div className="max-w-2xl mx-auto flex">
          {navItems.map((item) => (
            <button
              key={item.page}
              onClick={() => onNavigate(item.page)}
              className={cn(
                "flex-1 flex flex-col items-center justify-center gap-1 py-3 px-1 text-xs font-medium transition-colors",
                currentPage === item.page
                  ? "text-primary"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              {item.icon}
              <span className="text-[10px] leading-none">{item.short}</span>
            </button>
          ))}
        </div>
      </nav>
    </div>
  )
}
