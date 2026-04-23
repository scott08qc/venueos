import { useState } from "react"
import { cn } from "@/lib/utils"
import type { EventSummary, Page } from "@/types"
import { ChevronRight, Plus, ArrowUpDown, CalendarDays, RefreshCw } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"

interface Props {
  events: EventSummary[]
  loading: boolean
  onRefresh: () => void
  onNavigate: (page: Page) => void
  onSelectEvent: (event: EventSummary) => void
}

type SortField = "event_date" | "event_name" | "tier1_category"
type SortDir = "asc" | "desc"

const statusColors: Record<string, string> = {
  Complete: "bg-green-100 text-green-700 border-green-300",
  Draft: "bg-amber-50 text-amber-700 border-amber-300",
  "No Review": "bg-gray-100 text-gray-500 border-gray-300",
}

const tier1Colors: Record<string, string> = {
  Electronic: "bg-violet-100 text-violet-700",
  "Open Format": "bg-blue-100 text-blue-700",
  "Hip Hop and R&B": "bg-orange-100 text-orange-700",
  Latin: "bg-red-100 text-red-700",
  "Live Performance": "bg-emerald-100 text-emerald-700",
  "Corporate and Private": "bg-slate-100 text-slate-600",
  "Sports and Viewing": "bg-cyan-100 text-cyan-700",
  "Themed and Holiday": "bg-pink-100 text-pink-700",
}

export function EventListPage({ events, loading, onRefresh, onNavigate, onSelectEvent }: Props) {
  const [search, setSearch] = useState("")
  const [sortField, setSortField] = useState<SortField>("event_date")
  const [sortDir, setSortDir] = useState<SortDir>("desc")

  function toggleSort(field: SortField) {
    if (sortField === field) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortField(field)
      setSortDir("desc")
    }
  }

  const filtered = events
    .filter((e) => {
      const q = search.toLowerCase()
      return (
        e.event_name?.toLowerCase().includes(q) ||
        e.promoter_name?.toLowerCase().includes(q) ||
        e.tier1_category?.toLowerCase().includes(q)
      )
    })
    .sort((a, b) => {
      let va = a[sortField] ?? ""
      let vb = b[sortField] ?? ""
      if (typeof va === "string") va = va.toLowerCase()
      if (typeof vb === "string") vb = vb.toLowerCase()
      if (va < vb) return sortDir === "asc" ? -1 : 1
      if (va > vb) return sortDir === "asc" ? 1 : -1
      return 0
    })

  function formatDate(dateStr: string) {
    if (!dateStr) return ""
    const d = new Date(dateStr + "T12:00:00")
    return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })
  }

  function formatRevenue(n: number) {
    if (!n) return "$0"
    if (n >= 1000) return `$${(n / 1000).toFixed(1)}k`
    return `$${n.toFixed(0)}`
  }

  return (
    <div className="px-4 py-5">
      {/* Header */}
      <div className="flex items-center justify-between mb-4">
        <div>
          <h1 className="text-xl font-bold text-foreground">Event List</h1>
          <p className="text-xs text-muted-foreground mt-0.5">{events.length} event{events.length !== 1 ? "s" : ""} on record</p>
        </div>
        <div className="flex gap-2">
          <Button
            variant="ghost"
            size="icon"
            onClick={onRefresh}
            className="h-9 w-9 text-muted-foreground hover:text-foreground"
          >
            <RefreshCw className={cn("h-4 w-4", loading && "animate-spin")} />
          </Button>
          <Button
            size="sm"
            onClick={() => onNavigate("event-record")}
            className="h-9 bg-primary hover:bg-primary/90 text-primary-foreground gap-1.5 text-sm"
          >
            <Plus className="h-4 w-4" />
            New
          </Button>
        </div>
      </div>

      {/* Search */}
      <div className="mb-4">
        <Input
          className="bg-input border-border h-10 text-foreground placeholder:text-muted-foreground"
          placeholder="Search events, promoters, categories..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
      </div>

      {/* Sort row */}
      <div className="flex gap-2 mb-3">
        {(["event_date", "event_name", "tier1_category"] as SortField[]).map((field) => {
          const labels: Record<SortField, string> = {
            event_date: "Date",
            event_name: "Name",
            tier1_category: "Category",
          }
          return (
            <button
              key={field}
              onClick={() => toggleSort(field)}
              className={cn(
                "flex items-center gap-1 text-xs px-2.5 py-1.5 rounded-md border transition-colors",
                sortField === field
                  ? "bg-primary/15 text-primary border-primary/30"
                  : "text-muted-foreground border border-border/50 hover:border-border"
              )}
            >
              {labels[field]}
              <ArrowUpDown className="h-3 w-3" />
            </button>
          )
        })}
      </div>

      {/* Event cards */}
      {loading ? (
        <div className="space-y-3">
          {[1, 2, 3].map((i) => (
            <div key={i} className="h-20 rounded-xl animate-pulse" style={{ background: '#EEEEEE' }} />
          ))}
        </div>
      ) : filtered.length === 0 ? (
        <div className="text-center py-16 space-y-3">
          <CalendarDays className="h-10 w-10 text-muted-foreground/40 mx-auto" />
          <p className="text-muted-foreground text-sm">
            {search ? "No events match your search" : "No events yet. Create your first event record."}
          </p>
          {!search && (
            <Button
              size="sm"
              onClick={() => onNavigate("event-record")}
              className="bg-primary hover:bg-primary/90 text-primary-foreground"
            >
              <Plus className="h-4 w-4 mr-1.5" />
              Create Event
            </Button>
          )}
        </div>
      ) : (
        <div className="space-y-2.5">
          {filtered.map((event) => (
            <button
              key={event.id}
              onClick={() => onSelectEvent(event)}
              className="w-full text-left rounded-xl border transition-all p-4 group"
              style={{ background: '#FFFFFF', borderColor: 'rgba(0,0,0,0.08)', boxShadow: '0 1px 4px rgba(0,0,0,0.06)' }}
            >
              <div className="flex items-start justify-between gap-3">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 mb-1.5 flex-wrap">
                    <span className="font-semibold text-foreground text-sm leading-tight truncate">
                      {event.event_name}
                    </span>
                    <span className={cn(
                      "text-[10px] px-1.5 py-0.5 rounded font-medium shrink-0",
                      statusColors[event.review_status] || statusColors["No Review"]
                    )}>
                      {event.review_status}
                    </span>
                  </div>
                  <div className="flex items-center gap-3 text-xs text-muted-foreground">
                    <span className="flex items-center gap-1">
                      <CalendarDays className="h-3 w-3" />
                      {formatDate(event.event_date)}
                      {event.day_of_week && ` · ${event.day_of_week}`}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 mt-2 flex-wrap">
                    {event.tier1_category && (
                      <span className={cn(
                        "text-[10px] px-1.5 py-0.5 rounded font-medium",
                        tier1Colors[event.tier1_category] || "bg-muted text-muted-foreground"
                      )}>
                        {event.tier1_category}
                      </span>
                    )}
                    {event.promoter_name && (
                      <span className="text-xs text-muted-foreground truncate">
                        {event.promoter_name}
                      </span>
                    )}
                  </div>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  <div className="text-right">
                    <div className="text-sm font-bold text-primary">
                      {formatRevenue(event.projected_total_revenue)}
                    </div>
                    <div className="text-[10px] text-muted-foreground">projected</div>
                  </div>
                  <ChevronRight className="h-4 w-4 text-muted-foreground/50 group-hover:text-primary transition-colors" />
                </div>
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
