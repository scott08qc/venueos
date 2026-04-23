import { useState, useEffect } from "react"
import { useForm } from "react-hook-form"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Textarea } from "@/components/ui/textarea"
import { Label } from "@/components/ui/label"
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select"
import { Separator } from "@/components/ui/separator"
import { cn } from "@/lib/utils"
import type { EventSummary, PostEventReview, NightOfActuals } from "@/types"
import { HIT_MISSED_OPTIONS } from "@/types"
import { CheckCircle } from "lucide-react"

interface Props {
  events: EventSummary[]
  preselectedEventId?: number | null
}

type FormValues = {
  event_id: string
  actual_attendance: string
  actual_door_revenue: string; actual_bar_revenue: string; actual_table_revenue: string
  artist_cost_actual: string; staffing_cost_actual: string
  projected_effective_split: string; actual_effective_split: string
  crowd_demographic_observations: string; customer_service_observations: string
  operational_breakdowns: string; what_to_replicate: string; what_to_change: string
  promoter_performance_notes: string; inventory_observations: string; staffing_observations: string
  promoter_artwork_on_time: string; promoter_social_posting: string
  promoter_attendance_vs_projection: string; promoter_role_boundaries: string
  review_status: string; settlement_notes_reference: string
}

function Sec({ title }: { title: string }) {
  return (
    <div className="pt-4 pb-1">
      <p className="text-xs font-semibold uppercase tracking-widest text-primary/80">{title}</p>
      <Separator className="mt-2 bg-border/60" />
    </div>
  )
}

function F({ label, children, hint }: { label: string; children: React.ReactNode; hint?: string }) {
  return (
    <div className="space-y-1.5">
      <Label className="text-sm text-foreground/90">
        {label}
        {hint && <span className="text-muted-foreground font-normal ml-1 text-xs">({hint})</span>}
      </Label>
      {children}
    </div>
  )
}

const inp = "bg-input border-border text-foreground placeholder:text-muted-foreground focus:ring-1 focus:ring-primary h-11"
const textareaCls = "bg-input border-border text-foreground placeholder:text-muted-foreground min-h-[90px] focus:ring-1 focus:ring-primary resize-none"

type HMField = "promoter_artwork_on_time" | "promoter_social_posting" | "promoter_attendance_vs_projection" | "promoter_role_boundaries"

export function PostEventReviewPage({ events, preselectedEventId }: Props) {
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [selectedEventId, setSelectedEventId] = useState(preselectedEventId?.toString() || "")
  const [existingId, setExistingId] = useState<number | null>(null)
  const [selectVals, setSelectVals] = useState<Record<string, string>>({
    promoter_artwork_on_time: "N/A", promoter_social_posting: "N/A",
    promoter_attendance_vs_projection: "N/A", promoter_role_boundaries: "N/A",
    review_status: "Draft",
  })

  const blank: FormValues = {
    event_id: preselectedEventId?.toString() || "",
    actual_attendance: "", actual_door_revenue: "", actual_bar_revenue: "",
    actual_table_revenue: "", artist_cost_actual: "", staffing_cost_actual: "",
    projected_effective_split: "", actual_effective_split: "",
    crowd_demographic_observations: "", customer_service_observations: "",
    operational_breakdowns: "", what_to_replicate: "", what_to_change: "",
    promoter_performance_notes: "", inventory_observations: "", staffing_observations: "",
    promoter_artwork_on_time: "N/A", promoter_social_posting: "N/A",
    promoter_attendance_vs_projection: "N/A", promoter_role_boundaries: "N/A",
    review_status: "Draft", settlement_notes_reference: "",
  }

  const { register, handleSubmit, setValue, reset, watch } = useForm<FormValues>({ defaultValues: blank })
  const [watchedProj, watchedActual] = watch(["projected_effective_split", "actual_effective_split"])

  // Split variance computation
  const projSplit = parseFloat(watchedProj) || null
  const actualSplit = parseFloat(watchedActual) || null
  const variance = projSplit !== null && actualSplit !== null ? actualSplit - projSplit : null
  const varianceOver5 = variance !== null && variance > 5

  useEffect(() => {
    if (!selectedEventId) return
    const load = async () => {
      // Load review
      const res = await fetch(`/api/reviews/${selectedEventId}`)
      if (res.ok) {
        const review: PostEventReview | null = await res.json()
        if (review) {
          setExistingId(review.id || null)
          const sv = {
            promoter_artwork_on_time: review.promoter_artwork_on_time || "N/A",
            promoter_social_posting: review.promoter_social_posting || "N/A",
            promoter_attendance_vs_projection: review.promoter_attendance_vs_projection || "N/A",
            promoter_role_boundaries: review.promoter_role_boundaries || "N/A",
            review_status: review.review_status || "Draft",
          }
          setSelectVals(sv)
          reset({
            event_id: selectedEventId,
            actual_attendance: review.actual_attendance?.toString() || "",
            actual_door_revenue: review.actual_door_revenue?.toString() || "",
            actual_bar_revenue: review.actual_bar_revenue?.toString() || "",
            actual_table_revenue: review.actual_table_revenue?.toString() || "",
            artist_cost_actual: review.artist_cost_actual?.toString() || "",
            staffing_cost_actual: review.staffing_cost_actual?.toString() || "",
            projected_effective_split: review.projected_effective_split?.toString() || "",
            actual_effective_split: review.actual_effective_split?.toString() || "",
            crowd_demographic_observations: review.crowd_demographic_observations || "",
            customer_service_observations: review.customer_service_observations || "",
            operational_breakdowns: review.operational_breakdowns || "",
            what_to_replicate: review.what_to_replicate || "",
            what_to_change: review.what_to_change || "",
            promoter_performance_notes: review.promoter_performance_notes || "",
            inventory_observations: review.inventory_observations || "",
            staffing_observations: review.staffing_observations || "",
            settlement_notes_reference: review.settlement_notes_reference || "",
            ...sv,
          })
        } else {
          setExistingId(null)
          setSelectVals({ promoter_artwork_on_time: "N/A", promoter_social_posting: "N/A", promoter_attendance_vs_projection: "N/A", promoter_role_boundaries: "N/A", review_status: "Draft" })
          reset({ ...blank, event_id: selectedEventId })
        }
      }

      // Load Close actuals to auto-populate actual effective split
      const aRes = await fetch(`/api/actuals/${selectedEventId}`)
      if (aRes.ok) {
        const actuals: NightOfActuals[] = await aRes.json()
        const closeActual = actuals.find((a) => a.time_of_entry === "Close")
        if (closeActual?.effective_split_percentage != null) {
          setValue("actual_effective_split", closeActual.effective_split_percentage.toFixed(1))
          if (closeActual.settlement_notes) {
            // Pre-fill settlement notes ref if review is new
            setValue("settlement_notes_reference", closeActual.settlement_notes)
          }
        }
      }
    }
    load()
  }, [selectedEventId])

  function setSF(field: string, value: string) {
    setSelectVals((p) => ({ ...p, [field]: value }))
    setValue(field as keyof FormValues, value)
  }

  async function onSubmit(values: FormValues) {
    if (!values.event_id) return
    setSaving(true)
    const payload = {
      event_id: parseInt(values.event_id),
      actual_attendance: values.actual_attendance ? parseInt(values.actual_attendance) : null,
      actual_door_revenue: parseFloat(values.actual_door_revenue) || 0,
      actual_bar_revenue: parseFloat(values.actual_bar_revenue) || 0,
      actual_table_revenue: parseFloat(values.actual_table_revenue) || 0,
      artist_cost_actual: parseFloat(values.artist_cost_actual) || 0,
      staffing_cost_actual: parseFloat(values.staffing_cost_actual) || 0,
      projected_effective_split: values.projected_effective_split ? parseFloat(values.projected_effective_split) : null,
      actual_effective_split: values.actual_effective_split ? parseFloat(values.actual_effective_split) : null,
      crowd_demographic_observations: values.crowd_demographic_observations || null,
      customer_service_observations: values.customer_service_observations || null,
      operational_breakdowns: values.operational_breakdowns || null,
      what_to_replicate: values.what_to_replicate || null,
      what_to_change: values.what_to_change || null,
      promoter_performance_notes: values.promoter_performance_notes || null,
      inventory_observations: values.inventory_observations || null,
      staffing_observations: values.staffing_observations || null,
      promoter_artwork_on_time: selectVals.promoter_artwork_on_time,
      promoter_social_posting: selectVals.promoter_social_posting,
      promoter_attendance_vs_projection: selectVals.promoter_attendance_vs_projection,
      promoter_role_boundaries: selectVals.promoter_role_boundaries,
      review_status: selectVals.review_status,
      settlement_notes_reference: values.settlement_notes_reference || null,
    }
    try {
      const res = await fetch(existingId ? `/api/reviews/${existingId}` : "/api/reviews", {
        method: existingId ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      if (res.ok) {
        const data = await res.json()
        if (!existingId && data.id) setExistingId(data.id)
        setSaved(true)
        setTimeout(() => setSaved(false), 2000)
      }
    } finally { setSaving(false) }
  }

  const hmFields: { field: HMField; label: string }[] = [
    { field: "promoter_artwork_on_time", label: "Artwork On Time" },
    { field: "promoter_social_posting", label: "Social Posting Followed" },
    { field: "promoter_attendance_vs_projection", label: "Attendance vs Projection" },
    { field: "promoter_role_boundaries", label: "Role Boundaries Respected" },
  ]

  return (
    <div className="px-4 py-5">
      <div className="mb-5">
        <h1 className="text-xl font-bold text-foreground">Post-Event Review</h1>
        <p className="text-sm text-muted-foreground mt-0.5">Complete within 48 hours of the event</p>
      </div>

      <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">

        <Sec title="Link to Event" />
        <F label="Event">
          <Select value={selectedEventId} onValueChange={(v) => { setSelectedEventId(v); setValue("event_id", v) }}>
            <SelectTrigger className={cn(inp, "w-full")}>
              <SelectValue placeholder="Select an event" />
            </SelectTrigger>
            <SelectContent className="bg-card border-border">
              {events.map((e) => (
                <SelectItem key={e.id} value={e.id.toString()} className="text-foreground focus:bg-muted">
                  {e.event_date} — {e.event_name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </F>
        <input type="hidden" {...register("event_id")} />

        <Sec title="Actuals" />
        <F label="Actual Attendance">
          <Input type="number" className={inp} placeholder="0" {...register("actual_attendance")} />
        </F>
        <div className="grid grid-cols-2 gap-3">
          <F label="Door Revenue ($)">
            <Input type="number" className={inp} placeholder="0" {...register("actual_door_revenue")} />
          </F>
          <F label="Bar Revenue ($)">
            <Input type="number" className={inp} placeholder="0" {...register("actual_bar_revenue")} />
          </F>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <F label="Table Revenue ($)">
            <Input type="number" className={inp} placeholder="0" {...register("actual_table_revenue")} />
          </F>
          <F label="Artist Cost ($)">
            <Input type="number" className={inp} placeholder="0" {...register("artist_cost_actual")} />
          </F>
        </div>
        <F label="Staffing Cost ($)">
          <Input type="number" className={inp} placeholder="0" {...register("staffing_cost_actual")} />
        </F>

        <Sec title="Effective Split Tracking" />

        <div className="grid grid-cols-2 gap-3">
          <F label="Projected Split %" hint="from deal terms">
            <Input type="number" className={inp} placeholder="0.0" step="0.1" {...register("projected_effective_split")} />
          </F>
          <F label="Actual Split %" hint="auto-filled from Night Of">
            <Input type="number" className={inp} placeholder="0.0" step="0.1" {...register("actual_effective_split")} />
          </F>
        </div>

        {/* Split variance display */}
        {variance !== null && (
          <div className={cn(
            "rounded-lg border px-4 py-3 flex justify-between items-center",
            varianceOver5
              ? "bg-destructive/10 border-destructive/30"
              : "bg-primary/10 border-primary/20"
          )}>
            <div>
              <p className={cn("text-xs font-semibold uppercase tracking-wider", varianceOver5 ? "text-destructive" : "text-primary")}>
                Split Variance
              </p>
              <p className="text-xs text-muted-foreground mt-0.5">
                {varianceOver5 ? "Actual exceeds projected by more than 5 points" : "Within expected range"}
              </p>
            </div>
            <span className={cn("text-lg font-bold", varianceOver5 ? "text-destructive" : "text-primary")}>
              {variance > 0 ? "+" : ""}{variance.toFixed(1)}%
            </span>
          </div>
        )}

        <Sec title="Settlement Notes Reference" />
        <Textarea
          className={textareaCls}
          placeholder="Settlement notes from Night Of actuals (auto-filled if available)..."
          {...register("settlement_notes_reference")}
        />

        <Sec title="Observations" />
        {[
          { field: "crowd_demographic_observations", label: "Crowd Demographics" },
          { field: "customer_service_observations", label: "Customer Service" },
          { field: "operational_breakdowns", label: "Operational Breakdowns" },
          { field: "what_to_replicate", label: "What to Replicate Next Time" },
          { field: "what_to_change", label: "What to Change Next Time" },
          { field: "promoter_performance_notes", label: "Promoter Performance" },
          { field: "inventory_observations", label: "Inventory Observations" },
          { field: "staffing_observations", label: "Staffing Observations" },
        ].map(({ field, label }) => (
          <F key={field} label={label} hint="optional">
            <Textarea className={textareaCls} placeholder={`Notes on ${label.toLowerCase()}...`}
              {...register(field as keyof FormValues)} />
          </F>
        ))}

        <Sec title="Promoter Scorecard" />
        <div className="space-y-3">
          {hmFields.map(({ field, label }) => (
            <div key={field} className="flex items-center justify-between gap-3">
              <Label className="text-sm text-foreground/90 flex-1">{label}</Label>
              <div className="flex gap-1.5">
                {HIT_MISSED_OPTIONS.map((opt) => (
                  <button key={opt} type="button" onClick={() => setSF(field, opt)}
                    className={cn(
                      "text-xs px-2.5 py-1.5 rounded border transition-colors font-medium",
                      selectVals[field] === opt
                        ? opt === "Hit" ? "bg-green-500/20 text-green-400 border-green-500/40"
                          : opt === "Missed" ? "bg-red-500/20 text-red-400 border-red-500/40"
                          : opt === "Partial" ? "bg-yellow-500/20 text-yellow-400 border-yellow-500/40"
                          : "bg-muted text-muted-foreground border-border"
                        : "bg-transparent text-muted-foreground border-border/50 hover:border-border"
                    )}>
                    {opt}
                  </button>
                ))}
              </div>
              <input type="hidden" {...register(field as keyof FormValues)} />
            </div>
          ))}
        </div>

        <Sec title="Review Status" />
        <div className="flex gap-3">
          {["Draft", "Complete"].map((status) => (
            <button key={status} type="button" onClick={() => setSF("review_status", status)}
              className={cn(
                "flex-1 h-11 rounded-md border text-sm font-medium transition-colors",
                selectVals.review_status === status
                  ? "bg-primary/20 text-primary border-primary/40"
                  : "bg-transparent text-muted-foreground border-border hover:border-border/80"
              )}>
              {status}
            </button>
          ))}
        </div>
        <input type="hidden" {...register("review_status")} />

        <div className="pt-4 pb-2">
          <Button type="submit" disabled={saving || saved || !selectedEventId}
            className="w-full h-12 text-base font-semibold bg-primary hover:bg-primary/90 text-primary-foreground">
            {saved ? <span className="flex items-center gap-2"><CheckCircle className="h-4 w-4" />Saved!</span>
              : saving ? "Saving..." : existingId ? "Update Review" : "Save Review"}
          </Button>
        </div>

      </form>
    </div>
  )
}
