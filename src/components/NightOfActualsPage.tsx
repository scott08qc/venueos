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
import type { EventSummary, NightOfActuals, EventFull } from "@/types"
import { TIME_OF_ENTRY_OPTIONS } from "@/types"
import { CheckCircle, Calculator } from "lucide-react"

interface Props {
  events: EventSummary[]
  preselectedEventId?: number | null
}

type FormValues = {
  event_id: string; time_of_entry: string
  total_bar_sales: string; liquor_sales: string; beer_wine_sales: string
  table_bottle_service: string; comps_total: string; voids: string
  tax_collected: string; tips: string
  door_revenue_cash: string; door_revenue_card: string
  total_headcount: string; incident_description: string; incident_department: string; notes: string
  bar_cogs_deduction: string; bar_threshold_retained: string; door_threshold_retained: string
  charge_backs: string; promoter_bar_payout: string; promoter_door_payout: string
  promoter_table_payout: string; artist_cost_paid_by_venue: string
  effective_split_percentage: string; settlement_notes: string
  benchmark_effective_split: string
}

function Sec({ title }: { title: string }) {
  return (
    <div className="pt-4 pb-1">
      <p className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: '#2C5F8A' }}>{title}</p>
      <Separator className="mt-2" style={{ background: '#EEEEEE' }} />
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

// ── Settlement calculation display ────────────────────────────────────────────

function fmt(n: number) {
  return "$" + n.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function Row({ label, value, isSub, isTotal, isBottomLine }: {
  label: string; value: string; isSub?: boolean; isTotal?: boolean; isBottomLine?: boolean
}) {
  return (
    <div className={cn(
      "flex justify-between items-center",
      isSub && "pl-3 py-1",
      isTotal && "py-2 border-t border-primary/20 mt-1",
      isBottomLine && "py-2.5 border-t-2 border-primary/40 mt-1",
      !isSub && !isTotal && !isBottomLine && "py-1.5"
    )}>
      <span className={cn(
        "text-sm",
        isSub ? "text-muted-foreground/80" : isTotal ? "font-semibold text-foreground" : isBottomLine ? "font-bold text-primary" : "text-muted-foreground"
      )}>{label}</span>
      <span className={cn(
        "font-medium tabular-nums",
        isSub ? "text-sm text-foreground/70" : isTotal ? "text-sm text-foreground" : isBottomLine ? "text-lg font-bold text-primary" : "text-sm text-foreground"
      )}>{value}</span>
    </div>
  )
}

function ManualRow({ label, hint, children }: { label: string; hint?: string; children: React.ReactNode }) {
  return (
    <div className="space-y-1">
      <Label className="text-xs text-muted-foreground">
        {label}
        {hint && <span className="ml-1 opacity-70">{hint}</span>}
      </Label>
      {children}
    </div>
  )
}

const settInp = "bg-white border text-foreground placeholder:text-muted-foreground focus:ring-1 focus:ring-primary h-9 text-sm"

function SettlementSection({ w, register, eventDeal, setValue }: {
  w: FormValues
  register: ReturnType<typeof useForm<FormValues>>["register"]
  eventDeal: EventFull | null
  setValue: ReturnType<typeof useForm<FormValues>>["setValue"]
}) {
  const grossBar = parseFloat(w.total_bar_sales) || 0
  const grossDoor = (parseFloat(w.door_revenue_cash) || 0) + (parseFloat(w.door_revenue_card) || 0)
  const grossTable = parseFloat(w.table_bottle_service) || 0
  const totalGross = grossBar + grossDoor + grossTable

  const barCogs = parseFloat(w.bar_cogs_deduction) || 0
  const barThresh = parseFloat(w.bar_threshold_retained) || 0
  const doorThresh = parseFloat(w.door_threshold_retained) || 0
  const comps = parseFloat(w.comps_total) || 0
  const voids = parseFloat(w.voids) || 0
  const tax = parseFloat(w.tax_collected) || 0
  const tips = parseFloat(w.tips) || 0
  const chargeBacks = parseFloat(w.charge_backs) || 0
  const totalDeductions = barCogs + barThresh + doorThresh + comps + voids + tax + tips + chargeBacks

  const netBar = grossBar - barCogs - barThresh - comps - voids - tax - tips
  const netDoor = grossDoor - doorThresh
  const netTable = grossTable
  const totalNetSales = netBar + netDoor + netTable

  // Determine deal type
  const isNetRevSplit = eventDeal?.deal_structure_type === "Net revenue split"

  // Auto-compute artist cost from deal terms
  const totalArtistFee = (eventDeal?.artist_fee_landed || 0) + (eventDeal?.artist_fee_travel || 0)
  const artistResp = eventDeal?.artist_cost_responsibility || ""
  function calcAutoArtistCost(): number {
    if (artistResp === "Venue pays full cost") return totalArtistFee
    if (artistResp === "Promoter pays full cost") return 0
    if (artistResp === "Split per deal terms") {
      const method = eventDeal?.artist_cost_split_method || ""
      if (method === "By dollar amount") return eventDeal?.artist_venue_dollar || 0
      if (method === "By percentage") {
        const vPct = eventDeal?.artist_venue_pct || 0
        return totalArtistFee > 0 ? (totalArtistFee * vPct) / 100 : 0
      }
    }
    return 0
  }
  const autoArtistCost = calcAutoArtistCost()

  // Net revenue split auto payout
  const netRevPromoterPct = eventDeal?.net_revenue_promoter_pct || 0
  const autoNetRevPayout = isNetRevSplit ? (totalNetSales * netRevPromoterPct) / 100 : 0

  const promoBar = parseFloat(w.promoter_bar_payout) || 0
  const promoDoor = parseFloat(w.promoter_door_payout) || 0
  const promoTable = parseFloat(w.promoter_table_payout) || 0
  const promoNetRev = isNetRevSplit ? (parseFloat(w.promoter_bar_payout) || autoNetRevPayout) : 0
  const artistCost = parseFloat(w.artist_cost_paid_by_venue) || 0
  const totalPayout = isNetRevSplit
    ? promoNetRev + artistCost
    : promoBar + promoDoor + promoTable + artistCost
  const netToVenue = totalNetSales - totalPayout
  const promoPct = totalNetSales > 0
    ? (isNetRevSplit ? promoNetRev : promoBar + promoDoor + promoTable) / totalNetSales * 100
    : 0

  return (
    <div className="rounded-xl overflow-hidden mt-4" style={{ background: '#EEF4FF', border: '1px solid rgba(44,95,138,0.25)' }}>
      {/* Header */}
      <div className="flex items-center gap-2 px-4 py-3 border-b" style={{ background: 'rgba(44,95,138,0.08)', borderColor: 'rgba(44,95,138,0.2)' }}>
        <Calculator className="h-4 w-4" style={{ color: '#2C5F8A' }} />
        <span className="text-sm font-bold uppercase tracking-wider" style={{ color: '#2C5F8A' }}>Settlement Calculation</span>
      </div>

      <div className="px-4 py-3 space-y-1">

        {/* Gross Revenue */}
        <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground pt-1 pb-0.5">Gross Revenue</p>
        <Row label="Gross bar sales" value={fmt(grossBar)} isSub />
        <Row label="Gross door revenue" value={fmt(grossDoor)} isSub />
        <Row label="Gross table revenue" value={fmt(grossTable)} isSub />
        <Row label="Total Gross Revenue" value={fmt(totalGross)} isTotal />

        {/* Deductions */}
        <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground pt-3 pb-1">Deductions</p>

        <ManualRow label="Bar COGS deduction" hint="— enter $ or calculate from % in deal terms">
          <Input type="number" className={settInp} placeholder="0" {...register("bar_cogs_deduction")} />
        </ManualRow>

        <ManualRow label="Bar threshold retained by venue" hint="— first $X per deal terms">
          <Input type="number" className={settInp} placeholder="0" {...register("bar_threshold_retained")} />
        </ManualRow>

        <ManualRow label="Door threshold retained by venue" hint="— first $X per deal terms">
          <Input type="number" className={settInp} placeholder="0" {...register("door_threshold_retained")} />
        </ManualRow>

        <Row label="Less comps (from above)" value={fmt(comps)} isSub />
        <Row label="Less voids (from above)" value={fmt(voids)} isSub />
        <Row label="Less tax (from above)" value={fmt(tax)} isSub />
        <Row label="Less tips (from above)" value={fmt(tips)} isSub />

        <ManualRow label="Charge-backs" hint="— enter after 10–14 day window, leave blank night of">
          <Input type="number" className={settInp} placeholder="0" {...register("charge_backs")} />
        </ManualRow>

        <Row label="Total Deductions" value={fmt(totalDeductions)} isTotal />

        {/* Net Sales */}
        <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground pt-3 pb-0.5">Net Sales</p>
        <Row label="Net bar" value={fmt(netBar)} isSub />
        <Row label="Net door" value={fmt(netDoor)} isSub />
        <Row label="Net table" value={fmt(netTable)} isSub />
        <Row label="TOTAL NET SALES" value={fmt(totalNetSales)} isBottomLine />

        {/* Promoter Payout */}
        <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground pt-3 pb-1">Promoter Payout</p>

        {isNetRevSplit ? (
          <ManualRow label={`Promoter payout — net revenue split (${netRevPromoterPct}% of net)`} hint="— auto-calculated, override if needed">
            <Input type="number" className={settInp}
              placeholder={autoNetRevPayout.toFixed(2)}
              {...register("promoter_bar_payout")}
            />
          </ManualRow>
        ) : (<>
          <ManualRow label="Promoter bar payout" hint="— calculate from deal terms or enter manually">
            <Input type="number" className={settInp} placeholder="0" {...register("promoter_bar_payout")} />
          </ManualRow>
          <ManualRow label="Promoter door payout" hint="— calculate from deal terms or enter manually">
            <Input type="number" className={settInp} placeholder="0" {...register("promoter_door_payout")} />
          </ManualRow>
          <ManualRow label="Promoter table payout" hint="— enter if applicable">
            <Input type="number" className={settInp} placeholder="0" {...register("promoter_table_payout")} />
          </ManualRow>
        </>)}

        <ManualRow label={`Artist cost paid by venue${autoArtistCost > 0 ? ` — deal terms: $${autoArtistCost.toFixed(2)}` : ""}`} hint="— override if needed">
          <Input type="number" className={settInp}
            placeholder={autoArtistCost > 0 ? autoArtistCost.toFixed(2) : "0"}
            {...register("artist_cost_paid_by_venue")}
          />
        </ManualRow>

        <Row label="Total Promoter & Artist Payout" value={fmt(totalPayout)} isTotal />

        <div className="pt-1 space-y-0.5">
          <div className="flex justify-between items-center py-2 border-t border-primary/20">
            <span className="text-sm font-semibold text-foreground">Net to Venue</span>
            <span className={cn("text-base font-bold", netToVenue >= 0 ? "text-primary" : "text-destructive")}>
              {fmt(netToVenue)}
            </span>
          </div>
          <div className="flex justify-between items-center py-1.5 rounded-md px-2 mt-1" style={{ background: 'rgba(44,95,138,0.08)' }}>
            <span className="text-xs" style={{ color: '#555555' }}>Effective promoter split — used for benchmarking</span>
            <span className={cn("text-sm font-bold")} style={{ color: promoPct > 60 ? '#8B3A3A' : '#2C5F8A' }}>
              {promoPct.toFixed(1)}%
            </span>
          </div>

          <div className="mt-2 space-y-1">
            <Label className="text-xs text-muted-foreground">
              Benchmark effective split for this event type
            </Label>
            <Input
              className="bg-white border text-foreground placeholder:text-muted-foreground/50 h-9 text-sm focus:ring-1 focus:ring-primary/50"
              placeholder="Compare against historical average — update manually from Benchmarks page for now."
              {...register("benchmark_effective_split")}
            />
          </div>
        </div>

        {/* Hidden field to save the calculated split */}
        <input type="hidden" value={promoPct.toFixed(2)} {...register("effective_split_percentage")} />

        {/* Settlement Notes */}
        <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground pt-3 pb-1">Settlement Notes</p>
        <Textarea
          className="bg-white border text-foreground placeholder:text-muted-foreground/60 min-h-[80px] focus:ring-1 focus:ring-primary resize-none text-sm"
          placeholder="Document any manual adjustments, disputes, or deal mechanics that affected this calculation..."
          {...register("settlement_notes")}
        />
      </div>
    </div>
  )
}

// ── Main component ────────────────────────────────────────────────────────────

export function NightOfActualsPage({ events, preselectedEventId }: Props) {
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [selectedEventId, setSelectedEventId] = useState(preselectedEventId?.toString() || "")
  const [timeOfEntry, setTimeOfEntry] = useState("")
  const [existingId, setExistingId] = useState<number | null>(null)
  const [eventDeal, setEventDeal] = useState<EventFull | null>(null)

  const blank: FormValues = {
    event_id: preselectedEventId?.toString() || "", time_of_entry: "",
    total_bar_sales: "", liquor_sales: "", beer_wine_sales: "",
    table_bottle_service: "", comps_total: "", voids: "",
    tax_collected: "", tips: "", door_revenue_cash: "", door_revenue_card: "",
    total_headcount: "", incident_description: "", incident_department: "", notes: "",
    bar_cogs_deduction: "", bar_threshold_retained: "", door_threshold_retained: "",
    charge_backs: "", promoter_bar_payout: "", promoter_door_payout: "",
    promoter_table_payout: "", artist_cost_paid_by_venue: "",
    effective_split_percentage: "", settlement_notes: "",
    benchmark_effective_split: "",
  }

  const { register, handleSubmit, watch, setValue, reset } = useForm<FormValues>({ defaultValues: blank })
  const watchedValues = watch()
  const isClose = timeOfEntry === "Close"

  useEffect(() => {
    if (!selectedEventId) { setEventDeal(null); return }
    fetch(`/api/events/${selectedEventId}`).then(r => r.ok ? r.json() : null).then(d => setEventDeal(d || null))
  }, [selectedEventId])

  useEffect(() => {
    if (!selectedEventId || !timeOfEntry) return
    const load = async () => {
      const res = await fetch(`/api/actuals/${selectedEventId}`)
      if (!res.ok) return
      const rows: NightOfActuals[] = await res.json()
      const match = rows.find((r) => r.time_of_entry === timeOfEntry)
      if (match) {
        setExistingId(match.id || null)
        reset({
          event_id: selectedEventId, time_of_entry: timeOfEntry,
          total_bar_sales: match.total_bar_sales?.toString() || "",
          liquor_sales: match.liquor_sales?.toString() || "",
          beer_wine_sales: match.beer_wine_sales?.toString() || "",
          table_bottle_service: match.table_bottle_service?.toString() || "",
          comps_total: match.comps_total?.toString() || "",
          voids: match.voids?.toString() || "",
          tax_collected: match.tax_collected?.toString() || "",
          tips: match.tips?.toString() || "",
          door_revenue_cash: match.door_revenue_cash?.toString() || "",
          door_revenue_card: match.door_revenue_card?.toString() || "",
          total_headcount: match.total_headcount?.toString() || "",
          incident_description: match.incident_description || "",
          incident_department: match.incident_department || "",
          notes: match.notes || "",
          bar_cogs_deduction: match.bar_cogs_deduction?.toString() || "",
          bar_threshold_retained: match.bar_threshold_retained?.toString() || "",
          door_threshold_retained: match.door_threshold_retained?.toString() || "",
          charge_backs: match.charge_backs?.toString() || "",
          promoter_bar_payout: match.promoter_bar_payout?.toString() || "",
          promoter_door_payout: match.promoter_door_payout?.toString() || "",
          promoter_table_payout: match.promoter_table_payout?.toString() || "",
          artist_cost_paid_by_venue: match.artist_cost_paid_by_venue?.toString() || "",
          effective_split_percentage: match.effective_split_percentage?.toString() || "",
          settlement_notes: match.settlement_notes || "",
          benchmark_effective_split: match.benchmark_effective_split?.toString() || "",
        })
      } else {
        setExistingId(null)
        reset({ ...blank, event_id: selectedEventId, time_of_entry: timeOfEntry })
      }
    }
    load()
  }, [selectedEventId, timeOfEntry])

  async function onSubmit(values: FormValues) {
    if (!values.event_id || !values.time_of_entry) return
    setSaving(true)
    const payload = {
      event_id: parseInt(values.event_id),
      time_of_entry: values.time_of_entry,
      total_bar_sales: parseFloat(values.total_bar_sales) || 0,
      liquor_sales: parseFloat(values.liquor_sales) || 0,
      beer_wine_sales: parseFloat(values.beer_wine_sales) || 0,
      table_bottle_service: parseFloat(values.table_bottle_service) || 0,
      comps_total: parseFloat(values.comps_total) || 0,
      voids: parseFloat(values.voids) || 0,
      tax_collected: parseFloat(values.tax_collected) || 0,
      tips: parseFloat(values.tips) || 0,
      door_revenue_cash: parseFloat(values.door_revenue_cash) || 0,
      door_revenue_card: parseFloat(values.door_revenue_card) || 0,
      total_headcount: parseInt(values.total_headcount) || 0,
      incident_description: values.incident_description || null,
      incident_department: values.incident_department || null,
      notes: values.notes || null,
      bar_cogs_deduction: parseFloat(values.bar_cogs_deduction) || 0,
      bar_threshold_retained: parseFloat(values.bar_threshold_retained) || 0,
      door_threshold_retained: parseFloat(values.door_threshold_retained) || 0,
      charge_backs: parseFloat(values.charge_backs) || 0,
      promoter_bar_payout: parseFloat(values.promoter_bar_payout) || 0,
      promoter_door_payout: parseFloat(values.promoter_door_payout) || 0,
      promoter_table_payout: parseFloat(values.promoter_table_payout) || 0,
      artist_cost_paid_by_venue: parseFloat(values.artist_cost_paid_by_venue) || 0,
      effective_split_percentage: parseFloat(values.effective_split_percentage) || null,
      settlement_notes: values.settlement_notes || null,
      benchmark_effective_split: values.benchmark_effective_split || null,
    }
    try {
      const res = await fetch(existingId ? `/api/actuals/${existingId}` : "/api/actuals", {
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

  return (
    <div className="px-4 py-5">
      <div className="mb-5">
        <h1 className="text-xl font-bold text-foreground">Night Of Actuals</h1>
        <p className="text-sm text-muted-foreground mt-0.5">Record during and after the event</p>
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

        <F label="Time of Entry">
          <Select value={timeOfEntry} onValueChange={(v) => { setTimeOfEntry(v); setValue("time_of_entry", v) }}>
            <SelectTrigger className={cn(inp, "w-full")}>
              <SelectValue placeholder="Select check-in time" />
            </SelectTrigger>
            <SelectContent className="bg-card border-border">
              {TIME_OF_ENTRY_OPTIONS.map((t) => (
                <SelectItem key={t} value={t} className="text-foreground focus:bg-muted">{t}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </F>
        <input type="hidden" {...register("time_of_entry")} />

        <Sec title="Bar Sales" />
        <F label="Total Bar Sales Gross ($)">
          <Input type="number" className={inp} placeholder="0" {...register("total_bar_sales")} />
        </F>
        <div className="grid grid-cols-2 gap-3">
          <F label="Liquor Sales ($)">
            <Input type="number" className={inp} placeholder="0" {...register("liquor_sales")} />
          </F>
          <F label="Beer & Wine ($)">
            <Input type="number" className={inp} placeholder="0" {...register("beer_wine_sales")} />
          </F>
        </div>
        <F label="Table & Bottle Service ($)">
          <Input type="number" className={inp} placeholder="0" {...register("table_bottle_service")} />
        </F>

        <Sec title="Deductions" />
        <div className="grid grid-cols-2 gap-3">
          <F label="Comps Total ($)">
            <Input type="number" className={inp} placeholder="0" {...register("comps_total")} />
          </F>
          <F label="Voids ($)">
            <Input type="number" className={inp} placeholder="0" {...register("voids")} />
          </F>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <F label="Tax Collected ($)">
            <Input type="number" className={inp} placeholder="0" {...register("tax_collected")} />
          </F>
          <F label="Tips ($)">
            <Input type="number" className={inp} placeholder="0" {...register("tips")} />
          </F>
        </div>

        <Sec title="Door Revenue" />
        <div className="grid grid-cols-2 gap-3">
          <F label="Door Cash ($)">
            <Input type="number" className={inp} placeholder="0" {...register("door_revenue_cash")} />
          </F>
          <F label="Door Card ($)">
            <Input type="number" className={inp} placeholder="0" {...register("door_revenue_card")} />
          </F>
        </div>

        <Sec title="Headcount" />
        <F label="Total Headcount">
          <Input type="number" className={inp} placeholder="0" {...register("total_headcount")} />
        </F>

        {/* Settlement section — only at Close */}
        {isClose && <SettlementSection w={watchedValues} register={register} eventDeal={eventDeal} setValue={setValue} />}

        <Sec title="Incidents" />
        <F label="Incident Description" hint="optional">
          <Textarea
            className="bg-input border-border text-foreground placeholder:text-muted-foreground min-h-[80px] focus:ring-1 focus:ring-primary resize-none"
            placeholder="Describe any incidents..."
            {...register("incident_description")}
          />
        </F>
        <F label="Incident Department" hint="optional">
          <Input className={inp} placeholder="e.g. Security, Bar, Door" {...register("incident_department")} />
        </F>

        <Sec title="Notes" />
        <Textarea
          className="bg-input border-border text-foreground placeholder:text-muted-foreground min-h-[80px] focus:ring-1 focus:ring-primary resize-none"
          placeholder="Additional notes..."
          {...register("notes")}
        />

        <div className="pt-4 pb-2">
          <Button type="submit" disabled={saving || saved || !selectedEventId || !timeOfEntry}
            className="w-full h-12 text-base font-semibold bg-primary hover:bg-primary/90 text-primary-foreground">
            {saved ? <span className="flex items-center gap-2"><CheckCircle className="h-4 w-4" />Saved!</span>
              : saving ? "Saving..." : existingId ? "Update Check-In" : "Save Check-In"}
          </Button>
        </div>

      </form>
    </div>
  )
}
