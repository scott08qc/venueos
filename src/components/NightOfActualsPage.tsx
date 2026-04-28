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
import { CheckCircle, Calculator } from "lucide-react"

interface Props {
  events: EventSummary[]
  preselectedEventId?: number | null
  checkinTimes?: string[]
}

type FormValues = {
  event_id: string; time_of_entry: string
  total_bar_sales: string; liquor_sales: string; beer_wine_sales: string
  non_alcoholic_food_sales: string
  table_bottle_service: string; tables_active: string
  comps_total: string; voids: string
  tax_collected: string; tips: string
  door_revenue_cash: string; door_revenue_card: string
  ticket_scan_count: string; walk_up_count: string; comp_admissions: string
  total_headcount: string; incident_description: string; incident_department: string; notes: string
  bar_cogs_deduction: string; bar_threshold_retained: string; house_fee_deduction: string; door_threshold_retained: string
  charge_backs: string; promoter_bar_payout: string; promoter_door_payout: string
  promoter_table_payout: string; artist_cost_paid_by_venue: string
  effective_split_percentage: string; settlement_notes: string
  benchmark_effective_split: string
  cost_security_total: string
  cost_door_girls_count: string
  cost_door_girls_total: string
  cost_police_hours: string
  cost_production_staff_count: string
  cost_production_staff_total: string
  cost_equipment_total: string
  cost_equipment_notes: string
  cost_hospitality_actual: string
  cost_nightly_operating: string
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
  const houseFee = parseFloat(w.house_fee_deduction) || 0
  const doorThresh = parseFloat(w.door_threshold_retained) || 0
  const comps = parseFloat(w.comps_total) || 0
  const voids = parseFloat(w.voids) || 0
  const tax = parseFloat(w.tax_collected) || 0
  const tips = parseFloat(w.tips) || 0
  const chargeBacks = parseFloat(w.charge_backs) || 0
  const totalDeductions = barCogs + barThresh + doorThresh + houseFee + comps + voids + tax + tips + chargeBacks

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

        <ManualRow label="House fee deduction" hint="— auto-calculated from deal terms if applicable, or enter manually">
          <Input type="number" className={settInp} placeholder="0" {...register("house_fee_deduction")} />
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

export function NightOfActualsPage({ events, preselectedEventId, checkinTimes }: Props) {
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [selectedEventId, setSelectedEventId] = useState(preselectedEventId?.toString() || "")
  const [timeOfEntry, setTimeOfEntry] = useState("")
  const [existingId, setExistingId] = useState<number | null>(null)
  const [eventDeal, setEventDeal] = useState<EventFull | null>(null)
  const [costSummary, setCostSummary] = useState<any>(null)
  const [existingCostRecord, setExistingCostRecord] = useState<any>(null)

  const blank: FormValues = {
    event_id: preselectedEventId?.toString() || "", time_of_entry: "",
    total_bar_sales: "", liquor_sales: "", beer_wine_sales: "",
    non_alcoholic_food_sales: "",
    table_bottle_service: "", tables_active: "",
    comps_total: "", voids: "",
    tax_collected: "", tips: "", door_revenue_cash: "", door_revenue_card: "",
    ticket_scan_count: "", walk_up_count: "", comp_admissions: "",
    total_headcount: "", incident_description: "", incident_department: "", notes: "",
    bar_cogs_deduction: "", bar_threshold_retained: "", house_fee_deduction: "", door_threshold_retained: "",
    charge_backs: "", promoter_bar_payout: "", promoter_door_payout: "",
    promoter_table_payout: "", artist_cost_paid_by_venue: "",
    effective_split_percentage: "", settlement_notes: "",
    benchmark_effective_split: "",
    cost_security_total: "", cost_door_girls_count: "", cost_door_girls_total: "",
    cost_police_hours: "", cost_production_staff_count: "", cost_production_staff_total: "",
    cost_equipment_total: "", cost_equipment_notes: "", cost_hospitality_actual: "",
    cost_nightly_operating: "",
  }

  const { register, handleSubmit, watch, setValue, reset } = useForm<FormValues>({ defaultValues: blank })
  const watchedValues = watch()
  const isClose = timeOfEntry === "Close"

  useEffect(() => {
    if (!selectedEventId) { setEventDeal(null); return }
    fetch(`/api/events/${selectedEventId}`).then(r => r.ok ? r.json() : null).then(d => setEventDeal(d || null))
  }, [selectedEventId])

  useEffect(() => {
    if (!selectedEventId) return
    fetch(`/api/costs/${selectedEventId}`)
      .then(r => r.ok ? r.json() : null)
      .then(d => {
        if (d && d.exists !== false) {
          setExistingCostRecord(d)
          // Pre-populate nightly operating cost from pre-event entry
          setValue("cost_nightly_operating", d.nightly_operating_cost?.toString() || "")
        }
      })
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
          non_alcoholic_food_sales: match.non_alcoholic_food_sales?.toString() || "",
          table_bottle_service: match.table_bottle_service?.toString() || "",
          tables_active: match.tables_active?.toString() || "",
          comps_total: match.comps_total?.toString() || "",
          voids: match.voids?.toString() || "",
          tax_collected: match.tax_collected?.toString() || "",
          tips: match.tips?.toString() || "",
          door_revenue_cash: match.door_revenue_cash?.toString() || "",
          door_revenue_card: match.door_revenue_card?.toString() || "",
          ticket_scan_count: match.ticket_scan_count?.toString() || "",
          walk_up_count: match.walk_up_count?.toString() || "",
          comp_admissions: match.comp_admissions?.toString() || "",
          total_headcount: match.total_headcount?.toString() || "",
          incident_description: match.incident_description || "",
          incident_department: match.incident_department || "",
          notes: match.notes || "",
          bar_cogs_deduction: match.bar_cogs_deduction?.toString() || "",
          bar_threshold_retained: match.bar_threshold_retained?.toString() || "",
          house_fee_deduction: match.house_fee_deduction?.toString() || "",
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
      non_alcoholic_food_sales: parseFloat(values.non_alcoholic_food_sales) || 0,
      table_bottle_service: parseFloat(values.table_bottle_service) || 0,
      tables_active: parseInt(values.tables_active) || 0,
      comps_total: parseFloat(values.comps_total) || 0,
      voids: parseFloat(values.voids) || 0,
      tax_collected: parseFloat(values.tax_collected) || 0,
      tips: parseFloat(values.tips) || 0,
      door_revenue_cash: parseFloat(values.door_revenue_cash) || 0,
      door_revenue_card: parseFloat(values.door_revenue_card) || 0,
      ticket_scan_count: parseInt(values.ticket_scan_count) || 0,
      walk_up_count: parseInt(values.walk_up_count) || 0,
      comp_admissions: parseInt(values.comp_admissions) || 0,
      total_headcount: (parseInt(values.ticket_scan_count) || 0) + (parseInt(values.walk_up_count) || 0) + (parseInt(values.comp_admissions) || 0),
      incident_description: values.incident_description || null,
      incident_department: values.incident_department || null,
      notes: values.notes || null,
      bar_cogs_deduction: parseFloat(values.bar_cogs_deduction) || 0,
      bar_threshold_retained: parseFloat(values.bar_threshold_retained) || 0,
      house_fee_deduction: parseFloat(values.house_fee_deduction) || 0,
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
        if (isClose && values.event_id) {
          const hours = parseFloat(values.cost_police_hours) || 0
          const policeTotal = hours > 0 ? Math.max(hours * 50, 200) : 0
          const costsPayload = {
            event_id: parseInt(values.event_id),
            security_total: parseFloat(values.cost_security_total) || 0,
            door_girls_count: parseInt(values.cost_door_girls_count) || 0,
            door_girls_total: parseFloat(values.cost_door_girls_total) || 0,
            police_hours: hours,
            police_rate: 50,
            police_minimum: 200,
            police_total: policeTotal,
            production_staff_count: parseInt(values.cost_production_staff_count) || 0,
            production_staff_total: parseFloat(values.cost_production_staff_total) || 0,
            production_equipment_total: parseFloat(values.cost_equipment_total) || 0,
            production_equipment_notes: values.cost_equipment_notes || null,
            hospitality_rider_actual: parseFloat(values.cost_hospitality_actual) || 0,
            nightly_operating_cost: parseFloat(values.cost_nightly_operating) || 0,
          }
          await fetch("/api/costs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(costsPayload),
          })
          // Load summary after saving
          const summaryRes = await fetch(`/api/costs/${values.event_id}/summary`)
          if (summaryRes.ok) setCostSummary(await summaryRes.json())
        }
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
              {(checkinTimes ?? ["11 PM", "12 AM", "1 AM", "2 AM", "Close"]).map((t) => (
                <SelectItem key={t} value={t} className="text-foreground focus:bg-muted">{t}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </F>
        <input type="hidden" {...register("time_of_entry")} />

        <Sec title="Bar sales — exclude table and bottle service entirely" />
        <F label="Total bar gross — from POS export ($)">
          <Input type="number" className={inp} placeholder="0" {...register("total_bar_sales")} />
        </F>
        <div className="grid grid-cols-1 gap-3">
          <F label="Liquor — breakdown of bar gross above ($)">
            <Input type="number" className={inp} placeholder="0" {...register("liquor_sales")} />
          </F>
          <F label="Beer and wine — breakdown of bar gross above ($)">
            <Input type="number" className={inp} placeholder="0" {...register("beer_wine_sales")} />
          </F>
          <F label="Non-alcoholic and food — breakdown of bar gross above ($)">
            <Input type="number" className={inp} placeholder="0" {...register("non_alcoholic_food_sales")} />
          </F>
        </div>
        <p className="text-xs text-muted-foreground px-1 pb-1">
          Subcategory fields should sum to bar gross — they are breakdowns not additions.
        </p>

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

        <Sec title="Table and bottle service — completely separate from bar" />
        <F label="Total table and bottle service gross ($)">
          <Input type="number" className={inp} placeholder="0" {...register("table_bottle_service")} />
        </F>
        <div className="grid grid-cols-2 gap-3">
          <F label="Number of tables active">
            <Input type="number" className={inp} placeholder="0" {...register("tables_active")} />
          </F>
          <div className="space-y-1.5">
            <Label className="text-sm text-foreground/90">Average spend per table</Label>
            <div className={cn(inp, "flex items-center px-3 rounded-md border bg-muted/40 text-muted-foreground select-none")}>
              {(() => {
                const gross = parseFloat(watchedValues.table_bottle_service) || 0
                const tables = parseFloat(watchedValues.tables_active) || 0
                return tables > 0 ? "$" + (gross / tables).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "—"
              })()}
            </div>
          </div>
        </div>

        <Sec title="Door revenue" />
        <div className="grid grid-cols-2 gap-3">
          <F label="Door revenue cash ($)">
            <Input type="number" className={inp} placeholder="0" {...register("door_revenue_cash")} />
          </F>
          <F label="Door revenue card ($)">
            <Input type="number" className={inp} placeholder="0" {...register("door_revenue_card")} />
          </F>
        </div>
        <div className="space-y-1.5">
          <Label className="text-sm text-foreground/90">Total door revenue</Label>
          <div className={cn(inp, "flex items-center px-3 rounded-md border bg-muted/40 text-muted-foreground select-none")}>
            {(() => {
              const cash = parseFloat(watchedValues.door_revenue_cash) || 0
              const card = parseFloat(watchedValues.door_revenue_card) || 0
              return "$" + (cash + card).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
            })()}
          </div>
        </div>
        <div className="grid grid-cols-3 gap-3">
          <F label="Ticket scan count">
            <Input type="number" className={inp} placeholder="0" {...register("ticket_scan_count")} />
          </F>
          <F label="Walk-up count">
            <Input type="number" className={inp} placeholder="0" {...register("walk_up_count")} />
          </F>
          <F label="Comp admissions">
            <Input type="number" className={inp} placeholder="0" {...register("comp_admissions")} />
          </F>
        </div>
        <div className="space-y-1.5">
          <Label className="text-sm text-foreground/90">Total headcount</Label>
          <div className={cn(inp, "flex items-center px-3 rounded-md border bg-muted/40 text-muted-foreground select-none")}>
            {(() => {
              const scans = parseInt(watchedValues.ticket_scan_count) || 0
              const walkup = parseInt(watchedValues.walk_up_count) || 0
              const comps = parseInt(watchedValues.comp_admissions) || 0
              return (scans + walkup + comps).toLocaleString("en-US")
            })()}
          </div>
        </div>

        {/* Settlement section — only at Close */}
        {isClose && <SettlementSection w={watchedValues} register={register} eventDeal={eventDeal} setValue={setValue} />}

        {isClose && (
          <div className="mt-4 rounded-xl overflow-hidden" style={{ background: '#F8F9FB', border: '1px solid #E2E8F0' }}>
            <div className="px-4 py-3 border-b" style={{ background: 'rgba(44,95,138,0.06)', borderColor: '#E2E8F0' }}>
              <p className="text-sm font-bold uppercase tracking-wider" style={{ color: '#2C5F8A' }}>Variable Costs — Close</p>
            </div>
            <div className="px-4 py-3 space-y-4">

              <ManualRow label="Security ($)">
                <Input type="number" className={settInp} placeholder="0" {...register("cost_security_total")} />
              </ManualRow>

              <div className="grid grid-cols-2 gap-3">
                <ManualRow label="Door girls — count">
                  <Input type="number" className={settInp} placeholder="0" {...register("cost_door_girls_count")} />
                </ManualRow>
                <ManualRow label="Door girls — total ($)">
                  <Input type="number" className={settInp} placeholder="0" {...register("cost_door_girls_total")} />
                </ManualRow>
              </div>

              <div>
                <ManualRow label="Police — hours worked">
                  <Input type="number" className={settInp} placeholder="0" {...register("cost_police_hours")} />
                </ManualRow>
                {(() => {
                  const hours = parseFloat(watchedValues.cost_police_hours) || 0
                  const total = hours > 0 ? Math.max(hours * 50, 200) : 0
                  return hours > 0 ? (
                    <p className="text-xs mt-1" style={{ color: '#2C5F8A' }}>
                      Calculated total: ${total.toLocaleString("en-US", { minimumFractionDigits: 2 })} ($50/hr · $200 min)
                    </p>
                  ) : null
                })()}
              </div>

              <div className="grid grid-cols-2 gap-3">
                <ManualRow label="Production staff — count">
                  <Input type="number" className={settInp} placeholder="0" {...register("cost_production_staff_count")} />
                </ManualRow>
                <ManualRow label="Production staff — total ($)">
                  <Input type="number" className={settInp} placeholder="0" {...register("cost_production_staff_total")} />
                </ManualRow>
              </div>

              <ManualRow label="Production equipment + tech rider ($)">
                <Input type="number" className={settInp} placeholder="0" {...register("cost_equipment_total")} />
              </ManualRow>
              <ManualRow label="Production equipment notes" hint="optional">
                <Input className={settInp} placeholder="Describe equipment or tech rider items" {...register("cost_equipment_notes")} />
              </ManualRow>

              <div>
                <ManualRow label="Hospitality rider — actual ($)">
                  <Input type="number" className={settInp} placeholder="0" {...register("cost_hospitality_actual")} />
                </ManualRow>
                {existingCostRecord?.hospitality_rider_estimate > 0 && (
                  <p className="text-xs mt-1 text-muted-foreground">
                    Pre-event estimate: ${parseFloat(existingCostRecord.hospitality_rider_estimate).toLocaleString("en-US", { minimumFractionDigits: 2 })}
                  </p>
                )}
              </div>

              <ManualRow label="Nightly operating cost ($)" hint="pre-populated from event record">
                <Input type="number" className={settInp} placeholder="0" {...register("cost_nightly_operating")} />
              </ManualRow>

            </div>
          </div>
        )}

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

      {costSummary && (
        <div className="mt-4 rounded-xl overflow-hidden" style={{ background: '#F0F7FF', border: '1px solid rgba(44,95,138,0.3)' }}>
          <div className="px-4 py-3 border-b flex items-center gap-2" style={{ background: 'rgba(44,95,138,0.08)', borderColor: 'rgba(44,95,138,0.2)' }}>
            <span className="text-sm font-bold uppercase tracking-wider" style={{ color: '#2C5F8A' }}>Cost Summary — {costSummary.event_name}</span>
          </div>
          <div className="px-4 py-3 space-y-1">
            {/* Revenue */}
            <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground pt-1 pb-0.5">Revenue</p>
            <div className="flex justify-between text-sm py-1"><span className="text-muted-foreground">Bar</span><span className="font-medium">${costSummary.revenue?.bar?.toLocaleString("en-US", { minimumFractionDigits: 2 })}</span></div>
            <div className="flex justify-between text-sm py-1"><span className="text-muted-foreground">Door</span><span className="font-medium">${costSummary.revenue?.door?.toLocaleString("en-US", { minimumFractionDigits: 2 })}</span></div>
            <div className="flex justify-between text-sm font-semibold py-1.5 border-t border-primary/20 mt-1"><span>Total Revenue</span><span>${costSummary.revenue?.total?.toLocaleString("en-US", { minimumFractionDigits: 2 })}</span></div>

            {/* Cost lines */}
            <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground pt-3 pb-0.5">Costs</p>
            {costSummary.cost_lines?.map((line: any, i: number) => (
              <div key={i} className="flex justify-between text-sm py-1">
                <span className="text-muted-foreground">{line.label}</span>
                <span className="font-medium">${line.amount?.toLocaleString("en-US", { minimumFractionDigits: 2 })}</span>
              </div>
            ))}
            {costSummary.promoter_payouts > 0 && (
              <div className="flex justify-between text-sm py-1">
                <span className="text-muted-foreground">Promoter payouts</span>
                <span className="font-medium">${costSummary.promoter_payouts?.toLocaleString("en-US", { minimumFractionDigits: 2 })}</span>
              </div>
            )}
            <div className="flex justify-between text-sm font-semibold py-1.5 border-t border-primary/20 mt-1"><span>Total Costs</span><span>${costSummary.total_costs?.toLocaleString("en-US", { minimumFractionDigits: 2 })}</span></div>

            {/* Net */}
            <div className="mt-2 rounded-lg px-3 py-2.5 space-y-1" style={{ background: 'rgba(44,95,138,0.08)', border: '1px solid rgba(44,95,138,0.15)' }}>
              <div className="flex justify-between items-center">
                <span className="text-sm font-bold" style={{ color: '#2C5F8A' }}>Net</span>
                <span className={`text-lg font-bold ${costSummary.net >= 0 ? '' : 'text-red-600'}`} style={costSummary.net >= 0 ? { color: '#2C5F8A' } : {}}>
                  ${costSummary.net?.toLocaleString("en-US", { minimumFractionDigits: 2 })}
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-xs text-muted-foreground">Net margin</span>
                <span className="text-sm font-semibold" style={{ color: '#2C5F8A' }}>{costSummary.net_margin_pct}%</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
