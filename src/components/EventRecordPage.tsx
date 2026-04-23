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
import type { EventFull } from "@/types"
import {
  TIER1_CATEGORIES, DEAL_STRUCTURES,
  BAR_SPLIT_BASIS_OPTIONS, DOOR_SPLIT_BASIS_OPTIONS,
  DEPOSIT_COLLECTION_OPTIONS, ARTIST_COST_RESPONSIBILITY_OPTIONS,
  NET_REVENUE_BASIS_OPTIONS, TABLE_SPLIT_BASIS_OPTIONS, ARTIST_COST_SPLIT_METHOD_OPTIONS,
} from "@/types"
import { CheckCircle } from "lucide-react"

interface Props {
  editEvent?: EventFull | null
  onSaved: () => void
}

type FormValues = {
  event_name: string; event_date: string; tier1_category: string
  tier2_subcategory: string; promoter_name: string
  artist_name: string; artist_genre: string
  expected_attendance: string; venue_capacity: string
  deal_structure_type: string
  // Net revenue split
  net_revenue_promoter_pct: string; net_revenue_basis: string
  // Bar
  bar_split_promoter: string; bar_split_basis: string
  bar_cogs_percentage: string; bar_threshold_amount: string; bar_guarantee_amount: string
  // Door
  door_split_promoter: string; door_split_basis: string
  door_threshold_amount: string; door_guarantee_amount: string
  // Table (rebuilt)
  table_split_promoter: string; table_split_basis: string
  deposit_collection_method: string
  // Artist costs
  artist_fee_landed: string; artist_fee_travel: string
  artist_cost_responsibility: string
  artist_cost_split_method: string
  artist_venue_dollar: string; artist_venue_pct: string; artist_promoter_dollar: string
  deal_notes: string
  deposit_amount: string; deposit_due_date: string
  balance_due: string; balance_due_date: string
  projected_door_revenue: string; projected_bar_revenue: string; projected_table_revenue: string
  notes: string
}

function Sec({ title }: { title: string }) {
  return (
    <div className="pt-5 pb-1">
      <p className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: '#2C5F8A' }}>{title}</p>
      <Separator className="mt-2" style={{ background: '#EEEEEE' }} />
    </div>
  )
}

function SubSec({ title }: { title: string }) {
  return (
    <div className="pt-3 pb-0.5">
      <p className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: '#888888' }}>{title}</p>
    </div>
  )
}

function F({ label, required, hint, children }: {
  label: string; required?: boolean; hint?: string; children: React.ReactNode
}) {
  return (
    <div className="space-y-1.5">
      <Label className="text-sm text-foreground/90">
        {label}
        {required && <span className="text-primary ml-1">*</span>}
        {hint && <span className="text-muted-foreground font-normal ml-1 text-xs">({hint})</span>}
      </Label>
      {children}
    </div>
  )
}

function Sel({ value, onValueChange, placeholder, options, className }: {
  value: string; onValueChange: (v: string) => void; placeholder: string
  options: readonly string[]; className?: string
}) {
  return (
    <Select value={value} onValueChange={onValueChange}>
      <SelectTrigger className={cn("bg-input border-border text-foreground h-11", className)}>
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent className="bg-card border-border">
        {options.map((o) => (
          <SelectItem key={o} value={o} className="text-foreground focus:bg-muted">{o}</SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}

const inp = "bg-input border-border text-foreground placeholder:text-muted-foreground focus:ring-1 focus:ring-primary h-11"

export function EventRecordPage({ editEvent, onSaved }: Props) {
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [dayOfWeek, setDayOfWeek] = useState("")
  const [projTotal, setProjTotal] = useState(0)

  // Controlled select states
  const [tier1, setTier1] = useState("")
  const [dealType, setDealType] = useState("")
  const [barBasis, setBarBasis] = useState("")
  const [doorBasis, setDoorBasis] = useState("")
  const [netRevBasis, setNetRevBasis] = useState("")
  const [tableBasis, setTableBasis] = useState("")
  const [depositMethod, setDepositMethod] = useState("")
  const [artistResp, setArtistResp] = useState("")
  const [artistSplitMethod, setArtistSplitMethod] = useState("")

  const { register, handleSubmit, watch, setValue, reset } = useForm<FormValues>({
    defaultValues: { venue_capacity: "800", projected_door_revenue: "0", projected_bar_revenue: "0", projected_table_revenue: "0" }
  })

  useEffect(() => {
    if (!editEvent) return
    const v = editEvent
    setTier1(v.tier1_category || "")
    setDealType(v.deal_structure_type || "")
    setBarBasis(v.bar_split_basis || "")
    setDoorBasis(v.door_split_basis || "")
    setNetRevBasis(v.net_revenue_basis || "")
    setTableBasis(v.table_split_basis || "")
    setDepositMethod(v.deposit_collection_method || "")
    setArtistResp(v.artist_cost_responsibility || "")
    setArtistSplitMethod(v.artist_cost_split_method || "")
    if (v.event_date) {
      setDayOfWeek(new Date(v.event_date + "T12:00:00").toLocaleDateString("en-US", { weekday: "long" }))
    }
    reset({
      event_name: v.event_name || "", event_date: v.event_date || "",
      tier1_category: v.tier1_category || "", tier2_subcategory: v.tier2_subcategory || "",
      promoter_name: v.promoter_name || "", artist_name: v.artist_name || "",
      artist_genre: v.artist_genre || "",
      expected_attendance: v.expected_attendance?.toString() || "",
      venue_capacity: v.venue_capacity?.toString() || "800",
      deal_structure_type: v.deal_structure_type || "",
      net_revenue_promoter_pct: v.net_revenue_promoter_pct?.toString() || "",
      net_revenue_basis: v.net_revenue_basis || "",
      bar_split_promoter: v.bar_split_promoter?.toString() || "",
      bar_split_basis: v.bar_split_basis || "",
      bar_cogs_percentage: v.bar_cogs_percentage?.toString() || "",
      bar_threshold_amount: v.bar_threshold_amount?.toString() || "",
      bar_guarantee_amount: v.bar_guarantee_amount?.toString() || "",
      door_split_promoter: v.door_split_promoter?.toString() || "",
      door_split_basis: v.door_split_basis || "",
      door_threshold_amount: v.door_threshold_amount?.toString() || "",
      door_guarantee_amount: v.door_guarantee_amount?.toString() || "",
      table_split_promoter: v.table_split_promoter?.toString() || "",
      table_split_basis: v.table_split_basis || "",
      deposit_collection_method: v.deposit_collection_method || "",
      artist_fee_landed: v.artist_fee_landed?.toString() || "",
      artist_fee_travel: v.artist_fee_travel?.toString() || "",
      artist_cost_responsibility: v.artist_cost_responsibility || "",
      artist_cost_split_method: v.artist_cost_split_method || "",
      artist_venue_dollar: v.artist_venue_dollar?.toString() || "",
      artist_venue_pct: v.artist_venue_pct?.toString() || "",
      artist_promoter_dollar: v.artist_promoter_dollar?.toString() || "",
      deal_notes: v.deal_notes || "",
      deposit_amount: v.deposit_amount?.toString() || "",
      deposit_due_date: v.deposit_due_date || "",
      balance_due: v.balance_due?.toString() || "",
      balance_due_date: v.balance_due_date || "",
      projected_door_revenue: v.projected_door_revenue?.toString() || "0",
      projected_bar_revenue: v.projected_bar_revenue?.toString() || "0",
      projected_table_revenue: v.projected_table_revenue?.toString() || "0",
      notes: v.notes || "",
    })
  }, [editEvent, reset])

  const [pD, pB, pT] = watch(["projected_door_revenue", "projected_bar_revenue", "projected_table_revenue"])
  const watchNetRevPct = parseFloat(watch("net_revenue_promoter_pct")) || 0
  const watchArtistVenueDollar = parseFloat(watch("artist_venue_dollar")) || 0
  const watchArtistPromoterDollar = parseFloat(watch("artist_promoter_dollar")) || 0
  const watchArtistFeeLanded = parseFloat(watch("artist_fee_landed")) || 0
  const watchArtistFeeTravel = parseFloat(watch("artist_fee_travel")) || 0
  const watchArtistVenuePct = parseFloat(watch("artist_venue_pct")) || 0
  useEffect(() => {
    setProjTotal((parseFloat(pD) || 0) + (parseFloat(pB) || 0) + (parseFloat(pT) || 0))
  }, [pD, pB, pT])

  function syncSel(field: keyof FormValues, val: string, setter: (v: string) => void) {
    setter(val)
    setValue(field, val)
  }

  async function onSubmit(values: FormValues) {
    setSaving(true)
    const barPct = parseFloat(values.bar_split_promoter) || null
    const doorPct = parseFloat(values.door_split_promoter) || null
    const netRevPct = parseFloat(values.net_revenue_promoter_pct) || null
    // Artist cost split calc — if percentage, compute venue dollar from total fee
    const totalFee = (parseFloat(values.artist_fee_landed) || 0) + (parseFloat(values.artist_fee_travel) || 0)
    let artistVenueDollar: number | null = null
    if (values.artist_cost_responsibility === "Split per deal terms") {
      if (values.artist_cost_split_method === "By dollar amount") {
        artistVenueDollar = parseFloat(values.artist_venue_dollar) || null
      } else if (values.artist_cost_split_method === "By percentage") {
        const vPct = parseFloat(values.artist_venue_pct) || 0
        artistVenueDollar = totalFee > 0 ? (totalFee * vPct) / 100 : null
      }
    }
    const payload = {
      event_name: values.event_name, event_date: values.event_date,
      tier1_category: values.tier1_category || null,
      tier2_subcategory: values.tier2_subcategory || null,
      promoter_name: values.promoter_name || null,
      artist_name: values.artist_name || null, artist_genre: values.artist_genre || null,
      expected_attendance: values.expected_attendance ? parseInt(values.expected_attendance) : null,
      venue_capacity: values.venue_capacity ? parseInt(values.venue_capacity) : 800,
      deal_structure_type: values.deal_structure_type || null,
      // Net revenue split
      net_revenue_promoter_pct: netRevPct,
      net_revenue_venue_pct: netRevPct !== null ? 100 - netRevPct : null,
      net_revenue_basis: values.net_revenue_basis || null,
      // Bar
      bar_split_promoter: barPct,
      bar_split_venue: barPct !== null ? 100 - barPct : null,
      bar_split_basis: values.bar_split_basis || null,
      bar_cogs_percentage: values.bar_cogs_percentage ? parseFloat(values.bar_cogs_percentage) : null,
      bar_threshold_amount: values.bar_threshold_amount ? parseFloat(values.bar_threshold_amount) : null,
      bar_guarantee_amount: values.bar_guarantee_amount ? parseFloat(values.bar_guarantee_amount) : null,
      // Door
      door_split_promoter: doorPct,
      door_split_venue: doorPct !== null ? 100 - doorPct : null,
      door_split_basis: values.door_split_basis || null,
      door_threshold_amount: values.door_threshold_amount ? parseFloat(values.door_threshold_amount) : null,
      door_guarantee_amount: values.door_guarantee_amount ? parseFloat(values.door_guarantee_amount) : null,
      // Table (keep legacy fields null, use new ones)
      table_minimum: null,
      table_split_promoter_overage: null,
      table_split_promoter: values.table_split_promoter ? parseFloat(values.table_split_promoter) : null,
      table_split_basis: values.table_split_basis || null,
      deposit_collection_method: values.deposit_collection_method || null,
      // Artist
      artist_fee_landed: values.artist_fee_landed ? parseFloat(values.artist_fee_landed) : null,
      artist_fee_travel: values.artist_fee_travel ? parseFloat(values.artist_fee_travel) : null,
      artist_cost_responsibility: values.artist_cost_responsibility || null,
      artist_cost_split_note: null,
      artist_cost_split_method: values.artist_cost_split_method || null,
      artist_venue_dollar: artistVenueDollar,
      artist_venue_pct: values.artist_venue_pct ? parseFloat(values.artist_venue_pct) : null,
      artist_promoter_dollar: values.artist_promoter_dollar ? parseFloat(values.artist_promoter_dollar) : null,
      deal_notes: values.deal_notes || null,
      deposit_amount: values.deposit_amount ? parseFloat(values.deposit_amount) : null,
      deposit_due_date: values.deposit_due_date || null,
      balance_due: values.balance_due ? parseFloat(values.balance_due) : null,
      balance_due_date: values.balance_due_date || null,
      projected_door_revenue: parseFloat(values.projected_door_revenue) || 0,
      projected_bar_revenue: parseFloat(values.projected_bar_revenue) || 0,
      projected_table_revenue: parseFloat(values.projected_table_revenue) || 0,
      notes: values.notes || null,
    }
    try {
      const res = await fetch(editEvent ? `/api/events/${editEvent.id}` : "/api/events", {
        method: editEvent ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      if (res.ok) {
        setSaved(true)
        setTimeout(() => { setSaved(false); onSaved() }, 1200)
      }
    } finally { setSaving(false) }
  }

  return (
    <div className="px-4 py-5">
      <div className="mb-5">
        <h1 className="text-xl font-bold text-foreground">{editEvent ? "Edit Event Record" : "New Event Record"}</h1>
        <p className="text-sm text-muted-foreground mt-0.5">Fill out before each event</p>
      </div>

      <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">

        <Sec title="Event Details" />
        <F label="Event Name" required>
          <Input className={inp} placeholder="e.g. Saturday Night Live" {...register("event_name", { required: true })} />
        </F>
        <div className="grid grid-cols-2 gap-3">
          <F label="Event Date" required>
            <Input type="date" className={cn(inp, "appearance-none")}
              {...register("event_date", { required: true, onChange: (e) => {
                const v = e.target.value
                setDayOfWeek(v ? new Date(v + "T12:00:00").toLocaleDateString("en-US", { weekday: "long" }) : "")
              }})} />
          </F>
          <F label="Day of Week">
            <div className="h-11 flex items-center px-3 rounded-md border text-sm" style={{ background: '#F5F5F5', borderColor: '#CCCCCC', color: '#888888' }}>
              {dayOfWeek || <span className="italic opacity-50">Auto</span>}
            </div>
          </F>
        </div>
        <F label="Tier 1 Category">
          <Sel value={tier1} onValueChange={(v) => syncSel("tier1_category", v, setTier1)}
            placeholder="Select category" options={TIER1_CATEGORIES} />
        </F>
        <input type="hidden" {...register("tier1_category")} />
        <F label="Tier 2 Subcategory" hint="optional">
          <Input className={inp} placeholder="e.g. Techno, Latin trap, Comedy night" {...register("tier2_subcategory")} />
        </F>
        <F label="Promoter Name">
          <Input className={inp} placeholder="Promoter or company name" {...register("promoter_name")} />
        </F>

        <Sec title="Artist" />
        <F label="Artist Name" hint="optional">
          <Input className={inp} placeholder="Headliner name" {...register("artist_name")} />
        </F>
        <F label="Artist Genre" hint="optional">
          <Input className={inp} placeholder="e.g. House, Hip Hop" {...register("artist_genre")} />
        </F>

        <Sec title="Attendance & Capacity" />
        <div className="grid grid-cols-2 gap-3">
          <F label="Expected Attendance">
            <Input type="number" className={inp} placeholder="0" {...register("expected_attendance")} />
          </F>
          <F label="Venue Capacity">
            <Input type="number" className={inp} placeholder="800" {...register("venue_capacity")} />
          </F>
        </div>

        <Sec title="Deal Structure" />

        <F label="Deal Type">
          <Sel value={dealType} onValueChange={(v) => syncSel("deal_structure_type", v, setDealType)}
            placeholder="Select deal type" options={DEAL_STRUCTURES} />
        </F>
        <input type="hidden" {...register("deal_structure_type")} />

        {/* ── Net Revenue Split mode — collapses bar/door/table splits ── */}
        {dealType === "Net revenue split" && (<>
          <SubSec title="Net Revenue Split" />
          <F label="Promoter % of total net revenue" hint="0–100">
            <Input type="number" className={inp} placeholder="0" min={0} max={100} {...register("net_revenue_promoter_pct")} />
          </F>
          {(() => {
            const pct = watchNetRevPct
            return pct > 0 ? (
              <div className="rounded-md px-3 py-2 flex justify-between items-center text-sm" style={{ background: "#F5F5F5", border: "1px solid #CCCCCC" }}>
                <span className="text-muted-foreground">Venue %</span>
                <span className="font-semibold text-foreground">{(100 - pct).toFixed(1)}%</span>
              </div>
            ) : null
          })()}
          <F label="Net Revenue Basis">
            <Sel value={netRevBasis} onValueChange={(v) => syncSel("net_revenue_basis", v, setNetRevBasis)}
              placeholder="Select basis" options={NET_REVENUE_BASIS_OPTIONS} />
          </F>
          <input type="hidden" {...register("net_revenue_basis")} />
        </>)}

        {/* ── Bar Revenue (hidden when Net revenue split) ── */}
        {dealType !== "Net revenue split" && (<>
          <SubSec title="Bar Revenue" />
          <F label="Bar Split — Promoter %" hint="0–100">
            <Input type="number" className={inp} placeholder="0" min={0} max={100} {...register("bar_split_promoter")} />
          </F>
          <F label="Bar Split Basis">
            <Sel value={barBasis} onValueChange={(v) => syncSel("bar_split_basis", v, setBarBasis)}
              placeholder="Select basis" options={BAR_SPLIT_BASIS_OPTIONS} />
          </F>
          <input type="hidden" {...register("bar_split_basis")} />
          {barBasis === "Net after COGS" && (
            <F label="COGS % to deduct before split">
              <Input type="number" className={inp} placeholder="0" min={0} max={100} {...register("bar_cogs_percentage")} />
            </F>
          )}
          {barBasis === "Net after first threshold" && (
            <F label="First $X stays with venue before split kicks in">
              <Input type="number" className={inp} placeholder="0" {...register("bar_threshold_amount")} />
            </F>
          )}
          {barBasis === "Net after guarantee" && (
            <F label="Guaranteed bar minimum to venue before split">
              <Input type="number" className={inp} placeholder="0" {...register("bar_guarantee_amount")} />
            </F>
          )}
        </>)}

        {/* ── Door Revenue (hidden when Net revenue split) ── */}
        {dealType !== "Net revenue split" && (<>
          <SubSec title="Door Revenue" />
          <F label="Door Split — Promoter %" hint="0–100">
            <Input type="number" className={inp} placeholder="0" min={0} max={100} {...register("door_split_promoter")} />
          </F>
          <F label="Door Split Basis">
            <Sel value={doorBasis} onValueChange={(v) => syncSel("door_split_basis", v, setDoorBasis)}
              placeholder="Select basis" options={DOOR_SPLIT_BASIS_OPTIONS} />
          </F>
          <input type="hidden" {...register("door_split_basis")} />
          {doorBasis === "Net after first threshold to venue" && (
            <F label="First $X of door stays with venue">
              <Input type="number" className={inp} placeholder="0" {...register("door_threshold_amount")} />
            </F>
          )}
          {doorBasis === "After venue guarantee" && (
            <F label="Guaranteed door minimum to venue">
              <Input type="number" className={inp} placeholder="0" {...register("door_guarantee_amount")} />
            </F>
          )}
        </>)}

        {/* ── Table Revenue (hidden when Net revenue split) ── */}
        {dealType !== "Net revenue split" && (<>
          <SubSec title="Table Revenue" />
          <F label="Table Split — Promoter % of net table and bottle service revenue" hint="0–100">
            <Input type="number" className={inp} placeholder="0" min={0} max={100} {...register("table_split_promoter")} />
          </F>
          <F label="Table Split Basis">
            <Sel value={tableBasis} onValueChange={(v) => syncSel("table_split_basis", v, setTableBasis)}
              placeholder="Select basis" options={TABLE_SPLIT_BASIS_OPTIONS} />
          </F>
          <input type="hidden" {...register("table_split_basis")} />
          <F label="Deposit Collection Method">
            <Sel value={depositMethod} onValueChange={(v) => syncSel("deposit_collection_method", v, setDepositMethod)}
              placeholder="Select method" options={DEPOSIT_COLLECTION_OPTIONS} />
          </F>
          <input type="hidden" {...register("deposit_collection_method")} />
        </>)}

        {/* ── Artist Costs ── */}
        <SubSec title="Artist Costs" />
        <div className="grid grid-cols-2 gap-3">
          <F label="Fee — Landed ($)" hint="optional">
            <Input type="number" className={inp} placeholder="0" {...register("artist_fee_landed")} />
          </F>
          <F label="Fee — Travel ($)" hint="optional">
            <Input type="number" className={inp} placeholder="0" {...register("artist_fee_travel")} />
          </F>
        </div>
        <F label="Artist Cost Responsibility">
          <Sel value={artistResp} onValueChange={(v) => {
            syncSel("artist_cost_responsibility", v, setArtistResp)
            if (v !== "Split per deal terms") setArtistSplitMethod("")
          }} placeholder="Select responsibility" options={ARTIST_COST_RESPONSIBILITY_OPTIONS} />
        </F>
        <input type="hidden" {...register("artist_cost_responsibility")} />

        {artistResp === "Split per deal terms" && (<>
          <F label="Split Method">
            <Sel value={artistSplitMethod} onValueChange={(v) => syncSel("artist_cost_split_method", v, setArtistSplitMethod)}
              placeholder="Select split method" options={ARTIST_COST_SPLIT_METHOD_OPTIONS} />
          </F>
          <input type="hidden" {...register("artist_cost_split_method")} />

          {artistSplitMethod === "By dollar amount" && (() => {
            const vDollar = watchArtistVenueDollar
            const pDollar = watchArtistPromoterDollar
            const totalFeeWatch = watchArtistFeeLanded + watchArtistFeeTravel
            const mismatch = totalFeeWatch > 0 && Math.abs(vDollar + pDollar - totalFeeWatch) > 0.01
            return (
              <div className="space-y-3">
                <div className="grid grid-cols-2 gap-3">
                  <F label="Venue pays ($)">
                    <Input type="number" className={inp} placeholder="0" {...register("artist_venue_dollar")} />
                  </F>
                  <F label="Promoter pays ($)">
                    <Input type="number" className={inp} placeholder="0" {...register("artist_promoter_dollar")} />
                  </F>
                </div>
                {mismatch && (
                  <p className="text-xs px-1" style={{ color: '#8B3A3A' }}>⚠ Venue + promoter amounts don't match total artist fee (${totalFeeWatch.toLocaleString()})</p>
                )}
              </div>
            )
          })()}

          {artistSplitMethod === "By percentage" && (() => {
            const vPct = watchArtistVenuePct
            return (
              <div className="space-y-3">
                <F label="Venue % of total artist cost">
                  <Input type="number" className={inp} placeholder="0" min={0} max={100} {...register("artist_venue_pct")} />
                </F>
                {vPct > 0 && (
                  <div className="rounded-md px-3 py-2 flex justify-between items-center text-sm" style={{ background: "#F5F5F5", border: "1px solid #CCCCCC" }}>
                    <span className="text-muted-foreground">Promoter %</span>
                    <span className="font-semibold text-foreground">{(100 - vPct).toFixed(1)}%</span>
                  </div>
                )}
              </div>
            )
          })()}
        </>)}

        {/* ── Deal Notes ── */}
        <SubSec title="Deal Terms — Free Text" />
        <Textarea
          className="bg-input border-border text-foreground placeholder:text-muted-foreground min-h-[110px] focus:ring-1 focus:ring-primary resize-none text-sm"
          placeholder="Describe any deal terms that don't fit the fields above — thresholds, COGS treatment, guarantees, special arrangements, anything the structured fields don't capture"
          {...register("deal_notes")}
        />

        <Sec title="Payment Schedule" />
        <div className="grid grid-cols-2 gap-3">
          <F label="Deposit Amount ($)" hint="optional">
            <Input type="number" className={inp} placeholder="0" {...register("deposit_amount")} />
          </F>
          <F label="Deposit Due Date" hint="optional">
            <Input type="date" className={cn(inp, "appearance-none")} {...register("deposit_due_date")} />
          </F>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <F label="Balance Due ($)" hint="optional">
            <Input type="number" className={inp} placeholder="0" {...register("balance_due")} />
          </F>
          <F label="Balance Due Date" hint="optional">
            <Input type="date" className={cn(inp, "appearance-none")} {...register("balance_due_date")} />
          </F>
        </div>

        <Sec title="Revenue Projections" />
        <F label="Projected Door Revenue ($)">
          <Input type="number" className={inp} placeholder="0" {...register("projected_door_revenue")} />
        </F>
        <F label="Projected Bar Revenue ($)">
          <Input type="number" className={inp} placeholder="0" {...register("projected_bar_revenue")} />
        </F>
        <F label="Projected Table Revenue ($)">
          <Input type="number" className={inp} placeholder="0" {...register("projected_table_revenue")} />
        </F>
        <div className="rounded-lg px-4 py-3 flex justify-between items-center" style={{ background: 'rgba(44,95,138,0.08)', border: '1px solid rgba(44,95,138,0.2)' }}>
          <span className="text-sm font-semibold" style={{ color: '#2C5F8A' }}>Total Projected Revenue</span>
          <span className="text-lg font-bold" style={{ color: '#2C5F8A' }}>
            ${projTotal.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </span>
        </div>

        <Sec title="Notes" />
        <Textarea
          className="bg-input border-border text-foreground placeholder:text-muted-foreground min-h-[100px] focus:ring-1 focus:ring-primary resize-none"
          placeholder="Any additional notes for this event..."
          {...register("notes")}
        />

        <div className="pt-4 pb-2">
          <Button type="submit" disabled={saving || saved}
            className="w-full h-12 text-base font-semibold bg-primary hover:bg-primary/90 text-primary-foreground">
            {saved ? <span className="flex items-center gap-2"><CheckCircle className="h-4 w-4" />Saved!</span>
              : saving ? "Saving..." : editEvent ? "Update Event" : "Save Event Record"}
          </Button>
        </div>

      </form>
    </div>
  )
}
