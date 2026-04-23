import { useState } from "react"
import { useForm } from "react-hook-form"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Separator } from "@/components/ui/separator"
import { cn } from "@/lib/utils"
import {
  TIER1_CATEGORIES,
  DATA_SOURCE_OPTIONS,
  CLASSIFICATION_STATUS_OPTIONS,
} from "@/types"
import { CheckCircle } from "lucide-react"

type FormValues = {
  event_date: string
  event_name: string
  tier1_category: string
  tier2_subcategory: string
  promoter_name: string
  artist_name: string
  gross_revenue: string
  attendance: string
  data_source: string
  classification_status: string
}

function SectionHeader({ title }: { title: string }) {
  return (
    <div className="pt-4 pb-1">
      <p className="text-[11px] font-semibold uppercase tracking-widest" style={{ color: '#2C5F8A' }}>{title}</p>
      <Separator className="mt-2" style={{ background: '#EEEEEE' }} />
    </div>
  )
}

function FieldRow({ label, children, hint, required }: {
  label: string
  children: React.ReactNode
  hint?: string
  required?: boolean
}) {
  return (
    <div className="space-y-1.5">
      <Label className="text-sm text-foreground/90">
        {label}
        {required && <span className="text-primary ml-1">*</span>}
        {hint && <span className="text-muted-foreground font-normal ml-1">({hint})</span>}
      </Label>
      {children}
    </div>
  )
}

export function HistoricalDataPage() {
  const [saving, setSaving] = useState(false)
  const [saved, setSaved] = useState(false)
  const [tier1, setTier1] = useState("")
  const [dataSource, setDataSource] = useState("")
  const [classStatus, setClassStatus] = useState("Complete")

  const { register, handleSubmit, setValue, reset } = useForm<FormValues>({
    defaultValues: {
      gross_revenue: "", attendance: "", classification_status: "Complete"
    }
  })

  async function onSubmit(values: FormValues) {
    if (!values.event_date || !values.event_name) return
    setSaving(true)
    const payload = {
      event_date: values.event_date,
      event_name: values.event_name,
      tier1_category: values.tier1_category || null,
      tier2_subcategory: values.tier2_subcategory || null,
      promoter_name: values.promoter_name || null,
      artist_name: values.artist_name || null,
      gross_revenue: parseFloat(values.gross_revenue) || 0,
      attendance: parseInt(values.attendance) || 0,
      data_source: dataSource || null,
      classification_status: classStatus || "Complete",
    }
    try {
      const res = await fetch("/api/historical", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })
      if (res.ok) {
        setSaved(true)
        reset()
        setTier1("")
        setDataSource("")
        setClassStatus("Complete")
        setTimeout(() => setSaved(false), 2000)
      }
    } finally {
      setSaving(false)
    }
  }

  const inputCls = "bg-input border-border text-foreground placeholder:text-muted-foreground focus:ring-1 focus:ring-primary h-11"

  return (
    <div className="px-4 py-5">
      <div className="mb-5">
        <h1 className="text-xl font-bold text-foreground">Historical Data Entry</h1>
        <p className="text-sm text-muted-foreground mt-0.5">Enter data for past events</p>
      </div>

      <form onSubmit={handleSubmit(onSubmit)} className="space-y-4">

        <SectionHeader title="Event Details" />

        <FieldRow label="Event Date" required>
          <Input
            type="date"
            className={cn(inputCls, "appearance-none")}
            {...register("event_date", { required: true })}
          />
        </FieldRow>

        <FieldRow label="Event Name" required>
          <Input className={inputCls} placeholder="Event name" {...register("event_name", { required: true })} />
        </FieldRow>

        <FieldRow label="Tier 1 Category">
          <Select
            value={tier1}
            onValueChange={(v) => { setTier1(v); setValue("tier1_category", v) }}
          >
            <SelectTrigger className={cn(inputCls, "w-full")}>
              <SelectValue placeholder="Select category" />
            </SelectTrigger>
            <SelectContent className="bg-card border-border">
              {TIER1_CATEGORIES.map((cat) => (
                <SelectItem key={cat} value={cat} className="text-foreground hover:bg-muted focus:bg-muted">{cat}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </FieldRow>
        <input type="hidden" {...register("tier1_category")} />

        <FieldRow label="Tier 2 Subcategory" hint="optional">
          <Input className={inputCls} placeholder="Subcategory" {...register("tier2_subcategory")} />
        </FieldRow>

        <FieldRow label="Promoter Name" hint="optional">
          <Input className={inputCls} placeholder="Promoter name" {...register("promoter_name")} />
        </FieldRow>

        <FieldRow label="Artist Name" hint="optional">
          <Input className={inputCls} placeholder="Artist name" {...register("artist_name")} />
        </FieldRow>

        <SectionHeader title="Results" />

        <div className="grid grid-cols-2 gap-3">
          <FieldRow label="Gross Revenue ($)">
            <Input type="number" className={inputCls} placeholder="0" {...register("gross_revenue")} />
          </FieldRow>
          <FieldRow label="Attendance">
            <Input type="number" className={inputCls} placeholder="0" {...register("attendance")} />
          </FieldRow>
        </div>

        <SectionHeader title="Data Quality" />

        <FieldRow label="Data Source">
          <Select value={dataSource} onValueChange={(v) => { setDataSource(v); setValue("data_source", v) }}>
            <SelectTrigger className={cn(inputCls, "w-full")}>
              <SelectValue placeholder="Select source" />
            </SelectTrigger>
            <SelectContent className="bg-card border-border">
              {DATA_SOURCE_OPTIONS.map((s) => (
                <SelectItem key={s} value={s} className="text-foreground hover:bg-muted focus:bg-muted">{s}</SelectItem>
              ))}
            </SelectContent>
          </Select>
        </FieldRow>
        <input type="hidden" {...register("data_source")} />

        <FieldRow label="Classification Status">
          <div className="flex gap-2">
            {CLASSIFICATION_STATUS_OPTIONS.map((status) => (
              <button
                key={status}
                type="button"
                onClick={() => { setClassStatus(status); setValue("classification_status", status) }}
                className="flex-1 h-11 rounded-md border text-xs font-medium transition-colors"
                style={classStatus === status
                  ? { background: 'rgba(44,95,138,0.1)', color: '#2C5F8A', borderColor: 'rgba(44,95,138,0.4)' }
                  : { background: 'transparent', color: '#888888', borderColor: '#CCCCCC' }
                }
              >
                {status}
              </button>
            ))}
          </div>
        </FieldRow>
        <input type="hidden" {...register("classification_status")} />

        <div className="pt-4 pb-2">
          <Button
            type="submit"
            disabled={saving || saved}
            className="w-full h-12 text-base font-semibold bg-primary hover:bg-primary/90 text-primary-foreground"
          >
            {saved ? (
              <span className="flex items-center gap-2">
                <CheckCircle className="h-4 w-4" />
                Saved!
              </span>
            ) : saving ? "Saving..." : "Save Historical Record"}
          </Button>
        </div>

      </form>
    </div>
  )
}
