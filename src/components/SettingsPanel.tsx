import { useState, useEffect } from "react"
import { X, Plus, GripVertical, Trash2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"

interface Props {
  open: boolean
  onClose: () => void
  onSaved: (times: string[]) => void
}

const inp = "bg-white border text-foreground placeholder:text-muted-foreground focus:ring-1 focus:ring-primary h-10 text-sm"

export function SettingsPanel({ open, onClose, onSaved }: Props) {
  const [times, setTimes] = useState<string[]>(["11 PM", "12 AM", "1 AM", "2 AM", "Close"])
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [newTime, setNewTime] = useState("")

  useEffect(() => {
    if (!open) return
    setLoading(true)
    fetch("/api/settings")
      .then(r => r.ok ? r.json() : null)
      .then(d => { if (d?.checkin_times) setTimes(d.checkin_times) })
      .finally(() => setLoading(false))
  }, [open])

  function updateTime(idx: number, val: string) {
    setTimes(prev => prev.map((t, i) => i === idx ? val : t))
  }

  function removeTime(idx: number) {
    setTimes(prev => prev.filter((_, i) => i !== idx))
  }

  function addTime() {
    const val = newTime.trim()
    if (!val) return
    // Insert before Close (always last)
    setTimes(prev => {
      const withoutClose = prev.filter(t => t !== "Close")
      return [...withoutClose, val, "Close"]
    })
    setNewTime("")
  }

  async function save() {
    // Ensure Close is always last
    const cleaned = times.filter(t => t.trim())
    const withoutClose = cleaned.filter(t => t !== "Close")
    const final = [...withoutClose, "Close"]
    setSaving(true)
    try {
      const res = await fetch("/api/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ checkin_times: final }),
      })
      if (res.ok) {
        setTimes(final)
        onSaved(final)
        onClose()
      }
    } finally { setSaving(false) }
  }

  if (!open) return null

  const editableTimes = times.filter(t => t !== "Close")

  return (
    <>
      {/* Backdrop */}
      <div className="fixed inset-0 z-50 bg-black/30" onClick={onClose} />

      {/* Panel */}
      <div className="fixed inset-y-0 right-0 z-50 w-full max-w-sm bg-white shadow-xl flex flex-col"
        style={{ borderLeft: '1px solid #CCCCCC' }}>

        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b" style={{ borderColor: '#EEEEEE' }}>
          <div>
            <h2 className="text-base font-semibold" style={{ color: '#1A1A1A' }}>Venue Settings</h2>
            <p className="text-xs mt-0.5" style={{ color: '#888888' }}>Customize your check-in schedule</p>
          </div>
          <button onClick={onClose} className="p-1.5 rounded-md hover:bg-gray-100 transition-colors">
            <X className="h-5 w-5" style={{ color: '#555555' }} />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-y-auto px-5 py-4 space-y-4">

          <div>
            <p className="text-[11px] font-semibold uppercase tracking-widest mb-3" style={{ color: '#2C5F8A' }}>
              Check-In Times
            </p>
            <p className="text-xs mb-4" style={{ color: '#888888' }}>
              These appear as options in Night Of Actuals. <strong>Close</strong> is always last and cannot be removed.
            </p>

            {loading ? (
              <div className="space-y-2">
                {[1,2,3,4].map(i => <div key={i} className="h-10 rounded-md animate-pulse" style={{ background: '#EEEEEE' }} />)}
              </div>
            ) : (
              <div className="space-y-2">
                {editableTimes.map((t, i) => (
                  <div key={i} className="flex items-center gap-2">
                    <GripVertical className="h-4 w-4 flex-shrink-0" style={{ color: '#CCCCCC' }} />
                    <Input
                      className={inp}
                      value={t}
                      onChange={e => updateTime(times.indexOf(t), e.target.value)}
                    />
                    <button
                      onClick={() => removeTime(times.indexOf(t))}
                      className="p-1.5 rounded-md hover:bg-red-50 transition-colors flex-shrink-0">
                      <Trash2 className="h-4 w-4" style={{ color: '#8B3A3A' }} />
                    </button>
                  </div>
                ))}

                {/* Close — locked */}
                <div className="flex items-center gap-2">
                  <GripVertical className="h-4 w-4 flex-shrink-0" style={{ color: '#EEEEEE' }} />
                  <div className="flex-1 h-10 flex items-center px-3 rounded-md border text-sm font-medium"
                    style={{ background: '#F5F5F5', borderColor: '#CCCCCC', color: '#888888' }}>
                    Close
                  </div>
                  <div className="w-8 flex-shrink-0" />
                </div>
              </div>
            )}
          </div>

          {/* Add new time */}
          <div className="pt-2">
            <p className="text-[11px] font-semibold uppercase tracking-widest mb-2" style={{ color: '#2C5F8A' }}>
              Add Time
            </p>
            <div className="flex gap-2">
              <Input
                className={inp + " flex-1"}
                placeholder='e.g. "10 PM" or "9 PM"'
                value={newTime}
                onChange={e => setNewTime(e.target.value)}
                onKeyDown={e => e.key === "Enter" && addTime()}
              />
              <Button type="button" onClick={addTime}
                className="h-10 px-3"
                style={{ background: '#2C5F8A', color: 'white' }}
                disabled={!newTime.trim()}>
                <Plus className="h-4 w-4" />
              </Button>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="px-5 py-4 border-t space-y-2" style={{ borderColor: '#EEEEEE' }}>
          <Button onClick={save} disabled={saving} className="w-full h-11 font-semibold"
            style={{ background: '#2C5F8A', color: 'white' }}>
            {saving ? "Saving…" : "Save Settings"}
          </Button>
          <Button onClick={onClose} variant="outline" className="w-full h-10 text-sm"
            style={{ borderColor: '#CCCCCC', color: '#555555' }}>
            Cancel
          </Button>
        </div>
      </div>
    </>
  )
}
