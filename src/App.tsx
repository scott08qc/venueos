import { useState, useEffect, useCallback } from "react"
import type { Page, EventSummary, EventFull } from "./types"
import { Layout } from "./components/Layout"
import { EventListPage } from "./components/EventListPage"
import { EventRecordPage } from "./components/EventRecordPage"
import { NightOfActualsPage } from "./components/NightOfActualsPage"
import { PostEventReviewPage } from "./components/PostEventReviewPage"
import { HistoricalDataPage } from "./components/HistoricalDataPage"

function App() {
  const [page, setPage] = useState<Page>("list")
  const [events, setEvents] = useState<EventSummary[]>([])
  const [eventsLoading, setEventsLoading] = useState(true)
  const [editEvent, setEditEvent] = useState<EventFull | null>(null)
  const [preselectedEventId, setPreselectedEventId] = useState<number | null>(null)

  const loadEvents = useCallback(async () => {
    setEventsLoading(true)
    try {
      const res = await fetch("/api/events")
      if (res.ok) {
        const data = await res.json()
        setEvents(data)
      }
    } finally {
      setEventsLoading(false)
    }
  }, [])

  useEffect(() => {
    loadEvents()
  }, [loadEvents])

  function handleNavigate(newPage: Page) {
    if (newPage !== "event-record") {
      setEditEvent(null)
    }
    if (newPage !== "night-of-actuals" && newPage !== "post-event-review") {
      setPreselectedEventId(null)
    }
    setPage(newPage)
    // Scroll to top on page change
    window.scrollTo({ top: 0 })
  }

  async function handleSelectEvent(event: EventSummary) {
    // Load full event data then open edit form
    try {
      const res = await fetch(`/api/events/${event.id}`)
      if (res.ok) {
        const full: EventFull = await res.json()
        setEditEvent(full)
        setPage("event-record")
        window.scrollTo({ top: 0 })
      }
    } catch {
      // fallback: just open with summary
      setEditEvent(event as EventFull)
      setPage("event-record")
    }
  }

  function handleEventSaved() {
    loadEvents()
    setEditEvent(null)
    setPage("list")
  }

  return (
    <Layout currentPage={page} onNavigate={handleNavigate}>
      {page === "list" && (
        <EventListPage
          events={events}
          loading={eventsLoading}
          onRefresh={loadEvents}
          onNavigate={handleNavigate}
          onSelectEvent={handleSelectEvent}
        />
      )}
      {page === "event-record" && (
        <EventRecordPage
          editEvent={editEvent}
          onSaved={handleEventSaved}
        />
      )}
      {page === "night-of-actuals" && (
        <NightOfActualsPage
          events={events}
          preselectedEventId={preselectedEventId}
        />
      )}
      {page === "post-event-review" && (
        <PostEventReviewPage
          events={events}
          preselectedEventId={preselectedEventId}
        />
      )}
      {page === "historical" && (
        <HistoricalDataPage />
      )}
    </Layout>
  )
}

export default App
