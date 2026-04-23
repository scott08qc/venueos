## VenueOS Event Record — Project State

### What this app is
Mobile-first business app for a live music/nightclub venue operator. Managers use it to record, track, and review every event the venue runs.

### Tech stack
- React + TypeScript (Vite) frontend
- FastAPI (Python) backend
- PostgreSQL via Neon (`DATABASE_URL` secret is set)
- SQLAlchemy + psycopg2-binary
- shadcn/ui components, Tailwind CSS, react-hook-form
- Dark theme with blue accents (CSS vars in `src/index.css`)

### Running
`bash start.sh` — Vite on `$APP_PORT`, FastAPI on `$APP_PORT + 100`

### Database
- Neon Postgres, `DATABASE_URL` secret is configured
- Tables: `events`, `night_of_actuals`, `post_event_reviews`, `historical_events`
- Schema is fully migrated with all current fields (ALTER TABLE IF NOT EXISTS runs on startup)
- **Claim URL** (72hr expiry from Apr 23 2026): https://neon.new/claim/019db7db-b9ec-7589-80ca-8d0eb0f18018

### Pages built (5)
1. **Event List** (`EventListPage.tsx`) — searchable/sortable dashboard, tappable cards
2. **Event Record** (`EventRecordPage.tsx`) — full pre-event form
3. **Night Of Actuals** (`NightOfActualsPage.tsx`) — per-check-in form with settlement calculator at Close
4. **Post-Event Review** (`PostEventReviewPage.tsx`) — post-event observations + split tracking
5. **Historical Data Entry** (`HistoricalDataPage.tsx`) — past event entry form

### Event Record form — deal structure fields (current)
- Deal type dropdown
- Bar Revenue: promoter %, basis dropdown (Gross/Net after COGS/Net after first threshold/Net after guarantee/Custom), conditional COGS %/threshold/guarantee fields
- Door Revenue: promoter %, basis dropdown, conditional threshold/guarantee fields
- Table Revenue: minimum spend, promoter % on overages, deposit collection method
- Artist Costs: landed fee, travel fee, cost responsibility dropdown, conditional split note
- Deal Notes: free-text at bottom of deal section
- Payment Schedule: deposit amount/date, balance due/date
- Revenue Projections: door/bar/table + auto-calculated total
- Notes

### Night Of Actuals — settlement section (Close only)
Visually distinct card. Sections:
- Gross Revenue (auto from form fields)
- Deductions (COGS/threshold manual; comps/voids/tax/tips auto-pulled)
- Net Sales (netBar/netDoor/netTable/total)
- Promoter Payout (manual bar/door/table/artist cost inputs → auto totals)
- Net to Venue + Effective Split % (auto-calculated, saved to DB)
- Benchmark effective split field (manual reference, placeholder points to future Benchmarks page)
- Settlement Notes

### Post-Event Review additions
- Projected effective split % (manual)
- Actual effective split % (auto-populated from Night Of Close record)
- Split variance display with red highlight if actual > projected by 5+ points
- Settlement notes reference (pre-filled from Night Of)

### Key file locations
- `routes.py` — all API routes + DB init + Pydantic models
- `src/types.ts` — all TypeScript types and constants
- `src/App.tsx` — routing/navigation state
- `src/components/Layout.tsx` — nav bar
- `src/components/EventRecordPage.tsx`
- `src/components/NightOfActualsPage.tsx`
- `src/components/PostEventReviewPage.tsx`
- `src/components/EventListPage.tsx`
- `src/components/HistoricalDataPage.tsx`

### Known issues / notes
- `channel_binding=require` stripped from DATABASE_URL (psycopg2 incompatibility) — handled in routes.py
- `pool_pre_ping=True, pool_recycle=300` set on SQLAlchemy engine

### Connector/data source pattern
- Always invoke `connectors-python:connector-neon` skill when touching DB connection code
- DATABASE_URL secret is already configured — do not re-provision a database
