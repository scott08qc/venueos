import os
from datetime import datetime, timedelta
from typing import Optional
 
from fastapi import FastAPI, APIRouter, Request, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import create_engine, text
 
_raw_url = os.environ.get("DATABASE_URL", "")
DATABASE_URL = (
    _raw_url
    .replace("channel_binding=require&", "")
    .replace("&channel_binding=require", "")
    .replace("channel_binding=require", "")
) if _raw_url else None
 
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=300,
    connect_args={"sslmode": "require"},
) if DATABASE_URL else None
 
 
def init_db():
    if not engine:
        return
    with engine.connect() as conn:
 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                event_name TEXT NOT NULL,
                event_date DATE NOT NULL,
                day_of_week TEXT,
                tier1_category TEXT,
                tier2_subcategory TEXT,
                promoter_name TEXT,
                artist_name TEXT,
                artist_genre TEXT,
                expected_attendance INTEGER,
                venue_capacity INTEGER DEFAULT 800,
                deal_structure_type TEXT,
                door_split_venue NUMERIC,
                door_split_promoter NUMERIC,
                bar_split_venue NUMERIC,
                bar_split_promoter NUMERIC,
                table_minimum NUMERIC,
                artist_fee_landed NUMERIC,
                artist_fee_travel NUMERIC,
                deposit_amount NUMERIC,
                deposit_due_date DATE,
                balance_due NUMERIC,
                balance_due_date DATE,
                projected_door_revenue NUMERIC DEFAULT 0,
                projected_bar_revenue NUMERIC DEFAULT 0,
                projected_table_revenue NUMERIC DEFAULT 0,
                notes TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
 
        for col, dtype in [
            ("bar_split_basis", "TEXT"),
            ("bar_cogs_percentage", "NUMERIC"),
            ("bar_threshold_amount", "NUMERIC"),
            ("bar_guarantee_amount", "NUMERIC"),
            ("door_split_basis", "TEXT"),
            ("door_threshold_amount", "NUMERIC"),
            ("door_guarantee_amount", "NUMERIC"),
            ("table_split_promoter_overage", "NUMERIC"),
            ("deposit_collection_method", "TEXT"),
            ("artist_cost_responsibility", "TEXT"),
            ("artist_cost_split_note", "TEXT"),
            ("deal_notes", "TEXT"),
            ("net_revenue_promoter_pct", "NUMERIC"),
            ("net_revenue_venue_pct", "NUMERIC"),
            ("net_revenue_basis", "TEXT"),
            ("table_split_promoter", "NUMERIC"),
            ("table_split_basis", "TEXT"),
            ("artist_cost_split_method", "TEXT"),
            ("artist_venue_dollar", "NUMERIC"),
            ("artist_venue_pct", "NUMERIC"),
            ("artist_promoter_dollar", "NUMERIC"),
            ("house_fee_type", "TEXT"),
            ("house_fee_amount", "NUMERIC DEFAULT 0"),
            ("house_fee_percentage", "NUMERIC"),
            ("production_cost_total", "NUMERIC DEFAULT 0"),
            ("production_cost_responsibility", "TEXT"),
            ("production_promoter_pct", "NUMERIC"),
            ("security_cost_total", "NUMERIC DEFAULT 0"),
            ("security_cost_responsibility", "TEXT"),
            ("security_promoter_pct", "NUMERIC"),
            ("marketing_cost_total", "NUMERIC DEFAULT 0"),
            ("marketing_cost_responsibility", "TEXT"),
            ("marketing_promoter_pct", "NUMERIC"),
            # Revel API matching fields
            ("doors_open_time", "TIME"),
            ("event_close_time", "TIME"),
        ]:
            conn.execute(text(f"ALTER TABLE events ADD COLUMN IF NOT EXISTS {col} {dtype}"))
 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS night_of_actuals (
                id SERIAL PRIMARY KEY,
                event_id INTEGER REFERENCES events(id) ON DELETE CASCADE,
                time_of_entry TEXT NOT NULL,
                total_bar_sales NUMERIC DEFAULT 0,
                liquor_sales NUMERIC DEFAULT 0,
                beer_wine_sales NUMERIC DEFAULT 0,
                non_alcoholic_food_sales NUMERIC DEFAULT 0,
                table_bottle_service NUMERIC DEFAULT 0,
                comps_total NUMERIC DEFAULT 0,
                voids NUMERIC DEFAULT 0,
                tax_collected NUMERIC DEFAULT 0,
                tips NUMERIC DEFAULT 0,
                door_revenue_cash NUMERIC DEFAULT 0,
                door_revenue_card NUMERIC DEFAULT 0,
                total_headcount INTEGER DEFAULT 0,
                ticket_scan_count INTEGER DEFAULT 0,
                walk_up_count INTEGER DEFAULT 0,
                comp_admissions INTEGER DEFAULT 0,
                tables_active INTEGER DEFAULT 0,
                incident_description TEXT,
                incident_department TEXT,
                notes TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
 
        for col, dtype in [
            ("bar_cogs_deduction", "NUMERIC DEFAULT 0"),
            ("bar_threshold_retained", "NUMERIC DEFAULT 0"),
            ("door_threshold_retained", "NUMERIC DEFAULT 0"),
            ("charge_backs", "NUMERIC DEFAULT 0"),
            ("promoter_bar_payout", "NUMERIC DEFAULT 0"),
            ("promoter_door_payout", "NUMERIC DEFAULT 0"),
            ("promoter_table_payout", "NUMERIC DEFAULT 0"),
            ("artist_cost_paid_by_venue", "NUMERIC DEFAULT 0"),
            ("effective_split_percentage", "NUMERIC"),
            ("settlement_notes", "TEXT"),
            ("benchmark_effective_split", "TEXT"),
            ("house_fee_deduction", "NUMERIC DEFAULT 0"),
        ]:
            conn.execute(text(f"ALTER TABLE night_of_actuals ADD COLUMN IF NOT EXISTS {col} {dtype}"))
 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS post_event_reviews (
                id SERIAL PRIMARY KEY,
                event_id INTEGER REFERENCES events(id) ON DELETE CASCADE,
                actual_attendance INTEGER,
                actual_door_revenue NUMERIC DEFAULT 0,
                actual_bar_revenue NUMERIC DEFAULT 0,
                actual_table_revenue NUMERIC DEFAULT 0,
                artist_cost_actual NUMERIC DEFAULT 0,
                staffing_cost_actual NUMERIC DEFAULT 0,
                crowd_demographic_observations TEXT,
                customer_service_observations TEXT,
                operational_breakdowns TEXT,
                what_to_replicate TEXT,
                what_to_change TEXT,
                promoter_performance_notes TEXT,
                inventory_observations TEXT,
                staffing_observations TEXT,
                promoter_artwork_on_time TEXT DEFAULT 'N/A',
                promoter_social_posting TEXT DEFAULT 'N/A',
                promoter_attendance_vs_projection TEXT DEFAULT 'N/A',
                promoter_role_boundaries TEXT DEFAULT 'N/A',
                review_status TEXT DEFAULT 'Draft',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
 
        for col, dtype in [
            ("projected_effective_split", "NUMERIC"),
            ("actual_effective_split", "NUMERIC"),
            ("settlement_notes_reference", "TEXT"),
            ("net_revenue_actual", "NUMERIC DEFAULT 0"),
            ("spend_per_head_actual", "NUMERIC"),
        ]:
            conn.execute(text(f"ALTER TABLE post_event_reviews ADD COLUMN IF NOT EXISTS {col} {dtype}"))
 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS historical_events (
                id SERIAL PRIMARY KEY,
                event_date DATE NOT NULL,
                event_name TEXT NOT NULL,
                tier1_category TEXT,
                tier2_subcategory TEXT,
                promoter_name TEXT,
                artist_name TEXT,
                gross_revenue NUMERIC DEFAULT 0,
                attendance INTEGER DEFAULT 0,
                data_source TEXT,
                classification_status TEXT DEFAULT 'Complete',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
 
        for col, dtype in [
            ("bar_revenue", "NUMERIC DEFAULT 0"),
            ("door_revenue", "NUMERIC DEFAULT 0"),
            ("table_revenue", "NUMERIC DEFAULT 0"),
            ("deal_structure_type", "TEXT"),
            ("effective_split_pct", "NUMERIC"),
            ("expected_attendance", "INTEGER"),
        ]:
            conn.execute(text(f"ALTER TABLE historical_events ADD COLUMN IF NOT EXISTS {col} {dtype}"))
 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS event_item_sales (
                id SERIAL PRIMARY KEY,
                event_id INTEGER REFERENCES events(id) ON DELETE CASCADE,
                time_of_entry TEXT,
                item_name TEXT NOT NULL,
                item_category TEXT,
                item_subcategory TEXT,
                quantity_sold INTEGER DEFAULT 0,
                unit_price NUMERIC DEFAULT 0,
                total_revenue NUMERIC DEFAULT 0,
                cost_per_unit NUMERIC,
                total_cost NUMERIC,
                gross_margin NUMERIC,
                data_source TEXT DEFAULT 'Revel export',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
 
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS venue_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """))
 
        conn.execute(text("""
            INSERT INTO venue_settings (key, value)
            VALUES ('checkin_times', '["11 PM", "12 AM", "1 AM", "2 AM", "Close"]')
            ON CONFLICT (key) DO NOTHING
        """))
 
        conn.commit()
 
 
# ── Pydantic models ───────────────────────────────────────────────────────────
 
class EventCreate(BaseModel):
    event_name: str
    event_date: str
    tier1_category: Optional[str] = None
    tier2_subcategory: Optional[str] = None
    promoter_name: Optional[str] = None
    artist_name: Optional[str] = None
    artist_genre: Optional[str] = None
    expected_attendance: Optional[int] = None
    venue_capacity: Optional[int] = 800
    deal_structure_type: Optional[str] = None
    door_split_venue: Optional[float] = None
    door_split_promoter: Optional[float] = None
    bar_split_venue: Optional[float] = None
    bar_split_promoter: Optional[float] = None
    bar_split_basis: Optional[str] = None
    bar_cogs_percentage: Optional[float] = None
    bar_threshold_amount: Optional[float] = None
    bar_guarantee_amount: Optional[float] = None
    door_split_basis: Optional[str] = None
    door_threshold_amount: Optional[float] = None
    door_guarantee_amount: Optional[float] = None
    table_minimum: Optional[float] = None
    table_split_promoter_overage: Optional[float] = None
    deposit_collection_method: Optional[str] = None
    artist_fee_landed: Optional[float] = None
    artist_fee_travel: Optional[float] = None
    artist_cost_responsibility: Optional[str] = None
    artist_cost_split_note: Optional[str] = None
    deal_notes: Optional[str] = None
    net_revenue_promoter_pct: Optional[float] = None
    net_revenue_venue_pct: Optional[float] = None
    net_revenue_basis: Optional[str] = None
    table_split_promoter: Optional[float] = None
    table_split_basis: Optional[str] = None
    artist_cost_split_method: Optional[str] = None
    artist_venue_dollar: Optional[float] = None
    artist_venue_pct: Optional[float] = None
    artist_promoter_dollar: Optional[float] = None
    deposit_amount: Optional[float] = None
    deposit_due_date: Optional[str] = None
    balance_due: Optional[float] = None
    balance_due_date: Optional[str] = None
    projected_door_revenue: Optional[float] = 0
    projected_bar_revenue: Optional[float] = 0
    projected_table_revenue: Optional[float] = 0
    notes: Optional[str] = None
    # House fee
    house_fee_type: Optional[str] = None
    house_fee_amount: Optional[float] = None
    house_fee_percentage: Optional[float] = None
    # Variable costs
    production_cost_total: Optional[float] = None
    production_cost_responsibility: Optional[str] = None
    production_promoter_pct: Optional[float] = None
    security_cost_total: Optional[float] = None
    security_cost_responsibility: Optional[str] = None
    security_promoter_pct: Optional[float] = None
    marketing_cost_total: Optional[float] = None
    marketing_cost_responsibility: Optional[str] = None
    marketing_promoter_pct: Optional[float] = None
    # Revel API matching
    doors_open_time: Optional[str] = None
    event_close_time: Optional[str] = None
 
 
class NightOfActualsCreate(BaseModel):
    event_id: int
    time_of_entry: str
    # Bar revenue — master field plus breakdowns
    total_bar_sales: Optional[float] = 0
    liquor_sales: Optional[float] = 0
    beer_wine_sales: Optional[float] = 0
    non_alcoholic_food_sales: Optional[float] = 0
    # Table — completely separate from bar
    table_bottle_service: Optional[float] = 0
    tables_active: Optional[int] = 0
    # Deductions
    comps_total: Optional[float] = 0
    voids: Optional[float] = 0
    tax_collected: Optional[float] = 0
    tips: Optional[float] = 0
    # Door
    door_revenue_cash: Optional[float] = 0
    door_revenue_card: Optional[float] = 0
    ticket_scan_count: Optional[int] = 0
    walk_up_count: Optional[int] = 0
    comp_admissions: Optional[int] = 0
    total_headcount: Optional[int] = 0
    # Incidents and notes
    incident_description: Optional[str] = None
    incident_department: Optional[str] = None
    notes: Optional[str] = None
    # Settlement (Close only)
    bar_cogs_deduction: Optional[float] = 0
    bar_threshold_retained: Optional[float] = 0
    door_threshold_retained: Optional[float] = 0
    house_fee_deduction: Optional[float] = 0
    charge_backs: Optional[float] = 0
    promoter_bar_payout: Optional[float] = 0
    promoter_door_payout: Optional[float] = 0
    promoter_table_payout: Optional[float] = 0
    artist_cost_paid_by_venue: Optional[float] = 0
    effective_split_percentage: Optional[float] = None
    settlement_notes: Optional[str] = None
    benchmark_effective_split: Optional[str] = None
 
 
class PostEventReviewCreate(BaseModel):
    event_id: int
    actual_attendance: Optional[int] = None
    actual_door_revenue: Optional[float] = 0
    actual_bar_revenue: Optional[float] = 0
    actual_table_revenue: Optional[float] = 0
    artist_cost_actual: Optional[float] = 0
    staffing_cost_actual: Optional[float] = 0
    crowd_demographic_observations: Optional[str] = None
    customer_service_observations: Optional[str] = None
    operational_breakdowns: Optional[str] = None
    what_to_replicate: Optional[str] = None
    what_to_change: Optional[str] = None
    promoter_performance_notes: Optional[str] = None
    inventory_observations: Optional[str] = None
    staffing_observations: Optional[str] = None
    promoter_artwork_on_time: Optional[str] = "N/A"
    promoter_social_posting: Optional[str] = "N/A"
    promoter_attendance_vs_projection: Optional[str] = "N/A"
    promoter_role_boundaries: Optional[str] = "N/A"
    review_status: Optional[str] = "Draft"
    projected_effective_split: Optional[float] = None
    actual_effective_split: Optional[float] = None
    settlement_notes_reference: Optional[str] = None
    net_revenue_actual: Optional[float] = 0
    spend_per_head_actual: Optional[float] = None
 
 
class HistoricalEventCreate(BaseModel):
    event_date: str
    event_name: str
    tier1_category: Optional[str] = None
    tier2_subcategory: Optional[str] = None
    promoter_name: Optional[str] = None
    artist_name: Optional[str] = None
    gross_revenue: Optional[float] = 0
    bar_revenue: Optional[float] = 0
    door_revenue: Optional[float] = 0
    table_revenue: Optional[float] = 0
    attendance: Optional[int] = 0
    expected_attendance: Optional[int] = None
    deal_structure_type: Optional[str] = None
    effective_split_pct: Optional[float] = None
    data_source: Optional[str] = None
    classification_status: Optional[str] = "Complete"
 
 
# ── App factory ───────────────────────────────────────────────────────────────
 
def create_app(static_dir: str) -> FastAPI:
    api = APIRouter()
 
    @api.get("/health")
    def health():
        return {"ok": True}
 
    # ── Events ────────────────────────────────────────────────────────────────
 
    @api.get("/events")
    def list_events():
        if not engine:
            return []
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT e.id, e.event_name, e.event_date, e.day_of_week,
                       e.tier1_category, e.promoter_name,
                       e.projected_door_revenue, e.projected_bar_revenue, e.projected_table_revenue,
                       COALESCE(r.review_status, 'No Review') AS review_status
                FROM events e
                LEFT JOIN post_event_reviews r ON r.event_id = e.id
                ORDER BY e.event_date DESC
            """)).fetchall()
            result = []
            for row in rows:
                d = dict(row._mapping)
                d["event_date"] = str(d["event_date"])
                d["projected_total_revenue"] = (
                    float(d.get("projected_door_revenue") or 0) +
                    float(d.get("projected_bar_revenue") or 0) +
                    float(d.get("projected_table_revenue") or 0)
                )
                result.append(d)
            return result
 
    @api.get("/events/{event_id}")
    def get_event(event_id: int):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT id, event_name, event_date, day_of_week, tier1_category, tier2_subcategory,
                       promoter_name, artist_name, artist_genre, expected_attendance, venue_capacity,
                       deal_structure_type, door_split_venue, door_split_promoter,
                       bar_split_venue, bar_split_promoter,
                       bar_split_basis, bar_cogs_percentage, bar_threshold_amount, bar_guarantee_amount,
                       door_split_basis, door_threshold_amount, door_guarantee_amount,
                       table_minimum, table_split_promoter_overage, deposit_collection_method,
                       artist_fee_landed, artist_fee_travel,
                       artist_cost_responsibility, artist_cost_split_note, deal_notes,
                       net_revenue_promoter_pct, net_revenue_venue_pct, net_revenue_basis,
                       table_split_promoter, table_split_basis,
                       artist_cost_split_method, artist_venue_dollar, artist_venue_pct, artist_promoter_dollar,
                       deposit_amount, deposit_due_date, balance_due, balance_due_date,
                       projected_door_revenue, projected_bar_revenue, projected_table_revenue,
                       house_fee_type, house_fee_amount, house_fee_percentage,
                       production_cost_total, production_cost_responsibility, production_promoter_pct,
                       security_cost_total, security_cost_responsibility, security_promoter_pct,
                       marketing_cost_total, marketing_cost_responsibility, marketing_promoter_pct,
                       doors_open_time, event_close_time,
                       notes, created_at, updated_at
                FROM events WHERE id = :id
            """), {"id": event_id}).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Event not found")
            d = dict(row._mapping)
            for f in ["event_date", "deposit_due_date", "balance_due_date", "created_at", "updated_at"]:
                if d.get(f):
                    d[f] = str(d[f])
            for f in ["doors_open_time", "event_close_time"]:
                if d.get(f):
                    d[f] = str(d[f])
            return d
 
    @api.post("/events")
    def create_event(data: EventCreate):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        event_date = datetime.strptime(data.event_date, "%Y-%m-%d").date()
        day_of_week = event_date.strftime("%A")
        params = data.model_dump()
        params["day_of_week"] = day_of_week
        params["deposit_due_date"] = data.deposit_due_date or None
        params["balance_due_date"] = data.balance_due_date or None
        params["doors_open_time"] = data.doors_open_time or None
        params["event_close_time"] = data.event_close_time or None
        with engine.connect() as conn:
            row = conn.execute(text("""
                INSERT INTO events (
                    event_name, event_date, day_of_week, tier1_category, tier2_subcategory,
                    promoter_name, artist_name, artist_genre, expected_attendance, venue_capacity,
                    deal_structure_type, door_split_venue, door_split_promoter,
                    bar_split_venue, bar_split_promoter,
                    bar_split_basis, bar_cogs_percentage, bar_threshold_amount, bar_guarantee_amount,
                    door_split_basis, door_threshold_amount, door_guarantee_amount,
                    table_minimum, table_split_promoter_overage, deposit_collection_method,
                    artist_fee_landed, artist_fee_travel,
                    artist_cost_responsibility, artist_cost_split_note, deal_notes,
                    net_revenue_promoter_pct, net_revenue_venue_pct, net_revenue_basis,
                    table_split_promoter, table_split_basis,
                    artist_cost_split_method, artist_venue_dollar, artist_venue_pct, artist_promoter_dollar,
                    deposit_amount, deposit_due_date, balance_due, balance_due_date,
                    projected_door_revenue, projected_bar_revenue, projected_table_revenue,
                    house_fee_type, house_fee_amount, house_fee_percentage,
                    production_cost_total, production_cost_responsibility, production_promoter_pct,
                    security_cost_total, security_cost_responsibility, security_promoter_pct,
                    marketing_cost_total, marketing_cost_responsibility, marketing_promoter_pct,
                    doors_open_time, event_close_time, notes
                ) VALUES (
                    :event_name, :event_date, :day_of_week, :tier1_category, :tier2_subcategory,
                    :promoter_name, :artist_name, :artist_genre, :expected_attendance, :venue_capacity,
                    :deal_structure_type, :door_split_venue, :door_split_promoter,
                    :bar_split_venue, :bar_split_promoter,
                    :bar_split_basis, :bar_cogs_percentage, :bar_threshold_amount, :bar_guarantee_amount,
                    :door_split_basis, :door_threshold_amount, :door_guarantee_amount,
                    :table_minimum, :table_split_promoter_overage, :deposit_collection_method,
                    :artist_fee_landed, :artist_fee_travel,
                    :artist_cost_responsibility, :artist_cost_split_note, :deal_notes,
                    :net_revenue_promoter_pct, :net_revenue_venue_pct, :net_revenue_basis,
                    :table_split_promoter, :table_split_basis,
                    :artist_cost_split_method, :artist_venue_dollar, :artist_venue_pct, :artist_promoter_dollar,
                    :deposit_amount, :deposit_due_date, :balance_due, :balance_due_date,
                    :projected_door_revenue, :projected_bar_revenue, :projected_table_revenue,
                    :house_fee_type, :house_fee_amount, :house_fee_percentage,
                    :production_cost_total, :production_cost_responsibility, :production_promoter_pct,
                    :security_cost_total, :security_cost_responsibility, :security_promoter_pct,
                    :marketing_cost_total, :marketing_cost_responsibility, :marketing_promoter_pct,
                    :doors_open_time, :event_close_time, :notes
                ) RETURNING id
            """), params)
            conn.commit()
            return {"id": row.fetchone()[0], "day_of_week": day_of_week}
 
    @api.put("/events/{event_id}")
    def update_event(event_id: int, data: EventCreate):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        event_date = datetime.strptime(data.event_date, "%Y-%m-%d").date()
        day_of_week = event_date.strftime("%A")
        params = data.model_dump()
        params["id"] = event_id
        params["day_of_week"] = day_of_week
        params["deposit_due_date"] = data.deposit_due_date or None
        params["balance_due_date"] = data.balance_due_date or None
        params["doors_open_time"] = data.doors_open_time or None
        params["event_close_time"] = data.event_close_time or None
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE events SET
                    event_name=:event_name, event_date=:event_date, day_of_week=:day_of_week,
                    tier1_category=:tier1_category, tier2_subcategory=:tier2_subcategory,
                    promoter_name=:promoter_name, artist_name=:artist_name, artist_genre=:artist_genre,
                    expected_attendance=:expected_attendance, venue_capacity=:venue_capacity,
                    deal_structure_type=:deal_structure_type,
                    door_split_venue=:door_split_venue, door_split_promoter=:door_split_promoter,
                    bar_split_venue=:bar_split_venue, bar_split_promoter=:bar_split_promoter,
                    bar_split_basis=:bar_split_basis, bar_cogs_percentage=:bar_cogs_percentage,
                    bar_threshold_amount=:bar_threshold_amount, bar_guarantee_amount=:bar_guarantee_amount,
                    door_split_basis=:door_split_basis, door_threshold_amount=:door_threshold_amount,
                    door_guarantee_amount=:door_guarantee_amount,
                    table_minimum=:table_minimum, table_split_promoter_overage=:table_split_promoter_overage,
                    deposit_collection_method=:deposit_collection_method,
                    artist_fee_landed=:artist_fee_landed, artist_fee_travel=:artist_fee_travel,
                    artist_cost_responsibility=:artist_cost_responsibility,
                    artist_cost_split_note=:artist_cost_split_note, deal_notes=:deal_notes,
                    net_revenue_promoter_pct=:net_revenue_promoter_pct,
                    net_revenue_venue_pct=:net_revenue_venue_pct,
                    net_revenue_basis=:net_revenue_basis,
                    table_split_promoter=:table_split_promoter,
                    table_split_basis=:table_split_basis,
                    artist_cost_split_method=:artist_cost_split_method,
                    artist_venue_dollar=:artist_venue_dollar,
                    artist_venue_pct=:artist_venue_pct,
                    artist_promoter_dollar=:artist_promoter_dollar,
                    deposit_amount=:deposit_amount, deposit_due_date=:deposit_due_date,
                    balance_due=:balance_due, balance_due_date=:balance_due_date,
                    projected_door_revenue=:projected_door_revenue,
                    projected_bar_revenue=:projected_bar_revenue,
                    projected_table_revenue=:projected_table_revenue,
                    house_fee_type=:house_fee_type, house_fee_amount=:house_fee_amount,
                    house_fee_percentage=:house_fee_percentage,
                    production_cost_total=:production_cost_total,
                    production_cost_responsibility=:production_cost_responsibility,
                    production_promoter_pct=:production_promoter_pct,
                    security_cost_total=:security_cost_total,
                    security_cost_responsibility=:security_cost_responsibility,
                    security_promoter_pct=:security_promoter_pct,
                    marketing_cost_total=:marketing_cost_total,
                    marketing_cost_responsibility=:marketing_cost_responsibility,
                    marketing_promoter_pct=:marketing_promoter_pct,
                    doors_open_time=:doors_open_time,
                    event_close_time=:event_close_time,
                    notes=:notes, updated_at=NOW()
                WHERE id=:id
            """), params)
            conn.commit()
            return {"ok": True, "day_of_week": day_of_week}
 
    # ── Night of Actuals ──────────────────────────────────────────────────────
 
    @api.get("/actuals/{event_id}")
    def get_actuals(event_id: int):
        if not engine:
            return []
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, event_id, time_of_entry,
                       total_bar_sales, liquor_sales, beer_wine_sales, non_alcoholic_food_sales,
                       table_bottle_service, tables_active,
                       comps_total, voids, tax_collected, tips,
                       door_revenue_cash, door_revenue_card,
                       ticket_scan_count, walk_up_count, comp_admissions, total_headcount,
                       incident_description, incident_department, notes,
                       bar_cogs_deduction, bar_threshold_retained, door_threshold_retained,
                       house_fee_deduction, charge_backs,
                       promoter_bar_payout, promoter_door_payout,
                       promoter_table_payout, artist_cost_paid_by_venue,
                       effective_split_percentage, settlement_notes, benchmark_effective_split,
                       created_at, updated_at
                FROM night_of_actuals WHERE event_id = :eid ORDER BY created_at
            """), {"eid": event_id}).fetchall()
            return [dict(r._mapping) for r in rows]
 
    @api.post("/actuals")
    def create_actuals(data: NightOfActualsCreate):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        params = data.model_dump()
        with engine.connect() as conn:
            row = conn.execute(text("""
                INSERT INTO night_of_actuals (
                    event_id, time_of_entry,
                    total_bar_sales, liquor_sales, beer_wine_sales, non_alcoholic_food_sales,
                    table_bottle_service, tables_active,
                    comps_total, voids, tax_collected, tips,
                    door_revenue_cash, door_revenue_card,
                    ticket_scan_count, walk_up_count, comp_admissions, total_headcount,
                    incident_description, incident_department, notes,
                    bar_cogs_deduction, bar_threshold_retained, door_threshold_retained,
                    house_fee_deduction, charge_backs,
                    promoter_bar_payout, promoter_door_payout,
                    promoter_table_payout, artist_cost_paid_by_venue,
                    effective_split_percentage, settlement_notes, benchmark_effective_split
                ) VALUES (
                    :event_id, :time_of_entry,
                    :total_bar_sales, :liquor_sales, :beer_wine_sales, :non_alcoholic_food_sales,
                    :table_bottle_service, :tables_active,
                    :comps_total, :voids, :tax_collected, :tips,
                    :door_revenue_cash, :door_revenue_card,
                    :ticket_scan_count, :walk_up_count, :comp_admissions, :total_headcount,
                    :incident_description, :incident_department, :notes,
                    :bar_cogs_deduction, :bar_threshold_retained, :door_threshold_retained,
                    :house_fee_deduction, :charge_backs,
                    :promoter_bar_payout, :promoter_door_payout,
                    :promoter_table_payout, :artist_cost_paid_by_venue,
                    :effective_split_percentage, :settlement_notes, :benchmark_effective_split
                ) RETURNING id
            """), params)
            conn.commit()
            return {"id": row.fetchone()[0]}
 
    @api.put("/actuals/{actual_id}")
    def update_actuals(actual_id: int, data: NightOfActualsCreate):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        params = data.model_dump()
        params["id"] = actual_id
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE night_of_actuals SET
                    time_of_entry=:time_of_entry,
                    total_bar_sales=:total_bar_sales, liquor_sales=:liquor_sales,
                    beer_wine_sales=:beer_wine_sales, non_alcoholic_food_sales=:non_alcoholic_food_sales,
                    table_bottle_service=:table_bottle_service, tables_active=:tables_active,
                    comps_total=:comps_total, voids=:voids,
                    tax_collected=:tax_collected, tips=:tips,
                    door_revenue_cash=:door_revenue_cash, door_revenue_card=:door_revenue_card,
                    ticket_scan_count=:ticket_scan_count, walk_up_count=:walk_up_count,
                    comp_admissions=:comp_admissions, total_headcount=:total_headcount,
                    incident_description=:incident_description,
                    incident_department=:incident_department, notes=:notes,
                    bar_cogs_deduction=:bar_cogs_deduction,
                    bar_threshold_retained=:bar_threshold_retained,
                    door_threshold_retained=:door_threshold_retained,
                    house_fee_deduction=:house_fee_deduction,
                    charge_backs=:charge_backs,
                    promoter_bar_payout=:promoter_bar_payout,
                    promoter_door_payout=:promoter_door_payout,
                    promoter_table_payout=:promoter_table_payout,
                    artist_cost_paid_by_venue=:artist_cost_paid_by_venue,
                    effective_split_percentage=:effective_split_percentage,
                    settlement_notes=:settlement_notes,
                    benchmark_effective_split=:benchmark_effective_split,
                    updated_at=NOW()
                WHERE id=:id
            """), params)
            conn.commit()
            return {"ok": True}
 
    # ── Post-Event Reviews ────────────────────────────────────────────────────
 
    @api.get("/reviews/{event_id}")
    def get_review(event_id: int):
        if not engine:
            return None
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT id, event_id, actual_attendance,
                       actual_door_revenue, actual_bar_revenue, actual_table_revenue,
                       artist_cost_actual, staffing_cost_actual,
                       crowd_demographic_observations, customer_service_observations,
                       operational_breakdowns, what_to_replicate, what_to_change,
                       promoter_performance_notes, inventory_observations, staffing_observations,
                       promoter_artwork_on_time, promoter_social_posting,
                       promoter_attendance_vs_projection, promoter_role_boundaries,
                       review_status,
                       projected_effective_split, actual_effective_split,
                       settlement_notes_reference,
                       net_revenue_actual, spend_per_head_actual,
                       created_at, updated_at
                FROM post_event_reviews WHERE event_id = :eid ORDER BY id DESC LIMIT 1
            """), {"eid": event_id}).fetchone()
            if not row:
                return None
            return dict(row._mapping)
 
    @api.post("/reviews")
    def create_review(data: PostEventReviewCreate):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        params = data.model_dump()
        with engine.connect() as conn:
            row = conn.execute(text("""
                INSERT INTO post_event_reviews (
                    event_id, actual_attendance, actual_door_revenue, actual_bar_revenue,
                    actual_table_revenue, artist_cost_actual, staffing_cost_actual,
                    crowd_demographic_observations, customer_service_observations,
                    operational_breakdowns, what_to_replicate, what_to_change,
                    promoter_performance_notes, inventory_observations, staffing_observations,
                    promoter_artwork_on_time, promoter_social_posting,
                    promoter_attendance_vs_projection, promoter_role_boundaries, review_status,
                    projected_effective_split, actual_effective_split, settlement_notes_reference,
                    net_revenue_actual, spend_per_head_actual
                ) VALUES (
                    :event_id, :actual_attendance, :actual_door_revenue, :actual_bar_revenue,
                    :actual_table_revenue, :artist_cost_actual, :staffing_cost_actual,
                    :crowd_demographic_observations, :customer_service_observations,
                    :operational_breakdowns, :what_to_replicate, :what_to_change,
                    :promoter_performance_notes, :inventory_observations, :staffing_observations,
                    :promoter_artwork_on_time, :promoter_social_posting,
                    :promoter_attendance_vs_projection, :promoter_role_boundaries, :review_status,
                    :projected_effective_split, :actual_effective_split, :settlement_notes_reference,
                    :net_revenue_actual, :spend_per_head_actual
                ) RETURNING id
            """), params)
            conn.commit()
            return {"id": row.fetchone()[0]}
 
    @api.put("/reviews/{review_id}")
    def update_review(review_id: int, data: PostEventReviewCreate):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        params = data.model_dump()
        params["id"] = review_id
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE post_event_reviews SET
                    actual_attendance=:actual_attendance,
                    actual_door_revenue=:actual_door_revenue, actual_bar_revenue=:actual_bar_revenue,
                    actual_table_revenue=:actual_table_revenue, artist_cost_actual=:artist_cost_actual,
                    staffing_cost_actual=:staffing_cost_actual,
                    crowd_demographic_observations=:crowd_demographic_observations,
                    customer_service_observations=:customer_service_observations,
                    operational_breakdowns=:operational_breakdowns,
                    what_to_replicate=:what_to_replicate, what_to_change=:what_to_change,
                    promoter_performance_notes=:promoter_performance_notes,
                    inventory_observations=:inventory_observations,
                    staffing_observations=:staffing_observations,
                    promoter_artwork_on_time=:promoter_artwork_on_time,
                    promoter_social_posting=:promoter_social_posting,
                    promoter_attendance_vs_projection=:promoter_attendance_vs_projection,
                    promoter_role_boundaries=:promoter_role_boundaries,
                    review_status=:review_status,
                    projected_effective_split=:projected_effective_split,
                    actual_effective_split=:actual_effective_split,
                    settlement_notes_reference=:settlement_notes_reference,
                    net_revenue_actual=:net_revenue_actual,
                    spend_per_head_actual=:spend_per_head_actual,
                    updated_at=NOW()
                WHERE id=:id
            """), params)
            conn.commit()
            return {"ok": True}
 
    # ── Historical Events ─────────────────────────────────────────────────────
 
    @api.get("/historical")
    def list_historical():
        if not engine:
            return []
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT * FROM historical_events ORDER BY event_date DESC"
            )).fetchall()
            result = []
            for r in rows:
                d = dict(r._mapping)
                d["event_date"] = str(d["event_date"])
                result.append(d)
            return result
 
    @api.post("/historical")
    def create_historical(data: HistoricalEventCreate):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            row = conn.execute(text("""
                INSERT INTO historical_events (
                    event_date, event_name, tier1_category, tier2_subcategory,
                    promoter_name, artist_name, gross_revenue, bar_revenue, door_revenue,
                    table_revenue, attendance, expected_attendance, deal_structure_type,
                    effective_split_pct, data_source, classification_status
                ) VALUES (
                    :event_date, :event_name, :tier1_category, :tier2_subcategory,
                    :promoter_name, :artist_name, :gross_revenue, :bar_revenue, :door_revenue,
                    :table_revenue, :attendance, :expected_attendance, :deal_structure_type,
                    :effective_split_pct, :data_source, :classification_status
                ) RETURNING id
            """), data.model_dump())
            conn.commit()
            return {"id": row.fetchone()[0]}
 
    # ── Item Sales (Revel integration target) ─────────────────────────────────
 
    @api.get("/item-sales/{event_id}")
    def get_item_sales(event_id: int):
        if not engine:
            return []
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT * FROM event_item_sales
                WHERE event_id = :eid
                ORDER BY item_category, total_revenue DESC
            """), {"eid": event_id}).fetchall()
            return [dict(r._mapping) for r in rows]
 
    @api.post("/item-sales")
    def create_item_sale(data: dict):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            row = conn.execute(text("""
                INSERT INTO event_item_sales (
                    event_id, time_of_entry, item_name, item_category, item_subcategory,
                    quantity_sold, unit_price, total_revenue, cost_per_unit, total_cost,
                    gross_margin, data_source
                ) VALUES (
                    :event_id, :time_of_entry, :item_name, :item_category, :item_subcategory,
                    :quantity_sold, :unit_price, :total_revenue, :cost_per_unit, :total_cost,
                    :gross_margin, :data_source
                ) RETURNING id
            """), data)
            conn.commit()
            return {"id": row.fetchone()[0]}
 
    @api.post("/item-sales/batch")
    def create_item_sales_batch(items: list):
        """Batch insert for Revel API integration — accepts array of item sale records."""
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        inserted = 0
        with engine.connect() as conn:
            for item in items:
                conn.execute(text("""
                    INSERT INTO event_item_sales (
                        event_id, time_of_entry, item_name, item_category, item_subcategory,
                        quantity_sold, unit_price, total_revenue, cost_per_unit, total_cost,
                        gross_margin, data_source
                    ) VALUES (
                        :event_id, :time_of_entry, :item_name, :item_category, :item_subcategory,
                        :quantity_sold, :unit_price, :total_revenue, :cost_per_unit, :total_cost,
                        :gross_margin, :data_source
                    )
                """), item)
                inserted += 1
            conn.commit()
        return {"inserted": inserted}
    # ── Square Integration ────────────────────────────────────────────────────
 
    @api.get("/square/sync")
    def square_sync(date: Optional[str] = None):
        """
        Pull Square payments into Neon for a given event date.
        Usage: /api/square/sync?date=2026-04-25
        Omit date to pull last 24 hours.
        Venue day runs 6am–6am.
        """
        import httpx
        import json
        from datetime import timezone
 
        square_token = os.environ.get("SQUARE_ACCESS_TOKEN")
        square_location = os.environ.get("SQUARE_LOCATION_ID")
 
        if not square_token or not square_location:
            raise HTTPException(status_code=503, detail="Square credentials not configured")
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
 
        # ── Date window ───────────────────────────────────────────────────────
        if date:
            try:
                d = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")
            begin = d.replace(hour=6, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            end = begin + timedelta(hours=24)
        else:
            end = datetime.now(timezone.utc)
            begin = end - timedelta(hours=24)
 
        begin_str = begin.isoformat()
        end_str = end.isoformat()
 
        # ── Ensure Square tables exist ────────────────────────────────────────
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS square_payments (
                    id              TEXT PRIMARY KEY,
                    created_at      TIMESTAMPTZ NOT NULL,
                    location_id     TEXT,
                    status          TEXT,
                    source_type     TEXT,
                    amount_cents    INTEGER,
                    tip_cents       INTEGER,
                    total_cents     INTEGER,
                    currency        TEXT,
                    order_id        TEXT,
                    note            TEXT,
                    raw             JSONB,
                    synced_at       TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS square_line_items (
                    id                  TEXT PRIMARY KEY,
                    payment_id          TEXT REFERENCES square_payments(id),
                    order_id            TEXT,
                    created_at          TIMESTAMPTZ,
                    name                TEXT,
                    quantity            NUMERIC,
                    base_price_cents    INTEGER,
                    total_cents         INTEGER,
                    variation_name      TEXT,
                    raw                 JSONB
                )
            """))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sq_payments_created ON square_payments(created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sq_payments_location ON square_payments(location_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sq_items_payment ON square_line_items(payment_id)"))
            conn.commit()
 
        # ── Fetch payments from Square ────────────────────────────────────────
        headers = {
            "Authorization": f"Bearer {square_token}",
            "Content-Type": "application/json",
            "Square-Version": "2024-01-18",
        }
 
        payments = []
        cursor = None
 
        while True:
            params = {
                "location_id": square_location,
                "begin_time": begin_str,
                "end_time": end_str,
                "limit": 100,
            }
            if cursor:
                params["cursor"] = cursor
 
            try:
                r = httpx.get(
                    "https://connect.squareup.com/v2/payments",
                    headers=headers,
                    params=params,
                    timeout=15,
                )
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                raise HTTPException(status_code=502, detail=f"Square API error: {e.response.text}")
            except httpx.RequestError as e:
                raise HTTPException(status_code=502, detail=f"Square connection error: {str(e)}")
 
            data = r.json()
            batch = data.get("payments", [])
            payments.extend(batch)
            cursor = data.get("cursor")
            if not cursor:
                break
 
        if not payments:
            return {
                "ok": True,
                "window": {"begin": begin_str, "end": end_str},
                "payments_found": 0,
                "message": "No payments found in this window",
            }
 
        # ── Fetch order details for line items ────────────────────────────────
        order_ids = list({p["order_id"] for p in payments if p.get("order_id")})
        orders = {}
 
        for i in range(0, len(order_ids), 100):
            try:
                r = httpx.post(
                    "https://connect.squareup.com/v2/orders/batch-retrieve",
                    headers=headers,
                    json={"order_ids": order_ids[i:i+100]},
                    timeout=15,
                )
                r.raise_for_status()
                for o in r.json().get("orders", []):
                    orders[o["id"]] = o
            except Exception:
                pass  # line items are best-effort — payments still land
 
        # ── Write to Neon ─────────────────────────────────────────────────────
        inserted = 0
        updated = 0
        items_inserted = 0
 
        with engine.connect() as conn:
            for p in payments:
                money = p.get("amount_money", {})
                tip   = p.get("tip_money", {})
                total = p.get("total_money", {})
 
                result = conn.execute(text("""
                    INSERT INTO square_payments
                        (id, created_at, location_id, status, source_type,
                         amount_cents, tip_cents, total_cents, currency,
                         order_id, note, raw)
                    VALUES
                        (:id, :created_at, :location_id, :status, :source_type,
                         :amount_cents, :tip_cents, :total_cents, :currency,
                         :order_id, :note, :raw)
                    ON CONFLICT (id) DO UPDATE SET
                        status    = EXCLUDED.status,
                        raw       = EXCLUDED.raw,
                        synced_at = NOW()
                    RETURNING (xmax = 0) AS is_insert
                """), {
                    "id":           p["id"],
                    "created_at":   p["created_at"],
                    "location_id":  p.get("location_id"),
                    "status":       p.get("status"),
                    "source_type":  p.get("source_type", "UNKNOWN"),
                    "amount_cents": money.get("amount", 0),
                    "tip_cents":    tip.get("amount", 0),
                    "total_cents":  total.get("amount", 0),
                    "currency":     money.get("currency", "USD"),
                    "order_id":     p.get("order_id"),
                    "note":         p.get("note"),
                    "raw":          json.dumps(p),
                })
 
                row = result.fetchone()
                if row and row[0]:
                    inserted += 1
                else:
                    updated += 1
 
                # Line items
                order_id = p.get("order_id")
                if order_id and order_id in orders:
                    for item in orders[order_id].get("line_items", []):
                        item_id = f"{order_id}_{item.get('uid', '')}"
                        base  = item.get("base_price_money", {})
                        itotal = item.get("total_money", {})
                        try:
                            conn.execute(text("""
                                INSERT INTO square_line_items
                                    (id, payment_id, order_id, created_at, name,
                                     quantity, base_price_cents, total_cents,
                                     variation_name, raw)
                                VALUES
                                    (:id, :payment_id, :order_id, :created_at, :name,
                                     :quantity, :base_price_cents, :total_cents,
                                     :variation_name, :raw)
                                ON CONFLICT (id) DO NOTHING
                            """), {
                                "id":               item_id,
                                "payment_id":       p["id"],
                                "order_id":         order_id,
                                "created_at":       p["created_at"],
                                "name":             item.get("name"),
                                "quantity":         item.get("quantity", "1"),
                                "base_price_cents": base.get("amount", 0),
                                "total_cents":      itotal.get("amount", 0),
                                "variation_name":   item.get("variation_name", ""),
                                "raw":              json.dumps(item),
                            })
                            items_inserted += 1
                        except Exception:
                            pass
 
            conn.commit()
 
        # ── Summary query ─────────────────────────────────────────────────────
        with engine.connect() as conn:
            summary = conn.execute(text("""
                SELECT
                    COUNT(*)                                                        AS transactions,
                    ROUND(SUM(amount_cents) / 100.0, 2)                            AS gross_sales,
                    ROUND(SUM(tip_cents) / 100.0, 2)                               AS total_tips,
                    ROUND(SUM(total_cents) / 100.0, 2)                             AS total_collected,
                    ROUND(AVG(amount_cents) / 100.0, 2)                            AS avg_transaction,
                    ROUND(SUM(CASE WHEN source_type = 'CASH' THEN amount_cents ELSE 0 END) / 100.0, 2) AS cash_sales,
                    ROUND(SUM(CASE WHEN source_type = 'CARD' THEN amount_cents ELSE 0 END) / 100.0, 2) AS card_sales
                FROM square_payments
                WHERE created_at BETWEEN :begin AND :end
                  AND status = 'COMPLETED'
            """), {"begin": begin_str, "end": end_str}).fetchone()
 
            summary_dict = dict(summary._mapping) if summary else {}
 
        return {
            "ok": True,
            "window": {"begin": begin_str, "end": end_str},
            "payments_found": len(payments),
            "inserted": inserted,
            "updated": updated,
            "line_items_inserted": items_inserted,
            "summary": summary_dict,
        }
 
    @api.get("/square/summary")
    def square_summary(date: Optional[str] = None):
        """
        Query Square payment summary for a date without re-syncing.
        Usage: /api/square/summary?date=2026-04-25
        """
        from datetime import timezone
 
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
 
        if date:
            try:
                d = datetime.strptime(date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")
            begin = d.replace(hour=6, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            end = begin + timedelta(hours=24)
        else:
            end = datetime.now(timezone.utc)
            begin = end - timedelta(hours=24)
 
        with engine.connect() as conn:
            try:
                summary = conn.execute(text("""
                    SELECT
                        COUNT(*)                                                        AS transactions,
                        ROUND(SUM(amount_cents) / 100.0, 2)                            AS gross_sales,
                        ROUND(SUM(tip_cents) / 100.0, 2)                               AS total_tips,
                        ROUND(SUM(total_cents) / 100.0, 2)                             AS total_collected,
                        ROUND(AVG(amount_cents) / 100.0, 2)                            AS avg_transaction,
                        ROUND(SUM(CASE WHEN source_type = 'CASH' THEN amount_cents ELSE 0 END) / 100.0, 2) AS cash_sales,
                        ROUND(SUM(CASE WHEN source_type = 'CARD' THEN amount_cents ELSE 0 END) / 100.0, 2) AS card_sales
                    FROM square_payments
                    WHERE created_at BETWEEN :begin AND :end
                      AND status = 'COMPLETED'
                """), {"begin": begin.isoformat(), "end": end.isoformat()}).fetchone()
 
                by_type = conn.execute(text("""
                    SELECT source_type, COUNT(*) AS count,
                           ROUND(SUM(amount_cents) / 100.0, 2) AS total
                    FROM square_payments
                    WHERE created_at BETWEEN :begin AND :end AND status = 'COMPLETED'
                    GROUP BY source_type ORDER BY total DESC
                """), {"begin": begin.isoformat(), "end": end.isoformat()}).fetchall()
 
                return {
                    "ok": True,
                    "window": {"begin": begin.isoformat(), "end": end.isoformat()},
                    "summary": dict(summary._mapping) if summary else {},
                    "by_payment_type": [dict(r._mapping) for r in by_type],
                }
            except Exception:
                raise HTTPException(status_code=404, detail="No Square data found — run sync first")
    # ── Variable Event Costs ──────────────────────────────────────────────────

  @api.get("/costs/{event_id}")
  def get_event_costs(event_id: int):
    if not engine:
      return {}
    with engine.connect() as conn:
      conn.execute(text("""
        CREATE TABLE IF NOT EXISTS event_costs (
          id SERIAL PRIMARY KEY,
          event_id INTEGER REFERENCES events(id) ON DELETE CASCADE,
          nightly_operating_cost NUMERIC DEFAULT 0,
          security_total NUMERIC DEFAULT 0,
          security_notes TEXT,
          door_girls_count INTEGER DEFAULT 0,
          door_girls_total NUMERIC DEFAULT 0,
          police_hours NUMERIC DEFAULT 0,
          police_rate NUMERIC DEFAULT 50,
          police_minimum NUMERIC DEFAULT 200,
          police_total NUMERIC DEFAULT 0,
          production_staff_count INTEGER DEFAULT 0,
          production_staff_total NUMERIC DEFAULT 0,
          production_equipment_total NUMERIC DEFAULT 0,
          production_equipment_notes TEXT,
          hospitality_rider_estimate NUMERIC DEFAULT 0,
          hospitality_rider_actual NUMERIC DEFAULT 0,
          hospitality_rider_notes TEXT,
          marketing_internal NUMERIC DEFAULT 0,
          marketing_promoter_contribution NUMERIC DEFAULT 0,
          marketing_notes TEXT,
          artist_fee_total NUMERIC DEFAULT 0,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        )
      """))
      conn.execute(text("""
        INSERT INTO venue_settings (key, value)
        VALUES ('nightly_operating_cost', '0')
        ON CONFLICT (key) DO NOTHING
      """))
      conn.commit()

      row = conn.execute(text("""
        SELECT * FROM event_costs WHERE event_id = :eid
      """), {"eid": event_id}).fetchone()

      if not row:
        return {"event_id": event_id, "exists": False}

      d = dict(row._mapping)
      hours = float(d.get("police_hours") or 0)
      rate = float(d.get("police_rate") or 50)
      minimum = float(d.get("police_minimum") or 200)
      d["police_total_calculated"] = max(hours * rate, minimum) if hours > 0 else 0
      estimate = float(d.get("hospitality_rider_estimate") or 0)
      actual = float(d.get("hospitality_rider_actual") or 0)
      d["hospitality_rider_variance"] = actual - estimate
      d["total_variable_costs"] = sum([
        float(d.get("nightly_operating_cost") or 0),
        float(d.get("security_total") or 0),
        float(d.get("door_girls_total") or 0),
        d["police_total_calculated"],
        float(d.get("production_staff_total") or 0),
        float(d.get("production_equipment_total") or 0),
        float(d.get("hospitality_rider_actual") or 0),
        float(d.get("marketing_internal") or 0),
        float(d.get("marketing_promoter_contribution") or 0),
        float(d.get("artist_fee_total") or 0),
      ])
      return d

  @api.post("/costs")
  def create_event_costs(data: dict):
    if not engine:
      raise HTTPException(status_code=503, detail="DB not configured")
    with engine.connect() as conn:
      conn.execute(text("""
        CREATE TABLE IF NOT EXISTS event_costs (
          id SERIAL PRIMARY KEY,
          event_id INTEGER REFERENCES events(id) ON DELETE CASCADE,
          nightly_operating_cost NUMERIC DEFAULT 0,
          security_total NUMERIC DEFAULT 0,
          security_notes TEXT,
          door_girls_count INTEGER DEFAULT 0,
          door_girls_total NUMERIC DEFAULT 0,
          police_hours NUMERIC DEFAULT 0,
          police_rate NUMERIC DEFAULT 50,
          police_minimum NUMERIC DEFAULT 200,
          police_total NUMERIC DEFAULT 0,
          production_staff_count INTEGER DEFAULT 0,
          production_staff_total NUMERIC DEFAULT 0,
          production_equipment_total NUMERIC DEFAULT 0,
          production_equipment_notes TEXT,
          hospitality_rider_estimate NUMERIC DEFAULT 0,
          hospitality_rider_actual NUMERIC DEFAULT 0,
          hospitality_rider_notes TEXT,
          marketing_internal NUMERIC DEFAULT 0,
          marketing_promoter_contribution NUMERIC DEFAULT 0,
          marketing_notes TEXT,
          artist_fee_total NUMERIC DEFAULT 0,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        )
      """))
      conn.commit()

      hours = float(data.get("police_hours") or 0)
      rate = float(data.get("police_rate") or 50)
      minimum = float(data.get("police_minimum") or 200)
      data["police_total"] = max(hours * rate, minimum) if hours > 0 else 0

      existing = conn.execute(text("""
        SELECT id FROM event_costs WHERE event_id = :eid
      """), {"eid": data["event_id"]}).fetchone()

      if existing:
        fields = [
          "nightly_operating_cost", "security_total", "security_notes",
          "door_girls_count", "door_girls_total",
          "police_hours", "police_rate", "police_minimum", "police_total",
          "production_staff_count", "production_staff_total",
          "production_equipment_total", "production_equipment_notes",
          "hospitality_rider_estimate", "hospitality_rider_actual", "hospitality_rider_notes",
          "marketing_internal", "marketing_promoter_contribution", "marketing_notes",
          "artist_fee_total",
        ]
        set_clause = ", ".join([f"{f}=:{f}" for f in fields if f in data])
        set_clause += ", updated_at=NOW()"
        data["id"] = existing.id
        conn.execute(text(f"UPDATE event_costs SET {set_clause} WHERE id=:id"), data)
        conn.commit()
        return {"id": existing.id, "updated": True}
      else:
        row = conn.execute(text("""
          INSERT INTO event_costs (
            event_id, nightly_operating_cost,
            security_total, security_notes,
            door_girls_count, door_girls_total,
            police_hours, police_rate, police_minimum, police_total,
            production_staff_count, production_staff_total,
            production_equipment_total, production_equipment_notes,
            hospitality_rider_estimate, hospitality_rider_actual, hospitality_rider_notes,
            marketing_internal, marketing_promoter_contribution, marketing_notes,
            artist_fee_total
          ) VALUES (
            :event_id, :nightly_operating_cost,
            :security_total, :security_notes,
            :door_girls_count, :door_girls_total,
            :police_hours, :police_rate, :police_minimum, :police_total,
            :production_staff_count, :production_staff_total,
            :production_equipment_total, :production_equipment_notes,
            :hospitality_rider_estimate, :hospitality_rider_actual, :hospitality_rider_notes,
            :marketing_internal, :marketing_promoter_contribution, :marketing_notes,
            :artist_fee_total
          ) RETURNING id
        """), {
          "event_id": data.get("event_id"),
          "nightly_operating_cost": data.get("nightly_operating_cost", 0),
          "security_total": data.get("security_total", 0),
          "security_notes": data.get("security_notes"),
          "door_girls_count": data.get("door_girls_count", 0),
          "door_girls_total": data.get("door_girls_total", 0),
          "police_hours": data.get("police_hours", 0),
          "police_rate": data.get("police_rate", 50),
          "police_minimum": data.get("police_minimum", 200),
          "police_total": data["police_total"],
          "production_staff_count": data.get("production_staff_count", 0),
          "production_staff_total": data.get("production_staff_total", 0),
          "production_equipment_total": data.get("production_equipment_total", 0),
          "production_equipment_notes": data.get("production_equipment_notes"),
          "hospitality_rider_estimate": data.get("hospitality_rider_estimate", 0),
          "hospitality_rider_actual": data.get("hospitality_rider_actual", 0),
          "hospitality_rider_notes": data.get("hospitality_rider_notes"),
          "marketing_internal": data.get("marketing_internal", 0),
          "marketing_promoter_contribution": data.get("marketing_promoter_contribution", 0),
          "marketing_notes": data.get("marketing_notes"),
          "artist_fee_total": data.get("artist_fee_total", 0),
        })
        conn.commit()
        return {"id": row.fetchone()[0], "updated": False}

  @api.get("/costs/{event_id}/summary")
  def get_cost_summary(event_id: int):
    if not engine:
      raise HTTPException(status_code=503, detail="DB not configured")
    with engine.connect() as conn:
      event = conn.execute(text("""
        SELECT event_name, projected_bar_revenue, projected_door_revenue,
               artist_fee_landed, artist_fee_travel
        FROM events WHERE id = :id
      """), {"id": event_id}).fetchone()
      if not event:
        raise HTTPException(status_code=404, detail="Event not found")

      actuals = conn.execute(text("""
        SELECT total_bar_sales, door_revenue_cash, door_revenue_card,
               promoter_bar_payout, promoter_door_payout, promoter_table_payout
        FROM night_of_actuals
        WHERE event_id = :eid
        ORDER BY created_at DESC LIMIT 1
      """), {"eid": event_id}).fetchone()

      costs = conn.execute(text("""
        SELECT * FROM event_costs WHERE event_id = :eid
      """), {"eid": event_id}).fetchone()

      setting = conn.execute(text("""
        SELECT value FROM venue_settings WHERE key = 'nightly_operating_cost'
      """)).fetchone()
      nightly_op = float(setting.value) if setting else 0

      bar_revenue = float(actuals.total_bar_sales) if actuals else 0
      door_revenue = (float(actuals.door_revenue_cash or 0) + float(actuals.door_revenue_card or 0)) if actuals else 0
      total_revenue = bar_revenue + door_revenue

      if costs:
        hours = float(costs.police_hours or 0)
        rate = float(costs.police_rate or 50)
        minimum = float(costs.police_minimum or 200)
        police_total = max(hours * rate, minimum) if hours > 0 else 0
        cost_lines = [
          {"label": "Nightly operating cost", "amount": float(costs.nightly_operating_cost or nightly_op), "category": "fixed"},
          {"label": "Security", "amount": float(costs.security_total or 0), "category": "variable"},
          {"label": "Door girls", "amount": float(costs.door_girls_total or 0), "category": "variable"},
          {"label": "Police security", "amount": police_total, "category": "variable"},
          {"label": "Production staff", "amount": float(costs.production_staff_total or 0), "category": "variable"},
          {"label": "Production equipment + tech rider", "amount": float(costs.production_equipment_total or 0), "category": "variable"},
          {"label": "Hospitality rider", "amount": float(costs.hospitality_rider_actual or 0), "category": "variable",
           "estimate": float(costs.hospitality_rider_estimate or 0),
           "variance": float(costs.hospitality_rider_actual or 0) - float(costs.hospitality_rider_estimate or 0)},
          {"label": "Marketing — internal", "amount": float(costs.marketing_internal or 0), "category": "marketing"},
          {"label": "Marketing — promoter contribution", "amount": float(costs.marketing_promoter_contribution or 0), "category": "marketing"},
          {"label": "Artist fee", "amount": float(costs.artist_fee_total or 0) or (float(event.artist_fee_landed or 0) + float(event.artist_fee_travel or 0)), "category": "talent"},
        ]
      else:
        cost_lines = [{"label": "Nightly operating cost", "amount": nightly_op, "category": "fixed"}]

      promoter_payouts = 0
      if actuals:
        promoter_payouts = sum([
          float(actuals.promoter_bar_payout or 0),
          float(actuals.promoter_door_payout or 0),
          float(actuals.promoter_table_payout or 0),
        ])

      total_costs = sum(c["amount"] for c in cost_lines) + promoter_payouts
      net = total_revenue - total_costs

      return {
        "event_id": event_id,
        "event_name": event.event_name,
        "revenue": {
          "bar": bar_revenue,
          "door": door_revenue,
          "total": total_revenue,
          "projected_bar": float(event.projected_bar_revenue or 0),
          "projected_door": float(event.projected_door_revenue or 0),
        },
        "cost_lines": cost_lines,
        "promoter_payouts": promoter_payouts,
        "total_costs": total_costs,
        "net": net,
        "net_margin_pct": round((net / total_revenue * 100), 1) if total_revenue > 0 else 0,
      }
    # ── Venue Settings ────────────────────────────────────────────────────────
 
    @api.get("/settings")
    def get_settings():
        import json
        defaults = {"checkin_times": ["11 PM", "12 AM", "1 AM", "2 AM", "Close"]}
        if not engine:
            return defaults
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT key, value FROM venue_settings")).fetchall()
            result = {}
            for row in rows:
                try:
                    result[row.key] = json.loads(row.value)
                except Exception:
                    result[row.key] = row.value
            return {**defaults, **result}
 
    @api.put("/settings")
    def update_settings(data: dict):
        import json
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            for key, value in data.items():
                conn.execute(text("""
                    INSERT INTO venue_settings (key, value, updated_at)
                    VALUES (:key, :value, NOW())
                    ON CONFLICT (key) DO UPDATE SET value=:value, updated_at=NOW()
                """), {"key": key, "value": json.dumps(value)})
            conn.commit()
        return {"ok": True}
 
    # ── App wiring ────────────────────────────────────────────────────────────
 
    app = FastAPI()
 
    @app.on_event("startup")
    def startup():
        init_db()
 
    app.include_router(api, prefix="/api")
 
    if os.path.isdir(static_dir):
        assets_dir = os.path.join(static_dir, "assets")
        if os.path.isdir(assets_dir):
            app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
 
        @app.get("/{path:path}")
        async def spa_fallback(request: Request, path: str):
            file_path = os.path.join(static_dir, path)
            if path and os.path.isfile(file_path):
                return FileResponse(file_path)
            return FileResponse(
                os.path.join(static_dir, "index.html"),
                headers={
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                    "Expires": "0",
                },
            )
 
    return app

