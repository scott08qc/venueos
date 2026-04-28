import os
from datetime import datetime
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

