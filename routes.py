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

# ── Tier 1 category constants ─────────────────────────────────────────────────

TIER1_CATEGORIES = [
    "electronic",
    "open_format",
    "hip_hop_rnb",
    "latin",
    "live_performance",
    "corporate_private",
    "sports_viewing",
    "themed_holiday",
    "college_university",
]

TIER1_LABELS = {
    "electronic":          "Electronic",
    "open_format":         "Open Format",
    "hip_hop_rnb":         "Hip Hop & R&B",
    "latin":               "Latin",
    "live_performance":    "Live Performance",
    "corporate_private":   "Corporate & Private",
    "sports_viewing":      "Sports & Viewing",
    "themed_holiday":      "Themed & Holiday",
    "college_university":  "College & University",
}

VALID_TIER1_CATEGORIES = set(TIER1_CATEGORIES)


def get_deal_guidance(event_tier1_category: str) -> dict:
    guidance = {}
    if event_tier1_category == "college_university":
        guidance["door_structure"] = "venue_favorable"
        guidance["bar_yield_warning"] = True
        guidance["bar_yield_note"] = (
            "College events index lowest bar spend per head across all categories. "
            "Door revenue must carry margin. Ensure ticket split heavily favors venue."
        )
        guidance["security_flag"] = True
        guidance["security_note"] = "Elevated security staffing required. Budget accordingly."
        guidance["wristband_required"] = True
        guidance["wristband_note"] = "18+/21+ wristbanding required for alcohol service."
    return guidance


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
            ("status", "TEXT NOT NULL DEFAULT 'confirmed'"),
            ("revel_bar_gross", "NUMERIC"),
            ("headliner", "TEXT"),
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
                table_bottle_service NUMERIC DEFAULT 0,
                comps_total NUMERIC DEFAULT 0,
                voids NUMERIC DEFAULT 0,
                tax_collected NUMERIC DEFAULT 0,
                tips NUMERIC DEFAULT 0,
                door_revenue_cash NUMERIC DEFAULT 0,
                door_revenue_card NUMERIC DEFAULT 0,
                total_headcount INTEGER DEFAULT 0,
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
            ("house_fee_deduction", "NUMERIC DEFAULT 0"),
            ("door_threshold_retained", "NUMERIC DEFAULT 0"),
            ("charge_backs", "NUMERIC DEFAULT 0"),
            ("promoter_bar_payout", "NUMERIC DEFAULT 0"),
            ("promoter_door_payout", "NUMERIC DEFAULT 0"),
            ("promoter_table_payout", "NUMERIC DEFAULT 0"),
            ("artist_cost_paid_by_venue", "NUMERIC DEFAULT 0"),
            ("effective_split_percentage", "NUMERIC"),
            ("settlement_notes", "TEXT"),
            ("benchmark_effective_split", "TEXT"),
            ("non_alcoholic_food_sales", "NUMERIC DEFAULT 0"),
            ("tables_active", "INTEGER DEFAULT 0"),
            ("ticket_scan_count", "INTEGER DEFAULT 0"),
            ("walk_up_count", "INTEGER DEFAULT 0"),
            ("comp_admissions", "INTEGER DEFAULT 0"),
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

        # ── Deal Intelligence tables ──────────────────────────────────────────

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS distributors (
                id                  SERIAL PRIMARY KEY,
                name                VARCHAR(200) NOT NULL,
                license_number      VARCHAR(50) UNIQUE,
                provi_connected     BOOLEAN DEFAULT FALSE,
                primary_rep_name    VARCHAR(100),
                primary_rep_email   VARCHAR(150),
                primary_rep_phone   VARCHAR(20),
                last_contact_date   DATE,
                deal_sheet_received BOOLEAN DEFAULT FALSE,
                deal_sheet_date     DATE,
                notes               TEXT,
                active              BOOLEAN DEFAULT TRUE,
                created_at          TIMESTAMPTZ DEFAULT NOW(),
                updated_at          TIMESTAMPTZ DEFAULT NOW()
            )
        """))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS product_catalog (
                id                    SERIAL PRIMARY KEY,
                distributor_id        INTEGER NOT NULL REFERENCES distributors(id),
                sku                   VARCHAR(100),
                product_name          VARCHAR(200) NOT NULL,
                brand                 VARCHAR(100),
                category              VARCHAR(50) NOT NULL,
                subcategory           VARCHAR(50),
                unit_size_ml          INTEGER,
                case_pack             INTEGER,
                frontline_price_unit  NUMERIC(10,2),
                frontline_price_case  NUMERIC(10,2),
                price_effective_date  DATE,
                last_verified_date    DATE,
                source                VARCHAR(50) DEFAULT 'deal_sheet',
                provi_sku_id          VARCHAR(100),
                active                BOOLEAN DEFAULT TRUE,
                created_at            TIMESTAMPTZ DEFAULT NOW(),
                updated_at            TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_product_catalog_distributor ON product_catalog(distributor_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_product_catalog_category ON product_catalog(category, subcategory)"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS deal_schedules (
                id                       SERIAL PRIMARY KEY,
                product_catalog_id       INTEGER NOT NULL REFERENCES product_catalog(id),
                deal_type                VARCHAR(50) NOT NULL,
                quantity_threshold_cases INTEGER,
                free_cases_awarded       INTEGER,
                discounted_price_case    NUMERIC(10,2),
                discount_pct             NUMERIC(5,2),
                condition_text           TEXT,
                valid_from               DATE,
                valid_to                 DATE,
                source                   VARCHAR(50) NOT NULL,
                source_file_ref          VARCHAR(300),
                state_posting_ref        VARCHAR(100),
                verified                 BOOLEAN DEFAULT FALSE,
                verified_date            DATE,
                verified_by              VARCHAR(100),
                discrepancy_flag         BOOLEAN DEFAULT FALSE,
                discrepancy_notes        TEXT,
                active                   BOOLEAN DEFAULT TRUE,
                created_at               TIMESTAMPTZ DEFAULT NOW(),
                updated_at               TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_deal_schedules_product ON deal_schedules(product_catalog_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_deal_schedules_active ON deal_schedules(active, valid_to)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_deal_schedules_type ON deal_schedules(deal_type)"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS purchase_recommendations (
                id                       SERIAL PRIMARY KEY,
                product_catalog_id       INTEGER NOT NULL REFERENCES product_catalog(id),
                deal_schedule_id         INTEGER REFERENCES deal_schedules(id),
                lookahead_days           INTEGER DEFAULT 60,
                projected_cases_needed   NUMERIC(8,2),
                deal_threshold_cases     INTEGER,
                cases_to_order           INTEGER,
                cases_already_on_hand    NUMERIC(8,2),
                recommended_order_date   DATE,
                estimated_saving_total   NUMERIC(10,2),
                saving_per_case          NUMERIC(10,2),
                confidence_score         NUMERIC(4,2),
                status                   VARCHAR(30) DEFAULT 'pending',
                ordered_date             DATE,
                ordered_cases            INTEGER,
                actual_saving            NUMERIC(10,2),
                notes                    TEXT,
                generated_at             TIMESTAMPTZ DEFAULT NOW(),
                updated_at               TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchase_rec_product ON purchase_recommendations(product_catalog_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchase_rec_status ON purchase_recommendations(status)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_purchase_rec_date ON purchase_recommendations(recommended_order_date)"))

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS event_consumption_model (
                id                    SERIAL PRIMARY KEY,
                event_tier1_category  VARCHAR(50) NOT NULL,
                product_subcategory   VARCHAR(50) NOT NULL,
                cases_per_100_guests  NUMERIC(6,3),
                confidence_level      VARCHAR(20) DEFAULT 'estimated',
                sample_event_count    INTEGER DEFAULT 0,
                last_updated          DATE,
                notes                 TEXT,
                created_at            TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_consumption_model_unique
            ON event_consumption_model(event_tier1_category, product_subcategory)
        """))

        # Seed consumption model (skips rows that already exist)
        conn.execute(text("""
            INSERT INTO event_consumption_model
            (event_tier1_category, product_subcategory, cases_per_100_guests, confidence_level, notes)
            VALUES
            ('latin',             'tequila', 0.80, 'estimated', 'High tequila index — Don Julio, Patron dominant'),
            ('latin',             'vodka',   0.30, 'estimated', 'Secondary spirit'),
            ('latin',             'rum',     0.25, 'estimated', 'Bacardi, Malibu'),
            ('latin',             'beer',    0.60, 'estimated', 'Corona, Modelo heavy'),
            ('latin',             'whiskey', 0.10, 'estimated', 'Minimal'),
            ('electronic',        'vodka',   0.70, 'estimated', 'Grey Goose, Tito dominant'),
            ('electronic',        'tequila', 0.35, 'estimated', 'Secondary'),
            ('electronic',        'beer',    0.50, 'estimated', 'Domestic heavy'),
            ('electronic',        'whiskey', 0.15, 'estimated', 'Minimal'),
            ('hip_hop_rnb',       'whiskey', 0.55, 'estimated', 'Hennessy, Crown Royal dominant'),
            ('hip_hop_rnb',       'vodka',   0.45, 'estimated', 'Ciroc, Tito dominant'),
            ('hip_hop_rnb',       'tequila', 0.25, 'estimated', 'Secondary'),
            ('hip_hop_rnb',       'beer',    0.55, 'estimated', 'Domestic + premium mix'),
            ('open_format',       'vodka',   0.55, 'estimated', 'Broadest mix'),
            ('open_format',       'tequila', 0.40, 'estimated', NULL),
            ('open_format',       'whiskey', 0.30, 'estimated', NULL),
            ('open_format',       'beer',    0.55, 'estimated', NULL),
            ('corporate_private', 'vodka',   0.45, 'estimated', 'Premium spirits — client-dependent'),
            ('corporate_private', 'wine',    0.40, 'estimated', 'Higher wine index than nightlife'),
            ('corporate_private', 'whiskey', 0.30, 'estimated', NULL),
            ('corporate_private', 'beer',    0.35, 'estimated', 'Lower beer index'),
            ('corporate_private', 'tequila', 0.25, 'estimated', NULL),
            ('sports_viewing',    'beer',    1.10, 'estimated', 'Dominant category — domestic heavy'),
            ('sports_viewing',    'vodka',   0.30, 'estimated', 'Shots and simple drinks'),
            ('sports_viewing',    'whiskey', 0.25, 'estimated', 'Bourbon shots common'),
            ('sports_viewing',    'tequila', 0.15, 'estimated', 'Minimal'),
            ('live_performance',  'beer',    0.75, 'estimated', 'Highest beer index after sports'),
            ('live_performance',  'whiskey', 0.35, 'estimated', 'Genre-dependent'),
            ('live_performance',  'vodka',   0.35, 'estimated', NULL),
            ('live_performance',  'tequila', 0.25, 'estimated', NULL)
            ON CONFLICT (event_tier1_category, product_subcategory) DO NOTHING
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
    status: Optional[str] = 'confirmed'


class NightOfActualsCreate(BaseModel):
    event_id: int
    time_of_entry: str
    total_bar_sales: Optional[float] = 0
    liquor_sales: Optional[float] = 0
    beer_wine_sales: Optional[float] = 0
    table_bottle_service: Optional[float] = 0
    comps_total: Optional[float] = 0
    voids: Optional[float] = 0
    tax_collected: Optional[float] = 0
    tips: Optional[float] = 0
    door_revenue_cash: Optional[float] = 0
    door_revenue_card: Optional[float] = 0
    total_headcount: Optional[int] = 0
    incident_description: Optional[str] = None
    incident_department: Optional[str] = None
    notes: Optional[str] = None
    bar_cogs_deduction: Optional[float] = 0
    bar_threshold_retained: Optional[float] = 0
    house_fee_deduction: Optional[float] = 0
    door_threshold_retained: Optional[float] = 0
    charge_backs: Optional[float] = 0
    promoter_bar_payout: Optional[float] = 0
    promoter_door_payout: Optional[float] = 0
    promoter_table_payout: Optional[float] = 0
    artist_cost_paid_by_venue: Optional[float] = 0
    effective_split_percentage: Optional[float] = None
    settlement_notes: Optional[str] = None
    benchmark_effective_split: Optional[str] = None
    non_alcoholic_food_sales: Optional[float] = 0
    tables_active: Optional[int] = 0
    ticket_scan_count: Optional[int] = 0
    walk_up_count: Optional[int] = 0
    comp_admissions: Optional[int] = 0


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


class HistoricalEventCreate(BaseModel):
    event_date: str
    event_name: str
    tier1_category: Optional[str] = None
    tier2_subcategory: Optional[str] = None
    promoter_name: Optional[str] = None
    artist_name: Optional[str] = None
    gross_revenue: Optional[float] = 0
    attendance: Optional[int] = 0
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
                       COALESCE(e.artist_name, e.headliner) AS artist_name,
                       e.projected_door_revenue, e.projected_bar_revenue, e.projected_table_revenue,
                       e.revel_bar_gross,
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
                       notes, created_at, updated_at
                FROM events WHERE id = :id
            """), {"id": event_id}).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Event not found")
            d = dict(row._mapping)
            for f in ["event_date", "deposit_due_date", "balance_due_date", "created_at", "updated_at"]:
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
                    projected_door_revenue, projected_bar_revenue, projected_table_revenue, notes, status
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
                    :projected_door_revenue, :projected_bar_revenue, :projected_table_revenue, :notes, :status
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
                    notes=:notes, status=:status, updated_at=NOW()
                WHERE id=:id
            """), params)
            conn.commit()
            return {"ok": True, "day_of_week": day_of_week}

    @api.delete("/events/{event_id}")
    def delete_event(event_id: int):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            result = conn.execute(
                text("DELETE FROM events WHERE id = :eid RETURNING id"),
                {"eid": event_id}
            )
            conn.commit()
            if not result.fetchone():
                raise HTTPException(status_code=404, detail="Event not found")
            return {"ok": True}

    # ── Night of Actuals ──────────────────────────────────────────────────────

    @api.get("/actuals/{event_id}")
    def get_actuals(event_id: int):
        if not engine:
            return []
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, event_id, time_of_entry, total_bar_sales, liquor_sales,
                       beer_wine_sales, table_bottle_service, comps_total, voids,
                       tax_collected, tips, door_revenue_cash, door_revenue_card,
                       total_headcount, incident_description, incident_department, notes,
                       bar_cogs_deduction, bar_threshold_retained, house_fee_deduction, door_threshold_retained,
                       charge_backs, promoter_bar_payout, promoter_door_payout,
                       promoter_table_payout, artist_cost_paid_by_venue,
                       effective_split_percentage, settlement_notes, benchmark_effective_split,
                       non_alcoholic_food_sales, tables_active,
                       ticket_scan_count, walk_up_count, comp_admissions,
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
                    event_id, time_of_entry, total_bar_sales, liquor_sales, beer_wine_sales,
                    table_bottle_service, comps_total, voids, tax_collected, tips,
                    door_revenue_cash, door_revenue_card, total_headcount,
                    incident_description, incident_department, notes,
                    bar_cogs_deduction, bar_threshold_retained, house_fee_deduction, door_threshold_retained,
                    charge_backs, promoter_bar_payout, promoter_door_payout,
                    promoter_table_payout, artist_cost_paid_by_venue,
                    effective_split_percentage, settlement_notes, benchmark_effective_split,
                    non_alcoholic_food_sales, tables_active,
                    ticket_scan_count, walk_up_count, comp_admissions
                ) VALUES (
                    :event_id, :time_of_entry, :total_bar_sales, :liquor_sales, :beer_wine_sales,
                    :table_bottle_service, :comps_total, :voids, :tax_collected, :tips,
                    :door_revenue_cash, :door_revenue_card, :total_headcount,
                    :incident_description, :incident_department, :notes,
                    :bar_cogs_deduction, :bar_threshold_retained, :house_fee_deduction, :door_threshold_retained,
                    :charge_backs, :promoter_bar_payout, :promoter_door_payout,
                    :promoter_table_payout, :artist_cost_paid_by_venue,
                    :effective_split_percentage, :settlement_notes, :benchmark_effective_split,
                    :non_alcoholic_food_sales, :tables_active,
                    :ticket_scan_count, :walk_up_count, :comp_admissions
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
                    time_of_entry=:time_of_entry, total_bar_sales=:total_bar_sales,
                    liquor_sales=:liquor_sales, beer_wine_sales=:beer_wine_sales,
                    table_bottle_service=:table_bottle_service, comps_total=:comps_total,
                    voids=:voids, tax_collected=:tax_collected, tips=:tips,
                    door_revenue_cash=:door_revenue_cash, door_revenue_card=:door_revenue_card,
                    total_headcount=:total_headcount, incident_description=:incident_description,
                    incident_department=:incident_department, notes=:notes,
                    bar_cogs_deduction=:bar_cogs_deduction,
                    bar_threshold_retained=:bar_threshold_retained,
                    house_fee_deduction=:house_fee_deduction,
                    door_threshold_retained=:door_threshold_retained,
                    charge_backs=:charge_backs,
                    promoter_bar_payout=:promoter_bar_payout,
                    promoter_door_payout=:promoter_door_payout,
                    promoter_table_payout=:promoter_table_payout,
                    artist_cost_paid_by_venue=:artist_cost_paid_by_venue,
                    effective_split_percentage=:effective_split_percentage,
                    settlement_notes=:settlement_notes,
                    benchmark_effective_split=:benchmark_effective_split,
                    non_alcoholic_food_sales=:non_alcoholic_food_sales,
                    tables_active=:tables_active,
                    ticket_scan_count=:ticket_scan_count,
                    walk_up_count=:walk_up_count,
                    comp_admissions=:comp_admissions,
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
                       projected_effective_split, actual_effective_split, settlement_notes_reference,
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
                    projected_effective_split, actual_effective_split, settlement_notes_reference
                ) VALUES (
                    :event_id, :actual_attendance, :actual_door_revenue, :actual_bar_revenue,
                    :actual_table_revenue, :artist_cost_actual, :staffing_cost_actual,
                    :crowd_demographic_observations, :customer_service_observations,
                    :operational_breakdowns, :what_to_replicate, :what_to_change,
                    :promoter_performance_notes, :inventory_observations, :staffing_observations,
                    :promoter_artwork_on_time, :promoter_social_posting,
                    :promoter_attendance_vs_projection, :promoter_role_boundaries, :review_status,
                    :projected_effective_split, :actual_effective_split, :settlement_notes_reference
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
                INSERT INTO historical_events (event_date, event_name, tier1_category,
                    tier2_subcategory, promoter_name, artist_name, gross_revenue,
                    attendance, data_source, classification_status)
                VALUES (:event_date, :event_name, :tier1_category, :tier2_subcategory,
                    :promoter_name, :artist_name, :gross_revenue, :attendance,
                    :data_source, :classification_status)
                RETURNING id
            """), data.model_dump())
            conn.commit()
            return {"id": row.fetchone()[0]}

    # ── Square Sync ───────────────────────────────────────────────────────────

    @api.get("/square/sync")
    def square_sync(date: Optional[str] = None):
        import httpx, json
        from datetime import timezone
        square_token = os.environ.get("SQUARE_ACCESS_TOKEN")
        square_location = os.environ.get("SQUARE_LOCATION_ID")
        if not square_token or not square_location:
            raise HTTPException(status_code=503, detail="Square credentials not configured")
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        if date:
            d = datetime.strptime(date, "%Y-%m-%d")
            begin = d.replace(hour=6, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            end = begin + timedelta(hours=24)
        else:
            end = datetime.now(timezone.utc)
            begin = end - timedelta(hours=24)
        begin_str = begin.isoformat()
        end_str = end.isoformat()
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE IF NOT EXISTS square_payments (id TEXT PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL, location_id TEXT, status TEXT, source_type TEXT, amount_cents INTEGER, tip_cents INTEGER, total_cents INTEGER, currency TEXT, order_id TEXT, note TEXT, raw JSONB, synced_at TIMESTAMPTZ DEFAULT NOW())"))
            conn.commit()
        headers = {"Authorization": f"Bearer {square_token}", "Content-Type": "application/json", "Square-Version": "2024-01-18"}
        payments = []
        cursor = None
        while True:
            params = {"location_id": square_location, "begin_time": begin_str, "end_time": end_str, "limit": 100}
            if cursor:
                params["cursor"] = cursor
            r = httpx.get("https://connect.squareup.com/v2/payments", headers=headers, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()
            payments.extend(data.get("payments", []))
            cursor = data.get("cursor")
            if not cursor:
                break
        if not payments:
            return {"ok": True, "payments_found": 0, "message": "No payments in this window"}
        with engine.connect() as conn:
            for p in payments:
                money = p.get("amount_money", {})
                tip = p.get("tip_money", {})
                total = p.get("total_money", {})
                conn.execute(text("INSERT INTO square_payments (id, created_at, location_id, status, source_type, amount_cents, tip_cents, total_cents, currency, order_id, note, raw) VALUES (:id, :created_at, :location_id, :status, :source_type, :amount_cents, :tip_cents, :total_cents, :currency, :order_id, :note, :raw) ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, raw=EXCLUDED.raw, synced_at=NOW()"), {"id": p["id"], "created_at": p["created_at"], "location_id": p.get("location_id"), "status": p.get("status"), "source_type": p.get("source_type", "UNKNOWN"), "amount_cents": money.get("amount", 0), "tip_cents": tip.get("amount", 0), "total_cents": total.get("amount", 0), "currency": money.get("currency", "USD"), "order_id": p.get("order_id"), "note": p.get("note"), "raw": json.dumps(p)})
            conn.commit()
        with engine.connect() as conn:
            summary = conn.execute(text("SELECT COUNT(*) AS transactions, ROUND(SUM(amount_cents)/100.0,2) AS gross_sales, ROUND(SUM(tip_cents)/100.0,2) AS total_tips FROM square_payments WHERE created_at BETWEEN :begin AND :end AND status='COMPLETED'"), {"begin": begin_str, "end": end_str}).fetchone()
        return {"ok": True, "payments_found": len(payments), "summary": dict(summary._mapping)}

    @api.get("/square/sync-range")
    def square_sync_range(start: str, end: str):
        from datetime import datetime, timedelta
        try:
            start_date = datetime.strptime(start, "%Y-%m-%d")
            end_date = datetime.strptime(end, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Dates must be YYYY-MM-DD")
        results = []
        current = start_date
        while current <= end_date:
            day_of_week = current.weekday()
            if day_of_week in [3, 4, 5]:  # Thursday=3, Friday=4, Saturday=5

                date_str = current.strftime("%Y-%m-%d")
                try:
                    result = square_sync(date=date_str)
                    results.append({"date": date_str, "day": current.strftime("%A"), **result})
                except Exception as e:
                    results.append({"date": date_str, "day": current.strftime("%A"), "error": str(e)})
            current += timedelta(days=1)
        total_payments = sum(r.get("payments_found", 0) for r in results)
        total_sales = sum(r.get("summary", {}).get("gross_sales", 0) or 0 for r in results)
        return {"ok": True, "nights_synced": len(results), "total_payments": total_payments, "total_gross_sales": round(total_sales, 2), "results": results}

    # ── Event Costs ───────────────────────────────────────────────────────────

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
        conn.commit()
        row = conn.execute(text("SELECT * FROM event_costs WHERE event_id = :eid"), {"eid": event_id}).fetchone()
        if not row:
          return {"event_id": event_id, "exists": False}
        d = dict(row._mapping)
        hours = float(d.get("police_hours") or 0)
        rate = float(d.get("police_rate") or 50)
        minimum = float(d.get("police_minimum") or 200)
        d["police_total_calculated"] = max(hours * rate, minimum) if hours > 0 else 0
        d["hospitality_rider_variance"] = float(d.get("hospitality_rider_actual") or 0) - float(d.get("hospitality_rider_estimate") or 0)
        d["total_variable_costs"] = sum([float(d.get(f) or 0) for f in ["nightly_operating_cost","security_total","door_girls_total","production_staff_total","production_equipment_total","hospitality_rider_actual","marketing_internal","marketing_promoter_contribution","artist_fee_total"]]) + d["police_total_calculated"]
        return d

    @api.post("/costs")
    def create_event_costs(data: dict):
      if not engine:
        raise HTTPException(status_code=503, detail="DB not configured")
      with engine.connect() as conn:
        hours = float(data.get("police_hours") or 0)
        rate = float(data.get("police_rate") or 50)
        minimum = float(data.get("police_minimum") or 200)
        data["police_total"] = max(hours * rate, minimum) if hours > 0 else 0
        existing = conn.execute(text("SELECT id FROM event_costs WHERE event_id = :eid"), {"eid": data["event_id"]}).fetchone()
        if existing:
          fields = ["nightly_operating_cost","security_total","security_notes","door_girls_count","door_girls_total","police_hours","police_rate","police_minimum","police_total","production_staff_count","production_staff_total","production_equipment_total","production_equipment_notes","hospitality_rider_estimate","hospitality_rider_actual","hospitality_rider_notes","marketing_internal","marketing_promoter_contribution","marketing_notes","artist_fee_total"]
          set_clause = ", ".join([f"{f}=:{f}" for f in fields if f in data]) + ", updated_at=NOW()"
          data["id"] = existing.id
          conn.execute(text(f"UPDATE event_costs SET {set_clause} WHERE id=:id"), data)
          conn.commit()
          return {"id": existing.id, "updated": True}
        else:
          row = conn.execute(text("""INSERT INTO event_costs (event_id,nightly_operating_cost,security_total,security_notes,door_girls_count,door_girls_total,police_hours,police_rate,police_minimum,police_total,production_staff_count,production_staff_total,production_equipment_total,production_equipment_notes,hospitality_rider_estimate,hospitality_rider_actual,hospitality_rider_notes,marketing_internal,marketing_promoter_contribution,marketing_notes,artist_fee_total) VALUES (:event_id,:nightly_operating_cost,:security_total,:security_notes,:door_girls_count,:door_girls_total,:police_hours,:police_rate,:police_minimum,:police_total,:production_staff_count,:production_staff_total,:production_equipment_total,:production_equipment_notes,:hospitality_rider_estimate,:hospitality_rider_actual,:hospitality_rider_notes,:marketing_internal,:marketing_promoter_contribution,:marketing_notes,:artist_fee_total) RETURNING id"""), {"event_id":data.get("event_id"),"nightly_operating_cost":data.get("nightly_operating_cost",0),"security_total":data.get("security_total",0),"security_notes":data.get("security_notes"),"door_girls_count":data.get("door_girls_count",0),"door_girls_total":data.get("door_girls_total",0),"police_hours":data.get("police_hours",0),"police_rate":data.get("police_rate",50),"police_minimum":data.get("police_minimum",200),"police_total":data["police_total"],"production_staff_count":data.get("production_staff_count",0),"production_staff_total":data.get("production_staff_total",0),"production_equipment_total":data.get("production_equipment_total",0),"production_equipment_notes":data.get("production_equipment_notes"),"hospitality_rider_estimate":data.get("hospitality_rider_estimate",0),"hospitality_rider_actual":data.get("hospitality_rider_actual",0),"hospitality_rider_notes":data.get("hospitality_rider_notes"),"marketing_internal":data.get("marketing_internal",0),"marketing_promoter_contribution":data.get("marketing_promoter_contribution",0),"marketing_notes":data.get("marketing_notes"),"artist_fee_total":data.get("artist_fee_total",0)})
          conn.commit()
          return {"id": row.fetchone()[0], "updated": False}

    @api.get("/costs/{event_id}/summary")
    def get_cost_summary(event_id: int):
      if not engine:
        raise HTTPException(status_code=503, detail="DB not configured")
      with engine.connect() as conn:
        event = conn.execute(text("SELECT event_name, projected_bar_revenue, projected_door_revenue, artist_fee_landed, artist_fee_travel FROM events WHERE id = :id"), {"id": event_id}).fetchone()
        if not event:
          raise HTTPException(status_code=404, detail="Event not found")
        actuals = conn.execute(text("SELECT total_bar_sales, door_revenue_cash, door_revenue_card, promoter_bar_payout, promoter_door_payout, promoter_table_payout FROM night_of_actuals WHERE event_id = :eid ORDER BY created_at DESC LIMIT 1"), {"eid": event_id}).fetchone()
        costs = conn.execute(text("SELECT * FROM event_costs WHERE event_id = :eid"), {"eid": event_id}).fetchone()
        setting = conn.execute(text("SELECT value FROM venue_settings WHERE key = 'nightly_operating_cost'")).fetchone()
        nightly_op = float(setting.value) if setting else 0
        bar_revenue = float(actuals.total_bar_sales) if actuals else 0
        door_revenue = (float(actuals.door_revenue_cash or 0) + float(actuals.door_revenue_card or 0)) if actuals else 0
        total_revenue = bar_revenue + door_revenue
        if costs:
          hours = float(costs.police_hours or 0)
          police_total = max(hours * float(costs.police_rate or 50), float(costs.police_minimum or 200)) if hours > 0 else 0
          cost_lines = [
            {"label": "Nightly operating cost", "amount": float(costs.nightly_operating_cost or nightly_op), "category": "fixed"},
            {"label": "Security", "amount": float(costs.security_total or 0), "category": "variable"},
            {"label": "Door girls", "amount": float(costs.door_girls_total or 0), "category": "variable"},
            {"label": "Police security", "amount": police_total, "category": "variable"},
            {"label": "Production staff", "amount": float(costs.production_staff_total or 0), "category": "variable"},
            {"label": "Production equipment + tech rider", "amount": float(costs.production_equipment_total or 0), "category": "variable"},
            {"label": "Hospitality rider", "amount": float(costs.hospitality_rider_actual or 0), "category": "variable", "estimate": float(costs.hospitality_rider_estimate or 0), "variance": float(costs.hospitality_rider_actual or 0) - float(costs.hospitality_rider_estimate or 0)},
            {"label": "Marketing — internal", "amount": float(costs.marketing_internal or 0), "category": "marketing"},
            {"label": "Marketing — promoter", "amount": float(costs.marketing_promoter_contribution or 0), "category": "marketing"},
            {"label": "Artist fee", "amount": float(costs.artist_fee_total or 0) or (float(event.artist_fee_landed or 0) + float(event.artist_fee_travel or 0)), "category": "talent"},
          ]
        else:
          cost_lines = [{"label": "Nightly operating cost", "amount": nightly_op, "category": "fixed"}]
        promoter_payouts = sum([float(getattr(actuals, f) or 0) for f in ["promoter_bar_payout","promoter_door_payout","promoter_table_payout"]]) if actuals else 0
        total_costs = sum(c["amount"] for c in cost_lines) + promoter_payouts
        net = total_revenue - total_costs
        return {"event_id": event_id, "event_name": event.event_name, "revenue": {"bar": bar_revenue, "door": door_revenue, "total": total_revenue, "projected_bar": float(event.projected_bar_revenue or 0), "projected_door": float(event.projected_door_revenue or 0)}, "cost_lines": cost_lines, "promoter_payouts": promoter_payouts, "total_costs": total_costs, "net": net, "net_margin_pct": round((net / total_revenue * 100), 1) if total_revenue > 0 else 0}

    # ── Promoter Intelligence ─────────────────────────────────────────────────

    @api.get("/promoter-intelligence")
    def get_promoter_intelligence(promoter: str, event_type: str = None):
      if not engine:
        raise HTTPException(status_code=503, detail="DB not configured")
      with engine.connect() as conn:
        rows = conn.execute(text("""
          SELECT
            h.id,
            h.event_name,
            h.event_date,
            h.tier1_category,
            NULL::integer AS expected_attendance,
            NULL::numeric AS projected_bar_revenue,
            NULL::numeric AS projected_door_revenue,
            NULL::numeric AS projected_table_revenue,
            NULL::numeric AS door_split_promoter,
            NULL::numeric AS bar_split_promoter,
            NULL::numeric AS table_split_promoter,
            NULL::numeric AS artist_fee_landed,
            NULL::numeric AS artist_fee_travel,
            h.artist_name,
            h.gross_revenue AS revel_bar_gross,
            h.gross_revenue AS net_bar_revenue,
            NULL::integer AS actual_attendance,
            h.gross_revenue AS actual_bar_revenue,
            NULL::numeric AS actual_door_revenue,
            NULL::numeric AS actual_table_revenue,
            NULL::numeric AS actual_effective_split,
            'Complete' AS review_status,
            h.gross_revenue AS total_bar_sales,
            NULL::integer AS total_headcount,
            NULL::numeric AS door_revenue_cash,
            NULL::numeric AS door_revenue_card,
            NULL::numeric AS effective_split_percentage
          FROM historical_events h
          WHERE LOWER(h.promoter_name) LIKE LOWER(:promoter)

          UNION ALL

          SELECT
            e.id,
            e.event_name,
            e.event_date,
            e.tier1_category,
            e.expected_attendance,
            e.projected_bar_revenue,
            e.projected_door_revenue,
            e.projected_table_revenue,
            e.door_split_promoter,
            e.bar_split_promoter,
            e.table_split_promoter,
            e.artist_fee_landed,
            e.artist_fee_travel,
            e.artist_name,
            e.revel_bar_gross,
            COALESCE((
              SELECT SUM(eis.total_revenue)
              FROM event_item_sales eis
              WHERE eis.event_id = e.id
              AND eis.item_category IN ('Bar', 'Bottle Service')
            ), e.revel_bar_gross, 0) AS net_bar_revenue,
            r.actual_attendance,
            r.actual_bar_revenue,
            r.actual_door_revenue,
            r.actual_table_revenue,
            r.actual_effective_split,
            r.review_status,
            n.total_bar_sales,
            n.total_headcount,
            n.door_revenue_cash,
            n.door_revenue_card,
            n.effective_split_percentage
          FROM events e
          LEFT JOIN post_event_reviews r ON r.event_id = e.id
            AND LOWER(r.review_status) = 'complete'
          LEFT JOIN night_of_actuals n ON n.event_id = e.id
            AND n.time_of_entry = 'Close'
          WHERE LOWER(e.promoter_name) LIKE LOWER(:promoter)

          ORDER BY event_date DESC
          LIMIT 30
        """), {"promoter": f"%{promoter}%"}).fetchall()

        all_events = [dict(r._mapping) for r in rows]
        type_events = [e for e in all_events if not event_type or e.get("tier1_category") == event_type]

        if not all_events:
          return {
            "found": False,
            "promoter": promoter,
            "message": "No completed events found for this promoter",
            "reputation_score": None,
            "recommendations": None,
            "history": []
          }

        def safe_avg(lst):
          vals = [v for v in lst if v is not None]
          return round(sum(vals) / len(vals), 2) if vals else None

        def safe_pct(a, b):
          return round((a / b) * 100, 1) if b and b > 0 else None

        ref_events = type_events if type_events else all_events

        draw_ratios = []
        for e in ref_events:
          if e.get("actual_attendance") and e.get("expected_attendance") and e["expected_attendance"] > 0:
            draw_ratios.append(e["actual_attendance"] / e["expected_attendance"])
        avg_draw_ratio = safe_avg(draw_ratios)
        draw_score = min(10, round((avg_draw_ratio or 0.5) * 8, 1)) if avg_draw_ratio else None

        sph_actuals = [
          (e.get("actual_bar_revenue") or e.get("total_bar_sales") or 0) / e["actual_attendance"]
          for e in ref_events
          if e.get("actual_attendance") and e["actual_attendance"] > 0
          and (e.get("actual_bar_revenue") or e.get("total_bar_sales") or e.get("net_bar_revenue"))
        ]
        avg_sph = safe_avg(sph_actuals)
        sph_proj = [e["projected_bar_revenue"] / e["expected_attendance"]
                    for e in ref_events
                    if e.get("projected_bar_revenue") and e.get("expected_attendance") and e["expected_attendance"] > 0]
        avg_sph_proj = safe_avg(sph_proj)
        bar_yield_ratio = safe_pct(avg_sph, avg_sph_proj) / 100 if avg_sph and avg_sph_proj else None
        bar_score = min(10, round((bar_yield_ratio or 0.5) * 9, 1)) if bar_yield_ratio else None

        def calc_net(e):
          bar = float(e.get("actual_bar_revenue") or e.get("total_bar_sales") or e.get("net_bar_revenue") or 0)
          door = float(e.get("actual_door_revenue") or 0) + float(e.get("door_revenue_cash") or 0) + float(e.get("door_revenue_card") or 0)
          table = float(e.get("actual_table_revenue") or 0)
          total_rev = bar + door + table
          bar_split = float(e.get("bar_split_promoter") or 0) / 100
          door_split = float(e.get("door_split_promoter") or 0) / 100
          table_split = float(e.get("table_split_promoter") or 0) / 100
          promoter_take = (bar * bar_split) + (door * door_split) + (table * table_split)
          artist = float(e.get("artist_fee_landed") or 0) + float(e.get("artist_fee_travel") or 0)
          return total_rev - promoter_take - artist

        net_revenues = [calc_net(e) for e in ref_events]
        avg_net = safe_avg(net_revenues)
        profitability_score = None
        if avg_net is not None:
          if avg_net >= 8000: profitability_score = 10.0
          elif avg_net >= 6000: profitability_score = 8.0
          elif avg_net >= 4000: profitability_score = 6.5
          elif avg_net >= 2000: profitability_score = 5.0
          elif avg_net >= 0: profitability_score = 3.0
          else: profitability_score = 1.0

        consistency_score = None
        if len(draw_ratios) >= 2:
          import statistics
          variance = statistics.stdev(draw_ratios)
          consistency_score = min(10, round(max(0, 10 - variance * 15), 1))
        elif len(draw_ratios) == 1:
          consistency_score = 7.0

        scores = {"draw_accuracy": draw_score, "bar_yield": bar_score, "deal_profitability": profitability_score, "consistency": consistency_score}
        weights = {"draw_accuracy": 0.35, "bar_yield": 0.30, "deal_profitability": 0.25, "consistency": 0.10}
        available = {k: v for k, v in scores.items() if v is not None}
        if available:
          total_weight = sum(weights[k] for k in available)
          reputation_score = round(sum(float(v) * weights[k] / total_weight for k, v in available.items()), 1)
        else:
          reputation_score = None

        attendances = [e["actual_attendance"] for e in ref_events if e.get("actual_attendance")]
        door_splits = [e["door_split_promoter"] for e in ref_events if e.get("door_split_promoter") is not None]
        bar_splits = [e["bar_split_promoter"] for e in ref_events if e.get("bar_split_promoter") is not None]
        effective_splits = [e.get("actual_effective_split") or e.get("effective_split_percentage") for e in ref_events if e.get("actual_effective_split") or e.get("effective_split_percentage")]
        artist_fees = [e["artist_fee_landed"] for e in ref_events if e.get("artist_fee_landed")]
        sorted_by_net = sorted([(e, calc_net(e)) for e in ref_events], key=lambda x: x[1], reverse=True)

        history = []
        for e in ref_events[:8]:
          draw_pct = safe_pct(e.get("actual_attendance"), e.get("expected_attendance"))
          bar_rev = e.get("actual_bar_revenue") or e.get("total_bar_sales") or e.get("net_bar_revenue")
          sph = (float(bar_rev) / e["actual_attendance"]) if bar_rev and e.get("actual_attendance") and e["actual_attendance"] > 0 else None
          history.append({
            "event_id": e["id"],
            "event_name": e["event_name"],
            "event_date": str(e["event_date"]),
            "event_type": e["tier1_category"],
            "expected_attendance": e.get("expected_attendance"),
            "actual_attendance": e.get("actual_attendance"),
            "draw_pct": draw_pct,
            "spend_per_head_actual": round(sph, 2) if sph else None,
            "actual_bar_revenue": bar_rev,
            "net_revenue_actual": round(calc_net(e), 2),
            "door_split_promoter": e.get("door_split_promoter"),
            "bar_split_promoter": e.get("bar_split_promoter"),
            "actual_effective_split": e.get("actual_effective_split") or e.get("effective_split_percentage"),
            "artist_name": e.get("artist_name"),
          })

        return {
          "found": True,
          "promoter": promoter,
          "event_type": event_type,
          "total_events": len(all_events),
          "type_events": len(type_events),
          "reputation_score": reputation_score,
          "score_components": {
            "draw_accuracy": {
              "score": draw_score,
              "avg_draw_ratio": avg_draw_ratio,
              "label": f"Averages {round((avg_draw_ratio or 0) * 100)}% of promised attendance" if avg_draw_ratio else "Insufficient data",
              "weight": "35%"
            },
            "bar_yield": {
              "score": bar_score,
              "avg_spend_per_head": avg_sph,
              "label": f"${avg_sph}/head avg bar spend" if avg_sph else "Insufficient data",
              "weight": "30%"
            },
            "deal_profitability": {
              "score": profitability_score,
              "avg_net_revenue": avg_net,
              "label": f"${round(avg_net):,} avg net to venue" if avg_net else "Insufficient data",
              "weight": "25%"
            },
            "consistency": {
              "score": consistency_score,
              "label": f"Based on {len(draw_ratios)} shows" if draw_ratios else "Insufficient data",
              "weight": "10%"
            }
          },
          "recommendations": {
            "attendance": {
              "suggested": round(safe_avg(attendances)) if attendances else None,
              "range_low": min(attendances) if attendances else None,
              "range_high": max(attendances) if attendances else None,
              "based_on": len(attendances)
            },
            "spend_per_head": {
              "suggested": round(avg_sph, 2) if avg_sph else None,
              "based_on": len(sph_actuals)
            },
            "door_split_promoter": {
              "suggested": safe_avg(door_splits),
              "based_on": len(door_splits)
            },
            "bar_split_promoter": {
              "suggested": safe_avg(bar_splits),
              "based_on": len(bar_splits)
            },
            "effective_split_avg": safe_avg(effective_splits),
            "avg_net_to_venue": avg_net,
            "avg_artist_fee": safe_avg(artist_fees),
            "best_net": sorted_by_net[0][1] if sorted_by_net else None,
            "worst_net": sorted_by_net[-1][1] if sorted_by_net else None,
          },
          "history": history
        }

    # ── Artist Intelligence ───────────────────────────────────────────────────

    @api.get("/artist-intelligence")
    def get_artist_intelligence(artist: str, event_type: str = None):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                  e.id, e.event_name, e.event_date, e.tier1_category,
                  e.promoter_name, e.artist_name, e.artist_genre,
                  e.expected_attendance, e.projected_bar_revenue,
                  e.door_split_promoter, e.bar_split_promoter,
                  e.artist_fee_landed, e.artist_fee_travel,
                  r.actual_attendance, r.actual_bar_revenue,
                  r.actual_door_revenue, r.actual_table_revenue,
                  r.spend_per_head_actual, r.net_revenue_actual,
                  r.actual_effective_split, r.review_status,
                  n.total_bar_sales, n.total_headcount,
                  n.effective_split_percentage
                FROM events e
                LEFT JOIN post_event_reviews r ON r.event_id = e.id
                LEFT JOIN night_of_actuals n ON n.event_id = e.id AND n.time_of_entry = 'Close'
                WHERE LOWER(e.artist_name) LIKE LOWER(:artist)
                  AND r.review_status = 'Complete'
                ORDER BY e.event_date DESC
                LIMIT 20
            """), {"artist": f"%{artist}%"}).fetchall()

            all_events = [dict(r._mapping) for r in rows]
            type_events = [e for e in all_events if not event_type or e.get("tier1_category") == event_type]

            if not all_events:
                return {"found": False, "artist": artist, "message": "No completed events found for this artist", "history": []}

            def safe_avg(lst):
                vals = [v for v in lst if v is not None]
                return round(sum(float(v) for v in vals) / len(vals), 2) if vals else None

            ref = type_events if type_events else all_events

            attendances = [e["actual_attendance"] for e in ref if e.get("actual_attendance")]
            draw_ratios = [e["actual_attendance"] / e["expected_attendance"] for e in ref if e.get("actual_attendance") and e.get("expected_attendance") and e["expected_attendance"] > 0]
            sph_actuals = [float(e["spend_per_head_actual"]) for e in ref if e.get("spend_per_head_actual")]
            net_revenues = [float(e["net_revenue_actual"]) for e in ref if e.get("net_revenue_actual")]
            artist_fees = [float(e["artist_fee_landed"]) for e in ref if e.get("artist_fee_landed")]
            effective_splits = [float(e["actual_effective_split"] or e["effective_split_percentage"]) for e in ref if e.get("actual_effective_split") or e.get("effective_split_percentage")]

            avg_draw = safe_avg(draw_ratios)
            avg_sph = safe_avg(sph_actuals)
            avg_net = safe_avg(net_revenues)
            avg_fee = safe_avg(artist_fees)

            history = []
            for e in ref[:8]:
                history.append({
                    "event_id": e["id"],
                    "event_name": e["event_name"],
                    "event_date": str(e["event_date"]),
                    "event_type": e["tier1_category"],
                    "promoter_name": e.get("promoter_name"),
                    "artist_name": e.get("artist_name"),
                    "expected_attendance": e.get("expected_attendance"),
                    "actual_attendance": e.get("actual_attendance"),
                    "draw_pct": round(e["actual_attendance"] / e["expected_attendance"] * 100, 1) if e.get("actual_attendance") and e.get("expected_attendance") and e["expected_attendance"] > 0 else None,
                    "spend_per_head_actual": float(e["spend_per_head_actual"]) if e.get("spend_per_head_actual") else None,
                    "actual_bar_revenue": float(e["actual_bar_revenue"] or e["total_bar_sales"]) if (e.get("actual_bar_revenue") or e.get("total_bar_sales")) else None,
                    "net_revenue_actual": float(e["net_revenue_actual"]) if e.get("net_revenue_actual") else None,
                    "artist_fee_landed": float(e["artist_fee_landed"]) if e.get("artist_fee_landed") else None,
                    "actual_effective_split": float(e["actual_effective_split"] or e["effective_split_percentage"]) if (e.get("actual_effective_split") or e.get("effective_split_percentage")) else None,
                })

            return {
                "found": True,
                "artist": artist,
                "event_type": event_type,
                "total_events": len(all_events),
                "type_events": len(type_events),
                "avg_draw_ratio": avg_draw,
                "avg_spend_per_head": avg_sph,
                "avg_net_to_venue": avg_net,
                "avg_artist_fee": avg_fee,
                "attendance_range": {"low": min(attendances), "high": max(attendances), "avg": round(safe_avg(attendances))} if attendances else None,
                "effective_split_avg": safe_avg(effective_splits),
                "recommendations": {
                    "attendance": {"suggested": round(safe_avg(attendances)) if attendances else None, "range_low": min(attendances) if attendances else None, "range_high": max(attendances) if attendances else None},
                    "spend_per_head": {"suggested": avg_sph},
                    "artist_fee": {"suggested": avg_fee, "based_on": len(artist_fees)},
                },
                "history": history
            }

    # ── Event Detail ─────────────────────────────────────────────────────────

    @api.get("/event-detail/{event_id}")
    def get_event_detail(event_id: int):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS event_item_sales (
                  id SERIAL PRIMARY KEY,
                  event_id INTEGER NOT NULL,
                  item_name TEXT,
                  item_category TEXT,
                  item_subcategory TEXT,
                  quantity_sold NUMERIC,
                  unit_price NUMERIC,
                  total_revenue NUMERIC,
                  cost_per_unit NUMERIC,
                  total_cost NUMERIC,
                  gross_margin NUMERIC,
                  created_at TIMESTAMP DEFAULT NOW()
                )
            """))
            conn.commit()
            event = conn.execute(text("""
                SELECT e.*,
                  COALESCE((
                      SELECT SUM(eis.total_revenue)
                      FROM event_item_sales eis
                      WHERE eis.event_id = e.id
                      AND eis.item_category IN ('Bar', 'Bottle Service')
                  ), e.revel_bar_gross, 0) AS net_bar_revenue,
                  r.actual_attendance, r.actual_bar_revenue, r.actual_door_revenue,
                  r.actual_table_revenue, r.artist_cost_actual, r.staffing_cost_actual,
                  r.spend_per_head_actual, r.net_revenue_actual, r.actual_effective_split,
                  r.crowd_demographic_observations, r.promoter_performance_notes,
                  r.what_to_replicate, r.what_to_change, r.review_status,
                  n.total_bar_sales, n.total_headcount, n.door_revenue_cash,
                  n.door_revenue_card, n.table_bottle_service, n.tables_active,
                  n.comps_total, n.voids, n.tips, n.tax_collected,
                  n.promoter_bar_payout, n.promoter_door_payout, n.promoter_table_payout,
                  n.artist_cost_paid_by_venue, n.effective_split_percentage, n.settlement_notes
                FROM events e
                LEFT JOIN post_event_reviews r ON r.event_id = e.id
                LEFT JOIN night_of_actuals n ON n.event_id = e.id AND n.time_of_entry = 'Close'
                WHERE e.id = :eid
            """), {"eid": event_id}).fetchone()
            if not event:
                raise HTTPException(status_code=404, detail="Event not found")
            d = dict(event._mapping)
            for f in ["event_date", "deposit_due_date", "balance_due_date", "created_at", "updated_at"]:
                if d.get(f): d[f] = str(d[f])
            for f in ["doors_open_time", "event_close_time"]:
                if d.get(f): d[f] = str(d[f])
            for k, v in d.items():
                if hasattr(v, '__class__') and v.__class__.__name__ == 'Decimal':
                    d[k] = float(v)

            items = conn.execute(text("""
                SELECT item_name, item_category, item_subcategory,
                  quantity_sold, unit_price, total_revenue, cost_per_unit, total_cost, gross_margin
                FROM event_item_sales
                WHERE event_id = :eid
                ORDER BY total_revenue DESC
            """), {"eid": event_id}).fetchall()

            item_list = []
            for row in items:
                item = dict(row._mapping)
                for k, v in item.items():
                    if hasattr(v, '__class__') and v.__class__.__name__ == 'Decimal':
                        item[k] = float(v)
                item_list.append(item)

            bottles = [i for i in item_list if i.get('item_category') == 'Bottle Service']
            spirits = [i for i in item_list if i.get('item_category') == 'Spirits']
            beer = [i for i in item_list if i.get('item_category') == 'Beer']
            na = [i for i in item_list if i.get('item_category') == 'Non-Alcoholic']
            cocktails = [i for i in item_list if i.get('item_category') == 'Cocktails']
            fees = [i for i in item_list if i.get('item_category') == 'Fees']

            def cat_total(lst): return round(sum(i.get('total_revenue') or 0 for i in lst), 2)
            def cat_cogs(lst): return round(sum(i.get('total_cost') or 0 for i in lst), 2)

            return {
                "event": d,
                "items": item_list,
                "summary": {
                    "bottle_service_revenue": cat_total(bottles),
                    "bottle_service_cogs": cat_cogs(bottles),
                    "bottle_count": sum((i.get('quantity_sold') or 0) for i in bottles if 'Table Charge' not in i.get('item_name', '')),
                    "spirits_revenue": cat_total(spirits),
                    "beer_revenue": cat_total(beer),
                    "cocktail_revenue": cat_total(cocktails),
                    "na_revenue": cat_total(na),
                    "fee_revenue": cat_total(fees),
                    "total_item_revenue": round(sum((i.get('total_revenue') or 0) for i in item_list), 2),
                    "total_item_cogs": round(sum((i.get('total_cost') or 0) for i in item_list), 2),
                },
                "categories": {
                    "bottles": bottles,
                    "spirits": spirits,
                    "beer": beer,
                    "cocktails": cocktails,
                    "na": na,
                    "fees": fees,
                }
            }

    # ── AI Talking Points ─────────────────────────────────────────────────────

    @api.post("/talking-points")
    async def get_talking_points(data: dict):
        try:
            import anthropic
            api_key = os.environ.get("ANTHRO_WORKSHOP_API_KEY")
            base_url = os.environ.get("ANTHRO_WORKSHOP_BASE_URL")
            client = anthropic.Anthropic(
                api_key=api_key,
                **({"base_url": base_url} if base_url else {})
            )
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1000,
                messages=[{"role": "user", "content": data.get("prompt", "")}]
            )
            return {"text": message.content[0].text}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

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

    # ── Calculator ────────────────────────────────────────────────────────────

    @api.get("/calculator", response_class=FileResponse)
    def serve_calculator():
        calc_path = os.path.join(os.path.dirname(__file__), "calculator.html")
        return FileResponse(calc_path, media_type="text/html", headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

    @api.get("/hub/calculator", response_class=FileResponse)
    def serve_calculator_hub():
        calc_path = os.path.join(os.path.dirname(__file__), "calculator.html")
        return FileResponse(calc_path, media_type="text/html", headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

    # ── Calendar ──────────────────────────────────────────────────────────────

    @api.get("/calendar", response_class=FileResponse)
    def serve_calendar():
        cal_path = os.path.join(os.path.dirname(__file__), "calendar.html")
        return FileResponse(cal_path, media_type="text/html", headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

    @api.get("/hub/calendar", response_class=FileResponse)
    def serve_calendar_hub():
        cal_path = os.path.join(os.path.dirname(__file__), "calendar.html")
        return FileResponse(cal_path, media_type="text/html", headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

    # ── Promoter Hub ──────────────────────────────────────────────────────────

    @api.get("/promoters", response_class=FileResponse)
    def serve_promoters():
        promo_path = os.path.join(os.path.dirname(__file__), "promoters.html")
        return FileResponse(promo_path, media_type="text/html", headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

    @api.get("/hub/promoters", response_class=FileResponse)
    def serve_promoters_hub():
        promo_path = os.path.join(os.path.dirname(__file__), "promoters.html")
        return FileResponse(promo_path, media_type="text/html", headers={"Cache-Control": "no-store, no-cache, must-revalidate"})

    @api.get("/promoters/summary")
    @api.get("/promoters/summary")
    def get_promoters_summary():
      if not engine:
        raise HTTPException(status_code=503, detail="DB not configured")
      with engine.connect() as conn:
        rows = conn.execute(text("""
          SELECT
            promoter_name,
            tier1_category,
            COUNT(*) AS total_events,
            ROUND(AVG(gross_revenue)::numeric, 0) AS avg_bar_revenue,
            ROUND(AVG(gross_revenue)::numeric, 0) AS avg_net,
            NULL::numeric AS draw_accuracy,
            NULL::numeric AS avg_sph,
            NULL::numeric AS avg_attendance,
            NULL::numeric AS avg_expected,
            NULL::numeric AS avg_split,
            NULL::numeric AS avg_artist_cost,
            0 AS nights_above,
            0 AS nights_below,
            0 AS nights_met
          FROM historical_events
          WHERE promoter_name IS NOT NULL
            AND TRIM(promoter_name) != ''
          GROUP BY promoter_name, tier1_category

          UNION ALL

          SELECT
            e.promoter_name,
            e.tier1_category,
            COUNT(e.id) AS total_events,
            ROUND(AVG(COALESCE(r.actual_bar_revenue, e.revel_bar_gross))::numeric, 0) AS avg_bar_revenue,
            ROUND(AVG(COALESCE(r.actual_bar_revenue, e.revel_bar_gross))::numeric, 0) AS avg_net,
            ROUND(AVG(
              CASE WHEN r.actual_attendance IS NOT NULL AND e.expected_attendance > 0
              THEN r.actual_attendance::float / NULLIF(e.expected_attendance, 0) * 100
              END
            )::numeric, 1) AS draw_accuracy,
            ROUND(AVG(CASE WHEN r.actual_attendance > 0
              THEN COALESCE(r.actual_bar_revenue, e.revel_bar_gross) / NULLIF(r.actual_attendance, 0)
              END)::numeric, 2) AS avg_sph,
            ROUND(AVG(r.actual_attendance)::numeric, 0) AS avg_attendance,
            ROUND(AVG(e.expected_attendance)::numeric, 0) AS avg_expected,
            ROUND(AVG(r.actual_effective_split)::numeric, 1) AS avg_split,
            ROUND(AVG(r.artist_cost_actual)::numeric, 0) AS avg_artist_cost,
            COUNT(CASE WHEN r.promoter_attendance_vs_projection = 'above' THEN 1 END) AS nights_above,
            COUNT(CASE WHEN r.promoter_attendance_vs_projection = 'below' THEN 1 END) AS nights_below,
            COUNT(CASE WHEN r.promoter_attendance_vs_projection = 'met'   THEN 1 END) AS nights_met
          FROM events e
          LEFT JOIN post_event_reviews r ON r.event_id = e.id
            AND LOWER(r.review_status) = 'complete'
          WHERE e.promoter_name IS NOT NULL
            AND (e.revel_bar_gross IS NOT NULL OR r.id IS NOT NULL)
          GROUP BY e.promoter_name, e.tier1_category
        """)).mappings().fetchall()

        merged = {}
        for r in rows:
          name = r["promoter_name"]
          if name not in merged:
            merged[name] = dict(r)
          else:
            ex = merged[name]
            ex["total_events"] = (ex["total_events"] or 0) + (r["total_events"] or 0)
            if r["avg_bar_revenue"] and (not ex["avg_bar_revenue"] or r["avg_bar_revenue"] > ex["avg_bar_revenue"]):
              ex["avg_bar_revenue"] = r["avg_bar_revenue"]
              ex["avg_net"] = r["avg_net"]
            for col in ["draw_accuracy", "avg_sph", "avg_attendance", "avg_expected", "avg_split", "avg_artist_cost"]:
              ex[col] = ex[col] if ex[col] is not None else r[col]
            for col in ["nights_above", "nights_below", "nights_met"]:
              ex[col] = (ex[col] or 0) + (r[col] or 0)

        return list(merged.values())

    @api.get("/promoters/names")
    def get_promoter_names():
      if not engine:
        raise HTTPException(status_code=503, detail="DB not configured")
      with engine.connect() as conn:
        rows = conn.execute(text("""
          SELECT promoter_name, SUM(event_count) as event_count
          FROM (
            SELECT promoter_name, COUNT(*) as event_count
            FROM historical_events
            WHERE promoter_name IS NOT NULL AND TRIM(promoter_name) != ''
            GROUP BY promoter_name

            UNION ALL

            SELECT promoter_name, COUNT(*) as event_count
            FROM events
            WHERE promoter_name IS NOT NULL AND TRIM(promoter_name) != ''
            GROUP BY promoter_name
          ) combined
          GROUP BY promoter_name
          ORDER BY event_count DESC, promoter_name ASC
        """)).fetchall()
      return [{"name": r[0], "event_count": r[1]} for r in rows]

    def get_events_by_date(start: str, end: str):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT
                  e.id, e.event_name, e.event_date, e.day_of_week,
                  e.tier1_category, e.tier2_subcategory,
                  e.promoter_name, e.artist_name, e.artist_genre,
                  e.expected_attendance, e.venue_capacity,
                  e.deal_structure_type,
                  e.projected_door_revenue, e.projected_bar_revenue, e.projected_table_revenue,
                  e.artist_fee_landed, e.artist_fee_travel,
                  e.doors_open_time, e.event_close_time,
                  e.status, e.notes,
                  e.revel_bar_gross,
                  COALESCE((
                      SELECT SUM(eis.total_revenue)
                      FROM event_item_sales eis
                      WHERE eis.event_id = e.id
                      AND eis.item_category IN ('Bar', 'Bottle Service')
                  ), e.revel_bar_gross, 0) AS net_bar_revenue,
                  COALESCE(e.artist_name, e.headliner) AS artist_name
                FROM events e
                WHERE e.event_date >= :start AND e.event_date <= :end

                UNION ALL

                SELECT
                  h.id, h.event_name, h.event_date, NULL AS day_of_week,
                  h.tier1_category, h.tier2_subcategory,
                  h.promoter_name, h.artist_name, NULL AS artist_genre,
                  h.attendance AS expected_attendance, NULL AS venue_capacity,
                  NULL AS deal_structure_type,
                  NULL AS projected_door_revenue, NULL AS projected_bar_revenue, NULL AS projected_table_revenue,
                  NULL AS artist_fee_landed, NULL AS artist_fee_travel,
                  NULL AS doors_open_time, NULL AS event_close_time,
                  'completed' AS status, NULL AS notes,
                  h.gross_revenue AS revel_bar_gross,
                  h.gross_revenue AS net_bar_revenue,
                  h.artist_name
                FROM historical_events h
                WHERE h.event_date >= :start AND h.event_date <= :end

                ORDER BY event_date ASC
            """), {"start": start, "end": end}).fetchall()

            result = []
            for r in rows:
                d = dict(r._mapping)
                if d.get("event_date"):
                    d["event_date"] = str(d["event_date"])
                if d.get("doors_open_time"):
                    d["doors_open_time"] = str(d["doors_open_time"])
                if d.get("event_close_time"):
                    d["event_close_time"] = str(d["event_close_time"])
                for k, v in d.items():
                    if hasattr(v, '__class__') and v.__class__.__name__ == 'Decimal':
                        d[k] = float(v)
                result.append(d)
            return result

    # ── Deal Intelligence — Distributors ──────────────────────────────────────

    @api.get("/distributors")
    def list_distributors():
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT * FROM distributors WHERE active = TRUE ORDER BY name
            """)).mappings().fetchall()
        return [dict(r) for r in rows]

    @api.post("/distributors")
    def create_distributor(body: dict):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            row = conn.execute(text("""
                INSERT INTO distributors
                  (name, license_number, provi_connected, primary_rep_name,
                   primary_rep_email, primary_rep_phone, last_contact_date,
                   deal_sheet_received, deal_sheet_date, notes)
                VALUES
                  (:name, :license_number, :provi_connected, :primary_rep_name,
                   :primary_rep_email, :primary_rep_phone, :last_contact_date,
                   :deal_sheet_received, :deal_sheet_date, :notes)
                RETURNING *
            """), {
                "name": body.get("name"),
                "license_number": body.get("license_number"),
                "provi_connected": body.get("provi_connected", False),
                "primary_rep_name": body.get("primary_rep_name"),
                "primary_rep_email": body.get("primary_rep_email"),
                "primary_rep_phone": body.get("primary_rep_phone"),
                "last_contact_date": body.get("last_contact_date"),
                "deal_sheet_received": body.get("deal_sheet_received", False),
                "deal_sheet_date": body.get("deal_sheet_date"),
                "notes": body.get("notes"),
            }).mappings().fetchone()
            conn.commit()
        return dict(row)

    @api.put("/distributors/{dist_id}")
    def update_distributor(dist_id: int, body: dict):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            row = conn.execute(text("""
                UPDATE distributors SET
                  name = COALESCE(:name, name),
                  license_number = COALESCE(:license_number, license_number),
                  provi_connected = COALESCE(:provi_connected, provi_connected),
                  primary_rep_name = COALESCE(:primary_rep_name, primary_rep_name),
                  primary_rep_email = COALESCE(:primary_rep_email, primary_rep_email),
                  primary_rep_phone = COALESCE(:primary_rep_phone, primary_rep_phone),
                  last_contact_date = COALESCE(:last_contact_date, last_contact_date),
                  deal_sheet_received = COALESCE(:deal_sheet_received, deal_sheet_received),
                  deal_sheet_date = COALESCE(:deal_sheet_date, deal_sheet_date),
                  notes = COALESCE(:notes, notes),
                  updated_at = NOW()
                WHERE id = :id
                RETURNING *
            """), {**body, "id": dist_id}).mappings().fetchone()
            conn.commit()
        if not row:
            raise HTTPException(status_code=404, detail="Distributor not found")
        return dict(row)

    # ── Deal Intelligence — Product Catalog ───────────────────────────────────

    @api.get("/catalog")
    def list_catalog(distributor_id: Optional[int] = None, category: Optional[str] = None):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT p.*, d.name AS distributor_name
                FROM product_catalog p
                JOIN distributors d ON d.id = p.distributor_id
                WHERE p.active = TRUE
                  AND (:distributor_id IS NULL OR p.distributor_id = :distributor_id)
                  AND (:category IS NULL OR p.category = :category)
                ORDER BY d.name, p.category, p.product_name
            """), {"distributor_id": distributor_id, "category": category}).mappings().fetchall()
        return [dict(r) for r in rows]

    @api.post("/catalog/import")
    async def import_catalog(request: Request):
        """
        Accepts JSON array. Each record may include nested deal_schedules list.
        { distributor_id, product_name, category, subcategory,
          frontline_price_case, source, deal_schedules: [...] }
        """
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        body = await request.json()
        created_products = 0
        created_deals = 0
        with engine.connect() as conn:
            for item in body:
                source = item.get("source", "deal_sheet")
                if not source:
                    raise HTTPException(status_code=400, detail="source is required on every record")
                row = conn.execute(text("""
                    INSERT INTO product_catalog
                      (distributor_id, sku, product_name, brand, category, subcategory,
                       unit_size_ml, case_pack, frontline_price_unit, frontline_price_case,
                       price_effective_date, source, provi_sku_id)
                    VALUES
                      (:distributor_id, :sku, :product_name, :brand, :category, :subcategory,
                       :unit_size_ml, :case_pack, :frontline_price_unit, :frontline_price_case,
                       :price_effective_date, :source, :provi_sku_id)
                    RETURNING id
                """), {
                    "distributor_id": item.get("distributor_id"),
                    "sku": item.get("sku"),
                    "product_name": item.get("product_name"),
                    "brand": item.get("brand"),
                    "category": item.get("category"),
                    "subcategory": item.get("subcategory"),
                    "unit_size_ml": item.get("unit_size_ml"),
                    "case_pack": item.get("case_pack"),
                    "frontline_price_unit": item.get("frontline_price_unit"),
                    "frontline_price_case": item.get("frontline_price_case"),
                    "price_effective_date": item.get("price_effective_date"),
                    "source": source,
                    "provi_sku_id": item.get("provi_sku_id"),
                }).mappings().fetchone()
                product_id = row["id"]
                created_products += 1
                for ds in item.get("deal_schedules", []):
                    ds_source = ds.get("source", source)
                    if not ds_source:
                        raise HTTPException(status_code=400, detail="source is required on deal_schedules")
                    conn.execute(text("""
                        INSERT INTO deal_schedules
                          (product_catalog_id, deal_type, quantity_threshold_cases,
                           free_cases_awarded, discounted_price_case, discount_pct,
                           condition_text, valid_from, valid_to, source,
                           source_file_ref, state_posting_ref)
                        VALUES
                          (:product_catalog_id, :deal_type, :quantity_threshold_cases,
                           :free_cases_awarded, :discounted_price_case, :discount_pct,
                           :condition_text, :valid_from, :valid_to, :source,
                           :source_file_ref, :state_posting_ref)
                    """), {
                        "product_catalog_id": product_id,
                        "deal_type": ds.get("deal_type", "quantity_discount"),
                        "quantity_threshold_cases": ds.get("quantity_threshold_cases"),
                        "free_cases_awarded": ds.get("free_cases_awarded"),
                        "discounted_price_case": ds.get("discounted_price_case"),
                        "discount_pct": ds.get("discount_pct"),
                        "condition_text": ds.get("condition_text"),
                        "valid_from": ds.get("valid_from"),
                        "valid_to": ds.get("valid_to"),
                        "source": ds_source,
                        "source_file_ref": ds.get("source_file_ref"),
                        "state_posting_ref": ds.get("state_posting_ref"),
                    })
                    created_deals += 1
            conn.commit()
        return {"created_products": created_products, "created_deals": created_deals}

    # ── Deal Intelligence — Deal Schedules ────────────────────────────────────

    @api.get("/deals")
    def list_deals(distributor_id: Optional[int] = None, active_only: bool = True):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ds.*, p.product_name, p.brand, p.category, p.subcategory,
                       p.frontline_price_case, p.sku, d.name AS distributor_name, d.id AS distributor_id
                FROM deal_schedules ds
                JOIN product_catalog p ON p.id = ds.product_catalog_id
                JOIN distributors d ON d.id = p.distributor_id
                WHERE (:active_only = FALSE OR ds.active = TRUE)
                  AND (:distributor_id IS NULL OR d.id = :distributor_id)
                ORDER BY d.name, p.category, p.product_name, ds.quantity_threshold_cases
            """), {"active_only": active_only, "distributor_id": distributor_id}).mappings().fetchall()
        return [dict(r) for r in rows]

    @api.get("/deals/discrepancies")
    def list_discrepancies():
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ds.*, p.product_name, p.brand, p.category, p.subcategory,
                       p.frontline_price_case, p.sku, d.name AS distributor_name
                FROM deal_schedules ds
                JOIN product_catalog p ON p.id = ds.product_catalog_id
                JOIN distributors d ON d.id = p.distributor_id
                WHERE ds.discrepancy_flag = TRUE
                ORDER BY d.name, p.product_name
            """)).mappings().fetchall()
        return [dict(r) for r in rows]

    @api.post("/deals/verify")
    def verify_deal(body: dict):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        deal_id = body.get("deal_id")
        if not deal_id:
            raise HTTPException(status_code=400, detail="deal_id required")
        with engine.connect() as conn:
            row = conn.execute(text("""
                UPDATE deal_schedules
                SET verified = TRUE,
                    verified_date = CURRENT_DATE,
                    verified_by = :verified_by,
                    discrepancy_notes = COALESCE(:notes, discrepancy_notes),
                    updated_at = NOW()
                WHERE id = :id
                RETURNING *
            """), {
                "id": deal_id,
                "verified_by": body.get("verified_by"),
                "notes": body.get("notes"),
            }).mappings().fetchone()
            conn.commit()
        if not row:
            raise HTTPException(status_code=404, detail="Deal not found")
        return dict(row)

    # ── Deal Intelligence — Purchase Recommendations ──────────────────────────

    @api.get("/recommendations")
    def list_recommendations(lookahead_days: int = 60, status: Optional[str] = None):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT pr.*, p.product_name, p.brand, p.category, p.subcategory,
                       p.frontline_price_case, d.name AS distributor_name,
                       ds.deal_type, ds.quantity_threshold_cases AS deal_qty_threshold,
                       ds.discounted_price_case, ds.discount_pct
                FROM purchase_recommendations pr
                JOIN product_catalog p ON p.id = pr.product_catalog_id
                JOIN distributors d ON d.id = p.distributor_id
                LEFT JOIN deal_schedules ds ON ds.id = pr.deal_schedule_id
                WHERE (:status IS NULL OR pr.status = :status)
                ORDER BY pr.recommended_order_date ASC NULLS LAST, pr.estimated_saving_total DESC
            """), {"status": status}).mappings().fetchall()
        return [dict(r) for r in rows]

    @api.post("/recommendations/generate")
    async def generate_recommendations(request: Request):
        """
        Recommendation engine:
        1. Pull confirmed events in next N days from events table
        2. For each event look up tier1_category + expected_attendance
        3. Multiply attendance × consumption_model rates → projected cases per subcategory
        4. Sum projected cases by subcategory across the full window
        5. Match subcategory totals against active deal_schedules
        6. Where projected volume >= threshold: create recommendation record
        7. Calculate estimated_saving vs frontline × case count
        """
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        lookahead_days = body.get("lookahead_days", 60)

        with engine.connect() as conn:
            # Step 1-3: pull events and consumption rates
            events_rows = conn.execute(text("""
                SELECT e.id, e.event_name, e.event_date, e.tier1_category,
                       e.expected_attendance
                FROM events e
                WHERE e.status = 'confirmed'
                  AND e.event_date >= CURRENT_DATE
                  AND e.event_date <= CURRENT_DATE + :lookahead_days
                  AND e.expected_attendance IS NOT NULL
                  AND e.expected_attendance > 0
            """), {"lookahead_days": lookahead_days}).mappings().fetchall()

            if not events_rows:
                return {"message": "No confirmed events with attendance in lookahead window", "generated": 0}

            # Step 4: aggregate projected cases by subcategory
            subcategory_cases: dict = {}  # subcategory -> total projected cases
            for ev in events_rows:
                category = (ev["tier1_category"] or "").lower()
                attendance = float(ev["expected_attendance"])
                rates = conn.execute(text("""
                    SELECT product_subcategory, cases_per_100_guests
                    FROM event_consumption_model
                    WHERE event_tier1_category = :category
                """), {"category": category}).mappings().fetchall()
                for rate in rates:
                    sub = rate["product_subcategory"]
                    projected = attendance / 100.0 * float(rate["cases_per_100_guests"])
                    subcategory_cases[sub] = subcategory_cases.get(sub, 0.0) + projected

            if not subcategory_cases:
                return {"message": "No consumption model rates matched event categories", "generated": 0}

            # Step 5-7: match against active deals
            active_deals = conn.execute(text("""
                SELECT ds.id AS deal_id, ds.product_catalog_id, ds.deal_type,
                       ds.quantity_threshold_cases, ds.discounted_price_case,
                       ds.discount_pct,
                       p.subcategory, p.frontline_price_case
                FROM deal_schedules ds
                JOIN product_catalog p ON p.id = ds.product_catalog_id
                WHERE ds.active = TRUE
                  AND (ds.valid_to IS NULL OR ds.valid_to >= CURRENT_DATE)
                  AND ds.quantity_threshold_cases IS NOT NULL
            """)).mappings().fetchall()

            generated = 0
            order_date = datetime.now().date() + timedelta(days=7)

            for deal in active_deals:
                sub = deal["subcategory"]
                projected = subcategory_cases.get(sub, 0.0)
                threshold = deal["quantity_threshold_cases"]
                if projected < threshold:
                    continue

                # Calculate savings
                frontline = float(deal["frontline_price_case"] or 0)
                disc_price = deal["discounted_price_case"]
                disc_pct = deal["discount_pct"]
                if disc_price:
                    saving_per_case = frontline - float(disc_price)
                elif disc_pct:
                    saving_per_case = frontline * float(disc_pct) / 100.0
                else:
                    saving_per_case = 0.0

                cases_to_order = max(threshold, int(projected + 0.5))
                estimated_saving = saving_per_case * cases_to_order
                confidence = min(1.0, round(projected / max(threshold, 1) * 0.8, 2))

                conn.execute(text("""
                    INSERT INTO purchase_recommendations
                      (product_catalog_id, deal_schedule_id, lookahead_days,
                       projected_cases_needed, deal_threshold_cases, cases_to_order,
                       recommended_order_date, estimated_saving_total, saving_per_case,
                       confidence_score, status)
                    VALUES
                      (:product_catalog_id, :deal_schedule_id, :lookahead_days,
                       :projected_cases_needed, :deal_threshold_cases, :cases_to_order,
                       :recommended_order_date, :estimated_saving_total, :saving_per_case,
                       :confidence_score, 'pending')
                """), {
                    "product_catalog_id": deal["product_catalog_id"],
                    "deal_schedule_id": deal["deal_id"],
                    "lookahead_days": lookahead_days,
                    "projected_cases_needed": round(projected, 2),
                    "deal_threshold_cases": threshold,
                    "cases_to_order": cases_to_order,
                    "recommended_order_date": order_date,
                    "estimated_saving_total": round(estimated_saving, 2),
                    "saving_per_case": round(saving_per_case, 2),
                    "confidence_score": confidence,
                })
                generated += 1

            conn.commit()

        return {
            "generated": generated,
            "lookahead_days": lookahead_days,
            "subcategories_projected": {k: round(v, 2) for k, v in subcategory_cases.items()},
        }

    @api.patch("/recommendations/{rec_id}/status")
    def update_recommendation_status(rec_id: int, body: dict):
        if not engine:
            raise HTTPException(status_code=503, detail="DB not configured")
        allowed = {"pending", "ordered", "missed", "dismissed"}
        new_status = body.get("status")
        if new_status not in allowed:
            raise HTTPException(status_code=400, detail=f"status must be one of {allowed}")
        with engine.connect() as conn:
            row = conn.execute(text("""
                UPDATE purchase_recommendations
                SET status = :status,
                    ordered_date = CASE WHEN :status = 'ordered' THEN CURRENT_DATE ELSE ordered_date END,
                    ordered_cases = COALESCE(:ordered_cases, ordered_cases),
                    notes = COALESCE(:notes, notes),
                    updated_at = NOW()
                WHERE id = :id
                RETURNING *
            """), {
                "id": rec_id,
                "status": new_status,
                "ordered_cases": body.get("ordered_cases"),
                "notes": body.get("notes"),
            }).mappings().fetchone()
            conn.commit()
        if not row:
            raise HTTPException(status_code=404, detail="Recommendation not found")
        return dict(row)

    # ── Deal Intelligence — Admin Cross-Reference UI ──────────────────────────

    @api.get("/deals/admin")
    def deals_admin_ui():
        from fastapi.responses import HTMLResponse
        html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Deal Intelligence — Admin</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  :root {
    --bg: #f5f5f5;
    --surface: #ffffff;
    --border: #e0e0e0;
    --text: #1a1a1a;
    --text-muted: #6b7280;
    --accent: #2563eb;
    --accent-hover: #1d4ed8;
    --danger: #dc2626;
    --warning-bg: #fef3c7;
    --warning-border: #f59e0b;
    --success: #16a34a;
    --radius: 8px;
    --shadow: 0 1px 3px rgba(0,0,0,0.1);
  }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: var(--bg); color: var(--text); font-size: 14px; line-height: 1.5; }
  header { background: var(--surface); border-bottom: 1px solid var(--border);
           padding: 16px 24px; display: flex; align-items: center; gap: 12px;
           box-shadow: var(--shadow); }
  header h1 { font-size: 18px; font-weight: 600; }
  header .badge { background: var(--accent); color: #fff; font-size: 11px;
                  padding: 2px 8px; border-radius: 12px; font-weight: 500; }
  .main { padding: 24px; max-width: 1400px; margin: 0 auto; }
  .controls { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
              padding: 16px 20px; display: flex; flex-wrap: wrap; gap: 16px; align-items: flex-end;
              margin-bottom: 20px; box-shadow: var(--shadow); }
  .control-group { display: flex; flex-direction: column; gap: 4px; }
  .control-group label { font-size: 12px; font-weight: 500; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; }
  select, input[type=text], input[type=file] {
    border: 1px solid var(--border); border-radius: 6px; padding: 7px 10px;
    font-size: 13px; background: var(--surface); color: var(--text); outline: none; min-width: 160px;
  }
  select:focus, input:focus { border-color: var(--accent); }
  .btn { padding: 7px 14px; border-radius: 6px; font-size: 13px; font-weight: 500;
         border: none; cursor: pointer; transition: background 0.15s; }
  .btn-primary { background: var(--accent); color: #fff; }
  .btn-primary:hover { background: var(--accent-hover); }
  .btn-outline { background: transparent; border: 1px solid var(--border); color: var(--text); }
  .btn-outline:hover { background: var(--bg); }
  .btn-danger { background: #fee2e2; color: var(--danger); border: 1px solid #fca5a5; }
  .btn-danger:hover { background: #fecaca; }
  .btn-success { background: #dcfce7; color: var(--success); border: 1px solid #86efac; }
  .btn-success:hover { background: #bbf7d0; }
  .section { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius);
             margin-bottom: 20px; box-shadow: var(--shadow); }
  .section-header { padding: 14px 20px; border-bottom: 1px solid var(--border);
                    display: flex; align-items: center; justify-content: space-between; }
  .section-title { font-size: 15px; font-weight: 600; }
  .count-badge { background: var(--bg); border: 1px solid var(--border); border-radius: 12px;
                 font-size: 12px; padding: 2px 8px; color: var(--text-muted); }
  .table-wrap { overflow-x: auto; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { padding: 10px 12px; text-align: left; font-size: 11px; font-weight: 600;
       text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-muted);
       background: var(--bg); border-bottom: 1px solid var(--border); white-space: nowrap; }
  td { padding: 10px 12px; border-bottom: 1px solid var(--border); vertical-align: top; }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: #f9fafb; }
  .flag-disc { color: var(--danger); font-weight: 600; }
  .flag-ok { color: var(--success); }
  .verified-yes { color: var(--success); font-weight: 500; }
  .verified-no { color: var(--text-muted); }
  .discrepancy-row td { background: var(--warning-bg) !important; }
  .discrepancy-row:hover td { background: #fde68a !important; }
  .pill { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 500; }
  .pill-spirits { background: #ede9fe; color: #6d28d9; }
  .pill-beer { background: #fef9c3; color: #854d0e; }
  .pill-wine { background: #fce7f3; color: #9d174d; }
  .pill-na { background: #e0f2fe; color: #0369a1; }
  .pill-state_posting { background: #dcfce7; color: var(--success); }
  .pill-deal_sheet { background: #dbeafe; color: var(--accent); }
  .pill-provi { background: #ede9fe; color: #6d28d9; }
  .pill-manual { background: #f1f5f9; color: #475569; }
  .empty-state { padding: 40px; text-align: center; color: var(--text-muted); }
  .import-panel { padding: 20px; }
  .import-panel textarea { width: 100%; min-height: 120px; border: 1px solid var(--border);
    border-radius: 6px; padding: 10px; font-family: monospace; font-size: 12px;
    resize: vertical; background: #f8fafc; }
  .import-actions { display: flex; gap: 10px; margin-top: 12px; align-items: center; }
  .status-msg { font-size: 13px; padding: 6px 12px; border-radius: 6px; }
  .status-ok { background: #dcfce7; color: var(--success); }
  .status-err { background: #fee2e2; color: var(--danger); }
  .loading { color: var(--text-muted); font-style: italic; }
  .modal-overlay { display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.4);
                   z-index: 100; align-items: center; justify-content: center; }
  .modal-overlay.open { display: flex; }
  .modal { background: var(--surface); border-radius: var(--radius); padding: 24px;
           min-width: 360px; max-width: 540px; box-shadow: 0 8px 32px rgba(0,0,0,0.18); }
  .modal h3 { font-size: 16px; font-weight: 600; margin-bottom: 16px; }
  .modal .field { margin-bottom: 12px; }
  .modal label { display: block; font-size: 12px; font-weight: 500; color: var(--text-muted);
                 margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.5px; }
  .modal input, .modal textarea { width: 100%; border: 1px solid var(--border); border-radius: 6px;
    padding: 7px 10px; font-size: 13px; }
  .modal .modal-actions { display: flex; gap: 10px; justify-content: flex-end; margin-top: 16px; }
  @media (max-width: 600px) { .main { padding: 12px; } .controls { flex-direction: column; } }
</style>
</head>
<body>
<header>
  <h1>Deal Intelligence</h1>
  <span class="badge">Admin</span>
  <span style="margin-left:auto;color:var(--text-muted);font-size:12px;">VenueOS · Internal use only</span>
</header>

<div class="main">

  <!-- Controls -->
  <div class="controls">
    <div class="control-group">
      <label>Distributor</label>
      <select id="filter-dist">
        <option value="">All distributors</option>
      </select>
    </div>
    <div class="control-group">
      <label>Source</label>
      <select id="filter-source">
        <option value="">All sources</option>
        <option value="state_posting">State posting</option>
        <option value="deal_sheet">Deal sheet</option>
        <option value="provi">Provi</option>
        <option value="manual">Manual</option>
      </select>
    </div>
    <div class="control-group">
      <label>Category</label>
      <select id="filter-cat">
        <option value="">All categories</option>
        <option value="spirits">Spirits</option>
        <option value="beer">Beer</option>
        <option value="wine">Wine</option>
        <option value="na">N/A</option>
      </select>
    </div>
    <div class="control-group" style="flex-direction:row;gap:8px;align-self:flex-end;">
      <button class="btn btn-primary" onclick="applyFilters()">Apply</button>
      <button class="btn btn-outline" onclick="resetFilters()">Reset</button>
    </div>
  </div>

  <!-- Discrepancy panel -->
  <div class="section" id="disc-section">
    <div class="section-header">
      <span class="section-title" style="color:var(--danger)">⚠ Discrepancies</span>
      <span class="count-badge" id="disc-count">—</span>
    </div>
    <div class="table-wrap" id="disc-table-wrap">
      <div class="empty-state loading">Loading…</div>
    </div>
  </div>

  <!-- Product catalog table -->
  <div class="section">
    <div class="section-header">
      <span class="section-title">Product Catalog &amp; Deal Schedules</span>
      <span class="count-badge" id="deals-count">—</span>
    </div>
    <div class="table-wrap" id="deals-table-wrap">
      <div class="empty-state loading">Loading…</div>
    </div>
  </div>

  <!-- Import panel -->
  <div class="section">
    <div class="section-header">
      <span class="section-title">Import Deal Sheet</span>
    </div>
    <div class="import-panel">
      <p style="margin-bottom:10px;color:var(--text-muted);font-size:13px;">
        Paste a JSON array of product objects. Each record requires <code>distributor_id</code>,
        <code>product_name</code>, <code>category</code>, and <code>source</code>.
        Optional nested <code>deal_schedules</code> array.
      </p>
      <textarea id="import-json" placeholder='[{"distributor_id":1,"product_name":"Don Julio Blanco","category":"spirits","subcategory":"tequila","frontline_price_case":420.00,"source":"deal_sheet","deal_schedules":[{"deal_type":"quantity_discount","quantity_threshold_cases":3,"discounted_price_case":395.00,"source":"deal_sheet"}]}]'></textarea>
      <div class="import-actions">
        <button class="btn btn-primary" onclick="runImport()">Import</button>
        <span id="import-status"></span>
      </div>
    </div>
  </div>

</div>

<!-- Verify modal -->
<div class="modal-overlay" id="verify-modal">
  <div class="modal">
    <h3>Verify Deal Record</h3>
    <input type="hidden" id="verify-deal-id">
    <div class="field">
      <label>Verified by</label>
      <input type="text" id="verify-by" placeholder="Name">
    </div>
    <div class="field">
      <label>Notes (optional)</label>
      <textarea id="verify-notes" style="min-height:70px;resize:vertical;" placeholder="Any discrepancy notes…"></textarea>
    </div>
    <div class="modal-actions">
      <button class="btn btn-outline" onclick="closeVerifyModal()">Cancel</button>
      <button class="btn btn-success" onclick="submitVerify()">Mark Verified</button>
    </div>
  </div>
</div>

<script>
const BASE = '';
let allDeals = [];
let allDiscs = [];

async function loadDistributors() {
  const r = await fetch(BASE + '/api/distributors');
  const data = await r.json();
  const sel = document.getElementById('filter-dist');
  data.forEach(d => {
    const o = document.createElement('option');
    o.value = d.id;
    o.textContent = d.name;
    sel.appendChild(o);
  });
}

async function loadDeals(distId, cat) {
  const params = new URLSearchParams();
  if (distId) params.set('distributor_id', distId);
  if (cat) params.set('category', cat);
  const r = await fetch(BASE + '/api/deals?' + params.toString());
  allDeals = await r.json();
  renderDeals();
}

async function loadDiscrepancies() {
  const r = await fetch(BASE + '/api/deals/discrepancies');
  allDiscs = await r.json();
  renderDiscrepancies();
}

function sourcePill(src) {
  const cls = 'pill pill-' + (src || 'manual');
  return `<span class="${cls}">${src || 'manual'}</span>`;
}

function catPill(cat) {
  const cls = 'pill pill-' + (cat || 'na');
  return `<span class="${cls}">${cat || '—'}</span>`;
}

function fmt(v) { return v != null ? Number(v).toFixed(2) : '—'; }

function renderDeals() {
  const source = document.getElementById('filter-source').value;
  let rows = allDeals;
  if (source) rows = rows.filter(r => r.source === source);

  document.getElementById('deals-count').textContent = rows.length + ' records';
  const wrap = document.getElementById('deals-table-wrap');
  if (!rows.length) { wrap.innerHTML = '<div class="empty-state">No records match filters.</div>'; return; }

  wrap.innerHTML = `<table>
    <thead><tr>
      <th>Distributor</th><th>SKU</th><th>Product</th><th>Category</th><th>Subcategory</th>
      <th>Frontline/case</th><th>Deal type</th><th>Threshold</th><th>Disc. price/case</th>
      <th>Disc. %</th><th>Source</th><th>Verified</th><th>Discrepancy</th><th>Action</th>
    </tr></thead>
    <tbody>${rows.map(r => `
    <tr class="${r.discrepancy_flag ? 'discrepancy-row' : ''}">
      <td>${r.distributor_name || '—'}</td>
      <td>${r.sku || '—'}</td>
      <td style="max-width:180px">${r.product_name}</td>
      <td>${catPill(r.category)}</td>
      <td>${r.subcategory || '—'}</td>
      <td>$${fmt(r.frontline_price_case)}</td>
      <td>${r.deal_type}</td>
      <td>${r.quantity_threshold_cases != null ? r.quantity_threshold_cases + ' cs' : '—'}</td>
      <td>${r.discounted_price_case != null ? '$'+fmt(r.discounted_price_case) : '—'}</td>
      <td>${r.discount_pct != null ? r.discount_pct+'%' : '—'}</td>
      <td>${sourcePill(r.source)}</td>
      <td class="${r.verified ? 'verified-yes' : 'verified-no'}">${r.verified ? '✓ '+r.verified_by : 'Unverified'}</td>
      <td class="${r.discrepancy_flag ? 'flag-disc' : 'flag-ok'}">${r.discrepancy_flag ? '⚠ Yes' : '—'}</td>
      <td>${!r.verified ? `<button class="btn btn-success" style="font-size:12px;padding:4px 10px" onclick="openVerify(${r.id})">Verify</button>` : ''}</td>
    </tr>`).join('')}
    </tbody></table>`;
}

function renderDiscrepancies() {
  document.getElementById('disc-count').textContent = allDiscs.length + ' records';
  const wrap = document.getElementById('disc-table-wrap');
  if (!allDiscs.length) {
    wrap.innerHTML = '<div class="empty-state" style="color:var(--success)">✓ No discrepancies found.</div>';
    return;
  }
  wrap.innerHTML = `<table>
    <thead><tr>
      <th>Distributor</th><th>Product</th><th>Source</th>
      <th>Frontline/case</th><th>Disc. price (deal sheet)</th><th>State posting ref</th>
      <th>Notes</th><th>Action</th>
    </tr></thead>
    <tbody>${allDiscs.map(r => `
    <tr class="discrepancy-row">
      <td>${r.distributor_name}</td>
      <td>${r.product_name}</td>
      <td>${sourcePill(r.source)}</td>
      <td>$${fmt(r.frontline_price_case)}</td>
      <td>${r.discounted_price_case != null ? '$'+fmt(r.discounted_price_case) : '—'}</td>
      <td>${r.state_posting_ref || '—'}</td>
      <td style="max-width:220px;white-space:pre-wrap">${r.discrepancy_notes || '—'}</td>
      <td><button class="btn btn-success" style="font-size:12px;padding:4px 10px" onclick="openVerify(${r.id})">Verify</button></td>
    </tr>`).join('')}
    </tbody></table>`;
}

function applyFilters() {
  const distId = document.getElementById('filter-dist').value;
  const cat = document.getElementById('filter-cat').value;
  loadDeals(distId, cat);
}

function resetFilters() {
  document.getElementById('filter-dist').value = '';
  document.getElementById('filter-source').value = '';
  document.getElementById('filter-cat').value = '';
  loadDeals('', '');
}

async function runImport() {
  const raw = document.getElementById('import-json').value.trim();
  const status = document.getElementById('import-status');
  if (!raw) { status.innerHTML = '<span class="status-msg status-err">Paste JSON first.</span>'; return; }
  let payload;
  try { payload = JSON.parse(raw); } catch(e) { status.innerHTML = '<span class="status-msg status-err">Invalid JSON: ' + e.message + '</span>'; return; }
  status.innerHTML = '<span class="status-msg loading">Importing…</span>';
  try {
    const r = await fetch(BASE + '/api/catalog/import', {
      method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(payload)
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.detail || JSON.stringify(d));
    status.innerHTML = `<span class="status-msg status-ok">✓ Imported ${d.created_products} products, ${d.created_deals} deals.</span>`;
    loadDeals('','');
  } catch(e) {
    status.innerHTML = `<span class="status-msg status-err">Error: ${e.message}</span>`;
  }
}

function openVerify(dealId) {
  document.getElementById('verify-deal-id').value = dealId;
  document.getElementById('verify-by').value = '';
  document.getElementById('verify-notes').value = '';
  document.getElementById('verify-modal').classList.add('open');
}

function closeVerifyModal() {
  document.getElementById('verify-modal').classList.remove('open');
}

async function submitVerify() {
  const dealId = document.getElementById('verify-deal-id').value;
  const by = document.getElementById('verify-by').value.trim();
  const notes = document.getElementById('verify-notes').value.trim();
  const status = document.createElement('span');
  try {
    const r = await fetch(BASE + '/api/deals/verify', {
      method: 'POST', headers: {'Content-Type':'application/json'},
      body: JSON.stringify({deal_id: parseInt(dealId), verified_by: by || null, notes: notes || null})
    });
    if (!r.ok) { const d = await r.json(); throw new Error(d.detail); }
    closeVerifyModal();
    loadDeals(document.getElementById('filter-dist').value, document.getElementById('filter-cat').value);
    loadDiscrepancies();
  } catch(e) {
    alert('Verify failed: ' + e.message);
  }
}

// Close modal on overlay click
document.getElementById('verify-modal').addEventListener('click', function(e) {
  if (e.target === this) closeVerifyModal();
});

// Init
loadDistributors();
loadDeals('', '');
loadDiscrepancies();
</script>
</body>
</html>"""
        return HTMLResponse(content=html)

    # ── App wiring ────────────────────────────────────────────────────────────

    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI()
    app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
