"""
ingest_may1.py — Apply locked May 1 ruleset to event 131 (Disclosure / Therapy Sessions).

Idempotent. Run as a one-shot via the /api/admin/ingest-may1 endpoint.
"""

import os
from sqlalchemy import create_engine, text


# ─── Recipe unit costs (sourced from Recipe.xlsx · Cost Per cell) ─────────
# These get loaded into recipe_unit_costs once, then used to correct any
# event_item_sales row whose item_name maps to a recipe.
RECIPE_UNIT_COSTS = {
    # Pour recipes (1.5oz default unless noted)
    'Bombay Sapphire– 1.5 oz':              {'cost': 2.1419, 'portion': 1.5, 'type': 'pour'},
    'Casamigos Blanco 1.5oz':               {'cost': 2.4405, 'portion': 1.5, 'type': 'pour'},
    'Casamigos Repo 1.5oz':                 {'cost': 2.847,  'portion': 1.5, 'type': 'pour'},
    'Don Julio Blanco - 1.5oz pour':        {'cost': 3.0719, 'portion': 1.5, 'type': 'pour'},
    'Don Julio Repo - 1.5oz pour':          {'cost': 4.3158, 'portion': 1.5, 'type': 'pour'},
    'Milagro Repo - 1.5oz Pour':            {'cost': 1.7299, 'portion': 1.5, 'type': 'pour'},
    'Milagro Silver - 1.5oz Pour':          {'cost': 1.7299, 'portion': 1.5, 'type': 'pour'},
    'Patron Silver - 1.5oz pour':           {'cost': 3.0959, 'portion': 1.5, 'type': 'pour'},
    'Well Tequila – 1.5 oz':                {'cost': 0.4635, 'portion': 1.5, 'type': 'pour'},
    'Grey Goose - 1.5oz pour':              {'cost': 1.7237, 'portion': 1.5, 'type': 'pour'},
    "Tito's – 1.5 oz Pour":                 {'cost': 1.5294, 'portion': 1.5, 'type': 'pour'},
    'Well Vodka – 1.5 oz':                  {'cost': 0.3244, 'portion': 1.5, 'type': 'pour'},
    'Crown Royal 1.5oz Pour':               {'cost': 2.2866, 'portion': 1.5, 'type': 'pour'},
    'Crown Apple - 1.5oz Pour':             {'cost': 2.4617, 'portion': 1.5, 'type': 'pour'},
    'Fireball – 1.5 oz Pour':               {'cost': 1.1152, 'portion': 1.5, 'type': 'pour'},
    'Jack Daniels - 1.5oz Pour':            {'cost': 2.2246, 'portion': 1.5, 'type': 'pour'},
    'Jameson 1.5oz Pour':                   {'cost': 2.0333, 'portion': 1.5, 'type': 'pour'},
    'Makers Mark - 1.5oz Pour':             {'cost': 1.9495, 'portion': 1.5, 'type': 'pour'},
    'Slane 1.5oz Pour':                     {'cost': 1.3511, 'portion': 1.5, 'type': 'pour'},
    'Well Whiskey – 1.5 oz':                {'cost': 0.4762, 'portion': 1.5, 'type': 'pour'},
    'Woodford - 1.5oz Pour':                {'cost': 2.939,  'portion': 1.5, 'type': 'pour'},
    'Hennessy 1.5oz Pour':                  {'cost': 3.5091, 'portion': 1.5, 'type': 'pour'},
    'Bacardi - 1.5 oz Pour':                {'cost': 1.0958, 'portion': 1.5, 'type': 'pour'},
    'Well Rum – 1.5 oz':                    {'cost': 0.4966, 'portion': 1.5, 'type': 'pour'},
    'Well Gin – 1.5 oz':                    {'cost': 0.4072, 'portion': 1.5, 'type': 'pour'},
    'Buchanans - 1.5oz Pour':               {'cost': 2.6905, 'portion': 1.5, 'type': 'pour'},
    'Johnnie Walker Black 1.5oz Pour':      {'cost': 3.0013, 'portion': 1.5, 'type': 'pour'},
    'Jager Bomb':                           {'cost': 0.875,  'portion': 1,   'type': 'mix'},
    'Green Tea - Well':                     {'cost': 0.0738, 'portion': 1,   'type': 'mix'},
    'White Tea - Well':                     {'cost': 0.0738, 'portion': 1,   'type': 'mix'},
    'Well Lemon Drop':                      {'cost': 0.0643, 'portion': 1,   'type': 'mix'},
    'LIT Mix 1.5oz Pour':                   {'cost': 0.529,  'portion': 1.5, 'type': 'mix'},
    'Sex on the Beach':                     {'cost': 0.0738, 'portion': 1,   'type': 'mix'},
    'Tequila Sunrise':                      {'cost': 2.5799, 'portion': 1.5, 'type': 'mix'},
    'Add Red Bull12 Can':                   {'cost': 0.875,  'portion': 1,   'type': 'mix'},
    'Red Bull':                             {'cost': 1.75,   'portion': 1,   'type': 'can'},
    'Red Bull 6 Pack':                      {'cost': 10.5,   'portion': 1,   'type': 'can'},
    'Single Bottle Water':                  {'cost': 0.289,  'portion': 1,   'type': 'bottle'},
    'Bottle Water – 6 Pack':                {'cost': 1.734,  'portion': 1,   'type': 'pack'},
    'NUTRL Can':                            {'cost': 1.5,    'portion': 1,   'type': 'can'},
    'NUTRL Can 6 Pack':                     {'cost': 9.0,    'portion': 1,   'type': 'pack'},
    'Bud Light 16oz Bottle':                {'cost': 1.0042, 'portion': 1,   'type': 'bottle'},
    'Bud Lt 6 Pack':                        {'cost': 6.025,  'portion': 1,   'type': 'pack'},
    'Coors Banquet':                        {'cost': 1.1229, 'portion': 1,   'type': 'bottle'},
    'Corona 12oz Can':                      {'cost': 1.2667, 'portion': 1,   'type': 'can'},
    'Ultra 16oz Bottle':                    {'cost': 1.2667, 'portion': 1,   'type': 'bottle'},
    'Ultra 0.0 Can':                        {'cost': 1.2,    'portion': 1,   'type': 'can'},
    'Miller 16oz Bottle':                   {'cost': 1.1229, 'portion': 1,   'type': 'bottle'},
    'Modelo 12oz Can':                      {'cost': 1.1667, 'portion': 1,   'type': 'can'},
    # Flagged (needs update — recipe shows $0)
    'Jager - 1.5 oz Pour':                  {'cost': 0,      'portion': 1.5, 'type': 'pour', 'needs_update': True},
    'ONE59 Can':                            {'cost': 0,      'portion': 1,   'type': 'can',  'needs_update': True},
    'Appleton Estate – 1.5 oz':             {'cost': 0,      'portion': 1.5, 'type': 'pour', 'needs_update': True},
    'Jack Daniels Flavors - 1.5oz Po':      {'cost': 0,      'portion': 1.5, 'type': 'pour', 'needs_update': True},
}

# ─── Product-mix → recipe sheet mapping ───────────────────────────────────
PRODUCT_TO_RECIPE = {
    'Bud Light': 'Bud Light 16oz Bottle',
    'Bud Light 6 Pack': 'Bud Lt 6 Pack',
    'Coors Banquet': 'Coors Banquet',
    'Corona Extra': 'Corona 12oz Can',
    'Michelob Ultra': 'Ultra 16oz Bottle',
    'Michelob Ultra 0.0': 'Ultra 0.0 Can',
    'Miller Lite*': 'Miller 16oz Bottle',
    'Modelo Especial': 'Modelo 12oz Can',
    'Hennessy Cognac VS': 'Hennessy 1.5oz Pour',
    'LIT': 'LIT Mix 1.5oz Pour',
    'Sex on the Beach': 'Sex on the Beach',
    'Tequila Sunrise': 'Tequila Sunrise',
    'Bombay Sapphire': 'Bombay Sapphire– 1.5 oz',
    'Well Gin*': 'Well Gin – 1.5 oz',
    'Jagermeister Original': 'Jager - 1.5 oz Pour',
    'Add Red Bull': 'Add Red Bull12 Can',
    'Employee Red Bull': 'Add Red Bull12 Can',
    'ONE59 THC': 'ONE59 Can',
    'Red Bull': 'Red Bull',
    'Red Bull 6 pack': 'Red Bull 6 Pack',
    'Red Bull Sugar Free 6 Pack': 'Red Bull 6 Pack',
    'Water': 'Single Bottle Water',
    'Water 6 Pack': 'Bottle Water – 6 Pack',
    'NUTRL': 'NUTRL Can',
    'NUTRL 6 Pack': 'NUTRL Can 6 Pack',
    'Appleton Estate Aged Rum Signature 80': 'Appleton Estate – 1.5 oz',
    'Bacardi Rum Superior': 'Bacardi - 1.5 oz Pour',
    'Well Rum': 'Well Rum – 1.5 oz',
    'Buchanans Deluxe Aged 12 Years': 'Buchanans - 1.5oz Pour',
    'Johnnie Walker Black Label': 'Johnnie Walker Black 1.5oz Pour',
    'Green Tea': 'Green Tea - Well',
    'Jager Bomb': 'Jager Bomb',
    'Lemon Drop': 'Well Lemon Drop',
    'White Tea': 'White Tea - Well',
    'Casamigos Blanco': 'Casamigos Blanco 1.5oz',
    'Casamigos Reposado': 'Casamigos Repo 1.5oz',
    'Don Julio Blanco': 'Don Julio Blanco - 1.5oz pour',
    'Don Julio Reposado': 'Don Julio Repo - 1.5oz pour',
    'Milagro Reposado': 'Milagro Repo - 1.5oz Pour',
    'Milagro Silver': 'Milagro Silver - 1.5oz Pour',
    'Patron Silver 80': 'Patron Silver - 1.5oz pour',
    'Well Tequila': 'Well Tequila – 1.5 oz',
    'Grey Goose Original': 'Grey Goose - 1.5oz pour',
    'Grey Goose Original*': 'Grey Goose - 1.5oz pour',
    "Tito's Handmade Vodka*": "Tito's – 1.5 oz Pour",
    'Well Vodka*': 'Well Vodka – 1.5 oz',
    'Crown Royal Canadian Whiskey': 'Crown Royal 1.5oz Pour',
    'Crown Royal Regal Apple': 'Crown Apple - 1.5oz Pour',
    'Fireball Cinnamon Whisky': 'Fireball – 1.5 oz Pour',
    'Jack Daniels': 'Jack Daniels - 1.5oz Pour',
    'Jack Daniels Flavors $5': 'Jack Daniels Flavors - 1.5oz Po',
    'Jameson Irish Whiskey': 'Jameson 1.5oz Pour',
    'Makers Mark Bourbon': 'Makers Mark - 1.5oz Pour',
    'Slane Irish Whiskey': 'Slane 1.5oz Pour',
    'Well Whisky': 'Well Whiskey – 1.5 oz',
    'Woodford Reserve': 'Woodford - 1.5oz Pour',
}

# Special: zero-cogs override (Add Margarita is a tequila upcharge button, marginal mix-in)
ZERO_COGS_PRODUCTS = {'Add Margarita'}

# Table-service items that don't follow "Bottle/Magnum" naming but ARE table service
# (champagnes priced as bottles, premium tequilas served whole). Get 22% pour cost.
TABLE_SERVICE_RECLASS = {
    'Dom Perignon Champagne Brut',
    'Veuve Clicquot Yellow Label',
    'Moet & Chandon Champagne Nectar Imperial Rose',
    'Clase Azul Reposado',
    'Prince de RIchemont Brut Bottle',  # has "Bottle" but already caught
}

BOTTLE_STANDARD_POUR_COST = 0.22  # 22% — Atlanta nightlife industry standard for bottle service


def load_recipe_costs(conn):
    """Idempotent upsert of recipe unit costs into recipe_unit_costs table."""
    for name, info in RECIPE_UNIT_COSTS.items():
        conn.execute(text("""
            INSERT INTO recipe_unit_costs (recipe_name, unit_cost, portion_size, recipe_type, needs_update, updated_at)
            VALUES (:n, :c, :p, :t, :u, NOW())
            ON CONFLICT (recipe_name) DO UPDATE SET
                unit_cost = EXCLUDED.unit_cost,
                portion_size = EXCLUDED.portion_size,
                recipe_type = EXCLUDED.recipe_type,
                needs_update = EXCLUDED.needs_update,
                updated_at = NOW()
        """), {
            'n': name, 'c': info['cost'], 'p': info['portion'],
            't': info['type'], 'u': info.get('needs_update', False),
        })


def correct_event_cogs(conn, event_id: int):
    """
    Re-derive total_cost on each event_item_sales row using recipe lookup.
    Sets cogs_source to 'recipe' / 'flagged' / 'revel' as appropriate.
    Idempotent — safe to re-run.
    """
    rows = conn.execute(text("""
        SELECT id, item_name, quantity_sold, total_revenue, total_cost
        FROM event_item_sales WHERE event_id = :eid
    """), {'eid': event_id}).fetchall()

    corrections = 0
    flags = 0

    for r in rows:
        name = r.item_name or ''
        qty = float(r.quantity_sold or 0)
        revel_cost = float(r.total_cost or 0)
        rev = float(r.total_revenue or 0)

        # Identify if this is a table-service item (Bottle/Magnum naming OR explicit reclass)
        is_table = ('Bottle' in name or 'Magnum' in name or name in TABLE_SERVICE_RECLASS)

        if name in ZERO_COGS_PRODUCTS:
            new_cost = 0.0
            note = "Upcharge button — marginal mix-in cost ignored"
            source = 'recipe'
            corrections += 1
        elif is_table:
            # All table service: 22% standard pour cost (Revel typically reports $0)
            if revel_cost > 0 and revel_cost / max(rev, 1) <= 0.30:
                # Revel cost looks plausible (under 30% of revenue) — keep it
                new_cost = revel_cost
                note = "Table-service SKU — Revel cost retained"
                source = 'revel'
            elif rev > 0:
                new_cost = round(rev * BOTTLE_STANDARD_POUR_COST, 2)
                note = f"Table-service SKU — 22% standard pour cost (industry benchmark)"
                source = 'recipe'
                corrections += 1
            else:
                new_cost = 0
                note = "Table-service SKU with no revenue"
                source = 'revel'
        elif name in PRODUCT_TO_RECIPE:
            recipe_key = PRODUCT_TO_RECIPE[name]
            recipe = RECIPE_UNIT_COSTS.get(recipe_key, {})
            unit_cost = recipe.get('cost', 0)
            if unit_cost > 0:
                new_cost = round(qty * unit_cost, 2)
                note = f"Recipe: {recipe_key} (${unit_cost}/unit × {int(qty)} units)"
                source = 'recipe'
                corrections += 1
            else:
                # $0 recipe — keep revel and flag
                new_cost = revel_cost
                note = f"Recipe '{recipe_key}' shows $0 — needs update"
                source = 'flagged'
                flags += 1
        else:
            # No mapping — leave alone
            new_cost = revel_cost
            note = "No recipe mapping"
            source = 'revel'

        conn.execute(text("""
            UPDATE event_item_sales
            SET total_cost = :nc, cogs_source = :src, cogs_correction_note = :note,
                revel_original_cost = COALESCE(revel_original_cost, :rc)
            WHERE id = :id
        """), {'id': r.id, 'nc': new_cost, 'src': source, 'note': note, 'rc': revel_cost})

    return {'corrections': corrections, 'flags': flags}


def populate_may1_tickets(conn, event_id: int = 131):
    """Eventbrite tier breakdown + See Tickets for May 1 Disclosure."""
    conn.execute(text("DELETE FROM event_ticket_tiers WHERE event_id = :eid"), {'eid': event_id})

    eb_tiers = [
        ('General Admission (Final Tier)', 51, 65),
        ('VIP Tickets', 59, 100),
        ('Artist Presale (Tier 1)', 50, 45),
        ('Artist Presale (Tier 2)', 50, 55),
        ('Artist Presale (Final Tier)', 396, 65),
        ('Artist Presale (VIP)', 80, 85),
        ('Spotify Presale (VIP)', 3, 100),
        ('Spotify Presale (Final Tier)', 6, 65),
    ]
    for tier_name, qty, price in eb_tiers:
        conn.execute(text("""
            INSERT INTO event_ticket_tiers (event_id, platform, tier_name, qty, price, revenue, status)
            VALUES (:eid, 'eventbrite', :tn, :q, :p, :r, 'sold_out')
        """), {'eid': event_id, 'tn': tier_name, 'q': qty, 'p': price, 'r': qty * price})

    conn.execute(text("""
        INSERT INTO event_ticket_tiers (event_id, platform, tier_name, qty, price, revenue, status)
        VALUES (:eid, 'see_tickets', 'See Tickets (off-platform)', 230, 45, 10350, 'sold_out')
    """), {'eid': event_id})


def upsert_may1_costs(conn, event_id: int = 131):
    """Set May 1 costs from ops report + locked rules."""
    conn.execute(text("""
        INSERT INTO event_costs (
            event_id, security_total, security_staff_count,
            tipped_staff_count, tipped_hours_avg, hourly_wages_tipped_total,
            cleaning_total, production_staff_total, production_staff_count,
            cash_out_total, hospitality_rider_actual, tech_rider_actual,
            artist_fee_total, tips_collected_total,
            cc_processing_passthrough, excise_tax_collected
        ) VALUES (
            :eid, 4025, 23,
            23, 6.25, 313.38,
            492, 800, 5,
            1234, 1000, 800,
            65000, 12731.32,
            1792.17, 833.20
        )
        ON CONFLICT (event_id) DO UPDATE SET
            security_total = EXCLUDED.security_total,
            security_staff_count = EXCLUDED.security_staff_count,
            tipped_staff_count = EXCLUDED.tipped_staff_count,
            tipped_hours_avg = EXCLUDED.tipped_hours_avg,
            hourly_wages_tipped_total = EXCLUDED.hourly_wages_tipped_total,
            cleaning_total = EXCLUDED.cleaning_total,
            production_staff_total = EXCLUDED.production_staff_total,
            production_staff_count = EXCLUDED.production_staff_count,
            cash_out_total = EXCLUDED.cash_out_total,
            hospitality_rider_actual = EXCLUDED.hospitality_rider_actual,
            tech_rider_actual = EXCLUDED.tech_rider_actual,
            artist_fee_total = EXCLUDED.artist_fee_total,
            tips_collected_total = EXCLUDED.tips_collected_total,
            cc_processing_passthrough = EXCLUDED.cc_processing_passthrough,
            excise_tax_collected = EXCLUDED.excise_tax_collected,
            updated_at = NOW()
    """), {'eid': event_id})


def upsert_may1_event_fields(conn, event_id: int = 131):
    """Set Collectiv deal terms on the event row itself, plus projections."""
    conn.execute(text("""
        UPDATE events SET
            house_charge_base = 10500,
            collectiv_op_add = 1000,
            guest_list_count = 50,
            table_guest_count = 150,
            artist_fee_landed = COALESCE(NULLIF(artist_fee_landed, 0), 65000),
            -- Projections for May 1 Disclosure (sold-out night, projections were aggressive)
            -- Bar projection was higher than what 1,125 heads at $38 spend produced;
            -- door projection met (sold out), table projection met
            projected_bar_revenue   = 32000,
            projected_door_revenue  = 58000,
            projected_table_revenue = 24000,
            expected_attendance     = 1100,
            actual_attendance       = 1125,
            actual_bar_revenue      = 21107.24
        WHERE id = :eid
    """), {'eid': event_id})


def run_full_ingest(engine, event_id: int = 131):
    """Apply all May 1 corrections idempotently. Each step gets its own
    transaction so a failure in one step (e.g. constraint already exists)
    doesn't poison subsequent work.
    """
    # Step 0a: Ensure all required columns exist on event_costs.
    # Mirror of the migrations in /api/costs route — also run here in case the
    # costs route hasn't been hit yet on this DB.
    column_specs = [
        ("event_costs", "tech_rider_actual",         "NUMERIC DEFAULT 0"),
        ("event_costs", "tech_rider_estimate",       "NUMERIC DEFAULT 0"),
        ("event_costs", "cleaning_total",            "NUMERIC DEFAULT 492"),
        ("event_costs", "excise_tax_collected",      "NUMERIC DEFAULT 0"),
        ("event_costs", "cc_processing_passthrough", "NUMERIC DEFAULT 0"),
        ("event_costs", "security_staff_count",      "INTEGER DEFAULT 0"),
        ("event_costs", "tipped_staff_count",        "INTEGER DEFAULT 0"),
        ("event_costs", "tipped_hours_avg",          "NUMERIC DEFAULT 6.25"),
        ("event_costs", "production_staff_count",    "INTEGER DEFAULT 0"),
        ("events", "house_charge_base",         "NUMERIC"),
        ("events", "collectiv_op_add",          "NUMERIC DEFAULT 0"),
        ("events", "ticket_surcharge_revenue",  "NUMERIC"),
        ("events", "guest_list_count",          "INTEGER DEFAULT 0"),
        ("events", "table_guest_count",         "INTEGER DEFAULT 0"),
        ("event_item_sales", "cogs_source",          "TEXT DEFAULT 'revel'"),
        ("event_item_sales", "cogs_correction_note", "TEXT"),
        ("event_item_sales", "revel_original_cost",  "NUMERIC"),
    ]
    for table, col, dtype in column_specs:
        with engine.begin() as conn:
            try:
                conn.execute(text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {dtype}"))
            except Exception:
                pass  # column already exists or table doesn't — non-fatal

    # Step 0b: Ensure supporting tables exist
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS event_ticket_tiers (
              id SERIAL PRIMARY KEY,
              event_id INTEGER NOT NULL,
              platform TEXT NOT NULL,
              tier_name TEXT,
              qty INTEGER DEFAULT 0,
              price NUMERIC DEFAULT 0,
              revenue NUMERIC DEFAULT 0,
              status TEXT,
              created_at TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS recipe_unit_costs (
              id SERIAL PRIMARY KEY,
              recipe_name TEXT UNIQUE NOT NULL,
              unit_cost NUMERIC,
              portion_size NUMERIC,
              recipe_type TEXT,
              needs_update BOOLEAN DEFAULT FALSE,
              notes TEXT,
              updated_at TIMESTAMP DEFAULT NOW()
            )
        """))

    # Step 0c: Ensure unique constraint on event_costs.event_id (for ON CONFLICT)
    # First clean up any duplicate rows (keep most recent)
    with engine.begin() as conn:
        try:
            conn.execute(text("""
                DELETE FROM event_costs
                WHERE id NOT IN (
                    SELECT MAX(id) FROM event_costs GROUP BY event_id
                )
            """))
        except Exception:
            pass
    with engine.begin() as conn:
        try:
            conn.execute(text(
                "ALTER TABLE event_costs ADD CONSTRAINT event_costs_event_id_unique UNIQUE (event_id)"
            ))
        except Exception:
            pass  # constraint already exists — that's fine

    # Step 1: Load recipe unit costs
    with engine.begin() as conn:
        load_recipe_costs(conn)

    # Step 2: Correct COGS on existing item rows
    cogs_result = {'corrections': 0, 'flags': 0}
    with engine.begin() as conn:
        cogs_result = correct_event_cogs(conn, event_id)

    # Step 3: Populate ticket tiers
    with engine.begin() as conn:
        populate_may1_tickets(conn, event_id)

    # Step 4: Upsert costs row (uses ON CONFLICT — needs Step 0c constraint)
    with engine.begin() as conn:
        upsert_may1_costs(conn, event_id)

    # Step 5: Set event-level deal fields
    with engine.begin() as conn:
        upsert_may1_event_fields(conn, event_id)

    return {
        'event_id': event_id,
        'cogs_corrections': cogs_result['corrections'],
        'cogs_flags': cogs_result['flags'],
        'recipes_loaded': len(RECIPE_UNIT_COSTS),
        'eventbrite_tiers': 8,
    }
