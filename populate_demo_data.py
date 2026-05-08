"""
populate_demo_data.py — Bulk-populate all District events with the locked
ruleset for demo purposes. Idempotent.

Variance design (per Scott's spec):
  - Collectiv: ~30% land BELOW projections (carry gap visible)
  - Candela:  ~70% MEET or EXCEED projections
  - Utopia:   ~70% MEET or EXCEED projections
  - All others: ~level with projections (±15%)

Projections are reverse-engineered from actuals:
  - Bar projection = bar_actual / 0.65 (i.e., actual was 35% under for May 1)
  - Door projection = door_actual / 0.70 (30% under)
  - But for events that should MEET/EXCEED, we'll set actual ≥ projection
  - Costs roughly in line with actuals
  - Door and attendance in line with projections

Genre-band F&B per head targets:
  - College:    $12-15
  - Open Format: $25-35
  - Electronic: $30-45
  - Hip-hop:    $40-70
  - Latin:      $40-60
  - Corporate:  $35-75
"""

import random
import math
from sqlalchemy import text


# Variance behavior per promoter (probability of being BELOW projections)
PROMOTER_BELOW_RATE = {
    'Collectiv': 0.30,
    'Candela':   0.30,    # 70% meet/exceed → 30% below
    'RDJI':      0.30,    # Utopia
}
DEFAULT_BELOW_RATE = 0.45   # most other events: ~level (slight bias to under)

# Per-genre F&B target per head — used to back-derive bar revenue from headcount
GENRE_FB_TARGETS = {
    'electronic':         (30, 45),
    'latin':              (45, 60),     # latin runs heavy on tables
    'hip_hop_rnb':        (45, 65),
    'open_format':        (25, 35),
    'college_university': (12, 15),
    'corporate_private':  (50, 100),    # private buyouts
}

# Per-genre attendance baseline (headcount when not overridden)
GENRE_ATTENDANCE = {
    'electronic':         (650, 1100),
    'latin':              (350, 550),
    'hip_hop_rnb':        (450, 750),
    'open_format':        (250, 500),
    'college_university': (300, 700),
    'corporate_private':  (150, 250),
}

# Walk-up Square = 30% of pre-sold (per Scott's spec)
WALK_UP_PCT_OF_PRESOLD = 0.30

# Op carry variable — variance distribution
SECURITY_BASELINE_STAFF = 14            # most nights at floor
SECURITY_HEAVY_STAFF = 23               # ~15% of nights ceiling
SECURITY_HEAVY_PROBABILITY = 0.15
PRODUCTION_LABOR_FLOOR = 400
PRODUCTION_LABOR_CEILING = 800

# Riders — Collectiv standard for non-Disclosure
COLLECTIV_HOSPITALITY_STD = 800
COLLECTIV_TECH_STD = 500

# Locked event 131 (Disclosure) — don't touch, already canonically populated
LOCKED_EVENTS = {131}


def _seed_for_event(event_id: int) -> random.Random:
    """Deterministic per-event RNG so demo data is stable across re-runs."""
    return random.Random(event_id * 7919 + 11)


def _get_target_below_rate(promoter: str) -> float:
    return PROMOTER_BELOW_RATE.get(promoter or '', DEFAULT_BELOW_RATE)


def _get_attendance(genre: str, rng) -> int:
    lo, hi = GENRE_ATTENDANCE.get(genre, (300, 600))
    return rng.randint(lo, hi)


def _get_fb_target(genre: str, rng) -> float:
    lo, hi = GENRE_FB_TARGETS.get(genre, (25, 40))
    return rng.uniform(lo, hi)


def _is_collectiv(promoter: str) -> bool:
    return (promoter or '').strip().lower() == 'collectiv'


def _generate_event_scenario(event_id: int, promoter: str, genre: str, artist: str, event_date: str):
    """Return a dict with all the values to write to the event."""
    rng = _seed_for_event(event_id)
    below_rate = _get_target_below_rate(promoter)
    is_below = rng.random() < below_rate

    # Headcount — with some variance
    base_attendance = _get_attendance(genre, rng)
    fb_per_head_target = _get_fb_target(genre, rng)

    # Decide variance multiplier
    if is_below:
        # Below projections by 15-35%
        variance = rng.uniform(0.65, 0.85)
        bar_variance = rng.uniform(0.55, 0.75)        # bar takes a bigger hit
    else:
        # At or above by -10% to +20%
        variance = rng.uniform(0.90, 1.20)
        bar_variance = rng.uniform(0.85, 1.15)

    # ── Build actuals first ───────────────────────────────────────────────
    actual_attendance = int(base_attendance * variance)
    actual_bar = round(actual_attendance * fb_per_head_target * bar_variance, 2)

    # Table revenue — heavy for latin & hip-hop, light for college/open format
    table_pct = {
        'latin':              0.40,   # heavy table nights
        'hip_hop_rnb':        0.35,
        'electronic':         0.30,
        'open_format':        0.15,
        'college_university': 0.05,
        'corporate_private':  0.20,
    }.get(genre, 0.20)
    actual_table = round(actual_bar * table_pct / (1 - table_pct), 2)
    fb_total = actual_bar + actual_table

    # Door revenue — depends on event type
    if genre == 'corporate_private':
        actual_door = 0  # private buyout — no ticketed door
    else:
        # Average ticket price varies; back into door from headcount
        # Pre-sold = 75% of attendance, walk-up = 25%
        avg_ticket = {
            'electronic': 60, 'latin': 35, 'hip_hop_rnb': 45,
            'open_format': 25, 'college_university': 18, 'corporate_private': 0,
        }.get(genre, 35)
        presold_count = int(actual_attendance * 0.75)
        walk_up_count = int(actual_attendance * 0.25)
        actual_door = round(presold_count * avg_ticket + walk_up_count * (avg_ticket * 0.85), 2)

    # ── Now derive projections ────────────────────────────────────────────
    # Per Scott's spec: actuals are ~30% under projected, bar is ~35% under
    # We set projections ABOVE actuals so the variance shows
    # (or actuals slightly ABOVE projections for the meet/exceed cases)
    if is_below:
        proj_bar = round(actual_bar / 0.65, 2)
        proj_table = round(actual_table / 0.70, 2)
        proj_door = round(actual_door / 0.70, 2)
    else:
        # Met or exceeded — projections were modest, actuals beat them
        proj_bar = round(actual_bar / rng.uniform(1.00, 1.18), 2)
        proj_table = round(actual_table / rng.uniform(1.00, 1.15), 2)
        proj_door = round(actual_door / rng.uniform(0.92, 1.05), 2)  # door usually meets

    proj_attendance = int(actual_attendance / rng.uniform(0.92, 1.05))  # attendance usually meets

    # ── Op carry variable ──────────────────────────────────────────────────
    # Most nights at security floor; occasionally ceiling
    is_heavy_security = rng.random() < SECURITY_HEAVY_PROBABILITY
    sec_staff = SECURITY_HEAVY_STAFF if is_heavy_security else SECURITY_BASELINE_STAFF
    security = sec_staff * 175

    # Tipped staff — proportional to headcount
    if actual_attendance < 350:
        tipped_staff, tipped_hours = 12, 5.5
    elif actual_attendance < 600:
        tipped_staff, tipped_hours = 16, 6.0
    elif actual_attendance < 900:
        tipped_staff, tipped_hours = 20, 6.25
    else:
        tipped_staff, tipped_hours = 23, 6.25
    hourly_tipped = round(tipped_staff * tipped_hours * 2.18, 2)

    # Production labor — scaled to net sales
    net_sales_estimate = fb_total + actual_door
    if net_sales_estimate > 80000:
        production_labor = PRODUCTION_LABOR_CEILING
    elif net_sales_estimate > 40000:
        production_labor = round(rng.uniform(500, 700), 0)
    else:
        production_labor = PRODUCTION_LABOR_FLOOR
    prod_staff = 5 if production_labor >= 700 else (4 if production_labor >= 500 else 2)

    # Cash payouts (police, door girls, restroom, coat check) — variable
    cash_out = round(rng.uniform(800, 1500), 0)

    # Tips — typically 22-28% of bar
    tips = round(actual_bar * rng.uniform(0.22, 0.28), 2)

    # ── Riders (Collectiv only) ────────────────────────────────────────────
    if _is_collectiv(promoter):
        hospitality = COLLECTIV_HOSPITALITY_STD
        tech = COLLECTIV_TECH_STD
        # Disclosure (event 131) is locked — but for safety, override std for high-fee artists
        artist_fee = 22000 if artist and len(artist) > 5 else 15000  # rough demo numbers
    else:
        hospitality = 0
        tech = 0
        # Promoter-paid talent or corporate buyout — no artist fee on venue side typically
        artist_fee_map = {
            'Candela': 8000, 'RDJI': 5000, 'BOGi': 3500,
            'Bryan Michael Cox': 12000, 'Tyrell': 2500, 'Pratt': 2000,
        }
        artist_fee = artist_fee_map.get(promoter, 0)

    # ── Pass-throughs ──────────────────────────────────────────────────────
    sales_tax = round(fb_total * 0.089, 2)
    excise_tax = round(actual_bar * 0.03, 2)
    cc_processing = round(net_sales_estimate * 0.7 * 0.035, 2)

    # ── Walk-up Square (default for non-sold-out events) ──────────────────
    # 30% of pre-sold, but cap at remaining capacity assumption
    walk_up_qty = int(actual_attendance * 0.25)
    walk_up_revenue = round(walk_up_qty * (15 if genre == 'college_university' else 25), 2)

    return {
        'event_id': event_id,
        # Projections
        'projected_bar_revenue': proj_bar,
        'projected_door_revenue': proj_door,
        'projected_table_revenue': proj_table,
        'expected_attendance': proj_attendance,
        # Actuals (write to night_of_actuals + post_event_reviews)
        'actual_attendance': actual_attendance,
        'actual_bar_revenue': actual_bar,
        'actual_door_revenue': actual_door,
        'actual_table_revenue': actual_table,
        'total_bar_sales': actual_bar + actual_table,
        'table_bottle_service': actual_table,
        'door_revenue_card': round(actual_door * 0.70, 2),  # 70% pre-sold
        'door_revenue_cash': round(actual_door * 0.30, 2),  # 30% walk-up
        'total_headcount': actual_attendance,
        'tax_collected': sales_tax,
        'tips': tips,
        # Costs
        'security_total': security,
        'security_staff_count': sec_staff,
        'tipped_staff_count': tipped_staff,
        'tipped_hours_avg': tipped_hours,
        'hourly_wages_tipped_total': hourly_tipped,
        'cleaning_total': 492,
        'production_staff_total': production_labor,
        'production_staff_count': prod_staff,
        'cash_out_total': cash_out,
        'hospitality_rider_actual': hospitality,
        'tech_rider_actual': tech,
        'artist_fee_total': artist_fee,
        'tips_collected_total': tips,
        'cc_processing_passthrough': cc_processing,
        'excise_tax_collected': excise_tax,
        # Event-level Collectiv terms
        'house_charge_base': 10500 if _is_collectiv(promoter) else None,
        'collectiv_op_add': 1000 if _is_collectiv(promoter) else 0,
        # Headcount components — light table/GL for non-special events
        'table_guest_count': max(0, int(actual_attendance * 0.05)) if genre in ('electronic', 'latin', 'hip_hop_rnb') else 0,
        'guest_list_count': max(20, int(actual_attendance * 0.06)),
        # Walk-up Square (for ticket-tier population)
        'walk_up_qty': walk_up_qty,
        'walk_up_revenue': walk_up_revenue,
        # Variance flag for downstream debugging
        '_is_below': is_below,
        '_genre': genre,
        '_promoter': promoter,
    }


def populate_event(engine, event_id: int, promoter: str, genre: str, artist: str, event_date: str):
    """Apply scenario to one event idempotently."""
    if event_id in LOCKED_EVENTS:
        return {'event_id': event_id, 'skipped': True, 'reason': 'locked'}

    s = _generate_event_scenario(event_id, promoter, genre, artist, event_date)

    # ── Override with item-level reality if it exists ──────────────────────
    # Some events have real item sales already populated. Don't fight the data —
    # use actual item totals and back-derive a sensible headcount that puts
    # F&B/head inside the genre band.
    with engine.connect() as conn:
        item_totals = conn.execute(text("""
            SELECT
                COALESCE(SUM(total_revenue), 0) AS total_rev,
                COALESCE(SUM(CASE WHEN item_name ILIKE '%Bottle%' OR item_name ILIKE '%Magnum%'
                                  OR item_name IN ('Dom Perignon Champagne Brut', 'Veuve Clicquot Yellow Label',
                                                   'Moet & Chandon Champagne Nectar Imperial Rose',
                                                   'Clase Azul Reposado')
                              THEN total_revenue ELSE 0 END), 0) AS table_rev,
                COUNT(*) AS item_count
            FROM event_item_sales WHERE event_id = :eid
        """), {'eid': event_id}).fetchone()

    if item_totals and float(item_totals.total_rev) > 100:
        # Real items exist — use them and back-derive headcount
        actual_total_fb = float(item_totals.total_rev)
        actual_table = float(item_totals.table_rev)
        actual_bar = actual_total_fb - actual_table

        # Reverse: headcount = total_fb / target_per_head_in_band_midpoint
        lo, hi = GENRE_FB_TARGETS.get(genre, (25, 40))
        rng = _seed_for_event(event_id + 7)  # different seed for headcount vs scenario
        target_per_head = rng.uniform(lo, hi)

        # If "below" scenario, target lower spend per head (sparse crowd, lighter wallets)
        if s['_is_below']:
            target_per_head *= rng.uniform(0.85, 0.95)
        else:
            target_per_head *= rng.uniform(0.95, 1.10)

        actual_attendance_override = max(50, int(actual_total_fb / target_per_head))
        s['actual_attendance'] = actual_attendance_override
        s['total_headcount'] = actual_attendance_override
        s['actual_bar_revenue'] = round(actual_bar, 2)
        s['actual_table_revenue'] = round(actual_table, 2)
        s['total_bar_sales'] = round(actual_total_fb, 2)
        s['table_bottle_service'] = round(actual_table, 2)

        # Re-derive door / projections to match new attendance
        if genre != 'corporate_private':
            avg_ticket = {
                'electronic': 60, 'latin': 35, 'hip_hop_rnb': 45,
                'open_format': 25, 'college_university': 18,
            }.get(genre, 35)
            presold = int(actual_attendance_override * 0.75)
            walk_up_count = int(actual_attendance_override * 0.25)
            new_door = round(presold * avg_ticket + walk_up_count * (avg_ticket * 0.85), 2)
            s['actual_door_revenue'] = new_door
            s['door_revenue_card'] = round(new_door * 0.70, 2)
            s['door_revenue_cash'] = round(new_door * 0.30, 2)

        # Re-derive projections from variance flag
        if s['_is_below']:
            s['projected_bar_revenue']   = round(actual_bar / 0.65, 2)
            s['projected_table_revenue'] = round(actual_table / 0.70, 2)
            s['projected_door_revenue']  = round(s['actual_door_revenue'] / 0.70, 2)
        else:
            s['projected_bar_revenue']   = round(actual_bar / rng.uniform(1.00, 1.18), 2)
            s['projected_table_revenue'] = round(actual_table / rng.uniform(1.00, 1.15), 2)
            s['projected_door_revenue']  = round(s['actual_door_revenue'] / rng.uniform(0.92, 1.05), 2)

        s['expected_attendance'] = int(actual_attendance_override / rng.uniform(0.92, 1.05))

        # Re-derive cost variables that scale with headcount
        if actual_attendance_override < 350:
            s['tipped_staff_count'], s['tipped_hours_avg'] = 12, 5.5
        elif actual_attendance_override < 600:
            s['tipped_staff_count'], s['tipped_hours_avg'] = 16, 6.0
        elif actual_attendance_override < 900:
            s['tipped_staff_count'], s['tipped_hours_avg'] = 20, 6.25
        else:
            s['tipped_staff_count'], s['tipped_hours_avg'] = 23, 6.25
        s['hourly_wages_tipped_total'] = round(s['tipped_staff_count'] * s['tipped_hours_avg'] * 2.18, 2)

        # Re-derive table/GL counts
        s['table_guest_count'] = max(0, int(actual_attendance_override * 0.05)) if genre in ('electronic', 'latin', 'hip_hop_rnb') else 0
        s['guest_list_count'] = max(20, int(actual_attendance_override * 0.06))

    # Step 1: Update events table with projections + Collectiv terms
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE events SET
                projected_bar_revenue   = :pb,
                projected_door_revenue  = :pd,
                projected_table_revenue = :pt,
                expected_attendance     = :pa,
                actual_attendance       = COALESCE(:aa, actual_attendance),
                actual_bar_revenue      = COALESCE(:ab, actual_bar_revenue),
                house_charge_base       = :hcb,
                collectiv_op_add        = :coa,
                guest_list_count        = :gl,
                table_guest_count       = :tg
            WHERE id = :eid
        """), {
            'eid': event_id,
            'pb': s['projected_bar_revenue'], 'pd': s['projected_door_revenue'],
            'pt': s['projected_table_revenue'], 'pa': s['expected_attendance'],
            'aa': s['actual_attendance'], 'ab': s['actual_bar_revenue'],
            'hcb': s['house_charge_base'], 'coa': s['collectiv_op_add'],
            'gl': s['guest_list_count'], 'tg': s['table_guest_count'],
        })

    # Step 2: Upsert night_of_actuals (close-out entry)
    with engine.begin() as conn:
        existing = conn.execute(text(
            "SELECT id FROM night_of_actuals WHERE event_id = :eid AND time_of_entry ILIKE 'close%' LIMIT 1"
        ), {'eid': event_id}).fetchone()
        if existing:
            conn.execute(text("""
                UPDATE night_of_actuals SET
                    total_bar_sales        = :tbs,
                    table_bottle_service   = :tbb,
                    door_revenue_cash      = :drc,
                    door_revenue_card      = :drd,
                    total_headcount        = :thc,
                    tax_collected          = :tax,
                    tips                   = :tips,
                    updated_at             = NOW()
                WHERE id = :id
            """), {
                'id': existing.id, 'tbs': s['total_bar_sales'], 'tbb': s['table_bottle_service'],
                'drc': s['door_revenue_cash'], 'drd': s['door_revenue_card'],
                'thc': s['total_headcount'], 'tax': s['tax_collected'], 'tips': s['tips'],
            })
        else:
            conn.execute(text("""
                INSERT INTO night_of_actuals (
                    event_id, time_of_entry, total_bar_sales, table_bottle_service,
                    door_revenue_cash, door_revenue_card, total_headcount, tax_collected, tips
                ) VALUES (:eid, 'close', :tbs, :tbb, :drc, :drd, :thc, :tax, :tips)
            """), {
                'eid': event_id, 'tbs': s['total_bar_sales'], 'tbb': s['table_bottle_service'],
                'drc': s['door_revenue_cash'], 'drd': s['door_revenue_card'],
                'thc': s['total_headcount'], 'tax': s['tax_collected'], 'tips': s['tips'],
            })

    # Step 3: Upsert event_costs
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO event_costs (
                event_id, security_total, security_staff_count,
                tipped_staff_count, tipped_hours_avg, hourly_wages_tipped_total,
                cleaning_total, production_staff_total, production_staff_count,
                cash_out_total, hospitality_rider_actual, tech_rider_actual,
                artist_fee_total, tips_collected_total,
                cc_processing_passthrough, excise_tax_collected
            ) VALUES (
                :eid, :sec, :secs,
                :tpc, :tph, :hwt,
                :cln, :pls, :pss,
                :cot, :hra, :tra,
                :aft, :tit,
                :ccp, :etc
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
        """), {
            'eid': event_id,
            'sec': s['security_total'], 'secs': s['security_staff_count'],
            'tpc': s['tipped_staff_count'], 'tph': s['tipped_hours_avg'], 'hwt': s['hourly_wages_tipped_total'],
            'cln': s['cleaning_total'], 'pls': s['production_staff_total'], 'pss': s['production_staff_count'],
            'cot': s['cash_out_total'], 'hra': s['hospitality_rider_actual'], 'tra': s['tech_rider_actual'],
            'aft': s['artist_fee_total'], 'tit': s['tips_collected_total'],
            'ccp': s['cc_processing_passthrough'], 'etc': s['excise_tax_collected'],
        })

    # Step 4: Upsert post_event_reviews (for actuals)
    with engine.begin() as conn:
        existing = conn.execute(text(
            "SELECT id FROM post_event_reviews WHERE event_id = :eid LIMIT 1"
        ), {'eid': event_id}).fetchone()
        if existing:
            conn.execute(text("""
                UPDATE post_event_reviews SET
                    actual_attendance      = :aa,
                    actual_bar_revenue     = :ab,
                    actual_door_revenue    = :ad,
                    actual_table_revenue   = :at,
                    artist_cost_actual     = :ac
                WHERE id = :id
            """), {
                'id': existing.id, 'aa': s['actual_attendance'], 'ab': s['actual_bar_revenue'],
                'ad': s['actual_door_revenue'], 'at': s['actual_table_revenue'],
                'ac': s['artist_fee_total'],
            })
        else:
            conn.execute(text("""
                INSERT INTO post_event_reviews (
                    event_id, actual_attendance, actual_bar_revenue,
                    actual_door_revenue, actual_table_revenue, artist_cost_actual,
                    review_status
                ) VALUES (:eid, :aa, :ab, :ad, :at, :ac, 'completed')
            """), {
                'eid': event_id, 'aa': s['actual_attendance'], 'ab': s['actual_bar_revenue'],
                'ad': s['actual_door_revenue'], 'at': s['actual_table_revenue'],
                'ac': s['artist_fee_total'],
            })

    return {
        'event_id': event_id, 'genre': genre, 'promoter': promoter,
        'is_below': s['_is_below'],
        'projected_bar': s['projected_bar_revenue'],
        'actual_bar': s['actual_bar_revenue'],
        'projected_door': s['projected_door_revenue'],
        'actual_door': s['actual_door_revenue'],
        'attendance': s['actual_attendance'],
        'security': s['security_total'],
    }


def populate_all_events(engine):
    """Walk every event and populate."""
    with engine.connect() as conn:
        events = conn.execute(text("""
            SELECT id, event_name, event_date, tier1_category, promoter_name, artist_name
            FROM events
            ORDER BY event_date
        """)).fetchall()

    results = []
    for ev in events:
        try:
            r = populate_event(
                engine,
                event_id=ev.id,
                promoter=ev.promoter_name or '',
                genre=ev.tier1_category or 'open_format',
                artist=ev.artist_name or '',
                event_date=str(ev.event_date) if ev.event_date else '',
            )
            results.append(r)
        except Exception as ex:
            results.append({'event_id': ev.id, 'error': str(ex)})

    # Summary stats
    by_promoter = {}
    for r in results:
        if r.get('skipped') or r.get('error'):
            continue
        p = r.get('promoter', '?')
        by_promoter.setdefault(p, {'total': 0, 'below': 0})
        by_promoter[p]['total'] += 1
        if r.get('is_below'):
            by_promoter[p]['below'] += 1

    return {
        'total_events': len(events),
        'populated': len([r for r in results if not r.get('skipped') and not r.get('error')]),
        'skipped': len([r for r in results if r.get('skipped')]),
        'errors': [r for r in results if r.get('error')],
        'by_promoter_below_rate': {
            p: {
                'total': v['total'],
                'below': v['below'],
                'below_pct': round(v['below'] / v['total'] * 100, 1) if v['total'] > 0 else 0,
            }
            for p, v in by_promoter.items()
        },
    }
