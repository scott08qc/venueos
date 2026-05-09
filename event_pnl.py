"""
event_pnl.py — Canonical P&L calculation engine for District Atlanta event recaps.

Single source of truth. Takes raw event data + ops report + costs + items and
produces a fully-derived P&L with deal mechanics, recipe-corrected COGS,
ticket surcharge, riders, and operating carry attribution.

Rules locked May 8, 2026 — see methodology section in the dashboard for full ruleset.
"""

from typing import Optional


# ─── DISTRICT CANONICAL CONSTANTS ─────────────────────────────────────────
# Pulled from Operating_Cost_2025_v2_CANONICAL.xlsx, Section A
OP_CARRY_STATIC_PER_NIGHT = 8601.0           # rent, utilities, salaries, insurance, etc.
ANNUAL_OPERATING_NIGHTS = 140                # used for static-allocation math elsewhere

# Ticket surcharge — 15% additive on Eventbrite face value, splits 50/50
TICKET_SURCHARGE_RATE_EB = 0.15
EB_PLATFORM_FEE_RATE = 0.035                 # 3.5% of face
EB_PLATFORM_FEE_PER_TIX = 1.59               # + $1.59 / ticket

# Cleaning — fixed every night
CLEANING_STATIC = 492.0

# Hourly tipped wage (Georgia tipped base)
TIPPED_BASE_RATE = 2.18

# Static security baseline (annual / nights)
SECURITY_BASELINE_ANNUAL = 346435.0
SECURITY_BASELINE_PER_NIGHT = SECURITY_BASELINE_ANNUAL / ANNUAL_OPERATING_NIGHTS  # ≈ 2474.54
SECURITY_RATE_PER_STAFF = 175.0

# Production labor per-staff rate (varies; ceiling 5 × $160 = $800, floor 2 × $200 = $400)
PRODUCTION_LABOR_CEILING = 800.0
PRODUCTION_LABOR_FLOOR = 400.0

# Genre F&B-per-head bands (locked with Scott)
GENRE_FB_PER_HEAD_BANDS = {
    'college_university':   (12, 15),
    'open_format':          (25, 35),
    'electronic':           (30, 45),
    'hip_hop_rnb':          (40, 70),     # "$40+", upper bound estimated for AI flag logic
    'latin':                (40, 60),
    'corporate_private':    (35, 75),     # rough estimate, will refine
}

# Collectiv-specific deal terms
COLLECTIV_HOUSE_BASE_DEFAULT = 10500.0
COLLECTIV_OP_ADD_DEFAULT = 1000.0            # secret reduction Collectiv took
COLLECTIV_HOUSE_NET_DEFAULT = COLLECTIV_HOUSE_BASE_DEFAULT - COLLECTIV_OP_ADD_DEFAULT

# Collectiv non-Disclosure standard riders
COLLECTIV_HOSPITALITY_RIDER_STD = 800.0
COLLECTIV_TECH_RIDER_STD = 500.0

# CC processing — pass-through to customer (3.5% surcharge), not a P&L cost
CC_PROCESSING_RATE = 0.035


# ─── HELPERS ──────────────────────────────────────────────────────────────
def _f(v, default=0.0) -> float:
    """Safe float coercion for None / Decimal / str."""
    if v is None:
        return float(default)
    try:
        return float(v)
    except (TypeError, ValueError):
        return float(default)


def _is_collectiv(event: dict) -> bool:
    return (event.get('promoter_name') or '').strip().lower() == 'collectiv'


def _is_table_sku(item_name: str) -> bool:
    """
    Table revenue = SKUs with 'Bottle' or 'Magnum' in name, plus the manually
    reclassified champagnes / Clase Azul that don't follow naming convention.
    Once POS naming is fixed this list shrinks.
    """
    if not item_name:
        return False
    name = item_name.strip()
    if 'Bottle' in name or 'Magnum' in name:
        return True
    RECLASS = {
        'Dom Perignon Champagne Brut',
        'Veuve Clicquot Yellow Label',
        'Moet & Chandon Champagne Nectar Imperial Rose',
        'Clase Azul Reposado',
        'Prince de RIchemont Brut Bottle',  # already has Bottle but kept here for clarity
    }
    return name in RECLASS


# ─── MAIN CALCULATION ─────────────────────────────────────────────────────
def compute_event_pnl(
    event: dict,
    items: list,
    costs: dict,
    actuals: dict,
    ticket_breakdown: Optional[dict] = None,
) -> dict:
    """
    Compute the canonical event P&L.

    Inputs:
      event:              row from events table (dict)
      items:              list of event_item_sales rows (recipe-corrected costs)
      costs:              row from event_costs table (or {} if none)
      actuals:            row from night_of_actuals (or {} if none)
      ticket_breakdown:   optional structured ticket data:
                            {'eventbrite': [{'tier','qty','price'}, ...],
                             'see_tickets': {'qty','price'},
                             'walk_up_square': {'qty','revenue'},
                             'guest_list': N, 'table_guests': N}

    Output: dict with full derived P&L
    """
    event = event or {}
    costs = costs or {}
    actuals = actuals or {}
    ticket_breakdown = ticket_breakdown or {}

    # ─── F&B REVENUE (from item sales) ────────────────────────────────────
    bar_revenue = 0.0
    table_revenue = 0.0
    bar_cogs = 0.0
    table_cogs = 0.0
    cogs_corrections_made = 0
    cogs_zero_recipe_flags = 0

    for item in items:
        name = item.get('item_name') or ''
        rev = _f(item.get('total_revenue'))
        cogs = _f(item.get('total_cost'))
        source = (item.get('cogs_source') or '').lower()

        if source == 'recipe':
            cogs_corrections_made += 1
        elif source == 'flagged':
            cogs_zero_recipe_flags += 1

        if _is_table_sku(name):
            table_revenue += rev
            table_cogs += cogs
        else:
            bar_revenue += rev
            bar_cogs += cogs

    fb_revenue = bar_revenue + table_revenue
    total_cogs = bar_cogs + table_cogs

    # Fall back to actuals.total_bar_sales if no item-level data
    if fb_revenue == 0 and actuals.get('total_bar_sales'):
        fb_revenue = _f(actuals.get('total_bar_sales'))
        # bar_revenue gets the unattributed sum; table_revenue stays 0 unless declared
        bar_revenue = fb_revenue - _f(actuals.get('table_bottle_service'))
        table_revenue = _f(actuals.get('table_bottle_service'))

    # ─── TICKETING REVENUE ────────────────────────────────────────────────
    eb_tiers = ticket_breakdown.get('eventbrite', []) or []
    eb_qty = sum(int(t.get('qty', 0)) for t in eb_tiers)
    eb_face_revenue = sum(int(t.get('qty', 0)) * _f(t.get('price')) for t in eb_tiers)

    # If no structured EB data, fall back to actuals.door_revenue split
    if eb_qty == 0 and actuals:
        # Best guess: door_revenue_card = Eventbrite-ish, door_revenue_cash = walk-up
        eb_face_revenue = _f(actuals.get('door_revenue_card'))
        # Estimate qty from face / typical avg
        eb_qty = int(eb_face_revenue / 65) if eb_face_revenue > 0 else 0

    see_tix = ticket_breakdown.get('see_tickets') or {}
    see_qty = int(see_tix.get('qty', 0))
    see_price = _f(see_tix.get('price', 45))
    see_revenue = see_qty * see_price

    walk_up = ticket_breakdown.get('walk_up_square') or {}
    walk_up_qty = int(walk_up.get('qty', 0))
    walk_up_revenue = _f(walk_up.get('revenue', 0))
    if walk_up_revenue == 0 and actuals.get('door_revenue_cash'):
        walk_up_revenue = _f(actuals.get('door_revenue_cash'))

    # 15% additive surcharge on EB face value, splits 50/50
    ticket_surcharge = round(eb_face_revenue * TICKET_SURCHARGE_RATE_EB, 2)

    # Eventbrite platform fees (3.5% + $1.59/tix) — comes off EB face revenue, splits 50/50
    eb_platform_fee = round(eb_face_revenue * EB_PLATFORM_FEE_RATE + eb_qty * EB_PLATFORM_FEE_PER_TIX, 2)

    door_revenue = eb_face_revenue + see_revenue + walk_up_revenue
    total_ticketing_revenue = door_revenue + ticket_surcharge

    # ─── TOTAL NET SALES ──────────────────────────────────────────────────
    net_sales = fb_revenue + total_ticketing_revenue

    # ─── HEADCOUNT ────────────────────────────────────────────────────────
    table_guests = int(ticket_breakdown.get('table_guests', 0))
    guest_list = int(ticket_breakdown.get('guest_list', 0))
    ticketed = eb_qty + see_qty + walk_up_qty

    # Prefer event.actual_attendance / event.total_headcount when set — these
    # are the source of truth from the ops report. Only use derived ticketed
    # counts when no actuals are present (e.g., projecting a future event).
    actual_hc = int(_f(event.get('actual_attendance')) or _f(event.get('total_headcount')) or _f(actuals.get('total_headcount')))
    derived_hc = ticketed + table_guests + guest_list

    if actual_hc > 0:
        total_headcount = actual_hc
    elif derived_hc > 0:
        total_headcount = derived_hc
    else:
        total_headcount = 0

    fb_per_head = round(fb_revenue / total_headcount, 2) if total_headcount > 0 else 0

    # Genre band lookup
    genre = (event.get('tier1_category') or '').lower().strip()
    fb_band = GENRE_FB_PER_HEAD_BANDS.get(genre)

    # ─── ARTIST / RIDERS ──────────────────────────────────────────────────
    artist_fee = _f(event.get('artist_fee_landed')) + _f(event.get('artist_fee_travel'))
    if artist_fee == 0 and costs:
        artist_fee = _f(costs.get('artist_fee_total'))

    # Riders: tech + hospitality. If neither set and event is Collectiv, fall back to standard rider amounts.
    hospitality_rider = _f(costs.get('hospitality_rider_actual'))
    tech_rider = _f(costs.get('tech_rider_actual'))
    if hospitality_rider == 0 and tech_rider == 0 and _is_collectiv(event):
        hospitality_rider = COLLECTIV_HOSPITALITY_RIDER_STD
        tech_rider = COLLECTIV_TECH_RIDER_STD

    artist_total = artist_fee + hospitality_rider + tech_rider

    # ─── OP CARRY VARIABLE (District-only) ────────────────────────────────
    # Cleaning — always static
    cleaning = CLEANING_STATIC

    # Hourly tipped wages — staff × hours × $2.18
    tipped_staff_count = int(_f(costs.get('tipped_staff_count')) or 0)
    tipped_hours_avg = _f(costs.get('tipped_hours_avg'), 6.25)
    if tipped_staff_count > 0:
        hourly_tipped = round(tipped_staff_count * tipped_hours_avg * TIPPED_BASE_RATE, 2)
    else:
        hourly_tipped = _f(costs.get('hourly_wages_tipped_total'))

    # Security — staff × $175
    security_staff_count = int(_f(costs.get('security_staff_count')) or 0)
    if security_staff_count > 0:
        security = security_staff_count * SECURITY_RATE_PER_STAFF
    else:
        security = _f(costs.get('security_total'))

    # Production labor — entered total, or use ceiling/floor based on flag
    production_labor = _f(costs.get('production_staff_total'))

    # Cash payouts — sum or total
    cash_payouts = _f(costs.get('cash_out_total'))

    op_carry_variable = (
        hourly_tipped + security + cleaning + production_labor + cash_payouts
    )
    op_carry_static = OP_CARRY_STATIC_PER_NIGHT
    op_carry_total = op_carry_static + op_carry_variable

    # ─── DEAL MECHANICS ───────────────────────────────────────────────────
    is_collectiv = _is_collectiv(event)
    deal_type = (event.get('deal_type') or '').strip().lower()
    promoter_name = (event.get('promoter_name') or '').strip().lower()

    # Net house charge calc: base − Collectiv add (only for Collectiv events)
    house_base = _f(event.get('house_charge_base'), COLLECTIV_HOUSE_BASE_DEFAULT if is_collectiv else 0)
    collectiv_op_add = _f(event.get('collectiv_op_add'), COLLECTIV_OP_ADD_DEFAULT if is_collectiv else 0)
    house_net = house_base - collectiv_op_add

    # Candela / revenue-share deal config (% of net + 50/50 split on artist + production)
    promoter_pct_of_net = _f(event.get('promoter_pct_of_net'))
    split_costs_total = _f(event.get('split_costs_total'))   # Artist + production above-baseline costs the venue and promoter share 50/50

    is_revenue_share = (
        promoter_pct_of_net > 0
        or deal_type == 'revenue_share'
        or promoter_name == 'candela'  # Candela is locked at 27%
    )

    # Pre-init shared variables so all branches and output dict can reference them
    each_split_share = 0.0
    split_costs = 0.0
    each_half = 0.0
    district_50_share = 0.0
    collectiv_50_share = 0.0
    revenue_subject_to_split = 0.0
    profit_pool = 0.0
    deal_costs = 0.0
    district_net_operating_income = 0.0
    collectiv_net_take = 0.0
    district_total_revenue_in = 0.0
    carry_coverage_pct = 0.0
    carry_gap = 0.0

    if is_collectiv:
        # Collectiv: house off-top + 50/50 split on remainder
        revenue_subject_to_split = net_sales - walk_up_revenue - house_net
        deal_costs = total_cogs + artist_total + eb_platform_fee
        profit_pool = revenue_subject_to_split - deal_costs
        each_half = profit_pool / 2.0
        district_50_share = each_half
        collectiv_50_share = each_half
        district_total_revenue_in = district_50_share + house_net + walk_up_revenue
        district_net_operating_income = district_total_revenue_in - op_carry_total
        collectiv_net_take = collectiv_50_share
        carry_coverage_pct = round((house_net / op_carry_total) * 100, 1) if op_carry_total > 0 else 0
        carry_gap = house_net - op_carry_total

    elif is_revenue_share:
        # Candela / Utopia-style revenue share:
        #   1. Promoter takes X% of net sales (Candela = 27%)
        #   2. District gets remaining (1-X)% of net sales
        #   3. Artist + production split costs (above normal venue overhead) are split 50/50
        #   4. District then absorbs full op carry (rent/utilities/baseline staff/security baseline)
        pct = promoter_pct_of_net if promoter_pct_of_net > 0 else 27.0  # Candela default
        promoter_revenue_share = round(net_sales * pct / 100.0, 2)
        district_revenue_share = net_sales - promoter_revenue_share

        # 50/50 split costs — artist fee + tech rider + hospitality rider + above-baseline production
        # Use explicit split_costs_total if provided; otherwise sum the components
        if split_costs_total > 0:
            split_costs = split_costs_total
        else:
            split_costs = artist_total  # falls back to artist fee + riders

        each_split_share = round(split_costs / 2.0, 2)
        # District also pays full COGS, EB platform fee, and op carry (their share of the deal)
        district_net_operating_income = (
            district_revenue_share
            - total_cogs
            - eb_platform_fee
            - each_split_share
            - op_carry_total
        )
        collectiv_net_take = promoter_revenue_share - each_split_share

        # Reuse fields for display compatibility
        revenue_subject_to_split = net_sales
        deal_costs = total_cogs + split_costs + eb_platform_fee
        profit_pool = net_sales - deal_costs
        each_half = each_split_share
        district_50_share = district_revenue_share
        collectiv_50_share = promoter_revenue_share
        district_total_revenue_in = district_revenue_share
        carry_coverage_pct = 0  # not applicable
        carry_gap = 0

    else:
        # Standard / promoter-paid deals — fall through to existing splits in actuals
        bar_payout = _f(actuals.get('promoter_bar_payout'))
        door_payout = _f(actuals.get('promoter_door_payout'))
        table_payout = _f(actuals.get('promoter_table_payout'))
        promoter_take = bar_payout + door_payout + table_payout

        deal_costs = total_cogs + artist_total + eb_platform_fee
        profit_pool = net_sales - deal_costs - promoter_take
        each_half = 0
        district_50_share = 0
        collectiv_50_share = 0

        district_net_operating_income = profit_pool - op_carry_total
        collectiv_net_take = 0
        revenue_subject_to_split = 0
        carry_coverage_pct = 0
        carry_gap = 0
        district_total_revenue_in = 0

    # ─── PASS-THROUGHS ────────────────────────────────────────────────────
    sales_tax = _f(actuals.get('tax_collected'))
    excise_tax = _f(costs.get('excise_tax_collected'))
    tips = _f(actuals.get('tips')) or _f(costs.get('tips_collected_total'))
    cc_processing = _f(costs.get('cc_processing_passthrough'))

    pass_through_total = sales_tax + excise_tax + tips + cc_processing

    # ─── OUTPUT ───────────────────────────────────────────────────────────
    return {
        'revenue': {
            'bar': round(bar_revenue, 2),
            'table': round(table_revenue, 2),
            'fb_total': round(fb_revenue, 2),
            'eb_face': round(eb_face_revenue, 2),
            'eb_qty': eb_qty,
            'see_tix_revenue': round(see_revenue, 2),
            'see_tix_qty': see_qty,
            'walk_up_revenue': round(walk_up_revenue, 2),
            'walk_up_qty': walk_up_qty,
            'ticket_surcharge': round(ticket_surcharge, 2),
            'door_total': round(door_revenue, 2),
            'ticketing_total': round(total_ticketing_revenue, 2),
            'net_sales': round(net_sales, 2),
        },
        'cogs': {
            'bar': round(bar_cogs, 2),
            'table': round(table_cogs, 2),
            'total': round(total_cogs, 2),
            'corrections_applied': cogs_corrections_made,
            'zero_recipe_flags': cogs_zero_recipe_flags,
            'pour_pct': round((bar_cogs / bar_revenue * 100), 1) if bar_revenue > 0 else 0,
            'liquor_pour_pct': round((total_cogs / fb_revenue * 100), 1) if fb_revenue > 0 else 0,
            'gross_margin_pct': round(((fb_revenue - total_cogs) / fb_revenue * 100), 1) if fb_revenue > 0 else 0,
        },
        'headcount': {
            'ticketed': ticketed,
            'eventbrite': eb_qty,
            'see_tickets': see_qty,
            'walk_up': walk_up_qty,
            'table_guests': table_guests,
            'guest_list': guest_list,
            'total': total_headcount,
        },
        'spend': {
            'fb_per_head': fb_per_head,
            'genre_band_low': fb_band[0] if fb_band else None,
            'genre_band_high': fb_band[1] if fb_band else None,
            'genre': genre,
            'in_band': (fb_band[0] <= fb_per_head <= fb_band[1]) if fb_band else None,
        },
        'artist': {
            'fee': round(artist_fee, 2),
            'hospitality_rider': round(hospitality_rider, 2),
            'tech_rider': round(tech_rider, 2),
            'total': round(artist_total, 2),
        },
        'op_carry': {
            'static': round(op_carry_static, 2),
            'variable': round(op_carry_variable, 2),
            'total': round(op_carry_total, 2),
            'variable_breakdown': {
                'hourly_tipped': round(hourly_tipped, 2),
                'security': round(security, 2),
                'security_staff': security_staff_count,
                'security_baseline': round(SECURITY_BASELINE_PER_NIGHT, 2),
                'cleaning': round(cleaning, 2),
                'production_labor': round(production_labor, 2),
                'cash_payouts': round(cash_payouts, 2),
            },
        },
        'deal': {
            'is_collectiv': is_collectiv,
            'is_revenue_share': is_revenue_share,
            'deal_type': 'collectiv' if is_collectiv else ('revenue_share' if is_revenue_share else 'standard'),
            'promoter_pct_of_net': promoter_pct_of_net if is_revenue_share else 0,
            'split_costs_total': round(split_costs, 2) if is_revenue_share else 0,
            'split_costs_each_share': round(each_split_share, 2) if is_revenue_share else 0,
            'house_base': round(house_base, 2),
            'collectiv_op_add': round(collectiv_op_add, 2),
            'house_net': round(house_net, 2),
            'eb_platform_fee': round(eb_platform_fee, 2),
            'walk_up_district_only': round(walk_up_revenue, 2),
            'revenue_in_pool': round(revenue_subject_to_split, 2),
            'deal_costs_50_50': round(total_cogs + artist_total + eb_platform_fee, 2),
            'profit_pool': round(profit_pool, 2),
            'each_50_share': round(each_half, 2),
            'district_revenue_share': round(district_50_share, 2) if is_revenue_share else 0,
            'promoter_revenue_share': round(collectiv_50_share, 2) if is_revenue_share else 0,
            'district_bottom_line': round(district_net_operating_income, 2),
            'collectiv_bottom_line': round(collectiv_net_take, 2),
            'promoter_bottom_line': round(collectiv_net_take, 2),
            'carry_coverage_pct': carry_coverage_pct,
            'carry_gap': round(carry_gap, 2),
        },
        'pass_throughs': {
            'sales_tax': round(sales_tax, 2),
            'excise_tax': round(excise_tax, 2),
            'tips': round(tips, 2),
            'cc_processing': round(cc_processing, 2),
            'total': round(pass_through_total, 2),
        },
    }
