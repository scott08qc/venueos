"""
fix_promoter_data.py — One-shot fix to make promoter intelligence work with our
populated demo data.

Issues to fix:
  1. review_status is 'completed' but promoter query expects 'complete'
  2. promoter_attendance_vs_projection wasn't set — needed for nights_above/below/met
"""
from sqlalchemy import text


def fix_promoter_review_data(engine):
    """Update existing post_event_reviews to populate fields the promoter
    intelligence query needs."""

    # Step 1: Add the comparison column if missing
    with engine.begin() as conn:
        try:
            conn.execute(text("""
                ALTER TABLE post_event_reviews
                ADD COLUMN IF NOT EXISTS promoter_attendance_vs_projection TEXT
            """))
        except Exception:
            pass

    # Step 2: Normalize review_status to 'complete'
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE post_event_reviews
            SET review_status = 'complete'
            WHERE review_status IN ('completed', 'Completed', 'COMPLETED')
        """))
        normalized = result.rowcount

    # Step 3: Set the attendance vs projection comparison from existing data
    # Compare actual_attendance to expected_attendance via the events join
    with engine.begin() as conn:
        result = conn.execute(text("""
            UPDATE post_event_reviews r
            SET promoter_attendance_vs_projection = CASE
                WHEN r.actual_attendance >= e.expected_attendance * 1.05 THEN 'above'
                WHEN r.actual_attendance <= e.expected_attendance * 0.85 THEN 'below'
                ELSE 'met'
            END
            FROM events e
            WHERE r.event_id = e.id
              AND e.expected_attendance > 0
              AND r.actual_attendance > 0
        """))
        flagged = result.rowcount

    # Step 4: Fill in actual_effective_split for revenue-share / Collectiv events
    # so promoter intelligence shows their effective take percentage
    with engine.begin() as conn:
        # For Candela revenue-share events: split = promoter_pct_of_net
        conn.execute(text("""
            UPDATE post_event_reviews r
            SET actual_effective_split = e.promoter_pct_of_net
            FROM events e
            WHERE r.event_id = e.id
              AND e.promoter_pct_of_net IS NOT NULL
              AND e.promoter_pct_of_net > 0
              AND r.actual_effective_split IS NULL
        """))
        # For Collectiv: ~50% on the net-after-house-fee — approximate as 35%
        # of total net (since house fee absorbs ~$9.5K off top of typical ~$50K)
        conn.execute(text("""
            UPDATE post_event_reviews r
            SET actual_effective_split = 35
            FROM events e
            WHERE r.event_id = e.id
              AND LOWER(COALESCE(e.promoter_name, '')) = 'collectiv'
              AND r.actual_effective_split IS NULL
        """))

    # Count totals for the response
    with engine.connect() as conn:
        complete = conn.execute(text(
            "SELECT COUNT(*) FROM post_event_reviews WHERE LOWER(review_status) = 'complete'"
        )).scalar()
        with_attendance_flag = conn.execute(text(
            "SELECT COUNT(*) FROM post_event_reviews WHERE promoter_attendance_vs_projection IS NOT NULL"
        )).scalar()

    return {
        'review_status_normalized': normalized,
        'attendance_flag_set': flagged,
        'now_complete': complete,
        'now_with_attendance_flag': with_attendance_flag,
    }
