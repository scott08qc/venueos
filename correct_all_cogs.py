"""
correct_all_cogs.py — Apply the May 1 recipe-correction logic to every event
that has item-level data. One-shot, idempotent.
"""
from sqlalchemy import text
from ingest_may1 import (
    RECIPE_UNIT_COSTS, PRODUCT_TO_RECIPE, ZERO_COGS_PRODUCTS,
    TABLE_SERVICE_RECLASS, BOTTLE_STANDARD_POUR_COST,
    correct_event_cogs,
)


def correct_all_event_cogs(engine):
    """Walk every event with items and run the correction logic."""
    with engine.connect() as conn:
        events = conn.execute(text("""
            SELECT DISTINCT event_id FROM event_item_sales ORDER BY event_id
        """)).fetchall()

    results = []
    for ev in events:
        try:
            with engine.begin() as conn:
                r = correct_event_cogs(conn, ev.event_id)
            results.append({'event_id': ev.event_id, **r})
        except Exception as ex:
            results.append({'event_id': ev.event_id, 'error': str(ex)})

    total_corrections = sum(r.get('corrections', 0) for r in results if 'corrections' in r)
    total_flags = sum(r.get('flags', 0) for r in results if 'flags' in r)
    return {
        'events_processed': len(results),
        'total_corrections': total_corrections,
        'total_flags': total_flags,
        'errors': [r for r in results if 'error' in r],
        'results': results[:20],  # first 20 for inspection
    }
