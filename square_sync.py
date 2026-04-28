"""
square_sync.py
VenueOS — Square → Neon sync
Pulls payments from Square for a given date range and writes to Neon.
Usage:
    python square_sync.py                  # pulls last 24 hours
    python square_sync.py 2026-04-25       # pulls a specific event date (6am–6am window)
    python square_sync.py 2026-04-25 2026-04-26  # explicit start and end date
"""

import os
import sys
import json
import httpx
import psycopg2
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

SQUARE_TOKEN = os.getenv("SQUARE_ACCESS_TOKEN")
SQUARE_LOCATION = os.getenv("SQUARE_LOCATION_ID")
DATABASE_URL = os.getenv("DATABASE_URL")

SQUARE_BASE = "https://connect.squareup.com/v2"
HEADERS = {
    "Authorization": f"Bearer {SQUARE_TOKEN}",
    "Content-Type": "application/json",
    "Square-Version": "2024-01-18",
}


# ---------------------------------------------------------------------------
# Date range helpers — venue day runs 6am to 6am
# ---------------------------------------------------------------------------

def event_window(date_str: str) -> tuple[str, str]:
    """Given a date string like '2026-04-25', return the 6am–6am RFC3339 window."""
    d = datetime.strptime(date_str, "%Y-%m-%d")
    start = d.replace(hour=6, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    end = start + timedelta(hours=24)
    return start.isoformat(), end.isoformat()


def default_window() -> tuple[str, str]:
    """Default: last 24 hours."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    return start.isoformat(), end.isoformat()


# ---------------------------------------------------------------------------
# Square API — payments
# ---------------------------------------------------------------------------

def fetch_payments(begin_time: str, end_time: str) -> list[dict]:
    """Fetch all payments in the window, handling pagination."""
    payments = []
    cursor = None

    while True:
        params = {
            "location_id": SQUARE_LOCATION,
            "begin_time": begin_time,
            "end_time": end_time,
            "limit": 100,
        }
        if cursor:
            params["cursor"] = cursor

        r = httpx.get(f"{SQUARE_BASE}/payments", headers=HEADERS, params=params)
        r.raise_for_status()
        data = r.json()

        batch = data.get("payments", [])
        payments.extend(batch)
        print(f"  Fetched {len(batch)} payments (total so far: {len(payments)})")

        cursor = data.get("cursor")
        if not cursor:
            break

    return payments


def fetch_orders(order_ids: list[str]) -> dict[str, dict]:
    """Batch fetch order details for itemization."""
    if not order_ids:
        return {}

    r = httpx.post(
        f"{SQUARE_BASE}/orders/batch-retrieve",
        headers=HEADERS,
        json={"order_ids": order_ids},
    )
    r.raise_for_status()
    orders = r.json().get("orders", [])
    return {o["id"]: o for o in orders}


# ---------------------------------------------------------------------------
# Neon — schema + writes
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS square_payments (
    id                  TEXT PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL,
    location_id         TEXT,
    status              TEXT,
    source_type         TEXT,          -- CARD, CASH, OTHER
    amount_cents        INTEGER,       -- total charged in cents
    tip_cents           INTEGER,
    total_cents         INTEGER,       -- amount + tip
    currency            TEXT,
    order_id            TEXT,
    note                TEXT,
    raw                 JSONB,         -- full Square payload
    synced_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS square_line_items (
    id                  TEXT PRIMARY KEY,   -- order_id + uid
    payment_id          TEXT REFERENCES square_payments(id),
    order_id            TEXT,
    created_at          TIMESTAMPTZ,
    name                TEXT,
    quantity            NUMERIC,
    base_price_cents    INTEGER,
    total_cents         INTEGER,
    category_name       TEXT,
    variation_name      TEXT,
    raw                 JSONB
);

CREATE INDEX IF NOT EXISTS idx_square_payments_created   ON square_payments(created_at);
CREATE INDEX IF NOT EXISTS idx_square_payments_location  ON square_payments(location_id);
CREATE INDEX IF NOT EXISTS idx_square_line_items_payment ON square_line_items(payment_id);
CREATE INDEX IF NOT EXISTS idx_square_line_items_created ON square_line_items(created_at);
"""


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    print("  Schema verified.")


def upsert_payments(conn, payments: list[dict], orders: dict[str, dict]):
    inserted = 0
    skipped = 0

    with conn.cursor() as cur:
        for p in payments:
            money = p.get("amount_money", {})
            tip = p.get("tip_money", {})
            total = p.get("total_money", {})
            source = p.get("source_type", "UNKNOWN")

            try:
                cur.execute("""
                    INSERT INTO square_payments
                        (id, created_at, location_id, status, source_type,
                         amount_cents, tip_cents, total_cents, currency,
                         order_id, note, raw)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        status    = EXCLUDED.status,
                        raw       = EXCLUDED.raw,
                        synced_at = NOW()
                """, (
                    p["id"],
                    p["created_at"],
                    p.get("location_id"),
                    p.get("status"),
                    source,
                    money.get("amount", 0),
                    tip.get("amount", 0),
                    total.get("amount", 0),
                    money.get("currency", "USD"),
                    p.get("order_id"),
                    p.get("note"),
                    json.dumps(p),
                ))
                inserted += 1
            except Exception as e:
                print(f"  Warning: payment {p['id']} skipped — {e}")
                skipped += 1

            # Line items from order
            order_id = p.get("order_id")
            if order_id and order_id in orders:
                order = orders[order_id]
                for item in order.get("line_items", []):
                    item_id = f"{order_id}_{item.get('uid', '')}"
                    base = item.get("base_price_money", {})
                    total_item = item.get("total_money", {})
                    category = ""
                    if item.get("catalog_object_id"):
                        # category comes through variation_name or modifiers
                        pass

                    try:
                        cur.execute("""
                            INSERT INTO square_line_items
                                (id, payment_id, order_id, created_at, name,
                                 quantity, base_price_cents, total_cents,
                                 category_name, variation_name, raw)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO NOTHING
                        """, (
                            item_id,
                            p["id"],
                            order_id,
                            p["created_at"],
                            item.get("name"),
                            item.get("quantity", "1"),
                            base.get("amount", 0),
                            total_item.get("amount", 0),
                            item.get("catalog_object_id", ""),
                            item.get("variation_name", ""),
                            json.dumps(item),
                        ))
                    except Exception as e:
                        print(f"  Warning: line item {item_id} skipped — {e}")

    conn.commit()
    return inserted, skipped


# ---------------------------------------------------------------------------
# Convenience queries — run after sync to verify
# ---------------------------------------------------------------------------

def print_summary(conn, begin_time: str, end_time: str):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*)                                AS transactions,
                SUM(amount_cents) / 100.0               AS gross_sales,
                SUM(tip_cents) / 100.0                  AS total_tips,
                SUM(total_cents) / 100.0                AS total_collected,
                AVG(amount_cents) / 100.0               AS avg_transaction,
                SUM(CASE WHEN source_type = 'CASH' THEN amount_cents ELSE 0 END) / 100.0  AS cash_sales,
                SUM(CASE WHEN source_type = 'CARD' THEN amount_cents ELSE 0 END) / 100.0  AS card_sales
            FROM square_payments
            WHERE created_at BETWEEN %s AND %s
              AND status = 'COMPLETED'
        """, (begin_time, end_time))

        row = cur.fetchone()
        print("\n=== SQUARE SUMMARY ===")
        print(f"  Transactions:     {row[0]}")
        print(f"  Gross Sales:      ${row[1]:,.2f}")
        print(f"  Tips:             ${row[2]:,.2f}")
        print(f"  Total Collected:  ${row[3]:,.2f}")
        print(f"  Avg Transaction:  ${row[4]:,.2f}")
        print(f"  Cash:             ${row[5]:,.2f}")
        print(f"  Card:             ${row[6]:,.2f}")

        cur.execute("""
            SELECT source_type, COUNT(*), SUM(amount_cents)/100.0
            FROM square_payments
            WHERE created_at BETWEEN %s AND %s AND status = 'COMPLETED'
            GROUP BY source_type ORDER BY 3 DESC
        """, (begin_time, end_time))
        print("\n  By payment type:")
        for r in cur.fetchall():
            print(f"    {r[0]:<12} {r[1]:>4} transactions  ${r[2]:,.2f}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = sys.argv[1:]

    if len(args) == 0:
        begin_time, end_time = default_window()
        print(f"Pulling last 24 hours: {begin_time} → {end_time}")
    elif len(args) == 1:
        begin_time, end_time = event_window(args[0])
        print(f"Pulling event window for {args[0]}: {begin_time} → {end_time}")
    elif len(args) == 2:
        begin_time, _ = event_window(args[0])
        _, end_time = event_window(args[1])
        print(f"Pulling custom range: {begin_time} → {end_time}")
    else:
        print("Usage: python square_sync.py [start_date] [end_date]")
        sys.exit(1)

    print("\n1. Connecting to Neon...")
    conn = get_conn()
    ensure_schema(conn)

    print("\n2. Fetching payments from Square...")
    payments = fetch_payments(begin_time, end_time)
    print(f"  Total payments: {len(payments)}")

    if not payments:
        print("  No payments found in this window.")
        conn.close()
        return

    print("\n3. Fetching order details for line items...")
    order_ids = [p["order_id"] for p in payments if p.get("order_id")]
    # Square batch-retrieve limit is 100 orders
    orders = {}
    for i in range(0, len(order_ids), 100):
        batch = fetch_orders(order_ids[i:i+100])
        orders.update(batch)
    print(f"  Orders retrieved: {len(orders)}")

    print("\n4. Writing to Neon...")
    inserted, skipped = upsert_payments(conn, payments, orders)
    print(f"  Payments written: {inserted} | Skipped: {skipped}")

    print("\n5. Summary:")
    print_summary(conn, begin_time, end_time)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
