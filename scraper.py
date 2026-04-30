"""
Congress.gov FAST ETL Pipeline
Spyder / Jupyter / GitHub Actions Safe

Modes:
dev         = quick 5 row test
incremental = last 24 hour changes
backfill    = full refresh

Features:
✓ async requests
✓ Spyder-safe event loop
✓ retries
✓ concurrency control
✓ bulk DuckDB upserts
✓ progress logging
✓ JSON export
"""

import os
import json
import duckdb
import httpx
import asyncio
import pandas as pd
import argparse
import logging
import nest_asyncio

from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from time import perf_counter

# --------------------------------------------------
# SPYDER / JUPYTER FIX
# --------------------------------------------------

nest_asyncio.apply()

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

load_dotenv()

API_KEY = os.getenv("CONGRESS_API_KEY")
BASE_URL = "https://api.congress.gov/v3"
CONGRESS = 119

RUN_MODE = os.getenv("RUN_MODE", "incremental")
CONCURRENCY = int(os.getenv("CONCURRENCY", 20))

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "congress.duckdb"
DATA_DIR = BASE_DIR / "data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

log = logging.getLogger(__name__)

# --------------------------------------------------
# ASYNC RUNNER
# --------------------------------------------------

def run_async(coro):
    try:
        loop = asyncio.get_running_loop()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)

# --------------------------------------------------
# DATABASE
# --------------------------------------------------

def get_db():

    con = duckdb.connect(str(DB_PATH))

    con.execute("""
    CREATE TABLE IF NOT EXISTS bills (
        bill_id VARCHAR PRIMARY KEY,
        congress INTEGER,
        bill_type VARCHAR,
        bill_number VARCHAR,
        title VARCHAR,
        introduced_date DATE,
        latest_action VARCHAR,
        latest_action_date DATE,
        sponsor_name VARCHAR,
        sponsor_party VARCHAR,
        sponsor_state VARCHAR,
        origin_chamber VARCHAR,
        updated_at TIMESTAMP
    )
    """)

    return con

# --------------------------------------------------
# API
# --------------------------------------------------

def fetch_bills_page(limit=250, offset=0, from_date=None):

    params = {
        "api_key": API_KEY,
        "limit": limit,
        "offset": offset,
        "format": "json",
        "sort": "latestAction.actionDate+desc"
    }

    if from_date:
        params["fromDateTime"] = from_date

    r = httpx.get(
        f"{BASE_URL}/bill/{CONGRESS}",
        params=params,
        timeout=60
    )

    r.raise_for_status()

    return r.json().get("bills", [])


SEM = asyncio.Semaphore(CONCURRENCY)


async def fetch_detail(client, bill):

    congress = bill["congress"]
    bill_type = bill["type"].lower()
    bill_number = bill["number"]

    url = f"{BASE_URL}/bill/{congress}/{bill_type}/{bill_number}"

    async with SEM:

        for attempt in range(3):

            try:
                r = await client.get(
                    url,
                    params={
                        "api_key": API_KEY,
                        "format": "json"
                    },
                    timeout=60
                )

                r.raise_for_status()

                return r.json().get("bill", {})

            except Exception:

                await asyncio.sleep(1)

    return {}


def build_row(b, d):

    latest = d.get("latestAction") or b.get("latestAction") or {}

    sponsor = (d.get("sponsors") or [{}])[0]

    return {
        "bill_id": f"{b['congress']}-{b['type']}-{b['number']}",
        "congress": b["congress"],
        "bill_type": b["type"],
        "bill_number": b["number"],
        "title": d.get("title") or b.get("title"),
        "introduced_date": d.get("introducedDate"),
        "latest_action": latest.get("text"),
        "latest_action_date": latest.get("actionDate"),
        "sponsor_name": sponsor.get("fullName"),
        "sponsor_party": sponsor.get("party"),
        "sponsor_state": sponsor.get("state"),
        "origin_chamber": d.get("originChamber") or b.get("originChamber"),
        "updated_at": datetime.now(timezone.utc)
    }


async def process_bills(bills):

    async with httpx.AsyncClient() as client:

        tasks = [fetch_detail(client, b) for b in bills]

        details = await asyncio.gather(*tasks)

        rows = []

        for b, d in zip(bills, details):
            rows.append(build_row(b, d))

        return rows

# --------------------------------------------------
# BULK UPSERT
# --------------------------------------------------

def bulk_upsert(con, rows):

    if not rows:
        return

    df = pd.DataFrame(rows)

    con.register("tmp_bills", df)

    con.execute("""
        INSERT OR REPLACE INTO bills
        SELECT * FROM tmp_bills
    """)

# --------------------------------------------------
# MODES
# --------------------------------------------------

def scrape_dev(con):

    log.info("MODE: DEV")

    bills = fetch_bills_page(limit=5)

    rows = run_async(process_bills(bills))

    bulk_upsert(con, rows)

    return rows


def scrape_incremental(con):

    since = (
        datetime.now(timezone.utc)
        - timedelta(hours=72)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    log.info(f"MODE: INCREMENTAL since {since}")

    offset = 0
    total_rows = []
    start = perf_counter()

    while True:

        bills = fetch_bills_page(
            limit=250,
            offset=offset,
            from_date=since
        )

        if not bills:
            break

        rows = run_async(process_bills(bills))

        bulk_upsert(con, rows)

        total_rows.extend(rows)

        elapsed = perf_counter() - start

        log.info(
            f"Processed {len(total_rows)} rows "
            f"in {elapsed:.1f}s"
        )

        offset += 250

    return total_rows


def scrape_backfill(con):

    log.info("MODE: FULL REFRESH")

    con.execute("DELETE FROM bills")

    offset = 0
    total_rows = []

    start = perf_counter()

    while True:

        bills = fetch_bills_page(
            limit=250,
            offset=offset
        )

        if not bills:
            break

        rows = run_async(process_bills(bills))

        bulk_upsert(con, rows)

        total_rows.extend(rows)

        elapsed = perf_counter() - start
        rate = len(total_rows) / elapsed if elapsed else 0

        log.info(
            f"Loaded {len(total_rows)} rows | "
            f"{rate:.1f} rows/sec | "
            f"offset={offset}"
        )

        offset += 250

    total = perf_counter() - start

    log.info(
        f"FULL REFRESH COMPLETE: "
        f"{len(total_rows)} rows in {total:.1f}s"
    )

    return total_rows

# --------------------------------------------------
# EXPORT JSON
# --------------------------------------------------

def export_json(con):

    DATA_DIR.mkdir(exist_ok=True)

    df = con.execute("""
        SELECT *
        FROM bills
        ORDER BY introduced_date DESC NULLS LAST
    """).fetchdf()

    rows = df.to_dict(orient="records")

    payload = {
        "last_updated": datetime.now(
            timezone.utc
        ).isoformat(),
        "count": len(rows),
        "data": rows
    }

    with open(DATA_DIR / "bills.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    log.info("Exported data/bills.json")

# --------------------------------------------------
# CLI
# --------------------------------------------------

def parse_args():

    p = argparse.ArgumentParser()

    p.add_argument(
        "--mode",
        choices=["dev", "incremental", "backfill"]
    )

    return p.parse_args()

# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    if not API_KEY:
        raise SystemExit("Missing CONGRESS_API_KEY")

    args = parse_args()

    mode = args.mode or RUN_MODE

    con = get_db()

    if mode == "dev":
        rows = scrape_dev(con)

    elif mode == "incremental":
        rows = scrape_incremental(con)

    elif mode == "backfill":
        rows = scrape_backfill(con)

    else:
        raise SystemExit("Invalid mode")

    export_json(con)

    con.close()

    log.info(f"DONE ✓ {len(rows)} rows processed")