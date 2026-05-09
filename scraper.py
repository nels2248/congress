"""
Congress.gov ETL Pipeline (Ingest + Enrichment Upgrade)
-------------------------------------------------------
Adds:
- Real cosponsors + amendments via enrichment layer
- Async API calls with rate limiting
- bill_stats enrichment table
- Incremental enrichment mode
- Dashboard-ready merged output
"""

import os
import json
import duckdb
import httpx
import asyncio
import pandas as pd
import logging
import nest_asyncio

from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timezone

nest_asyncio.apply()

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

load_dotenv()

API_KEY = os.getenv("CONGRESS_API_KEY")
BASE_URL = "https://api.congress.gov/v3"
CONGRESS = 119

RUN_MODE = os.getenv("RUN_MODE", "chunk")

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 20000))
API_PAGE_SIZE = 250

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "congress.duckdb"
DATA_DIR = BASE_DIR / "data"
CHECKPOINT_FILE = BASE_DIR / "checkpoint.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --------------------------------------------------
# ASYNC CONTROL
# --------------------------------------------------

semaphore = asyncio.Semaphore(5)

# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def safe_dict(x):
    return x if isinstance(x, dict) else {}

def safe_list(x):
    return x if isinstance(x, list) else []

def run_async(coro):
    try:
        loop = asyncio.get_running_loop()
        return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)

# --------------------------------------------------
# CHECKPOINT
# --------------------------------------------------

def get_checkpoint():
    if CHECKPOINT_FILE.exists():
        return int(CHECKPOINT_FILE.read_text())
    return 0

def save_checkpoint(offset):
    CHECKPOINT_FILE.write_text(str(offset))

# --------------------------------------------------
# DB
# --------------------------------------------------

def get_db():
    con = duckdb.connect(str(DB_PATH))

    con.execute("""
    CREATE TABLE IF NOT EXISTS bills (
        bill_id VARCHAR,
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
        update_date DATE
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS bill_stats (
        bill_id VARCHAR PRIMARY KEY,
        cosponsors_count INTEGER,
        amendments_count INTEGER,
        last_updated TIMESTAMP
    )
    """)

    return con

# --------------------------------------------------
# API
# --------------------------------------------------

def fetch_bills_page(limit, offset=0):

    params = {
        "api_key": API_KEY,
        "limit": limit,
        "offset": offset,
        "format": "json",
        "sort": "updateDate+desc"
    }

    r = httpx.get(
        f"{BASE_URL}/bill/{CONGRESS}",
        params=params,
        timeout=60
    )

    r.raise_for_status()
    return r.json().get("bills", [])

# --------------------------------------------------
# BASE PROCESSING (FAST INGEST)
# --------------------------------------------------

async def process_bills(bills):

    out = []

    for b in bills:

        latest = safe_dict(b.get("latestAction"))

        sponsor = safe_dict(
            b.get("sponsors")[0]
            if isinstance(b.get("sponsors"), list) and b.get("sponsors")
            else {}
        )

        out.append({
            "bill_id": f"{b.get('congress')}-{b.get('type')}-{b.get('number')}",
            "congress": b.get("congress"),
            "bill_type": b.get("type"),
            "bill_number": b.get("number"),
            "title": b.get("title"),
            "introduced_date": b.get("introducedDate"),
            "latest_action": latest.get("text"),
            "latest_action_date": latest.get("actionDate"),
            "sponsor_name": sponsor.get("fullName"),
            "sponsor_party": sponsor.get("party"),
            "sponsor_state": sponsor.get("state"),
            "origin_chamber": b.get("originChamber"),
            "update_date": b.get("updateDate")
        })

    return out

# --------------------------------------------------
# ENRICHMENT LAYER (NEW)
# --------------------------------------------------

async def enrich_bill(client, b):

    async with semaphore:

        base = f"{BASE_URL}/bill/{b['congress']}/{b['bill_type']}/{b['bill_number']}"

        try:
            cos_url = f"{base}/cosponsors"
            amd_url = f"{base}/amendments"

            cos_resp, amd_resp = await asyncio.gather(
                client.get(cos_url, params={"api_key": API_KEY}),
                client.get(amd_url, params={"api_key": API_KEY})
            )

            cos_json = cos_resp.json()
            amd_json = amd_resp.json()

            cos_count = (
                len(cos_json.get("cosponsors", []))
                if isinstance(cos_json.get("cosponsors"), list)
                else cos_json.get("cosponsors", {}).get("count", 0)
            )

            amd_count = (
                len(amd_json.get("amendments", []))
                if isinstance(amd_json.get("amendments"), list)
                else amd_json.get("amendments", {}).get("count", 0)
            )

            return {
                "bill_id": b["bill_id"],
                "cosponsors_count": cos_count,
                "amendments_count": amd_count
            }

        except Exception as e:
            log.warning(f"Enrichment failed {b['bill_id']}: {e}")
            return {
                "bill_id": b["bill_id"],
                "cosponsors_count": 0,
                "amendments_count": 0
            }

async def run_enrichment(bills):

    async with httpx.AsyncClient(timeout=60) as client:
        tasks = [enrich_bill(client, b) for b in bills]
        return await asyncio.gather(*tasks)

# --------------------------------------------------
# UPSERT BASE BILLS
# --------------------------------------------------

def bulk_upsert(con, rows):

    if not rows:
        return

    df = pd.DataFrame(rows)

    cols = [
        "bill_id","congress","bill_type","bill_number","title",
        "introduced_date","latest_action","latest_action_date",
        "sponsor_name","sponsor_party","sponsor_state",
        "origin_chamber","update_date"
    ]

    for c in cols:
        if c not in df.columns:
            df[c] = None

    df = df[cols]

    con.register("tmp", df)

    con.execute("""
        INSERT INTO bills
        SELECT * FROM tmp
    """)

# --------------------------------------------------
# SAVE ENRICHMENT (MERGE FIX)
# --------------------------------------------------

def save_enrichment(con, rows):

    if not rows:
        return

    df = pd.DataFrame(rows)
    df["last_updated"] = datetime.now(timezone.utc)

    con.register("tmp_stats", df)

    con.execute("""
        MERGE INTO bill_stats t
        USING tmp_stats s
        ON t.bill_id = s.bill_id
        WHEN MATCHED THEN UPDATE SET
            cosponsors_count = s.cosponsors_count,
            amendments_count = s.amendments_count,
            last_updated = s.last_updated
        WHEN NOT MATCHED THEN INSERT *
    """)

# --------------------------------------------------
# EXPORT JSON (WITH ENRICHMENT JOIN)
# --------------------------------------------------

def export_json(con):

    DATA_DIR.mkdir(exist_ok=True)

    df = con.execute("""
        SELECT 
            b.*,
            COALESCE(s.cosponsors_count, 0) AS cosponsors_count,
            COALESCE(s.amendments_count, 0) AS amendments_count
        FROM bills b
        LEFT JOIN bill_stats s
        ON b.bill_id = s.bill_id
        ORDER BY update_date DESC NULLS LAST
    """).fetchdf()

    payload = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "count": len(df),
        "data": df.to_dict("records")
    }

    with open(DATA_DIR / "bills.json", "w") as f:
        json.dump(payload, f, indent=2, default=str)

    log.info("Exported enriched bills.json")

# --------------------------------------------------
# INGEST BATCH
# --------------------------------------------------

def run_batch(con, start_offset):

    processed = 0
    offset = start_offset

    while processed < CHUNK_SIZE:

        remaining = CHUNK_SIZE - processed
        limit = min(API_PAGE_SIZE, remaining)

        bills = fetch_bills_page(limit, offset)

        if not bills:
            break

        rows = run_async(process_bills(bills))
        bulk_upsert(con, rows)

        processed += len(bills)
        offset += len(bills)

        log.info(f"Ingest: {processed}/{CHUNK_SIZE}")

    save_checkpoint(offset)
    export_json(con)

# --------------------------------------------------
# ENRICH MODE (NEW)
# --------------------------------------------------

def run_enrichment_mode(con):

    bills = con.execute("""
        SELECT bill_id, congress, bill_type, bill_number
        FROM bills
        WHERE bill_id NOT IN (SELECT bill_id FROM bill_stats)
    """).fetchdf().to_dict("records")

    log.info(f"Enriching {len(bills)} bills")

    rows = run_async(run_enrichment(bills))

    save_enrichment(con, rows)

    export_json(con)

# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    if not API_KEY:
        raise SystemExit("Missing CONGRESS_API_KEY")

    con = get_db()

    mode = RUN_MODE

    if mode == "chunk":
        start = get_checkpoint()
        run_batch(con, start)

    elif mode == "dev":
        bills = fetch_bills_page(25, 0)
        rows = run_async(process_bills(bills))
        bulk_upsert(con, rows)
        export_json(con)

    elif mode == "enrich":
        run_enrichment_mode(con)

    elif mode == "full":
        con.execute("DELETE FROM bills")
        run_batch(con, 0)
        run_enrichment_mode(con)

    con.close()