"""
Congress.gov ETL Pipeline

Modes (set via RUN_MODE env var or --mode CLI arg):
  dev         - 5 records, quick test (default)
  incremental - bills updated in last 24hrs (nightly cron)
  backfill    - paginated batches, use BATCH_SIZE + BATCH_OFFSET to chunk
"""

import httpx
import duckdb
import time
import os
import logging
import json
import argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
load_dotenv()

API_KEY  = os.getenv("CONGRESS_API_KEY")
BASE_URL = "https://api.congress.gov/v3"

CONGRESS = 119

# Override via CLI args or env vars
RUN_MODE     = os.getenv("RUN_MODE", "dev")       # dev | incremental | backfill
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", 50))   # records per backfill run
BATCH_OFFSET = int(os.getenv("BATCH_OFFSET", 0))  # where to start in backfill

BASE_DIR = Path(__file__).resolve().parent
DB_PATH  = BASE_DIR / "congress.duckdb"
DATA_DIR = BASE_DIR / "data"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────
def get_db():
    con = duckdb.connect(str(DB_PATH))
    con.execute("""
        CREATE TABLE IF NOT EXISTS bills (
            bill_id             VARCHAR PRIMARY KEY,
            congress            INTEGER,
            bill_type           VARCHAR,
            bill_number         VARCHAR,
            title               VARCHAR,
            introduced_date     DATE,
            latest_action       VARCHAR,
            latest_action_date  DATE,
            sponsor_name        VARCHAR,
            sponsor_party       VARCHAR,
            sponsor_state       VARCHAR,
            origin_chamber      VARCHAR,
            updated_at          TIMESTAMP
        )
    """)
    return con


# ─────────────────────────────────────────────
# API
# ─────────────────────────────────────────────
def fetch_bills(congress, limit=250, offset=0, from_date=None):
    params = {
        "api_key": API_KEY,
        "limit":   limit,
        "offset":  offset,
        "format":  "json",
    }
    if from_date:
        params["fromDateTime"] = from_date
    r = httpx.get(f"{BASE_URL}/bill/{congress}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_bill_detail(congress, bill_type, bill_number):
    r = httpx.get(
        f"{BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}",
        params={"api_key": API_KEY, "format": "json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("bill", {})


def build_row(b, detail):
    latest  = detail.get("latestAction") or b.get("latestAction") or {}
    sponsor = (detail.get("sponsors") or [])[0:1]
    sponsor = sponsor[0] if sponsor else {}
    bill_type   = b.get("type", "")
    bill_number = b.get("number", "")
    congress    = b.get("congress")
    return {
        "bill_id":            f"{congress}-{bill_type}-{bill_number}",
        "congress":           congress,
        "bill_type":          bill_type,
        "bill_number":        bill_number,
        "title":              detail.get("title") or b.get("title"),
        "introduced_date":    detail.get("introducedDate"),
        "latest_action":      latest.get("text"),
        "latest_action_date": latest.get("actionDate"),
        "origin_chamber":     detail.get("originChamber") or b.get("originChamber"),
        "updated_at":         datetime.now(timezone.utc).isoformat(),
        "sponsor_name":       sponsor.get("fullName"),
        "sponsor_party":      sponsor.get("party"),
        "sponsor_state":      sponsor.get("state"),
    }


def upsert(con, row):
    con.execute("""
        INSERT OR REPLACE INTO bills VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        row["bill_id"], row["congress"], row["bill_type"], row["bill_number"],
        row["title"], row["introduced_date"], row["latest_action"],
        row["latest_action_date"], row["sponsor_name"], row["sponsor_party"],
        row["sponsor_state"], row["origin_chamber"], row["updated_at"],
    ])


# ─────────────────────────────────────────────
# SCRAPE MODES
# ─────────────────────────────────────────────
def scrape_dev(con):
    """Quick 5-record test."""
    log.info("MODE: dev (5 records)")
    data  = fetch_bills(CONGRESS, limit=5, offset=0)
    bills = data.get("bills", [])
    out   = []
    for b in bills[:5]:
        try:
            detail = fetch_bill_detail(b["congress"], b["type"], b["number"])
            time.sleep(0.15)
        except Exception as e:
            log.warning(f"Detail failed: {e}")
            detail = {}
        row = build_row(b, detail)
        upsert(con, row)
        out.append(row)
        log.info(f"  {row['bill_id']} | {row['sponsor_party']} | {row['introduced_date']}")
    return out


def scrape_incremental(con):
    """Bills updated in the last 24 hours. Used for nightly cron."""
    since = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%SZ")
    log.info(f"MODE: incremental (since {since})")
    offset = 0
    out    = []

    while True:
        data  = fetch_bills(CONGRESS, limit=250, offset=offset, from_date=since)
        bills = data.get("bills", [])
        if not bills:
            break

        for b in bills:
            try:
                detail = fetch_bill_detail(b["congress"], b["type"], b["number"])
                time.sleep(0.15)
            except Exception as e:
                log.warning(f"Detail failed for {b.get('type')}{b.get('number')}: {e}")
                detail = {}
            row = build_row(b, detail)
            upsert(con, row)
            out.append(row)
            log.info(f"  {row['bill_id']} | {row['sponsor_party']} | {row['introduced_date']}")

        offset += 250
        time.sleep(0.2)

    log.info(f"Incremental: {len(out)} bills updated")
    return out


def scrape_backfill(con, batch_size, batch_offset):
    """
    Fetch a fixed-size batch starting at batch_offset.
    Run repeatedly tonight with increasing offsets to build up the full DB.
    Example:
        python scraper.py --mode backfill --offset 0   --size 50
        python scraper.py --mode backfill --offset 50  --size 50
        python scraper.py --mode backfill --offset 100 --size 50
    """
    log.info(f"MODE: backfill | offset={batch_offset} size={batch_size}")

    # Figure out which page(s) we need (API max 250/page)
    api_limit  = 250
    api_offset = (batch_offset // api_limit) * api_limit
    skip       = batch_offset % api_limit

    out = []

    while len(out) < batch_size:
        data  = fetch_bills(CONGRESS, limit=api_limit, offset=api_offset)
        bills = data.get("bills", [])
        if not bills:
            log.info("Reached end of available bills.")
            break

        for b in bills[skip:]:
            if len(out) >= batch_size:
                break
            try:
                detail = fetch_bill_detail(b["congress"], b["type"], b["number"])
                time.sleep(0.15)
            except Exception as e:
                log.warning(f"Detail failed for {b.get('type')}{b.get('number')}: {e}")
                detail = {}
            row = build_row(b, detail)
            upsert(con, row)
            out.append(row)
            total_in_db = con.execute("SELECT COUNT(*) FROM bills").fetchone()[0]
            log.info(
                f"  [{total_in_db} in DB] {row['bill_id']} | "
                f"{row['sponsor_party']} | {row['introduced_date']}"
            )

        api_offset += api_limit
        skip = 0
        time.sleep(0.2)

    log.info(f"Backfill batch done: {len(out)} fetched")
    log.info(f"Next batch offset: {batch_offset + len(out)}")
    return out


# ─────────────────────────────────────────────
# EXPORT — always reads full DB
# ─────────────────────────────────────────────
def export_json(con):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()

    rows = con.execute("""
        SELECT
            bill_id, congress, bill_type, bill_number, title,
            CAST(introduced_date    AS VARCHAR) AS introduced_date,
            latest_action,
            CAST(latest_action_date AS VARCHAR) AS latest_action_date,
            sponsor_name, sponsor_party, sponsor_state, origin_chamber,
            CAST(updated_at         AS VARCHAR) AS updated_at
        FROM bills
        ORDER BY introduced_date DESC NULLS LAST
    """).fetchdf().to_dict(orient="records")

    # Clean up pandas NaN/NaT
    for row in rows:
        for k, v in row.items():
            if v != v or str(v) in ("NaT", "nan", "None"):
                row[k] = None

    payload = {
        "last_updated": timestamp,
        "count":        len(rows),
        "data":         rows,
    }
    (DATA_DIR / "bills.json").write_text(
        json.dumps(payload, indent=2, default=str), encoding="utf-8"
    )

    def write(name, data):
        (DATA_DIR / name).write_text(json.dumps(data, indent=2), encoding="utf-8")

    party = {}
    for r in rows:
        k = r.get("sponsor_party")
        if k: party[k] = party.get(k, 0) + 1
    write("by_party.json", party)

    chamber = {}
    for r in rows:
        k = r.get("origin_chamber")
        if k: chamber[k] = chamber.get(k, 0) + 1
    write("by_chamber.json", chamber)

    state = {}
    for r in rows:
        k = r.get("sponsor_state")
        if k: state[k] = state.get(k, 0) + 1
    write("by_state.json", state)

    total = con.execute("SELECT COUNT(*) FROM bills").fetchone()[0]
    log.info(f"Exported {total} bills from DB → {DATA_DIR / 'bills.json'}")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser(description="Congress.gov scraper")
    p.add_argument("--mode",   default=None,
                   choices=["dev", "incremental", "backfill"],
                   help="Override RUN_MODE env var")
    p.add_argument("--size",   type=int, default=None,
                   help="Backfill batch size (overrides BATCH_SIZE env var)")
    p.add_argument("--offset", type=int, default=None,
                   help="Backfill start offset (overrides BATCH_OFFSET env var)")
    return p.parse_args()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if not API_KEY:
        raise SystemExit("Missing CONGRESS_API_KEY — set it in .env or as an env var")

    args = parse_args()

    mode         = args.mode   or RUN_MODE
    batch_size   = args.size   or BATCH_SIZE
    batch_offset = args.offset if args.offset is not None else BATCH_OFFSET

    log.info(f"RUN_MODE = {mode}")
    log.info(f"CONGRESS = {CONGRESS}")
    log.info(f"DB PATH  = {DB_PATH}")

    con = get_db()

    if mode == "dev":
        this_run = scrape_dev(con)

    elif mode == "incremental":
        this_run = scrape_incremental(con)

    elif mode == "backfill":
        this_run = scrape_backfill(con, batch_size, batch_offset)

    else:
        raise SystemExit(f"Unknown RUN_MODE: {mode}")

    export_json(con)
    con.close()

    log.info(f"DONE ✓  {len(this_run)} bills this run")