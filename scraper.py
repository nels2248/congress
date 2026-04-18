"""
Congress.gov ETL Pipeline (Static Frontend Ready)

- DuckDB = internal storage (optional/local analysis)
- JSON exports = frontend (GitHub Pages / static HTML)
- Designed for nightly GitHub Actions runs
"""

import httpx
import duckdb
import time
import os
import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
load_dotenv()

API_KEY  = os.getenv("CONGRESS_API_KEY")
BASE_URL = "https://api.congress.gov/v3"

CONGRESS = 119
RUN_MODE = os.getenv("RUN_MODE", "dev")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH  = BASE_DIR / "congress.duckdb"
DATA_DIR = BASE_DIR / "data"

CONFIG = {
    "limit":     250,
    "dev_limit": 3,
}

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
def fetch_bills(congress, limit=250, offset=0):
    r = httpx.get(
        f"{BASE_URL}/bill/{congress}",
        params={
            "api_key": API_KEY,
            "limit":   limit,
            "offset":  offset,
            "format":  "json",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def fetch_bill_detail(congress, bill_type, bill_number):
    """Fetch full detail for a single bill to get sponsors, introducedDate, etc."""
    r = httpx.get(
        f"{BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}",
        params={"api_key": API_KEY, "format": "json"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("bill", {})


# ─────────────────────────────────────────────
# SCRAPE
# ─────────────────────────────────────────────
def scrape(con):
    offset    = 0
    limit     = CONFIG["limit"]
    total     = 0
    bills_out = []

    while True:
        log.info(f"Fetching list offset={offset}")

        data  = fetch_bills(CONGRESS, limit, offset)
        bills = data.get("bills", [])

        if not bills:
            log.info("No more bills returned — done.")
            break

        for b in bills:
            bill_type   = b.get("type", "")
            bill_number = b.get("number", "")
            congress    = b.get("congress")
            bill_id     = f"{congress}-{bill_type}-{bill_number}"

            # Fetch detail record to get introducedDate + sponsors
            try:
                detail = fetch_bill_detail(congress, bill_type, bill_number)
                time.sleep(0.15)
            except Exception as e:
                log.warning(f"Detail fetch failed for {bill_id}: {e}")
                detail = {}

            latest  = detail.get("latestAction") or b.get("latestAction") or {}
            sponsor = (detail.get("sponsors") or [])[0:1]
            sponsor = sponsor[0] if sponsor else {}

            row = {
                "bill_id":            bill_id,
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

            con.execute("""
                INSERT OR REPLACE INTO bills VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, [
                row["bill_id"],
                row["congress"],
                row["bill_type"],
                row["bill_number"],
                row["title"],
                row["introduced_date"],
                row["latest_action"],
                row["latest_action_date"],
                row["sponsor_name"],
                row["sponsor_party"],
                row["sponsor_state"],
                row["origin_chamber"],
                row["updated_at"],
            ])

            bills_out.append(row)
            total += 1

            log.info(
                f"  [{total}] {bill_id} | "
                f"{sponsor.get('party', '?')} | "
                f"{sponsor.get('state', '?')} | "
                f"{detail.get('introducedDate', 'no date')}"
            )

            if RUN_MODE == "dev" and total >= CONFIG["dev_limit"]:
                log.info(f"DEV LIMIT HIT ({total})")
                return bills_out

        offset += limit
        time.sleep(0.2)

    return bills_out


# ─────────────────────────────────────────────
# EXPORT — reads full DB, not just current run
# ─────────────────────────────────────────────
def export_json(con):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()

    rows = con.execute("""
        SELECT
            bill_id,
            congress,
            bill_type,
            bill_number,
            title,
            CAST(introduced_date    AS VARCHAR) AS introduced_date,
            latest_action,
            CAST(latest_action_date AS VARCHAR) AS latest_action_date,
            sponsor_name,
            sponsor_party,
            sponsor_state,
            origin_chamber,
            CAST(updated_at         AS VARCHAR) AS updated_at
        FROM bills
        ORDER BY introduced_date DESC NULLS LAST
    """).fetchdf().to_dict(orient="records")

    # Replace pandas NaT/NaN with None so json.dumps doesn't choke
    for row in rows:
        for k, v in row.items():
            if v != v:          # NaN check
                row[k] = None
            elif str(v) in ("NaT", "nan", "None"):
                row[k] = None

    payload = {
        "last_updated": timestamp,
        "count":        len(rows),
        "data":         rows,
    }

    (DATA_DIR / "bills.json").write_text(
        json.dumps(payload, indent=2, default=str),
        encoding="utf-8",
    )
    log.info(f"Exported {len(rows)} bills from DB → {DATA_DIR / 'bills.json'}")

    def write(name, data):
        (DATA_DIR / name).write_text(
            json.dumps(data, indent=2),
            encoding="utf-8",
        )

    party = {}
    for r in rows:
        k = r.get("sponsor_party")
        if k:
            party[k] = party.get(k, 0) + 1
    write("by_party.json", party)

    chamber = {}
    for r in rows:
        k = r.get("origin_chamber")
        if k:
            chamber[k] = chamber.get(k, 0) + 1
    write("by_chamber.json", chamber)

    state = {}
    for r in rows:
        k = r.get("sponsor_state")
        if k:
            state[k] = state.get(k, 0) + 1
    write("by_state.json", state)

    log.info(f"Derived exports written (party / chamber / state)")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if not API_KEY:
        raise SystemExit("Missing CONGRESS_API_KEY — set it in .env or as an env var")

    log.info(f"RUN_MODE = {RUN_MODE}")
    log.info(f"CONGRESS = {CONGRESS}")
    log.info(f"DB PATH  = {DB_PATH}")
    log.info(f"DATA DIR = {DATA_DIR}")

    con = get_db()

    this_run = scrape(con)

    export_json(con)        # reads full accumulated DB, not just this run

    con.close()

    log.info(f"DONE ✓  {len(this_run)} bills fetched this run")
    log.info(f"       (export reflects full DB across all runs)")