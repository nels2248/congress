"""
Congress.gov FAST ETL Pipeline (Stable Version)

Includes:
- Bills
- Cosponsors
- Actions
- Committees
- Related Bills
- Amendments
- JSON export with child counts
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
from datetime import datetime, timezone, timedelta

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

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --------------------------------------------------
# SAFE HELPERS
# --------------------------------------------------

def safe_dict(x):
    return x if isinstance(x, dict) else {}

def safe_list(x):
    return x if isinstance(x, list) else []

# --------------------------------------------------
# RUNNER
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
        updated_at TIMESTAMP
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS bill_cosponsors (
        bill_id VARCHAR,
        bioguide_id VARCHAR,
        full_name VARCHAR,
        party VARCHAR,
        state VARCHAR,
        district VARCHAR,
        sponsorship_date DATE,
        is_original BOOLEAN
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS bill_actions (
        bill_id VARCHAR,
        action_date DATE,
        action_text VARCHAR,
        action_type VARCHAR,
        action_code VARCHAR,
        source_system VARCHAR
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS bill_committees (
        bill_id VARCHAR,
        committee_code VARCHAR,
        committee_name VARCHAR,
        chamber VARCHAR,
        committee_type VARCHAR,
        activity_name VARCHAR,
        activity_date TIMESTAMP
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS bill_relatedbills (
        bill_id VARCHAR,
        related_congress INTEGER,
        related_type VARCHAR,
        related_number VARCHAR,
        related_title VARCHAR,
        relationship_type VARCHAR,
        identified_by VARCHAR,
        latest_action_date DATE
    )
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS bill_amendments (
        bill_id VARCHAR,
        amendment_number VARCHAR,
        amendment_type VARCHAR,
        congress INTEGER,
        purpose VARCHAR,
        description VARCHAR,
        update_date TIMESTAMP,
        action_date DATE,
        action_text VARCHAR,
        url VARCHAR
    )
    """)

    return con

# --------------------------------------------------
# FETCH
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

    r = httpx.get(f"{BASE_URL}/bill/{CONGRESS}", params=params, timeout=60)
    r.raise_for_status()

    return r.json().get("bills", [])

SEM = asyncio.Semaphore(CONCURRENCY)

# --------------------------------------------------
# PAGINATION
# --------------------------------------------------

async def fetch_all(client, url, root):

    offset, limit = 0, 250
    out = []

    while True:

        async with SEM:
            r = await client.get(
                url,
                params={
                    "api_key": API_KEY,
                    "format": "json",
                    "limit": limit,
                    "offset": offset
                },
                timeout=60
            )

        data = safe_dict(r.json())
        items = safe_list(data.get(root))

        if not items:
            break

        out.extend(items)

        if len(items) < limit:
            break

        offset += limit

    return out

# --------------------------------------------------
# ENDPOINTS
# --------------------------------------------------

async def fetch_detail(client, b):
    url = f"{BASE_URL}/bill/{b['congress']}/{b['type'].lower()}/{b['number']}"
    res = await fetch_all(client, url, "bill")
    return res[0] if res else {}

async def fetch_cosponsors(client, c, t, n):
    return await fetch_all(client, f"{BASE_URL}/bill/{c}/{t}/{n}/cosponsors", "cosponsors")

async def fetch_actions(client, c, t, n):
    return await fetch_all(client, f"{BASE_URL}/bill/{c}/{t}/{n}/actions", "actions")

async def fetch_committees(client, c, t, n):
    return await fetch_all(client, f"{BASE_URL}/bill/{c}/{t}/{n}/committees", "committees")

async def fetch_related(client, c, t, n):
    return await fetch_all(client, f"{BASE_URL}/bill/{c}/{t}/{n}/relatedbills", "relatedBills")

async def fetch_amendments(client, c, t, n):
    return await fetch_all(client, f"{BASE_URL}/bill/{c}/{t}/{n}/amendments", "amendments")

# --------------------------------------------------
# BUILDERS
# --------------------------------------------------

def build_bill(b, d):

    b = safe_dict(b)
    d = safe_dict(d)

    latest = safe_dict(b.get("latestAction") or d.get("latestAction"))
    sponsor = safe_dict((d.get("sponsors") or [{}])[0] if isinstance(d.get("sponsors"), list) else {})

    return {
        "bill_id": f"{b.get('congress')}-{b.get('type')}-{b.get('number')}",
        "congress": b.get("congress"),
        "bill_type": b.get("type"),
        "bill_number": b.get("number"),
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

# --------------------------------------------------
# CHILD BUILDERS
# --------------------------------------------------

def build_cosponsors(bill_id, rows):
    return [{
        "bill_id": bill_id,
        "bioguide_id": r.get("bioguideId"),
        "full_name": r.get("fullName"),
        "party": r.get("party"),
        "state": r.get("state"),
        "district": r.get("district"),
        "sponsorship_date": r.get("sponsorshipDate"),
        "is_original": r.get("isOriginalCosponsor")
    } for r in safe_list(rows)]

def build_actions(bill_id, rows):
    return [{
        "bill_id": bill_id,
        "action_date": r.get("actionDate"),
        "action_text": r.get("text"),
        "action_type": r.get("type"),
        "action_code": r.get("actionCode"),
        "source_system": safe_dict(r.get("sourceSystem")).get("name")
    } for r in safe_list(rows)]

def build_committees(bill_id, rows):

    out = []

    for r in safe_list(rows):

        acts = safe_list(r.get("activities"))

        if not acts:
            out.append({
                "bill_id": bill_id,
                "committee_code": r.get("systemCode"),
                "committee_name": r.get("name"),
                "chamber": r.get("chamber"),
                "committee_type": r.get("type"),
                "activity_name": None,
                "activity_date": None
            })

        for a in acts:
            out.append({
                "bill_id": bill_id,
                "committee_code": r.get("systemCode"),
                "committee_name": r.get("name"),
                "chamber": r.get("chamber"),
                "committee_type": r.get("type"),
                "activity_name": a.get("name"),
                "activity_date": a.get("date")
            })

    return out

def build_related(bill_id, rows):

    out = []

    for r in safe_list(rows):
        for rel in safe_list(r.get("relationshipDetails")):
            out.append({
                "bill_id": bill_id,
                "related_congress": r.get("congress"),
                "related_type": r.get("type"),
                "related_number": r.get("number"),
                "related_title": r.get("title"),
                "relationship_type": rel.get("type"),
                "identified_by": rel.get("identifiedBy"),
                "latest_action_date": safe_dict(r.get("latestAction")).get("actionDate")
            })

    return out

def build_amendments(bill_id, rows):

    out = []

    for r in safe_list(rows):

        latest = safe_dict(r.get("latestAction"))

        out.append({
            "bill_id": bill_id,
            "amendment_number": r.get("number"),
            "amendment_type": r.get("type"),
            "congress": r.get("congress"),
            "purpose": r.get("purpose"),
            "description": r.get("description"),
            "update_date": r.get("updateDate"),
            "action_date": latest.get("actionDate"),
            "action_text": latest.get("text"),
            "url": r.get("url")
        })

    return out

# --------------------------------------------------
# PROCESS
# --------------------------------------------------

async def process_bills(bills):

    async with httpx.AsyncClient() as client:

        bill_rows, cos, act, com, rel, amz = [], [], [], [], [], []

        for b in bills:

            c, t, n = b["congress"], b["type"].lower(), b["number"]
            bill_id = f"{c}-{b['type']}-{n}"

            detail, cs, ac, cm, rl, am = await asyncio.gather(
                fetch_detail(client, b),
                fetch_cosponsors(client, c, t, n),
                fetch_actions(client, c, t, n),
                fetch_committees(client, c, t, n),
                fetch_related(client, c, t, n),
                fetch_amendments(client, c, t, n)
            )

            bill_rows.append(build_bill(b, detail))
            cos.extend(build_cosponsors(bill_id, cs))
            act.extend(build_actions(bill_id, ac))
            com.extend(build_committees(bill_id, cm))
            rel.extend(build_related(bill_id, rl))
            amz.extend(build_amendments(bill_id, am))

        return {
            "bills": bill_rows,
            "cosponsors": cos,
            "actions": act,
            "committees": com,
            "relatedbills": rel,
            "amendments": amz
        }

# --------------------------------------------------
# UPSERT
# --------------------------------------------------

def bulk_upsert_all(con, data):

    def upsert(table, df):
        con.register("tmp", df)
        con.execute(f"DELETE FROM {table} WHERE bill_id IN (SELECT bill_id FROM tmp)")
        con.execute(f"INSERT INTO {table} SELECT * FROM tmp")

    for k, t in [
        ("bills", "bills"),
        ("cosponsors", "bill_cosponsors"),
        ("actions", "bill_actions"),
        ("committees", "bill_committees"),
        ("relatedbills", "bill_relatedbills"),
        ("amendments", "bill_amendments")
    ]:
        if data.get(k):
            upsert(t, pd.DataFrame(data[k]))

# --------------------------------------------------
# JSON EXPORT WITH COUNTS
# --------------------------------------------------

def get_child_counts(con, bill_id):

    return {
        "cosponsors_count": con.execute(
            "SELECT COUNT(*) FROM bill_cosponsors WHERE bill_id = ?",
            [bill_id]
        ).fetchone()[0],

        "actions_count": con.execute(
            "SELECT COUNT(*) FROM bill_actions WHERE bill_id = ?",
            [bill_id]
        ).fetchone()[0],

        "committees_count": con.execute(
            "SELECT COUNT(*) FROM bill_committees WHERE bill_id = ?",
            [bill_id]
        ).fetchone()[0],

        "relatedbills_count": con.execute(
            "SELECT COUNT(*) FROM bill_relatedbills WHERE bill_id = ?",
            [bill_id]
        ).fetchone()[0],

        "amendments_count": con.execute(
            "SELECT COUNT(*) FROM bill_amendments WHERE bill_id = ?",
            [bill_id]
        ).fetchone()[0],
    }

def export_json(con):

    DATA_DIR.mkdir(exist_ok=True)

    df = con.execute("""
        SELECT *
        FROM bills
        ORDER BY introduced_date DESC NULLS LAST
    """).fetchdf()

    rows = []

    for r in df.to_dict("records"):
        rows.append({
            **r,
            **get_child_counts(con, r["bill_id"])
        })

    payload = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "count": len(rows),
        "data": rows
    }

    with open(DATA_DIR / "bills.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)

    log.info("Exported data/bills.json")

# --------------------------------------------------
# MAIN
# --------------------------------------------------

if __name__ == "__main__":

    if not API_KEY:
        raise SystemExit("Missing CONGRESS_API_KEY")

    con = get_db()

    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else RUN_MODE

    if mode == "dev":
        bills = fetch_bills_page(limit=5)
        data = run_async(process_bills(bills))
        bulk_upsert_all(con, data)
        export_json(con)

    elif mode == "incremental":
        since = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

        offset = 0
        while True:
            bills = fetch_bills_page(limit=250, offset=offset, from_date=since)
            if not bills:
                break

            data = run_async(process_bills(bills))
            bulk_upsert_all(con, data)

            offset += 250

        export_json(con)

    else:
        con.execute("DELETE FROM bills")

        offset = 0
        while True:
            bills = fetch_bills_page(limit=250, offset=offset)
            if not bills:
                break

            data = run_async(process_bills(bills))
            bulk_upsert_all(con, data)

            offset += 250

        export_json(con)

    con.close()