# -*- coding: utf-8 -*-
"""
Created on Fri Apr 24 21:45:37 2026

@author: nels2
"""

import duckdb
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "congress.duckdb"
DATA_DIR = BASE_DIR / "data"

def build_state_metrics():
    con = duckdb.connect(str(DB_PATH))

    rows = con.execute("""
        SELECT
            sponsor_state,
            COUNT(*) as bill_count
        FROM bills
        WHERE sponsor_state IS NOT NULL
        GROUP BY sponsor_state
    """).fetchall()

    result = {
        "type": "state_metrics",
        "data": [
            {"state": r[0], "value": r[1]} for r in rows
        ]
    }

    out_file = DATA_DIR / "state_metrics.json"
    out_file.write_text(json.dumps(result, indent=2))

    print(f"Saved {out_file}")

if __name__ == "__main__":
    build_state_metrics()