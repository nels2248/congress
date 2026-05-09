import os
import xml.etree.ElementTree as ET
import duckdb
import json
import pandas as pd
import numpy as np

# ----------------------------
# CONFIG
# ----------------------------
FOLDER = "BILLSTATUS-119-hres"
DB_FILE = "congress.duckdb"

con = duckdb.connect(DB_FILE)

# ----------------------------
# RESET TABLES
# ----------------------------
for t in ["bills", "bill_actions", "bill_cosponsors"]:
    con.execute(f"DELETE FROM {t}")

print("Tables truncated")

# ----------------------------
# CREATE TABLES
# ----------------------------
con.execute("""
CREATE TABLE IF NOT EXISTS bills (
    file_name TEXT,
    bill_number TEXT,
    congress TEXT,
    bill_type TEXT,
    origin_chamber TEXT,
    introduced_date TEXT,
    title TEXT
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS bill_actions (
    file_name TEXT,
    bill_number TEXT,
    action_date TEXT,
    action_time TEXT,
    text TEXT,
    type TEXT,
    action_code TEXT
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS bill_cosponsors (
    file_name TEXT,
    bill_number TEXT,
    full_name TEXT,
    party TEXT,
    state TEXT,
    district TEXT
)
""")

# ----------------------------
# HELPERS
# ----------------------------
def get_text(node, path):
    el = node.find(path)
    return el.text.strip() if el is not None and el.text else None

def clean(val):
    return str(val).strip() if val is not None else None

# ----------------------------
# STORAGE
# ----------------------------
bills_rows = []
actions_rows = []
cosponsor_rows = []

# ----------------------------
# PROCESS FILES
# ----------------------------
for file in os.listdir(FOLDER):
    if not file.endswith(".xml"):
        continue

    path = os.path.join(FOLDER, file)

    try:
        tree = ET.parse(path)
        root = tree.getroot()

        bill = root.find("bill")
        if bill is None:
            continue

        bill_number = clean(get_text(bill, "number"))

        bills_rows.append((
            file,
            bill_number,
            clean(get_text(bill, "congress")),
            get_text(bill, "type"),
            get_text(bill, "originChamber"),
            get_text(bill, "introducedDate"),
            get_text(bill, "title")
        ))

        actions = bill.find("actions")
        if actions is not None:
            for item in actions.findall(".//item"):
                actions_rows.append((
                    file,
                    bill_number,
                    get_text(item, "actionDate"),
                    get_text(item, "actionTime"),
                    get_text(item, "text"),
                    get_text(item, "type"),
                    get_text(item, "actionCode")
                ))

        cosponsors = bill.find("cosponsors")
        if cosponsors is not None:
            for s in cosponsors.findall("item"):

                name = get_text(s, "fullName")
                if not name:
                    continue

                cosponsor_rows.append((
                    file,
                    bill_number,
                    name,
                    get_text(s, "party"),
                    get_text(s, "state"),
                    get_text(s, "district")
                ))

    except Exception as e:
        print(f"Error in {file}: {e}")

# ----------------------------
# LOAD INTO DUCKDB
# ----------------------------
con.executemany("INSERT INTO bills VALUES (?, ?, ?, ?, ?, ?, ?)", bills_rows)
con.executemany("INSERT INTO bill_actions VALUES (?, ?, ?, ?, ?, ?, ?)", actions_rows)
con.executemany("INSERT INTO bill_cosponsors VALUES (?, ?, ?, ?, ?, ?)", cosponsor_rows)

print("ETL complete")

# ----------------------------
# DATAFRAME EXPORT
# ----------------------------
bills_df = con.execute("SELECT * FROM bills").df()
actions_df = con.execute("SELECT * FROM bill_actions").df()
cosponsors_df = con.execute("SELECT * FROM bill_cosponsors").df()

# CLEAN KEYS
for df in [bills_df, actions_df, cosponsors_df]:
    df["bill_number"] = df["bill_number"].astype(str).str.strip()

# ----------------------------
# COUNTS
# ----------------------------
action_counts = actions_df.groupby("bill_number").size()
cosponsor_counts = cosponsors_df.groupby("bill_number").size()

bills_df["action_count"] = bills_df["bill_number"].map(action_counts).fillna(0).astype(int)
bills_df["cosponsor_count"] = bills_df["bill_number"].map(cosponsor_counts).fillna(0).astype(int)

# ----------------------------
# SPONSOR SAFETY
# ----------------------------
try:
    sponsor_df = con.execute("SELECT * FROM bill_sponsors").df()
    sponsor_df["bill_number"] = sponsor_df["bill_number"].astype(str).str.strip()

    sponsor_lookup = (
        sponsor_df.groupby("bill_number")["full_name"]
        .first()
        .reset_index()
        .rename(columns={"full_name": "sponsor"})
    )

except Exception:
    sponsor_lookup = pd.DataFrame(columns=["bill_number", "sponsor"])

bills_df = bills_df.merge(sponsor_lookup, on="bill_number", how="left")

fallback = (
    cosponsors_df.groupby("bill_number")["full_name"]
    .first()
    .reset_index()
    .rename(columns={"full_name": "fallback_sponsor"})
)

bills_df = bills_df.merge(fallback, on="bill_number", how="left")

bills_df["sponsor"] = bills_df["sponsor"].fillna(bills_df["fallback_sponsor"])
bills_df["sponsor"] = bills_df["sponsor"].fillna("Unknown")

# ----------------------------
# 🚨 JSON SAFETY CLEANUP (CRITICAL FIX)
# ----------------------------
bills_df = bills_df.replace([np.nan, np.inf, -np.inf], None)
actions_df = actions_df.replace([np.nan, np.inf, -np.inf], None)
cosponsors_df = cosponsors_df.replace([np.nan, np.inf, -np.inf], None)

# ----------------------------
# EXPORT JSON
# ----------------------------
output = {
    "bills": bills_df.to_dict(orient="records"),
    "actions": actions_df.to_dict(orient="records"),
    "cosponsors": cosponsors_df.to_dict(orient="records")
}

with open("congress_dashboard.json", "w") as f:
    json.dump(output, f, default=str)

print("Exported dashboard JSON safely")