import duckdb
import pandas as pd
import numpy as np
import json
import os

from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

os.environ["OMP_NUM_THREADS"] = "5"

# ----------------------------
# LOAD DATA
# ----------------------------
con = duckdb.connect("congress.duckdb")

bills_df = con.execute("SELECT * FROM bills").df()
actions_df = con.execute("SELECT * FROM bill_actions").df()

# optional tables (safe load)
try:
    sponsors_df = con.execute("SELECT * FROM bill_sponsors").df()
except:
    sponsors_df = pd.DataFrame(columns=["bill_number", "full_name"])

try:
    cosponsors_df = con.execute("SELECT * FROM bill_cosponsors").df()
except:
    cosponsors_df = pd.DataFrame(columns=["bill_number", "full_name"])

# normalize
for df in [bills_df, actions_df, sponsors_df, cosponsors_df]:
    df.columns = df.columns.str.lower()
    if "bill_number" in df.columns:
        df["bill_number"] = df["bill_number"].astype(str)

# ----------------------------
# SPONSOR FIX (REAL SOURCE)
# ----------------------------
if not sponsors_df.empty:
    sponsor_map = sponsors_df.groupby("bill_number")["full_name"].first()
    bills_df["sponsor"] = bills_df["bill_number"].map(sponsor_map)

bills_df["sponsor"] = bills_df.get("sponsor", pd.Series(index=bills_df.index)).fillna("Unknown Sponsor")

# ----------------------------
# COSPONSORS FIX (REAL SOURCE)
# ----------------------------
if not cosponsors_df.empty:
    cos_map = cosponsors_df.groupby("bill_number")["full_name"].apply(list)
    cos_count = cosponsors_df.groupby("bill_number").size()

    bills_df["cosponsor_list"] = bills_df["bill_number"].map(cos_map).apply(
        lambda x: x if isinstance(x, list) else []
    )
    bills_df["cosponsor_count"] = bills_df["bill_number"].map(cos_count).fillna(0).astype(int)
else:
    bills_df["cosponsor_list"] = [[] for _ in range(len(bills_df))]
    bills_df["cosponsor_count"] = 0

# ----------------------------
# ACTION FEATURES
# ----------------------------
actions_df["action_date"] = pd.to_datetime(actions_df["action_date"], errors="coerce")
actions_df = actions_df.dropna(subset=["action_date"])

actions_df = actions_df.sort_values(["bill_number", "action_date"])
actions_df["step"] = actions_df.groupby("bill_number").cumcount()

early = actions_df[actions_df["step"] <= 3]

features = early.groupby("bill_number").agg(
    early_action_count=("step", "count"),
    text_variety=("text", lambda x: len(set(x.astype(str).str[:30]))),
    first_action=("action_date", "min"),
    last_action=("action_date", "max")
).reset_index()

features["span_days"] = (
    (features["last_action"] - features["first_action"]).dt.days
).fillna(0)

features["velocity"] = features["early_action_count"] / (features["span_days"] + 1)

# ----------------------------
# MERGE
# ----------------------------
df = bills_df.merge(features, on="bill_number", how="left")

for col in ["early_action_count", "text_variety", "span_days", "velocity"]:
    df[col] = df[col].fillna(0)

# ----------------------------
# FULL ACTION TIMELINE
# ----------------------------
action_history = actions_df.groupby("bill_number").apply(
    lambda x: x[["action_date", "text", "type"]]
    .sort_values("action_date")
    .to_dict("records")
).to_dict()

df["actions_timeline"] = df["bill_number"].map(action_history)
df["actions_timeline"] = df["actions_timeline"].apply(
    lambda x: x if isinstance(x, list) else []
)

# ----------------------------
# STAGE
# ----------------------------
def stage(r):
    if r["early_action_count"] <= 1:
        return "🟡 Introduced"
    if r["cosponsor_count"] >= 10:
        return "🟣 High Support"
    if r["velocity"] >= 1.5:
        return "🔵 Fast Moving"
    if r["velocity"] >= 0.5:
        return "🟠 Moderate Activity"
    return "⚪ Low Activity"

df["stage"] = df.apply(stage, axis=1)

# ----------------------------
# SUMMARY
# ----------------------------
df["activity_summary"] = (
    "Actions: " + df["early_action_count"].astype(int).astype(str) +
    " | Velocity: " + df["velocity"].round(2).astype(str) +
    " | Span Days: " + df["span_days"].astype(int).astype(str) +
    " | Cosponsors: " + df["cosponsor_count"].astype(str)
)

# ----------------------------
# CLUSTERING
# ----------------------------
cols = ["early_action_count", "velocity", "span_days", "text_variety", "cosponsor_count"]

X = df[cols].fillna(0)

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
df["cluster"] = kmeans.fit_predict(X_scaled)

# ----------------------------
# CLEAN EXPORT
# ----------------------------
df = df.replace({np.nan: None})

output = {
    "bills": df[[
        "bill_number",
        "title",
        "sponsor",
        "cosponsor_count",
        "cosponsor_list",
        "early_action_count",
        "velocity",
        "span_days",
        "text_variety",
        "stage",
        "cluster",
        "activity_summary",
        "actions_timeline"
    ]].to_dict(orient="records")
}

with open("cluster_results.json", "w") as f:
    json.dump(output, f, indent=2, default=str)

print("✅ FULL SPONSOR + COSPONSOR PIPELINE FIXED")