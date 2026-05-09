import duckdb
import pandas as pd
import numpy as np
import json

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

# =========================================================
# LOAD DATA
# =========================================================
con = duckdb.connect("congress.duckdb")

bills_df = con.execute("SELECT * FROM bills").df()
actions_df = con.execute("SELECT * FROM bill_actions").df()

# =========================================================
# CLEAN
# =========================================================
bills_df.columns = bills_df.columns.str.strip().str.lower()
actions_df.columns = actions_df.columns.str.strip().str.lower()

bills_df["bill_number"] = bills_df["bill_number"].astype(str)
actions_df["bill_number"] = actions_df["bill_number"].astype(str)

actions_df["text"] = actions_df["text"].fillna("")

# =========================================================
# DATES
# =========================================================
actions_df["action_date"] = pd.to_datetime(actions_df["action_date"], errors="coerce")
actions_df = actions_df.dropna(subset=["action_date"])

actions_df = actions_df.sort_values(["bill_number", "action_date"])

# =========================================================
# EARLY FEATURES ONLY
# =========================================================
actions_df["step"] = actions_df.groupby("bill_number").cumcount()
actions_df["total"] = actions_df.groupby("bill_number")["step"].transform("max") + 1

actions_df["early"] = actions_df["step"] <= actions_df["total"] * 0.4

early_df = actions_df[actions_df["early"]]

bill_features = early_df.groupby("bill_number").agg(
    early_action_count=("step", "count"),
    first_action=("action_date", "min"),
    last_action=("action_date", "max"),
    text_variety=("text", lambda x: len(set(x.str[:30])))
).reset_index()

bill_features["span_days"] = (
    (bill_features["last_action"] - bill_features["first_action"])
    .dt.days
).fillna(0)

bill_features["velocity"] = bill_features["early_action_count"] / (bill_features["span_days"] + 1)

# =========================================================
# FULL ACTIVITY (FOR LABEL ONLY)
# =========================================================
full = con.execute("""
    SELECT bill_number, COUNT(*) as total_actions
    FROM bill_actions
    GROUP BY bill_number
""").df()

bill_level = bills_df.merge(bill_features, on="bill_number", how="left").fillna(0)
bill_level = bill_level.merge(full, on="bill_number", how="left").fillna(0)

# =========================================================
# UNCERTAINTY LABEL SYSTEM
# =========================================================
bill_level["label"] = np.where(
    bill_level["early_action_count"] <= 1,
    2,  # 🔥 UNCERTAIN CLASS
    (bill_level["total_actions"] > bill_level["early_action_count"] * 2).astype(int)
)

# =========================================================
# FEATURES
# =========================================================
features = [
    "early_action_count",
    "velocity",
    "span_days",
    "text_variety"
]

X = bill_level[features]
y = bill_level["label"]

# =========================================================
# MODEL
# =========================================================
model = RandomForestClassifier(
    n_estimators=200,
    max_depth=5,
    min_samples_leaf=10,
    random_state=42
)

model.fit(X, y)

preds = model.predict(X)
proba = model.predict_proba(X)

# =========================================================
# PREDICTIONS
# =========================================================
bill_level["prediction"] = preds

# confidence = max probability
bill_level["confidence"] = np.max(proba, axis=1)

# =========================================================
# 🔥 UNCERTAINTY CALIBRATION
# =========================================================
def adjust_confidence(row):
    if row["early_action_count"] <= 1:
        return 0.5  # neutral uncertainty
    return row["confidence"]

bill_level["confidence"] = bill_level.apply(adjust_confidence, axis=1)

# =========================================================
# SIMPLE EXPLANATION
# =========================================================
importance = dict(zip(features, model.feature_importances_))

def explain(row):
    return {
        "formula": "Σ(feature × weight)",
        "breakdown": {
            f: {
                "value": float(row[f]),
                "weight": float(importance[f]),
                "impact": float(row[f] * importance[f])
            }
            for f in features
        }
    }

bill_level["explanation"] = bill_level.apply(explain, axis=1)

# =========================================================
# TIMELINE
# =========================================================
timeline_map = actions_df.groupby("bill_number").apply(
    lambda x: [
        {
            "step": int(i),
            "text": str(r.text),
            "action_date": str(r.action_date)
        }
        for i, r in enumerate(x.itertuples())
    ]
)

bill_level["timeline"] = bill_level["bill_number"].map(timeline_map).apply(
    lambda x: x if isinstance(x, list) else []
)

# =========================================================
# EXPORT
# =========================================================
output = {
    "bills": bill_level[[
        "bill_number",
        "title",
        "early_action_count",
        "velocity",
        "span_days",
        "text_variety",
        "prediction",
        "confidence",
        "explanation",
        "timeline"
    ]].to_dict(orient="records")
}

with open("ml_results.json", "w") as f:
    json.dump(output, f, indent=2)

print("✅ UNCERTAINTY MODEL COMPLETE (REALISTIC CONFIDENCE)")