"""
classifications.py
───────────────────
Multi-model Congress bill classification system.

Fixes:
- JSON serialization (numpy → python types)
- sklearn feature name warnings
- slow loops removed
- clean multi-model output
"""

import json
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

from sklearn.ensemble import (
    RandomForestClassifier,
    ExtraTreesClassifier,
    GradientBoostingClassifier
)

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score


# ─────────────────────────────
BASE = Path(__file__).parent
DB = BASE / "congress.duckdb"
OUT = BASE / "data" / "predictions.json"


# ─────────────────────────────
def to_py(x):
    """Convert numpy types → native Python types"""
    if isinstance(x, (np.integer, np.int64)):
        return int(x)
    if isinstance(x, (np.floating, np.float32, np.float64)):
        return float(x)
    return x


# ─────────────────────────────
def load_data():
    con = duckdb.connect(str(DB))
    df = con.execute("""
        SELECT bill_id, bill_type, origin_chamber,
               sponsor_party, sponsor_state,
               introduced_date, latest_action, title
        FROM bills
        WHERE latest_action IS NOT NULL
    """).fetchdf()
    con.close()
    return df


# ─────────────────────────────
def make_label(text):
    if not text:
        return 0
    keys = ["Passed", "Agreed", "Law", "Veto", "Failed"]
    t = str(text).lower()
    return int(any(k.lower() in t for k in keys))


# ─────────────────────────────
def build_features(df):
    df = df.copy()

    df["y"] = df["latest_action"].apply(make_label)

    df["title_len"] = df["title"].fillna("").str.len()
    df["title_words"] = df["title"].fillna("").str.split().str.len()

    cats = ["bill_type", "origin_chamber", "sponsor_party", "sponsor_state"]

    for c in cats:
        df[c] = df[c].fillna("UNK")
        df[c] = LabelEncoder().fit_transform(df[c])

    features = [
        "bill_type",
        "origin_chamber",
        "sponsor_party",
        "sponsor_state",
        "title_len",
        "title_words"
    ]

    return df, features


# ─────────────────────────────
def train_models(X_train, y_train):
    models = {
        "random_forest": RandomForestClassifier(n_estimators=200, random_state=42),
        "extra_trees": ExtraTreesClassifier(n_estimators=200, random_state=42),
        "gradient_boost": GradientBoostingClassifier(random_state=42),
        "logistic": LogisticRegression(max_iter=1000)
    }

    for name, model in models.items():
        model.fit(X_train, y_train)

    return models


# ─────────────────────────────
def run():
    df = load_data()
    df, features = build_features(df)

    X = df[features]
    y = df["y"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        stratify=y,
        random_state=42
    )

    models = train_models(X_train, y_train)

    # ─────────────────────────────
    # PREDICTIONS (vectorized, fast)
    # ─────────────────────────────
    preds = {
        name: model.predict_proba(X)[:, 1]
        for name, model in models.items()
    }

    # ─────────────────────────────
    # METRIC (ExtraTrees baseline)
    # ─────────────────────────────
    auc = roc_auc_score(
        y_test,
        models["extra_trees"].predict_proba(X_test)[:, 1]
    )

    # ─────────────────────────────
    # BUILD OUTPUT (SAFE JSON)
    # ─────────────────────────────
    results = []

    for i in range(len(df)):
        results.append({
            "bill_id": str(df.iloc[i]["bill_id"]),
            "title": str(df.iloc[i]["title"]),
            "party": str(df.iloc[i]["sponsor_party"]),
            "state": str(df.iloc[i]["sponsor_state"]),
            "chamber": str(df.iloc[i]["origin_chamber"]),

            "models": {
                k: float(to_py(v[i]))
                for k, v in preds.items()
            }
        })

    # sort by best model (ExtraTrees)
    results.sort(
        key=lambda x: x["models"]["extra_trees"],
        reverse=True
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "auc": float(auc),
            "best_model": "extra_trees"
        },
        "top_100": results[:100]
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)

    OUT.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8"
    )

    print("DONE ✔")


# ─────────────────────────────
if __name__ == "__main__":
    run()