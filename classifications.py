"""
analyze.py
──────────
Likelihood-of-vote model for Congress bills.
Reads from congress.duckdb, trains a Random Forest with class balancing,
exports predictions to data/predictions.json for the dashboard.

Run:
    python analyze.py

Requirements:
    pip install scikit-learn pandas duckdb
"""

import json
import logging
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    roc_auc_score,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH  = BASE_DIR / "congress.duckdb"
DATA_DIR = BASE_DIR / "data"

# ─────────────────────────────────────────────
# VOTE LABEL
# ─────────────────────────────────────────────
VOTE_KEYWORDS = [
    "Passed",
    "Agreed to",
    "Became Public Law",
    "Signed by President",
    "Vetoed",
    "Failed",
    "Recorded Vote",
    "Roll call",
]

def is_vote(action_text):
    if not action_text:
        return 0
    t = action_text.lower()
    return int(any(kw.lower() in t for kw in VOTE_KEYWORDS))


# ─────────────────────────────────────────────
# LOAD
# ─────────────────────────────────────────────
def load_data():
    log.info(f"Loading from {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))
    df  = con.execute("""
        SELECT
            bill_id,
            bill_type,
            origin_chamber,
            sponsor_party,
            sponsor_state,
            introduced_date,
            latest_action,
            latest_action_date,
            title
        FROM bills
        WHERE latest_action IS NOT NULL
    """).fetchdf()
    con.close()
    log.info(f"Loaded {len(df):,} bills")
    return df


# ─────────────────────────────────────────────
# FEATURES
# ─────────────────────────────────────────────
def build_features(df):
    df = df.copy()

    # Label
    df["got_vote"] = df["latest_action"].apply(is_vote)
    log.info(f"Positive labels: {df['got_vote'].sum()} ({df['got_vote'].mean():.1%})")

    # Date features
    df["introduced_date"] = pd.to_datetime(df["introduced_date"], errors="coerce")
    df["intro_month"]      = df["introduced_date"].dt.month.fillna(0).astype(int)
    df["intro_dayofweek"]  = df["introduced_date"].dt.dayofweek.fillna(0).astype(int)
    df["intro_quarter"]    = df["introduced_date"].dt.quarter.fillna(0).astype(int)

    # Days since introduction
    ref_date = pd.Timestamp("2026-04-18")
    df["days_since_intro"] = (
        (ref_date - df["introduced_date"]).dt.days.fillna(-1).astype(int)
    )

    # Title features
    df["title_length"] = df["title"].fillna("").str.len()
    df["title_words"]  = df["title"].fillna("").str.split().str.len()

    # Encode categoricals
    cat_cols = ["bill_type", "origin_chamber", "sponsor_party", "sponsor_state"]
    encoders = {}
    for col in cat_cols:
        df[col] = df[col].fillna("UNKNOWN")
        le = LabelEncoder()
        df[f"{col}_enc"] = le.fit_transform(df[col])
        encoders[col] = le

    feature_cols = [
        "bill_type_enc",
        "origin_chamber_enc",
        "sponsor_party_enc",
        "sponsor_state_enc",
        "intro_month",
        "intro_dayofweek",
        "intro_quarter",
        "days_since_intro",
        "title_length",
        "title_words",
    ]

    return df, feature_cols, encoders


# ─────────────────────────────────────────────
# TRAIN
# ─────────────────────────────────────────────
def train(df, feature_cols):
    X = df[feature_cols]
    y = df["got_vote"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=8,
        min_samples_leaf=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )

    log.info("Training Random Forest...")
    model.fit(X_train, y_train)

    # Evaluation
    y_pred      = model.predict(X_test)
    y_pred_prob = model.predict_proba(X_test)[:, 1]
    auc         = roc_auc_score(y_test, y_pred_prob)

    log.info(f"\n{classification_report(y_test, y_pred)}")
    log.info(f"ROC-AUC: {auc:.3f}")
    log.info(f"\nConfusion matrix:\n{confusion_matrix(y_test, y_pred)}")

    # Cross-validation
    cv_scores = cross_val_score(model, X, y, cv=5, scoring="roc_auc")
    log.info(f"5-fold CV AUC: {cv_scores.mean():.3f} (+/- {cv_scores.std():.3f})")

    # Feature importance
    importances = pd.Series(
        model.feature_importances_, index=feature_cols
    ).sort_values(ascending=False)
    log.info(f"\nFeature importances:\n{importances.to_string()}")

    metrics = {
        "auc":                  round(float(auc), 3),
        "cv_auc":               round(float(cv_scores.mean()), 3),
        "cv_auc_std":           round(float(cv_scores.std()), 3),
        "n_train":              int(len(X_train)),
        "n_test":               int(len(X_test)),
        "n_positive":           int(y.sum()),
        "positive_rate":        round(float(y.mean()), 4),
        "feature_importances":  {
            k: round(float(v), 4)
            for k, v in importances.items()
        },
    }

    return model, metrics


# ─────────────────────────────────────────────
# PREDICT + EXPORT
# ─────────────────────────────────────────────
def export_predictions(df, feature_cols, model, metrics=None):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    df = df.copy()
    df["vote_probability"] = model.predict_proba(df[feature_cols])[:, 1].round(4)

    def tier(p):
        if p >= 0.60: return "high"
        if p >= 0.30: return "medium"
        return "low"

    df["vote_tier"] = df["vote_probability"].apply(tier)

    def row_to_dict(r):
        intro = r["introduced_date"]
        return {
            "bill_id":          r["bill_id"],
            "bill_type":        r["bill_type"],
            "bill_number":      r["bill_id"].split("-")[-1] if r["bill_id"] else None,
            "title":            r["title"],
            "sponsor_party":    r["sponsor_party"] if r["sponsor_party"] != "UNKNOWN" else None,
            "sponsor_state":    r["sponsor_state"] if r["sponsor_state"] != "UNKNOWN" else None,
            "origin_chamber":   r["origin_chamber"],
            "introduced_date":  str(intro)[:10] if pd.notna(intro) else None,
            "latest_action":    r["latest_action"],
            "vote_probability": float(r["vote_probability"]),
            "vote_tier":        r["vote_tier"],
            "got_vote":         int(r["got_vote"]),
        }

    # All bills sorted by probability
    all_sorted = df.sort_values("vote_probability", ascending=False)
    all_rows   = [row_to_dict(r) for _, r in all_sorted.iterrows()]

    # Top 100 that haven't gotten a vote yet
    no_vote_yet = df[df["got_vote"] == 0].sort_values("vote_probability", ascending=False)
    top_100     = [row_to_dict(r) for _, r in no_vote_yet.head(100).iterrows()]

    # Tier counts
    tier_counts = df["vote_tier"].value_counts().to_dict()

    # Avg probability by party
    by_party = (
        df[df["sponsor_party"] != "UNKNOWN"]
        .groupby("sponsor_party")["vote_probability"]
        .agg(avg_probability="mean", bill_count="count")
        .round(4)
        .reset_index()
        .sort_values("avg_probability", ascending=False)
        .to_dict(orient="records")
    )

    # Avg probability by bill type
    by_type = (
        df.groupby("bill_type")["vote_probability"]
        .agg(avg_probability="mean", bill_count="count")
        .round(4)
        .reset_index()
        .sort_values("avg_probability", ascending=False)
        .to_dict(orient="records")
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics":      metrics or {},
        "top_100":      top_100,
        "tier_counts":  tier_counts,
        "by_party":     by_party,
        "by_type":      by_type,
        "all_bills":    all_rows,
    }

    out = DATA_DIR / "predictions.json"
    out.write_text(
        json.dumps(payload, indent=2, default=str),
        encoding="utf-8",
    )

    log.info(f"Exported predictions → {out}")
    log.info(f"  Total bills scored: {len(all_rows):,}")
    log.info(f"  High tier:          {tier_counts.get('high', 0):,}")
    log.info(f"  Medium tier:        {tier_counts.get('medium', 0):,}")
    log.info(f"  Low tier:           {tier_counts.get('low', 0):,}")
    log.info(f"  Top 100 (no vote):  {len(top_100)}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    df                         = load_data()
    df, feature_cols, encoders = build_features(df)
    model, metrics             = train(df, feature_cols)

    log.info("\n=== Model metrics ===")
    for k, v in metrics.items():
        if k != "feature_importances":
            log.info(f"  {k:25s} {v}")

    export_predictions(df, feature_cols, model, metrics)

    log.info("DONE ✓")