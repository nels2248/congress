"""
Congress Bill Topic Clustering
================================
Embeds bill titles with a sentence transformer, reduces dimensions with UMAP,
clusters with HDBSCAN, and generates human-readable cluster labels via TF-IDF
keywords.

Output -> data/clusters.json (consumed by clusters.html)

Usage:
    pip install sentence-transformers umap-learn hdbscan scikit-learn numpy
    python cluster_topics.py

Env / config knobs:
    BILLS_JSON    path to bills.json   (default: data/bills.json)
    MAX_CLUSTERS  target upper bound on cluster count (default: 15)
    N_COMPONENTS  UMAP dimensions before clustering   (default: 10)
    N_DISPLAY     UMAP 2-D display dimensions         (default: 2)

How MAX_CLUSTERS works:
    The script starts with min_cluster_size = total_bills / (MAX_CLUSTERS * 2),
    runs HDBSCAN, then iteratively increases min_cluster_size until the cluster
    count is at or below MAX_CLUSTERS. This gives you a reliable ceiling
    without sacrificing cluster quality.
"""

import json
import os
import logging
import numpy as np
from pathlib import Path
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).resolve().parent
DATA_DIR   = BASE_DIR / "data"
BILLS_JSON = Path(os.getenv("BILLS_JSON", DATA_DIR / "bills.json"))
OUT_PATH   = DATA_DIR / "clusters.json"

MAX_CLUSTERS = int(os.getenv("MAX_CLUSTERS", 25))
N_COMPONENTS = int(os.getenv("N_COMPONENTS", 10))
N_DISPLAY    = int(os.getenv("N_DISPLAY",     2))
TOP_KW       = 8   # keywords per cluster label

# ── keyword stopwords ─────────────────────────────────────────────────────────
# These are added ON TOP of sklearn's built-in English stopwords.
# Any word here will never appear as a cluster label keyword.
# Add/remove freely — all comparisons are lowercased.
EXTRA_STOPWORDS = {
    # generic legislative boilerplate
    "act", "acts",
    "purposes", "purpose",
    "providing", "provided", "provide",
    "related", "relating", "relates",
    "amend", "amending", "amends", "amendment", "amendments",
    "require", "requires", "required", "requiring",
    "establish", "establishes", "establishing",
    "authorize", "authorizes", "authorizing", "authorized",
    "make", "makes", "making",
    "certain", "thereof", "thereto", "therein",
    # calendar / time noise
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
    "month", "months", "year", "years", "annual", "annually",
    "fiscal", "fy",
    # generic bill language
    "bill", "bills", "law", "laws", "section", "sections",
    "title", "subtitle", "subsection", "paragraph",
    "code", "united", "states", "federal", "national",
    "new", "use", "used", "uses", "using",
}


# ── load bills ────────────────────────────────────────────────────────────────
def load_bills():
    log.info(f"Loading bills from {BILLS_JSON}")
    payload = json.loads(BILLS_JSON.read_text(encoding="utf-8"))
    bills   = payload["data"]
    bills   = [b for b in bills if b.get("title")]
    log.info(f"  {len(bills)} bills with titles")
    return bills


# ── embed ─────────────────────────────────────────────────────────────────────
def embed_titles(titles):
    from sentence_transformers import SentenceTransformer
    log.info("Loading sentence-transformers model (all-MiniLM-L6-v2) ...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    log.info(f"Embedding {len(titles)} titles ...")
    embeddings = model.encode(titles, batch_size=64, show_progress_bar=True,
                              convert_to_numpy=True)
    log.info(f"  embedding shape: {embeddings.shape}")
    return embeddings


# ── reduce + cluster ──────────────────────────────────────────────────────────
def reduce_and_cluster(embeddings):
    import umap
    import hdbscan

    n = len(embeddings)

    # high-dim UMAP for clustering
    log.info(f"UMAP {embeddings.shape[1]}d -> {N_COMPONENTS}d ...")
    reducer_hi = umap.UMAP(n_components=N_COMPONENTS, random_state=42,
                           n_neighbors=20, min_dist=0.0, metric="cosine")
    reduced_hi = reducer_hi.fit_transform(embeddings)

    # 2-D UMAP for display
    log.info(f"UMAP {embeddings.shape[1]}d -> {N_DISPLAY}d (display) ...")
    reducer_lo = umap.UMAP(n_components=N_DISPLAY, random_state=42,
                           n_neighbors=20, min_dist=0.1, metric="cosine")
    reduced_lo = reducer_lo.fit_transform(embeddings)

    # auto-tune min_cluster_size to stay at or below MAX_CLUSTERS
    min_cs       = max(8, n // (MAX_CLUSTERS * 2))
    step         = max(4, n // 500)
    max_attempts = 40

    labels     = None
    n_clusters = None

    for attempt in range(max_attempts):
        log.info(f"  HDBSCAN attempt {attempt + 1}: min_cluster_size={min_cs}")
        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cs,
            metric="euclidean",
            cluster_selection_method="eom",
            prediction_data=True,
        )
        labels     = clusterer.fit_predict(reduced_hi)
        n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        n_noise    = int((labels == -1).sum())
        log.info(f"    -> {n_clusters} clusters, {n_noise} noise points")

        if n_clusters <= MAX_CLUSTERS:
            log.info(f"  Settled on min_cluster_size={min_cs} -> {n_clusters} clusters")
            break

        min_cs += step
    else:
        log.warning(
            f"Could not reach <={MAX_CLUSTERS} clusters after {max_attempts} "
            f"attempts; using last result ({n_clusters} clusters)."
        )

    return labels, reduced_lo


# ── keyword labels ────────────────────────────────────────────────────────────
def cluster_keywords(titles, labels):
    """TF-IDF top-N keywords per cluster, with EXTRA_STOPWORDS applied."""
    from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS

    # merge sklearn's list with our custom set
    combined_stopwords = list(ENGLISH_STOP_WORDS | EXTRA_STOPWORDS)

    cluster_docs = defaultdict(list)
    for title, label in zip(titles, labels):
        if label != -1:
            cluster_docs[int(label)].append(title)

    all_labels_sorted = sorted(cluster_docs.keys())
    corpus = [" ".join(cluster_docs[lbl]) for lbl in all_labels_sorted]

    vec = TfidfVectorizer(
        stop_words=combined_stopwords,
        ngram_range=(1, 2),
        max_features=10_000,
        token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]+\b",  # skip single chars & pure numbers
    )
    X = vec.fit_transform(corpus)
    feature_names = vec.get_feature_names_out()

    kw_map = {}
    for i, lbl in enumerate(all_labels_sorted):
        row  = X[i].toarray()[0]
        idxs = row.argsort()[::-1][:TOP_KW]
        # also filter out any stopword that snuck through as part of a bigram
        kws  = []
        for j in idxs:
            if row[j] == 0:
                continue
            term = feature_names[j]
            # drop bigrams where either word is in the stoplist
            words = term.split()
            if any(w.lower() in EXTRA_STOPWORDS for w in words):
                continue
            kws.append(term)
            if len(kws) >= TOP_KW:
                break
        kw_map[lbl] = kws

    # warn if any cluster ended up with very few keywords
    for lbl, kws in kw_map.items():
        if len(kws) < 2:
            log.warning(f"  Cluster {lbl} has only {len(kws)} keyword(s): {kws}  "
                        "— consider trimming EXTRA_STOPWORDS")

    return kw_map


# ── party breakdown per cluster ───────────────────────────────────────────────
def party_breakdown(bills, labels):
    breakdown = defaultdict(lambda: defaultdict(int))
    for bill, label in zip(bills, labels):
        if label == -1:
            continue
        party = bill.get("sponsor_party") or "Unknown"
        breakdown[int(label)][party] += 1
    return {k: dict(v) for k, v in breakdown.items()}


# ── chamber breakdown per cluster ─────────────────────────────────────────────
def chamber_breakdown(bills, labels):
    breakdown = defaultdict(lambda: defaultdict(int))
    for bill, label in zip(bills, labels):
        if label == -1:
            continue
        chamber = bill.get("origin_chamber") or "Unknown"
        breakdown[int(label)][chamber] += 1
    return {k: dict(v) for k, v in breakdown.items()}


# ── assemble output ───────────────────────────────────────────────────────────
def build_output(bills, labels, xy, kw_map, party_map, chamber_map):
    from datetime import datetime, timezone

    points = []
    for i, (bill, label) in enumerate(zip(bills, labels)):
        points.append({
            "bill_id":        bill["bill_id"],
            "title":          bill["title"],
            "cluster":        int(label),
            "x":              float(xy[i, 0]),
            "y":              float(xy[i, 1]),
            "sponsor_party":  bill.get("sponsor_party"),
            "sponsor_state":  bill.get("sponsor_state"),
            "origin_chamber": bill.get("origin_chamber"),
            "introduced_date":bill.get("introduced_date"),
        })

    clusters = []
    for lbl in sorted(kw_map.keys()):
        kws = kw_map[lbl]
        clusters.append({
            "id":               lbl,
            "label":            ", ".join(kws[:3]).title() if kws else f"Cluster {lbl}",
            "keywords":         kws,
            "size":             int((np.array(labels) == lbl).sum()),
            "party_breakdown":  party_map.get(lbl, {}),
            "chamber_breakdown":chamber_map.get(lbl, {}),
        })

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_bills":  len(bills),
        "n_clusters":   len(clusters),
        "noise_count":  int((np.array(labels) == -1).sum()),
        "clusters":     clusters,
        "points":       points,
    }
    return payload


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    bills  = load_bills()
    titles = [b["title"] for b in bills]

    log.info(f"Target: <={MAX_CLUSTERS} clusters")
    log.info(f"Extra stopwords ({len(EXTRA_STOPWORDS)}): "
             f"{sorted(EXTRA_STOPWORDS)[:10]} ...")

    embeddings  = embed_titles(titles)
    labels, xy  = reduce_and_cluster(embeddings)
    kw_map      = cluster_keywords(titles, labels)
    party_map   = party_breakdown(bills, labels)
    chamber_map = chamber_breakdown(bills, labels)

    payload = build_output(bills, labels, xy, kw_map, party_map, chamber_map)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    log.info(f"Done: {len(payload['points'])} points across "
             f"{payload['n_clusters']} clusters -> {OUT_PATH}")

    log.info("Cluster labels:")
    for c in payload["clusters"]:
        log.info(f"  [{c['id']:>3}] {c['size']:>5} bills  |  {c['label']}")


if __name__ == "__main__":
    main()
