"""
Train the fraud scorer with a time-based split and report held-out metrics.

The dataset is ordered by Time and cut into three consecutive windows:

    fit   (first 60%)  — Isolation Forest + rule thresholds + Amount stats
    tune  (next 20%)   — decision threshold chosen by best F1
    test  (last 20%)   — touched exactly once, for the reported metrics

Everything that is learned or tuned — including the rule thresholds — sees
only rows that happen *before* the test window, which mirrors deploying a
model and scoring the transactions that arrive afterwards.

Run:
    python src/train_detector.py                       # reads PostgreSQL
    python src/train_detector.py --csv data/creditcard.csv
    python src/train_detector.py --if-missing          # no-op if a model exists
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    precision_recall_curve,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
)
from sqlalchemy import create_engine, text

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Make sibling imports work when run as a script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.detection.scorer import DEFAULT_MODEL_PATH, CombinedScorer

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

FIT_FRACTION = 0.60
TUNE_FRACTION = 0.20   # test = the remaining 20%


def get_engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def load_transactions(csv_path: str | None) -> pd.DataFrame:
    if csv_path:
        log.info("Loading transactions from %s …", csv_path)
        df = pd.read_csv(csv_path)
    else:
        log.info("Loading all transactions from PostgreSQL …")
        feature_cols = ", ".join(f'"{c}"' for c in [f"V{i}" for i in range(1, 29)])
        query = text(f'SELECT "Time", {feature_cols}, "Amount", "Class" FROM transactions')
        df = pd.read_sql(query, get_engine())
    log.info("Loaded %d rows, %d columns", *df.shape)
    return df


def time_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Splits by Time into consecutive fit / tune / test windows."""
    df = df.sort_values("Time", kind="stable").reset_index(drop=True)
    n = len(df)
    fit_end = int(n * FIT_FRACTION)
    tune_end = int(n * (FIT_FRACTION + TUNE_FRACTION))
    return df.iloc[:fit_end], df.iloc[fit_end:tune_end], df.iloc[tune_end:]


def best_f1_threshold(y_true: pd.Series, y_score: pd.Series) -> float:
    precision, recall, thresholds = precision_recall_curve(y_true, y_score)
    f1 = 2 * precision * recall / (precision + recall + 1e-9)
    # the last precision/recall pair has no threshold
    return float(thresholds[np.argmax(f1[:-1])])


def window_metrics(y_true: pd.Series, y_score: pd.Series, threshold: float) -> dict:
    y_pred = (y_score >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    return {
        "rows": int(len(y_true)),
        "fraud_rows": int(y_true.sum()),
        "roc_auc": round(float(roc_auc_score(y_true, y_score)), 4),
        "pr_auc": round(float(average_precision_score(y_true, y_score)), 4),
        "precision": round(float(precision_score(y_true, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_true, y_pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y_true, y_pred, zero_division=0)), 4),
        "tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp),
    }


def print_report(metrics: dict, threshold: float) -> None:
    print("\n" + "=" * 64)
    print("  DETECTION EVALUATION  (threshold = {:.4f}, tuned on 'tune')".format(threshold))
    print("=" * 64)
    print(f"  {'window':<6} {'rows':>8} {'fraud':>6} {'ROC-AUC':>8} {'PR-AUC':>7} "
          f"{'prec':>6} {'recall':>6} {'F1':>6}")
    for name, m in metrics.items():
        print(f"  {name:<6} {m['rows']:>8,} {m['fraud_rows']:>6} {m['roc_auc']:>8.4f} "
              f"{m['pr_auc']:>7.4f} {m['precision']:>6.3f} {m['recall']:>6.3f} {m['f1']:>6.3f}")
    t = metrics["test"]
    print()
    print("  Held-out test confusion matrix")
    print(f"    True Negatives     : {t['tn']:>8,}")
    print(f"    False Positives    : {t['fp']:>8,}  (legit flagged as fraud)")
    print(f"    False Negatives    : {t['fn']:>8,}  (fraud missed)")
    print(f"    True Positives     : {t['tp']:>8,}")
    print("=" * 64 + "\n")


def train(df: pd.DataFrame) -> CombinedScorer:
    fit_df, tune_df, test_df = time_split(df)
    log.info(
        "Time split — fit: Time < %.0f (%d rows) | tune: < %.0f (%d rows) | test: %d rows",
        tune_df["Time"].iloc[0], len(fit_df), test_df["Time"].iloc[0], len(tune_df), len(test_df),
    )

    scorer = CombinedScorer.fit(fit_df.drop(columns=["Class"]), fit_df["Class"])

    tune_scores = scorer.score(tune_df)["combined_score"]
    scorer.threshold = best_f1_threshold(tune_df["Class"], tune_scores)
    log.info("Tuned threshold on 'tune' window: %.4f", scorer.threshold)

    metrics = {
        name: window_metrics(part["Class"], scorer.score(part)["combined_score"], scorer.threshold)
        for name, part in (("fit", fit_df), ("tune", tune_df), ("test", test_df))
    }
    print_report(metrics, scorer.threshold)

    scorer.metadata = {
        "model_version": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "tune_start_time": float(tune_df["Time"].iloc[0]),
        "test_start_time": float(test_df["Time"].iloc[0]),
        "metrics": metrics,
    }
    return scorer


def publish_metrics(engine, scorer: CombinedScorer) -> None:
    """Appends the model's per-window metrics to model_metrics (for Grafana)."""
    rows = [
        {
            "model_version": scorer.metadata["model_version"],
            "split": split,
            "n_rows": m["rows"],
            "fraud_rows": m["fraud_rows"],
            "roc_auc": m["roc_auc"],
            "pr_auc": m["pr_auc"],
            "alert_precision": m["precision"],
            "recall": m["recall"],
            "f1": m["f1"],
            "threshold": scorer.threshold,
        }
        for split, m in scorer.metadata["metrics"].items()
    ]
    pd.DataFrame(rows).to_sql("model_metrics", engine, if_exists="append", index=False)
    log.info("Published metrics for model %s", scorer.metadata["model_version"])


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    parser.add_argument("--csv", help="read transactions from a CSV instead of PostgreSQL")
    parser.add_argument("--model-path", default=DEFAULT_MODEL_PATH)
    parser.add_argument("--if-missing", action="store_true",
                        help="skip training when a model bundle already exists")
    args = parser.parse_args()

    if args.if_missing and os.path.exists(args.model_path):
        log.info("Model already exists at %s — skipping training", args.model_path)
        return 0

    scorer = train(load_transactions(args.csv))
    scorer.save(args.model_path)
    if not args.csv:
        publish_metrics(get_engine(), scorer)

    metrics_path = os.path.join(os.path.dirname(os.path.abspath(args.model_path)), "metrics.json")
    with open(metrics_path, "w", encoding="utf-8") as f:
        json.dump({"threshold": scorer.threshold, **scorer.metadata}, f, indent=2)
    log.info("Done. Metrics written to %s", metrics_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
