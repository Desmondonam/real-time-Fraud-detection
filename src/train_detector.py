"""
Train the Isolation Forest model and evaluate the combined scorer.

Loads all transactions from PostgreSQL, trains the IF model on the full dataset
(unsupervised — Class label is held out), then evaluates detection performance
using Class as ground truth.

Run:
    python src/train_detector.py
"""

import logging
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sklearn.metrics import (
    classification_report,
    confusion_matrix,
    precision_recall_curve,
    roc_auc_score,
)
from sqlalchemy import create_engine, text

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# Make sibling imports work when run as a script
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.detection.isolation_forest import IsolationForestScorer
from src.detection.rule_engine import RuleEngine
from src.detection.scorer import COMBINED_THRESHOLD, IF_WEIGHT, RULE_WEIGHT

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "models", "isolation_forest.pkl")


def get_engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def load_transactions(engine) -> pd.DataFrame:
    log.info("Loading all transactions from PostgreSQL …")
    # Load all feature columns + Time + Class
    feature_cols = ", ".join(f'"{c}"' for c in [f"V{i}" for i in range(1, 29)])
    query = text(f'SELECT "Time", {feature_cols}, "Amount", "Class" FROM transactions')
    df = pd.read_sql(query, engine)
    log.info("Loaded %d rows, %d columns", *df.shape)
    return df


def evaluate(df: pd.DataFrame, if_scorer: IsolationForestScorer) -> None:
    rules = RuleEngine()

    log.info("Scoring all transactions …")
    df = df.copy()
    df["rule_score"] = rules.score(df)
    df["if_score"] = if_scorer.score(df)
    df["combined_score"] = (RULE_WEIGHT * df["rule_score"] + IF_WEIGHT * df["if_score"]).round(4)
    df["is_high_risk"] = df["combined_score"] >= COMBINED_THRESHOLD

    y_true = df["Class"].astype(int)
    y_pred = df["is_high_risk"].astype(int)
    y_score = df["combined_score"]

    roc_auc = roc_auc_score(y_true, y_score)
    cm = confusion_matrix(y_true, y_pred)
    tn, fp, fn, tp = cm.ravel()

    print("\n" + "=" * 60)
    print("  DETECTION EVALUATION (threshold = {:.2f})".format(COMBINED_THRESHOLD))
    print("=" * 60)
    print(f"  ROC-AUC              : {roc_auc:.4f}")
    print(f"  Confusion matrix")
    print(f"    True Negatives     : {tn:>8,}")
    print(f"    False Positives    : {fp:>8,}  (legit flagged as fraud)")
    print(f"    False Negatives    : {fn:>8,}  (fraud missed)")
    print(f"    True Positives     : {tp:>8,}")
    print()
    print(classification_report(y_true, y_pred, target_names=["Legit", "Fraud"], digits=4))

    # Find best threshold by F1
    precision, recall, thresholds = precision_recall_curve(y_true, y_score)
    f1_scores = 2 * precision * recall / (precision + recall + 1e-9)
    best_idx = f1_scores.argmax()
    print(
        "  Best F1 threshold    : {:.4f}  | precision={:.4f}  recall={:.4f}  F1={:.4f}".format(
            thresholds[best_idx],
            precision[best_idx],
            recall[best_idx],
            f1_scores[best_idx],
        )
    )
    print("=" * 60 + "\n")

    # Score distribution by class
    log.info(
        "Combined score stats:\n%s",
        df.groupby("Class")["combined_score"].describe().round(4).to_string(),
    )


def main():
    engine = get_engine()
    df = load_transactions(engine)

    # Train Isolation Forest (unsupervised — no Class label used)
    train_df = df.drop(columns=["Class"])
    if_scorer = IsolationForestScorer(model_path=MODEL_PATH)
    if_scorer.fit(train_df)
    if_scorer.save()

    evaluate(df, if_scorer)
    log.info("Done. Model saved to %s", MODEL_PATH)


if __name__ == "__main__":
    sys.exit(main())
