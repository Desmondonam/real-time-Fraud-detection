"""
Standalone test: runs the ETL pipeline logic for three consecutive 1-hour
batches using the CombinedScorer (Isolation Forest + rule engine).
Does not require Airflow to be running.

Run:
    python src/test_etl_run.py
"""

import logging
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.detection.scorer import CombinedScorer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

_ALL_FEATURES = [f"V{i}" for i in range(1, 29)]
MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "models", "isolation_forest.pkl")


def engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def reset_fraud_alerts(eng):
    with eng.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS fraud_alerts CASCADE"))
        conn.execute(text("""
            CREATE TABLE fraud_alerts (
                id                 SERIAL PRIMARY KEY,
                batch_start_sec    DOUBLE PRECISION NOT NULL,
                batch_end_sec      DOUBLE PRECISION NOT NULL,
                "Time"             DOUBLE PRECISION,
                "Amount"           DOUBLE PRECISION,
                rule_score         DOUBLE PRECISION,
                if_score           DOUBLE PRECISION,
                combined_score     DOUBLE PRECISION,
                is_confirmed_fraud BOOLEAN,
                flagged_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
    log.info("fraud_alerts table (re)created with new schema")


def run_batch(eng, scorer: CombinedScorer, batch_start: float, batch_end: float):
    log.info("Processing window [%.0f, %.0f) seconds", batch_start, batch_end)

    feature_cols = ", ".join(f'"{c}"' for c in _ALL_FEATURES)
    query = text(f"""
        SELECT "Time", {feature_cols}, "Amount", "Class"
        FROM   transactions
        WHERE  "Time" >= :start AND "Time" < :end
    """)
    df = pd.read_sql(query, eng, params={"start": batch_start, "end": batch_end})
    log.info("Batch rows: %d", len(df))

    if df.empty:
        return

    df = scorer.score(df)
    high_risk = df[df["is_high_risk"] | (df["Class"] == 1)].copy()
    log.info("High-risk: %d (rule_score>0: %d, if_score>0.4: %d, confirmed fraud: %d)",
             len(high_risk),
             (high_risk["rule_score"] > 0).sum(),
             (high_risk["if_score"] > 0.4).sum(),
             (high_risk["Class"] == 1).sum())

    if high_risk.empty:
        return

    high_risk["batch_start_sec"] = batch_start
    high_risk["batch_end_sec"] = batch_end
    out = high_risk.rename(columns={"Class": "is_confirmed_fraud"})
    out["is_confirmed_fraud"] = out["is_confirmed_fraud"].astype(bool)

    cols = ["batch_start_sec", "batch_end_sec", "Time", "Amount",
            "rule_score", "if_score", "combined_score", "is_confirmed_fraud"]
    out[[c for c in cols if c in out.columns]].to_sql(
        "fraud_alerts", eng, if_exists="append", index=False, method="multi"
    )


def verify(eng):
    with eng.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM fraud_alerts")).scalar()
        confirmed = conn.execute(
            text("SELECT COUNT(*) FROM fraud_alerts WHERE is_confirmed_fraud = true")
        ).scalar()
        rule_only = conn.execute(
            text("SELECT COUNT(*) FROM fraud_alerts WHERE is_confirmed_fraud = false")
        ).scalar()
        avg_score = conn.execute(
            text("SELECT ROUND(AVG(combined_score)::numeric, 4) FROM fraud_alerts")
        ).scalar()

    log.info(
        "fraud_alerts | total: %d | confirmed fraud: %d | rule/IF-flagged: %d | avg combined_score: %s",
        total, confirmed, rule_only, avg_score,
    )


def main():
    eng = engine()
    reset_fraud_alerts(eng)

    scorer = CombinedScorer.load(model_path=MODEL_PATH)

    for window in range(3):
        run_batch(eng, scorer, float(window * 3600), float((window + 1) * 3600))

    verify(eng)
    log.info("Test run complete.")


if __name__ == "__main__":
    sys.exit(main())
