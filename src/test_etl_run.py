"""
Standalone test: runs the ETL pipeline logic for one batch without Airflow.
Simulates the first DAG run (window 0–3600 seconds).
"""

import logging
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

RISK_RULES = {
    "V14": (-5.0, 2.0),
    "V17": (-5.0, 2.0),
    "V12": (-4.0, 2.5),
    "V10": (-4.0, 2.0),
}
RISK_SCORE_THRESHOLD = 2.0


def engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def ensure_fraud_alerts_table(eng):
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fraud_alerts (
                id                 SERIAL PRIMARY KEY,
                batch_start_sec    DOUBLE PRECISION NOT NULL,
                batch_end_sec      DOUBLE PRECISION NOT NULL,
                "Time"             DOUBLE PRECISION,
                "Amount"           DOUBLE PRECISION,
                "V14"              DOUBLE PRECISION,
                "V17"              DOUBLE PRECISION,
                "V12"              DOUBLE PRECISION,
                "V10"              DOUBLE PRECISION,
                risk_score         DOUBLE PRECISION,
                is_confirmed_fraud BOOLEAN,
                flagged_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
    log.info("fraud_alerts table ready")


def compute_risk_score(df: pd.DataFrame) -> pd.Series:
    score = pd.Series(0.0, index=df.index)
    for col, (lo, hi) in RISK_RULES.items():
        if col in df.columns:
            score += ((df[col] < lo) | (df[col] > hi)).astype(float)
    if "Amount" in df.columns:
        score += (df["Amount"].abs() > 2.0).astype(float) * 0.5
    return score


def run_batch(eng, batch_start: float, batch_end: float):
    log.info("Processing window [%.0f, %.0f) seconds", batch_start, batch_end)

    query = text("""
        SELECT "Time", "Amount", "V10", "V12", "V14", "V17", "Class"
        FROM   transactions
        WHERE  "Time" >= :start AND "Time" < :end
    """)
    df = pd.read_sql(query, eng, params={"start": batch_start, "end": batch_end})
    log.info("Batch rows: %d", len(df))

    if df.empty:
        log.info("Empty batch — nothing to flag")
        return

    df["risk_score"] = compute_risk_score(df)
    high_risk = df[(df["risk_score"] >= RISK_SCORE_THRESHOLD) | (df["Class"] == 1)].copy()
    log.info("High-risk rows: %d / %d", len(high_risk), len(df))

    if high_risk.empty:
        log.info("No alerts for this window")
        return

    high_risk["batch_start_sec"] = batch_start
    high_risk["batch_end_sec"] = batch_end
    high_risk = high_risk.rename(columns={"Class": "is_confirmed_fraud"})
    high_risk["is_confirmed_fraud"] = high_risk["is_confirmed_fraud"].astype(bool)

    cols = [
        "batch_start_sec", "batch_end_sec",
        "Time", "Amount", "V14", "V17", "V12", "V10",
        "risk_score", "is_confirmed_fraud",
    ]
    high_risk[[c for c in cols if c in high_risk.columns]].to_sql(
        "fraud_alerts", eng, if_exists="append", index=False, method="multi"
    )
    log.info("Inserted %d rows into fraud_alerts", len(high_risk))


def verify(eng):
    with eng.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM fraud_alerts")).scalar()
        confirmed = conn.execute(
            text("SELECT COUNT(*) FROM fraud_alerts WHERE is_confirmed_fraud = true")
        ).scalar()
        rule_only = conn.execute(
            text("SELECT COUNT(*) FROM fraud_alerts WHERE is_confirmed_fraud = false")
        ).scalar()
    log.info(
        "fraud_alerts — total: %d | confirmed fraud: %d | rule-flagged only: %d",
        total, confirmed, rule_only,
    )


def main():
    eng = engine()
    ensure_fraud_alerts_table(eng)

    # Run 3 consecutive 1-hour batches to demonstrate the pipeline
    for window in range(3):
        batch_start = window * 3600.0
        batch_end = batch_start + 3600.0
        run_batch(eng, batch_start, batch_end)

    verify(eng)
    log.info("Test run complete.")


if __name__ == "__main__":
    sys.exit(main())
