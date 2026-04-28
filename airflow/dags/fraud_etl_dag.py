"""
Fraud Detection ETL DAG
-----------------------
Simulates streaming by processing the transactions table in 1-hour batches
(each batch = 3600 seconds of the dataset's Time column).

Schedule: hourly.  Each run maps its execution hour to a dataset window so
the 48-hour dataset cycles indefinitely.

Pipeline per run
----------------
1. ensure_fraud_alerts_table  — idempotent DDL
2. fetch_and_score_batch      — reads window, computes risk score
3. write_alerts               — inserts high-risk rows into fraud_alerts
4. log_summary                — prints batch stats to Airflow logs
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(
    dotenv_path=os.path.join(
        os.path.dirname(__file__), "..", "..", ".env"
    )
)

log = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────

DATASET_DURATION_SECS = 172_800   # ~48 h — full span of the creditcard dataset
BATCH_SIZE_SECS = 3_600           # 1 hour per batch
DAG_START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)

# Risk thresholds derived from known high-signal PCA components in this dataset
RISK_RULES = {
    "V14": (-5.0, 2.0),   # (low_threshold, high_threshold) — extreme values flag risk
    "V17": (-5.0, 2.0),
    "V12": (-4.0, 2.5),
    "V10": (-4.0, 2.0),
}
RISK_SCORE_THRESHOLD = 2.0        # flag if risk_score >= this OR Class == 1


# ── DB connection ─────────────────────────────────────────────────────────────

def _engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER', 'postgres')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST', 'localhost')}:"
        f"{os.getenv('DB_PORT', '5432')}/"
        f"{os.getenv('DB_NAME', 'fraud_detection')}"
    )
    return create_engine(url)


# ── risk scoring ──────────────────────────────────────────────────────────────

def _compute_risk_score(df: pd.DataFrame) -> pd.Series:
    """
    Returns a float risk score per row.
    Each PCA feature that crosses its threshold contributes +1.
    Normalized Amount > 2 adds +0.5.
    """
    score = pd.Series(0.0, index=df.index)

    for col, (lo, hi) in RISK_RULES.items():
        if col in df.columns:
            score += ((df[col] < lo) | (df[col] > hi)).astype(float)

    if "Amount" in df.columns:
        score += (df["Amount"].abs() > 2.0).astype(float) * 0.5

    return score


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="fraud_etl_pipeline",
    description="Hourly batch ETL — flags high-risk transactions into fraud_alerts",
    schedule="@hourly",
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fraud", "etl"],
)
def fraud_etl_pipeline():

    @task()
    def ensure_fraud_alerts_table():
        engine = _engine()
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS fraud_alerts (
                    id               SERIAL PRIMARY KEY,
                    batch_start_sec  DOUBLE PRECISION NOT NULL,
                    batch_end_sec    DOUBLE PRECISION NOT NULL,
                    "Time"           DOUBLE PRECISION,
                    "Amount"         DOUBLE PRECISION,
                    "V14"            DOUBLE PRECISION,
                    "V17"            DOUBLE PRECISION,
                    "V12"            DOUBLE PRECISION,
                    "V10"            DOUBLE PRECISION,
                    risk_score       DOUBLE PRECISION,
                    is_confirmed_fraud BOOLEAN,
                    flagged_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
                )
            """))
        log.info("fraud_alerts table is ready")

    @task()
    def fetch_and_score_batch(logical_date: str) -> dict:
        """
        Determine the dataset window for this run, load it from transactions,
        apply risk scoring, and return the high-risk rows as a JSON payload.
        """
        run_dt = datetime.fromisoformat(logical_date)
        hours_since_start = int(
            (run_dt - DAG_START_DATE).total_seconds() // BATCH_SIZE_SECS
        )
        # Wrap around the dataset so the DAG can run indefinitely
        window_idx = hours_since_start % (DATASET_DURATION_SECS // BATCH_SIZE_SECS)
        batch_start = window_idx * BATCH_SIZE_SECS
        batch_end = batch_start + BATCH_SIZE_SECS

        log.info(
            "Run hour %d → dataset window [%d, %d) seconds",
            hours_since_start, batch_start, batch_end,
        )

        engine = _engine()
        query = text("""
            SELECT "Time", "Amount", "V10", "V12", "V14", "V17", "Class"
            FROM   transactions
            WHERE  "Time" >= :start AND "Time" < :end
        """)
        df = pd.read_sql(query, engine, params={"start": batch_start, "end": batch_end})

        log.info("Batch contains %d transactions", len(df))

        if df.empty:
            return {
                "batch_start": batch_start,
                "batch_end": batch_end,
                "alerts": [],
                "total": 0,
                "flagged": 0,
            }

        df["risk_score"] = _compute_risk_score(df)
        high_risk = df[
            (df["risk_score"] >= RISK_SCORE_THRESHOLD) | (df["Class"] == 1)
        ].copy()

        log.info(
            "Flagged %d / %d transactions as high-risk", len(high_risk), len(df)
        )

        high_risk["batch_start_sec"] = batch_start
        high_risk["batch_end_sec"] = batch_end

        return {
            "batch_start": batch_start,
            "batch_end": batch_end,
            "total": len(df),
            "flagged": len(high_risk),
            "alerts": high_risk.to_dict(orient="records"),
        }

    @task()
    def write_alerts(batch_result: dict) -> int:
        if not batch_result["alerts"]:
            log.info("No alerts to write for this batch")
            return 0

        alerts_df = pd.DataFrame(batch_result["alerts"])
        alerts_df = alerts_df.rename(columns={"Class": "is_confirmed_fraud"})
        alerts_df["is_confirmed_fraud"] = alerts_df["is_confirmed_fraud"].astype(bool)

        cols = [
            "batch_start_sec", "batch_end_sec",
            "Time", "Amount", "V14", "V17", "V12", "V10",
            "risk_score", "is_confirmed_fraud",
        ]
        alerts_df = alerts_df[[c for c in cols if c in alerts_df.columns]]

        engine = _engine()
        alerts_df.to_sql(
            "fraud_alerts",
            engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        log.info("Wrote %d alert rows to fraud_alerts", len(alerts_df))
        return len(alerts_df)

    @task()
    def log_summary(batch_result: dict, alerts_written: int):
        log.info(
            "=== Batch Summary ===\n"
            "  Window   : [%.0f, %.0f) seconds\n"
            "  Total tx : %d\n"
            "  Flagged  : %d\n"
            "  Inserted : %d alerts",
            batch_result["batch_start"],
            batch_result["batch_end"],
            batch_result["total"],
            batch_result["flagged"],
            alerts_written,
        )

    # ── wire tasks ────────────────────────────────────────────────────────────
    table_ready = ensure_fraud_alerts_table()
    batch = fetch_and_score_batch(logical_date="{{ logical_date | ts }}")
    written = write_alerts(batch)
    table_ready >> batch
    log_summary(batch, written)


fraud_etl_pipeline()
