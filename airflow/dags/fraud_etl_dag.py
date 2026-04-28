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
2. fetch_and_score_batch      — reads window, scores with CombinedScorer
3. write_alerts               — inserts high-risk rows into fraud_alerts
4. log_summary                — prints batch stats to Airflow logs
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow.decorators import dag, task
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Make the src package importable from the DAG
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

load_dotenv(dotenv_path=os.path.join(_PROJECT_ROOT, ".env"))

log = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────

DATASET_DURATION_SECS = 172_800
BATCH_SIZE_SECS = 3_600
DAG_START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)

_ALL_FEATURES = [f"V{i}" for i in range(1, 29)]
_MODEL_PATH = os.path.join(_PROJECT_ROOT, "models", "isolation_forest.pkl")

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


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="fraud_etl_pipeline",
    description="Hourly batch ETL — scores transactions with IF + rule engine, writes alerts",
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
        log.info("fraud_alerts table is ready")

    @task()
    def fetch_and_score_batch(logical_date: str) -> dict:
        from src.detection.scorer import CombinedScorer

        run_dt = datetime.fromisoformat(logical_date)
        hours_since_start = int(
            (run_dt - DAG_START_DATE).total_seconds() // BATCH_SIZE_SECS
        )
        window_idx = hours_since_start % (DATASET_DURATION_SECS // BATCH_SIZE_SECS)
        batch_start = float(window_idx * BATCH_SIZE_SECS)
        batch_end = batch_start + BATCH_SIZE_SECS

        log.info("Run hour %d => dataset window [%.0f, %.0f) s",
                 hours_since_start, batch_start, batch_end)

        # Load all features needed by the scorer
        feature_cols = ", ".join(f'"{c}"' for c in _ALL_FEATURES)
        query = text(f"""
            SELECT "Time", {feature_cols}, "Amount", "Class"
            FROM   transactions
            WHERE  "Time" >= :start AND "Time" < :end
        """)
        engine = _engine()
        df = pd.read_sql(query, engine, params={"start": batch_start, "end": batch_end})
        log.info("Batch contains %d transactions", len(df))

        if df.empty:
            return {"batch_start": batch_start, "batch_end": batch_end,
                    "total": 0, "flagged": 0, "alerts": []}

        scorer = CombinedScorer.load(model_path=_MODEL_PATH)
        df = scorer.score(df)

        high_risk = df[df["is_high_risk"] | (df["Class"] == 1)].copy()
        log.info("Flagged %d / %d as high-risk", len(high_risk), len(df))

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
            log.info("No alerts for this batch")
            return 0

        alerts_df = pd.DataFrame(batch_result["alerts"])
        alerts_df = alerts_df.rename(columns={"Class": "is_confirmed_fraud"})
        alerts_df["is_confirmed_fraud"] = alerts_df["is_confirmed_fraud"].astype(bool)

        cols = [
            "batch_start_sec", "batch_end_sec",
            "Time", "Amount",
            "rule_score", "if_score", "combined_score",
            "is_confirmed_fraud",
        ]
        alerts_df = alerts_df[[c for c in cols if c in alerts_df.columns]]

        engine = _engine()
        alerts_df.to_sql(
            "fraud_alerts", engine,
            if_exists="append", index=False, method="multi",
        )
        log.info("Wrote %d alert rows to fraud_alerts", len(alerts_df))
        return len(alerts_df)

    @task()
    def log_summary(batch_result: dict, alerts_written: int):
        log.info(
            "=== Batch Summary ===\n"
            "  Window      : [%.0f, %.0f) seconds\n"
            "  Total tx    : %d\n"
            "  Flagged     : %d\n"
            "  Inserted    : %d alerts",
            batch_result["batch_start"], batch_result["batch_end"],
            batch_result["total"], batch_result["flagged"], alerts_written,
        )

    # ── wire tasks ────────────────────────────────────────────────────────────
    table_ready = ensure_fraud_alerts_table()
    batch = fetch_and_score_batch(logical_date="{{ logical_date | ts }}")
    written = write_alerts(batch)
    table_ready >> batch
    log_summary(batch, written)


fraud_etl_pipeline()
