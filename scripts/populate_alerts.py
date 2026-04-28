"""
Populate fraud_alerts for all 48 one-hour windows in the dataset.
Resets the table first so results are clean and consistent.

Run:  python scripts/populate_alerts.py
"""

import logging
import os
import sys
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.detection.scorer import CombinedScorer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "models", "isolation_forest.pkl")
ALL_FEATURES = [f"V{i}" for i in range(1, 29)]
DATASET_HOURS = 48
BATCH_SECS = 3_600


def get_engine():
    url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )
    return create_engine(url)


def reset_table(eng):
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
    log.info("fraud_alerts reset")


def run_window(eng, scorer, window_idx: int):
    batch_start = float(window_idx * BATCH_SECS)
    batch_end = batch_start + BATCH_SECS

    feature_cols = ", ".join(f'"{c}"' for c in ALL_FEATURES)
    df = pd.read_sql(
        text(f"""
            SELECT "Time", {feature_cols}, "Amount", "Class"
            FROM   transactions
            WHERE  "Time" >= :start AND "Time" < :end
        """),
        eng,
        params={"start": batch_start, "end": batch_end},
    )

    if df.empty:
        return 0

    df = scorer.score(df)
    high_risk = df[df["is_high_risk"] | (df["Class"] == 1)].copy()

    if high_risk.empty:
        return 0

    # Stagger flagged_at so Grafana time axis spans the 48-hour window
    # Map dataset Time (seconds) → real timestamp starting from midnight today
    base_ts = datetime.now(tz=timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    high_risk["flagged_at"] = base_ts + pd.to_timedelta(
        high_risk["Time"], unit="s"
    )

    high_risk["batch_start_sec"] = batch_start
    high_risk["batch_end_sec"] = batch_end
    out = high_risk.rename(columns={"Class": "is_confirmed_fraud"})
    out["is_confirmed_fraud"] = out["is_confirmed_fraud"].astype(bool)

    cols = [
        "batch_start_sec", "batch_end_sec", "Time", "Amount",
        "rule_score", "if_score", "combined_score",
        "is_confirmed_fraud", "flagged_at",
    ]
    out[[c for c in cols if c in out.columns]].to_sql(
        "fraud_alerts", eng, if_exists="append", index=False, method="multi"
    )
    return len(out)


def main():
    eng = get_engine()
    reset_table(eng)
    scorer = CombinedScorer.load(model_path=MODEL_PATH)

    total_alerts = 0
    for w in range(DATASET_HOURS):
        n = run_window(eng, scorer, w)
        total_alerts += n
        if n:
            log.info("Window %02d/48 → %d alerts", w, n)

    with eng.connect() as conn:
        confirmed = conn.execute(
            text("SELECT COUNT(*) FROM fraud_alerts WHERE is_confirmed_fraud")
        ).scalar()

    log.info(
        "Done — %d total alerts across 48 windows | %d confirmed fraud",
        total_alerts, confirmed,
    )


if __name__ == "__main__":
    sys.exit(main())
