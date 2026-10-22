"""
Kafka consumer — scores every transaction event as it arrives.

Loop:
  1. poll up to MAX_BATCH events (or wait at most POLL_TIMEOUT seconds)
  2. score each event with CombinedScorer (vectorised over the micro-batch)
  3. upsert the results into scored_transactions
  4. commit the Kafka offsets

Delivery is at-least-once: offsets are committed only after the database
transaction commits. Writes use ON CONFLICT (txn_id) DO NOTHING, so a
re-delivered event never produces a duplicate row — effectively once.

The model bundle is reloaded whenever its file changes, so a model promoted
by the Airflow retraining DAG is picked up without a restart.

Run:
    python -m src.streaming.consumer
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from src.detection.scorer import DEFAULT_MODEL_PATH, CombinedScorer

log = logging.getLogger(__name__)

MAX_BATCH = int(os.getenv("CONSUMER_MAX_BATCH", "500"))
POLL_TIMEOUT = float(os.getenv("CONSUMER_POLL_TIMEOUT", "0.1"))
MODEL_CHECK_SECS = 30

OUTPUT_COLUMNS = [
    "txn_id", "event_time_sec", "amount", "rule_score", "if_score",
    "combined_score", "is_high_risk", "threshold", "model_version", "split",
    "produced_at", "scored_at", "latency_ms",
]

INSERT_SQL = f"""
    INSERT INTO scored_transactions ({", ".join(OUTPUT_COLUMNS)})
    VALUES %s
    ON CONFLICT (txn_id) DO NOTHING
"""


def parse_events(payloads: list[bytes]) -> pd.DataFrame:
    """Decodes Kafka message values into a DataFrame; skips malformed ones."""
    events = []
    for raw in payloads:
        try:
            events.append(json.loads(raw))
        except (TypeError, ValueError):
            log.warning("Skipping malformed event: %r", raw[:200] if raw else raw)
    return pd.DataFrame(events)


def score_events(scorer: CombinedScorer, events: pd.DataFrame, now: datetime | None = None) -> list[tuple]:
    """Scores a micro-batch and returns rows ready for INSERT_SQL."""
    if events.empty:
        return []
    scored = scorer.score(events)
    scored_at = now or datetime.now(timezone.utc)
    produced_at = pd.to_datetime(events["produced_at"], utc=True, errors="coerce")
    latency_ms = (scored_at - produced_at).dt.total_seconds() * 1000.0
    split = scorer.split_for(events["Time"])
    version = scorer.metadata.get("model_version", "unknown")

    rows = []
    for i in scored.index:
        rows.append((
            int(scored.at[i, "txn_id"]),
            float(scored.at[i, "Time"]),
            float(scored.at[i, "Amount"]),
            float(scored.at[i, "rule_score"]),
            float(scored.at[i, "if_score"]),
            float(scored.at[i, "combined_score"]),
            bool(scored.at[i, "is_high_risk"]),
            float(scorer.threshold),
            version,
            split.at[i],
            None if pd.isna(produced_at.at[i]) else produced_at.at[i].to_pydatetime(),
            scored_at,
            None if pd.isna(latency_ms.at[i]) else round(float(latency_ms.at[i]), 2),
        ))
    return rows


class ModelHolder:
    """Keeps the current CombinedScorer and reloads it when the file changes."""

    def __init__(self, path: str):
        self.path = path
        self._mtime = None
        self._last_check = 0.0
        self.scorer: CombinedScorer | None = None
        self.refresh(force=True)

    def refresh(self, force: bool = False) -> None:
        if not force and time.monotonic() - self._last_check < MODEL_CHECK_SECS:
            return
        self._last_check = time.monotonic()
        mtime = os.path.getmtime(self.path)
        if mtime != self._mtime:
            self.scorer = CombinedScorer.load(model_path=self.path)
            self._mtime = mtime
            log.info("Using model %s (threshold %.4f)",
                     self.scorer.metadata.get("model_version"), self.scorer.threshold)


def connect_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "fraud_detection"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
    )


def wait_for_model(path: str, timeout: float = 600) -> None:
    deadline = time.monotonic() + timeout
    while not os.path.exists(path):
        if time.monotonic() > deadline:
            raise FileNotFoundError(f"No model at {path} after {timeout:.0f}s")
        log.info("Waiting for model bundle at %s …", path)
        time.sleep(5)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    from confluent_kafka import Consumer, KafkaException

    model_path = os.getenv("MODEL_PATH", DEFAULT_MODEL_PATH)
    topic = os.getenv("KAFKA_TOPIC", "transactions")
    wait_for_model(model_path)
    model = ModelHolder(model_path)

    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "fraud-scorer"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.wait.max.ms": 100,      # librdkafka default (500 ms) dominates latency
    })
    consumer.subscribe([topic])
    db = connect_db()

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("Consuming '%s' …", topic)
    total = alerts = 0
    try:
        while running:
            msgs = consumer.consume(num_messages=MAX_BATCH, timeout=POLL_TIMEOUT)
            if not msgs:
                model.refresh()
                continue

            payloads = []
            for m in msgs:
                if m.error():
                    raise KafkaException(m.error())
                payloads.append(m.value())

            rows = score_events(model.scorer, parse_events(payloads))
            with db, db.cursor() as cur:       # commits (or rolls back) the transaction
                execute_values(cur, INSERT_SQL, rows)
            consumer.commit(asynchronous=False)

            total += len(rows)
            alerts += sum(r[6] for r in rows)
            if total and total % 5_000 < len(rows):
                log.info("Scored %d events, %d alerts so far", total, alerts)
            model.refresh()
    finally:
        consumer.close()
        db.close()
        log.info("Consumer stopped — %d events scored, %d alerts", total, alerts)
    return 0


if __name__ == "__main__":
    sys.exit(main())
