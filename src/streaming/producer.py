"""
Kafka producer — replays creditcard.csv as a live transaction stream.

Each CSV row becomes one JSON event on the `transactions` topic, keyed by
txn_id (the row's position in the CSV, matching transactions.txn_id).
Events are sent in Time order and paced so that `speedup` dataset-seconds
pass per wall-clock second (speedup=60 replays the 48-hour dataset in ~48 min).

The fraud label (Class) is deliberately NOT included: at authorisation time
nobody knows it yet. Labels are joined later from Postgres by txn_id.

Run:
    python -m src.streaming.producer                     # uses env defaults
    python -m src.streaming.producer --speedup 600 --start-time 145248
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd

log = logging.getLogger(__name__)

FEATURE_COLS = [f"V{i}" for i in range(1, 29)]
CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "creditcard.csv")


def load_events(csv_path: str, start_time: float = 0.0, limit: int | None = None) -> pd.DataFrame:
    """Reads the CSV, assigns txn_id, drops the label, and orders by Time."""
    df = pd.read_csv(csv_path)
    df.insert(0, "txn_id", df.index.astype("int64"))
    df = df.drop(columns=["Class"], errors="ignore")
    df = df[df["Time"] >= start_time].sort_values("Time", kind="stable")
    return df.head(limit) if limit else df


def to_event(row: dict) -> dict:
    """Builds the wire payload for one transaction."""
    return {
        "txn_id": int(row["txn_id"]),
        "Time": float(row["Time"]),
        **{c: float(row[c]) for c in FEATURE_COLS},
        "Amount": float(row["Amount"]),
        "produced_at": datetime.now(timezone.utc).isoformat(),
    }


def ensure_topic(bootstrap: str, topic: str, partitions: int) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    try:
        futures[topic].result()
        log.info("Created topic '%s' with %d partitions", topic, partitions)
    except Exception as exc:  # another producer may have created it first
        log.warning("Topic creation for '%s' returned: %s", topic, exc)


def replay(producer, topic: str, events: pd.DataFrame, speedup: float) -> int:
    """Sends events paced by their dataset Time. Returns the number sent."""
    if events.empty:
        return 0
    t0_data = events["Time"].iloc[0]
    t0_wall = time.monotonic()
    sent = 0

    for row in events.to_dict(orient="records"):
        if speedup > 0:
            due = t0_wall + (row["Time"] - t0_data) / speedup
            delay = due - time.monotonic()
            if delay > 0:
                time.sleep(delay)

        event = to_event(row)
        producer.produce(topic, key=str(event["txn_id"]), value=json.dumps(event))
        producer.poll(0)  # serve delivery callbacks
        sent += 1
        if sent % 5_000 == 0:
            log.info("Sent %d events (dataset time %.0f s)", sent, row["Time"])

    producer.flush()
    return sent


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    parser = argparse.ArgumentParser(description="Replay creditcard.csv into Kafka")
    parser.add_argument("--csv", default=os.getenv("REPLAY_CSV", CSV_PATH))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "transactions"))
    parser.add_argument("--partitions", type=int, default=int(os.getenv("KAFKA_PARTITIONS", "3")))
    parser.add_argument("--speedup", type=float, default=float(os.getenv("REPLAY_SPEEDUP", "60")),
                        help="dataset seconds per wall-clock second; 0 = as fast as possible")
    parser.add_argument("--start-time", type=float, default=float(os.getenv("REPLAY_START_TIME", "0")),
                        help="skip events before this dataset Time (seconds)")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    from confluent_kafka import Producer

    ensure_topic(args.bootstrap, args.topic, args.partitions)
    events = load_events(args.csv, args.start_time, args.limit)
    log.info("Replaying %d events to '%s' at %.0fx speed", len(events), args.topic, args.speedup)

    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 5,
        "enable.idempotence": True,
    })
    sent = replay(producer, args.topic, events, args.speedup)
    log.info("Replay finished — %d events sent", sent)
    return 0


if __name__ == "__main__":
    sys.exit(main())
