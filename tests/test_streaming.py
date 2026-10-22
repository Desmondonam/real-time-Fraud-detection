import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

pytest.importorskip("psycopg2")

from src.streaming.consumer import OUTPUT_COLUMNS, parse_events, score_events
from src.streaming.producer import load_events, to_event


@pytest.fixture
def csv_path(transactions, tmp_path):
    # shuffle so the producer has to re-sort by Time
    path = tmp_path / "tx.csv"
    transactions.sample(frac=1, random_state=3).to_csv(path, index=False)
    return path


# ── producer ──────────────────────────────────────────────────────────────────

def test_load_events_orders_by_time_and_drops_label(csv_path):
    events = load_events(str(csv_path))
    assert "Class" not in events.columns
    assert events["Time"].is_monotonic_increasing


def test_load_events_txn_id_is_csv_row_position(csv_path):
    raw = pd.read_csv(csv_path)
    events = load_events(str(csv_path)).set_index("txn_id")
    pd.testing.assert_series_equal(events["Amount"].sort_index(), raw["Amount"], check_names=False)


def test_load_events_start_time_and_limit(csv_path):
    events = load_events(str(csv_path), start_time=100_000, limit=10)
    assert len(events) == 10
    assert (events["Time"] >= 100_000).all()


def test_to_event_payload_has_features_and_no_label(transactions):
    row = transactions.assign(txn_id=7).iloc[0].to_dict()
    event = json.loads(json.dumps(to_event(row)))   # must be JSON-serialisable
    assert event["txn_id"] == 7
    assert "Class" not in event
    assert {f"V{i}" for i in range(1, 29)} <= set(event)
    assert datetime.fromisoformat(event["produced_at"]).tzinfo is not None


# ── consumer ──────────────────────────────────────────────────────────────────

def _payloads(transactions, n=20, produced_at=None):
    produced_at = produced_at or datetime.now(timezone.utc)
    out = []
    for i, row in enumerate(transactions.head(n).to_dict(orient="records")):
        event = to_event({**row, "txn_id": i})
        event["produced_at"] = produced_at.isoformat()
        out.append(json.dumps(event).encode())
    return out


def test_parse_events_skips_malformed_messages(transactions):
    payloads = _payloads(transactions, n=3) + [b"not json", None]
    assert len(parse_events(payloads)) == 3


def test_score_events_builds_one_row_per_event(trained_scorer, transactions):
    sent = datetime(2026, 1, 1, tzinfo=timezone.utc)
    now = sent + timedelta(milliseconds=250)
    rows = score_events(trained_scorer, parse_events(_payloads(transactions, 20, sent)), now=now)

    assert len(rows) == 20
    assert all(len(r) == len(OUTPUT_COLUMNS) for r in rows)
    record = dict(zip(OUTPUT_COLUMNS, rows[0]))
    assert record["txn_id"] == 0
    assert record["latency_ms"] == pytest.approx(250.0)
    assert record["scored_at"] == now
    assert record["split"] in {"fit", "tune", "test"}
    assert record["is_high_risk"] == (record["combined_score"] >= trained_scorer.threshold)
    assert record["model_version"] == trained_scorer.metadata["model_version"]


def test_score_events_matches_batch_scoring(trained_scorer, transactions):
    rows = score_events(trained_scorer, parse_events(_payloads(transactions, 50)))
    streamed = [dict(zip(OUTPUT_COLUMNS, r))["combined_score"] for r in rows]
    batch = trained_scorer.score(transactions.head(50))["combined_score"].tolist()
    assert streamed == pytest.approx(batch)


def test_score_events_empty_batch(trained_scorer):
    assert score_events(trained_scorer, pd.DataFrame()) == []
