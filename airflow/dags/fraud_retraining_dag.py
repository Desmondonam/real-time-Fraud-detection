"""
Fraud Model Retraining DAG
--------------------------
Real-time scoring happens in the Kafka consumer (src/streaming/consumer.py).
Airflow owns the batch side: retraining the model on a schedule and
promoting it only if it passes a quality gate on the held-out window.

Schedule: daily.

Pipeline per run
----------------
1. train_candidate   — time-split training (src/train_detector.train),
                       saves a candidate bundle next to the live model
2. quality_gate      — short-circuits unless the candidate's held-out PR-AUC
                       clears an absolute floor and does not regress the
                       live model by more than MAX_PR_AUC_REGRESSION
3. promote           — atomically replaces the live bundle (the consumer
                       hot-reloads it) and records metrics in model_metrics

On this static dataset every retrain sees the same rows; in production the
training window would roll forward as newly labelled transactions arrive.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

# Make the src package importable from the DAG
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

log = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────

MODEL_PATH = os.getenv("MODEL_PATH", os.path.join(_PROJECT_ROOT, "models", "fraud_scorer.pkl"))
CANDIDATE_PATH = MODEL_PATH.replace(".pkl", ".candidate.pkl")

MIN_TEST_PR_AUC = 0.10          # absolute floor — well above the ~0.0013 base rate
MAX_PR_AUC_REGRESSION = 0.02    # allowed drop vs the live model


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="fraud_model_retraining",
    description="Daily retrain with time-based split; promote only if held-out metrics hold up",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["fraud", "ml"],
)
def fraud_model_retraining():

    @task()
    def train_candidate() -> dict:
        from src.train_detector import load_transactions, train

        scorer = train(load_transactions(csv_path=None))
        scorer.save(CANDIDATE_PATH)
        return {
            "model_version": scorer.metadata["model_version"],
            "test_pr_auc": scorer.metadata["metrics"]["test"]["pr_auc"],
        }

    @task.short_circuit()
    def quality_gate(candidate: dict) -> bool:
        from src.detection.scorer import CombinedScorer

        new = candidate["test_pr_auc"]
        if new < MIN_TEST_PR_AUC:
            log.warning("Rejected %s: test PR-AUC %.4f < floor %.2f",
                        candidate["model_version"], new, MIN_TEST_PR_AUC)
            return False

        if os.path.exists(MODEL_PATH):
            live = CombinedScorer.load(MODEL_PATH).metadata.get("metrics", {})
            old = live.get("test", {}).get("pr_auc", 0.0)
            if new < old - MAX_PR_AUC_REGRESSION:
                log.warning("Rejected %s: test PR-AUC %.4f regresses live %.4f",
                            candidate["model_version"], new, old)
                return False

        log.info("Candidate %s passed (test PR-AUC %.4f)", candidate["model_version"], new)
        return True

    @task()
    def promote(candidate: dict) -> None:
        from src.detection.scorer import CombinedScorer
        from src.train_detector import get_engine, publish_metrics

        os.replace(CANDIDATE_PATH, MODEL_PATH)   # atomic on the same volume
        publish_metrics(get_engine(), CombinedScorer.load(MODEL_PATH))
        log.info("Promoted model %s to %s", candidate["model_version"], MODEL_PATH)

    candidate = train_candidate()
    quality_gate(candidate) >> promote(candidate)


fraud_model_retraining()
