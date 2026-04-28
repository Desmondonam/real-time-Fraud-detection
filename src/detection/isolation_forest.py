"""
Isolation Forest scorer.

Trains an unsupervised anomaly detector on all 28 PCA features + Amount +
time_hour (30 features total), then maps its decision function to [0, 1]
where 1 = most anomalous.

The model is serialised to disk with joblib so the DAG can load it without
re-training on every run.
"""

from __future__ import annotations

import logging
import os

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler

log = logging.getLogger(__name__)

FEATURE_COLS = [f"V{i}" for i in range(1, 29)] + ["Amount", "time_hour"]

# Known fraud rate in the creditcard dataset — guides the IF contamination param
_CONTAMINATION = 0.00173

_DEFAULT_MODEL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "isolation_forest.pkl"
)


def _add_time_hour(df: pd.DataFrame) -> pd.DataFrame:
    """Derives hour-of-day (0–23) from the Time column in-place."""
    df = df.copy()
    df["time_hour"] = ((df["Time"] % 86_400) / 3_600).astype(int)
    return df


class IsolationForestScorer:
    """
    Wrapper around sklearn IsolationForest.

    Usage:
        scorer = IsolationForestScorer()
        scorer.fit(train_df)          # train_df has V1–V28, Amount, Time
        scorer.save()
        # later …
        scorer = IsolationForestScorer.load()
        scores = scorer.score(batch_df)   # returns Series in [0, 1]
    """

    def __init__(self, model_path: str = _DEFAULT_MODEL_PATH):
        self.model_path = model_path
        self._model: IsolationForest | None = None
        self._scaler: MinMaxScaler | None = None  # maps raw IF scores → [0,1]

    # ── training ──────────────────────────────────────────────────────────────

    def fit(self, df: pd.DataFrame) -> "IsolationForestScorer":
        df = _add_time_hour(df)
        X = df[FEATURE_COLS].values

        log.info(
            "Training IsolationForest on %d samples, %d features …", *X.shape
        )
        self._model = IsolationForest(
            n_estimators=200,
            contamination=_CONTAMINATION,
            max_samples="auto",
            random_state=42,
            n_jobs=-1,
        )
        self._model.fit(X)

        # Calibrate scaler on training data so scores are comparable at inference
        raw = -self._model.decision_function(X)   # higher = more anomalous
        self._scaler = MinMaxScaler()
        self._scaler.fit(raw.reshape(-1, 1))

        log.info("Training complete.")
        return self

    # ── persistence ───────────────────────────────────────────────────────────

    def save(self, path: str | None = None) -> None:
        dest = path or self.model_path
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        joblib.dump({"model": self._model, "scaler": self._scaler}, dest)
        log.info("Model saved to: %s", dest)

    @classmethod
    def load(cls, path: str = _DEFAULT_MODEL_PATH) -> "IsolationForestScorer":
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"No trained model at {path}. Run train_detector.py first."
            )
        bundle = joblib.load(path)
        instance = cls(model_path=path)
        instance._model = bundle["model"]
        instance._scaler = bundle["scaler"]
        log.info("Model loaded from: %s", path)
        return instance

    # ── inference ─────────────────────────────────────────────────────────────

    def score(self, df: pd.DataFrame) -> pd.Series:
        """Returns anomaly scores in [0, 1], indexed like df."""
        if self._model is None:
            raise RuntimeError("Model not trained or loaded.")

        df = _add_time_hour(df)
        available = [c for c in FEATURE_COLS if c in df.columns]
        missing = set(FEATURE_COLS) - set(available)
        if missing:
            log.warning("Missing features — zero-filling: %s", missing)

        X = df.reindex(columns=FEATURE_COLS, fill_value=0.0).values
        raw = -self._model.decision_function(X)
        normalised = self._scaler.transform(raw.reshape(-1, 1)).ravel()
        return pd.Series(normalised.clip(0.0, 1.0), index=df.index)
