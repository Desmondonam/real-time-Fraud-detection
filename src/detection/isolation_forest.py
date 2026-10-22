"""
Isolation Forest scorer.

Trains an unsupervised anomaly detector on all 28 PCA features + Amount +
time_hour (30 features total), then maps its decision function to [0, 1]
where 1 = most anomalous.

contamination is left at "auto". It used to be set to the known fraud rate
(0.00173), which leaks label information into the model. It only shifts the
decision offset, and that is absorbed by the [0, 1] rescaling anyway, so
CombinedScorer's tuned threshold is the only cut-off that matters.

Persistence is handled by CombinedScorer, which bundles this model with the
rule parameters and tuned threshold in a single file.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import MinMaxScaler

log = logging.getLogger(__name__)

FEATURE_COLS = [f"V{i}" for i in range(1, 29)] + ["Amount", "time_hour"]


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
        scorer.fit(train_df)              # train_df has V1–V28, Amount, Time
        scores = scorer.score(batch_df)   # returns Series in [0, 1]
    """

    def __init__(self):
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
            contamination="auto",
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

    def to_dict(self) -> dict:
        return {"model": self._model, "scaler": self._scaler}

    @classmethod
    def from_dict(cls, d: dict) -> "IsolationForestScorer":
        instance = cls()
        instance._model = d["model"]
        instance._scaler = d["scaler"]
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
