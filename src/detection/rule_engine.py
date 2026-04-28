"""
Rule-based fraud scorer.

Produces a score in [0, 1] per transaction using three signal families:

  1. Amount (already Z-scored by the ingest pipeline):
       > 2σ  → +1.0 pt      (unusual spend)
       > 3σ  → extra +0.5   (very unusual spend)

  2. Time-of-day weighting:
       The dataset Time column is seconds since first transaction.
       Assuming t=0 is midnight, we derive hour-of-day and apply a multiplier
       that reflects lower human oversight during off-hours:
         00–05 (night)          → 1.5×
         09–17 (business hours) → 0.75×
         all other hours        → 1.0×

  3. High-signal PCA components (known from published EDA on this dataset):
       V14 < -5  → +1.5   (strongest fraud predictor)
       V17 < -5  → +1.0
       V12 < -4  → +0.75
       V10 < -4  → +0.5
       V3  < -3  → +0.5
       V4  > 4   → +0.25

The raw score is normalised by the theoretical maximum (5.5) so the output
is always in [0, 1].
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ── constants ─────────────────────────────────────────────────────────────────

_NIGHT_HOURS = set(range(0, 6))          # 00:00–05:59
_BUSINESS_HOURS = set(range(9, 18))      # 09:00–17:59

_AMOUNT_THRESHOLD_2 = 2.0
_AMOUNT_THRESHOLD_3 = 3.0

# (column, operator, threshold, points)
_PCA_RULES: list[tuple[str, str, float, float]] = [
    ("V14", "<", -5.0, 1.50),
    ("V17", "<", -5.0, 1.00),
    ("V12", "<", -4.0, 0.75),
    ("V10", "<", -4.0, 0.50),
    ("V3",  "<", -3.0, 0.50),
    ("V4",  ">",  4.0, 0.25),
]

# sum of all possible points before the time multiplier
_MAX_RAW_SCORE = 1.0 + 0.5 + 1.50 + 1.00 + 0.75 + 0.50 + 0.50 + 0.25  # = 6.0
_MAX_SCORE_WITH_NIGHT = _MAX_RAW_SCORE * 1.5                              # = 9.0


class RuleEngine:
    """Stateless rule-based scorer — no training required."""

    def score(self, df: pd.DataFrame) -> pd.Series:
        """
        Returns a Series of floats in [0, 1], indexed like df.
        Missing feature columns are silently skipped.
        """
        raw = pd.Series(0.0, index=df.index)

        # ── Amount signal ─────────────────────────────────────────────────────
        if "Amount" in df.columns:
            amt = df["Amount"].abs()
            raw += (amt > _AMOUNT_THRESHOLD_2).astype(float) * 1.0
            raw += (amt > _AMOUNT_THRESHOLD_3).astype(float) * 0.5

        # ── PCA signals ───────────────────────────────────────────────────────
        for col, op, thresh, pts in _PCA_RULES:
            if col not in df.columns:
                continue
            if op == "<":
                mask = df[col] < thresh
            else:
                mask = df[col] > thresh
            raw += mask.astype(float) * pts

        # ── Time-of-day multiplier ────────────────────────────────────────────
        if "Time" in df.columns:
            time_hour = ((df["Time"] % 86_400) / 3_600).astype(int)
            multiplier = np.where(
                time_hour.isin(_NIGHT_HOURS), 1.5,
                np.where(time_hour.isin(_BUSINESS_HOURS), 0.75, 1.0),
            )
            raw = raw * multiplier

        return (raw / _MAX_SCORE_WITH_NIGHT).clip(0.0, 1.0)
