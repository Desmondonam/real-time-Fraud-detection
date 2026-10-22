"""
Rule-based fraud scorer.

Produces a score in [0, 1] per transaction using three signal families:

  1. Amount (standardised with mean/std from the *training window only*):
       > 2σ  → +1.0 pt      (unusual spend)
       > 3σ  → extra +0.5   (very unusual spend)

  2. Time-of-day weighting:
       The dataset Time column is seconds since first transaction.
       Assuming t=0 is midnight, we derive hour-of-day and apply a multiplier
       that reflects lower human oversight during off-hours:
         00–05 (night)          → 1.5×
         09–17 (business hours) → 0.75×
         all other hours        → 1.0×

  3. High-signal PCA components — *learned from the training window*:
       RuleEngine.fit() ranks V1–V28 by how far the fraud median sits from the
       legitimate median (in units of legitimate IQR), keeps the top N, and sets
       each threshold at an extreme quantile of the *legitimate* training
       distribution. Points decay with rank: 1.5, 1.0, 0.75, 0.5, 0.5, 0.25.

       Earlier versions hard-coded V14 < -5, V17 < -5, … taken from published
       EDA on the full dataset — i.e. thresholds that had already seen the test
       labels. Fitting them on the training window removes that leak.

The raw score is normalised by its theoretical maximum (all points × 1.5)
so the output is always in [0, 1].
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ── constants ─────────────────────────────────────────────────────────────────

_NIGHT_HOURS = set(range(0, 6))          # 00:00–05:59
_BUSINESS_HOURS = set(range(9, 18))      # 09:00–17:59
_NIGHT_MULTIPLIER = 1.5

_AMOUNT_THRESHOLD_2 = 2.0
_AMOUNT_THRESHOLD_3 = 3.0
_AMOUNT_POINTS = 1.0 + 0.5

_PCA_COLS = [f"V{i}" for i in range(1, 29)]

# points awarded to the 1st, 2nd, … strongest learned rule
_RULE_POINTS = [1.50, 1.00, 0.75, 0.50, 0.50, 0.25]

# a rule fires when a value is more extreme than this share of legit training rows
_TAIL_QUANTILE = 0.01

# (column, operator, threshold, points)
Rule = tuple[str, str, float, float]


class RuleEngine:
    """
    Rule-based scorer. Call fit() on the training window, or pass learned
    parameters directly (e.g. when loading a saved model).
    """

    def __init__(
        self,
        pca_rules: list[Rule] | None = None,
        amount_mean: float = 0.0,
        amount_std: float = 1.0,
    ):
        self.pca_rules: list[Rule] = list(pca_rules or [])
        self.amount_mean = float(amount_mean)
        self.amount_std = float(amount_std) or 1.0

    # ── training ──────────────────────────────────────────────────────────────

    @classmethod
    def fit(
        cls,
        df: pd.DataFrame,
        y: pd.Series,
        n_rules: int = len(_RULE_POINTS),
        tail_quantile: float = _TAIL_QUANTILE,
    ) -> "RuleEngine":
        """Learns amount stats and PCA rules from training rows only."""
        y = pd.Series(np.asarray(y), index=df.index).astype(int)
        legit, fraud = df[y == 0], df[y == 1]
        if fraud.empty:
            raise ValueError("Training window contains no fraud rows — cannot learn rules.")

        separation = {}
        for col in _PCA_COLS:
            l_med, f_med = legit[col].median(), fraud[col].median()
            iqr = legit[col].quantile(0.75) - legit[col].quantile(0.25)
            separation[col] = (f_med - l_med) / (iqr or 1.0)

        ranked = sorted(separation, key=lambda c: abs(separation[c]), reverse=True)
        rules: list[Rule] = []
        for col, pts in zip(ranked[:n_rules], _RULE_POINTS):
            if separation[col] < 0:
                rules.append((col, "<", float(legit[col].quantile(tail_quantile)), pts))
            else:
                rules.append((col, ">", float(legit[col].quantile(1 - tail_quantile)), pts))

        return cls(
            pca_rules=rules,
            amount_mean=df["Amount"].mean(),
            amount_std=df["Amount"].std(),
        )

    def to_dict(self) -> dict:
        return {
            "pca_rules": [list(r) for r in self.pca_rules],
            "amount_mean": self.amount_mean,
            "amount_std": self.amount_std,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "RuleEngine":
        return cls(
            pca_rules=[tuple(r) for r in d["pca_rules"]],
            amount_mean=d["amount_mean"],
            amount_std=d["amount_std"],
        )

    # ── inference ─────────────────────────────────────────────────────────────

    @property
    def max_score(self) -> float:
        raw_max = _AMOUNT_POINTS + sum(pts for *_, pts in self.pca_rules)
        return raw_max * _NIGHT_MULTIPLIER

    def score(self, df: pd.DataFrame) -> pd.Series:
        """
        Returns a Series of floats in [0, 1], indexed like df.
        Amount must be in raw currency units. Missing columns are skipped.
        """
        raw = pd.Series(0.0, index=df.index)

        # ── Amount signal ─────────────────────────────────────────────────────
        if "Amount" in df.columns:
            amt = ((df["Amount"] - self.amount_mean) / self.amount_std).abs()
            raw += (amt > _AMOUNT_THRESHOLD_2).astype(float) * 1.0
            raw += (amt > _AMOUNT_THRESHOLD_3).astype(float) * 0.5

        # ── PCA signals ───────────────────────────────────────────────────────
        for col, op, thresh, pts in self.pca_rules:
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
                time_hour.isin(_NIGHT_HOURS), _NIGHT_MULTIPLIER,
                np.where(time_hour.isin(_BUSINESS_HOURS), 0.75, 1.0),
            )
            raw = raw * multiplier

        return (raw / self.max_score).clip(0.0, 1.0)
