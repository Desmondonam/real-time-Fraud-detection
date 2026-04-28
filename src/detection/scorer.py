"""
CombinedScorer — merges rule-based and Isolation Forest signals.

Final score = 0.4 × rule_score + 0.6 × if_score

The IF component carries more weight because it captures multivariate
anomalies that individual feature thresholds miss.  The rule engine is kept
as an interpretable fast-path that ensures obvious violations are always flagged.

A transaction is marked high-risk when combined_score >= COMBINED_THRESHOLD.
"""

from __future__ import annotations

import logging

import pandas as pd

from .isolation_forest import IsolationForestScorer
from .rule_engine import RuleEngine

log = logging.getLogger(__name__)

RULE_WEIGHT = 0.4
IF_WEIGHT = 0.6
COMBINED_THRESHOLD = 0.40   # tuned for reasonable precision/recall trade-off


class CombinedScorer:
    """
    Scores a batch DataFrame and annotates it with:
      rule_score      — output of RuleEngine  [0, 1]
      if_score        — output of IsolationForest  [0, 1]
      combined_score  — weighted average  [0, 1]
      is_high_risk    — bool flag

    Usage:
        scorer = CombinedScorer.load()          # loads trained IF model
        scored_df = scorer.score(batch_df)
    """

    def __init__(self, if_scorer: IsolationForestScorer):
        self._rules = RuleEngine()
        self._if = if_scorer

    @classmethod
    def load(cls, model_path: str | None = None) -> "CombinedScorer":
        kwargs = {"path": model_path} if model_path else {}
        return cls(if_scorer=IsolationForestScorer.load(**kwargs))

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns df with four new columns appended.
        The input df must contain V1–V28, Amount, and Time.
        """
        out = df.copy()
        out["rule_score"] = self._rules.score(df)
        out["if_score"] = self._if.score(df)
        out["combined_score"] = (
            RULE_WEIGHT * out["rule_score"] + IF_WEIGHT * out["if_score"]
        ).round(4)
        out["is_high_risk"] = out["combined_score"] >= COMBINED_THRESHOLD
        return out
