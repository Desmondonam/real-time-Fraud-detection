"""
CombinedScorer — merges rule-based and Isolation Forest signals.

Final score = 0.4 × rule_score + 0.6 × if_score

The IF component carries more weight because it captures multivariate
anomalies that individual feature thresholds miss.  The rule engine is kept
as an interpretable fast-path that ensures obvious violations are always flagged.

A transaction is marked high-risk when combined_score >= threshold. The
threshold is tuned on a validation slice of the training window (see
src/train_detector.py) and saved in the model bundle together with the rule
parameters, the IF model and the time-split cut-offs.
"""

from __future__ import annotations

import logging
import os

import joblib
import pandas as pd

from .isolation_forest import IsolationForestScorer
from .rule_engine import RuleEngine

log = logging.getLogger(__name__)

RULE_WEIGHT = 0.4
IF_WEIGHT = 0.6
DEFAULT_THRESHOLD = 0.40   # used only until a tuned threshold is set

DEFAULT_MODEL_PATH = os.getenv(
    "MODEL_PATH",
    os.path.join(os.path.dirname(__file__), "..", "..", "models", "fraud_scorer.pkl"),
)


class CombinedScorer:
    """
    Scores a batch DataFrame and annotates it with:
      rule_score      — output of RuleEngine  [0, 1]
      if_score        — output of IsolationForest  [0, 1]
      combined_score  — weighted average  [0, 1]
      is_high_risk    — bool flag

    Usage:
        scorer = CombinedScorer.fit(train_df, y_train)
        scorer.save()
        # later …
        scorer = CombinedScorer.load()
        scored_df = scorer.score(batch_df)
    """

    def __init__(
        self,
        rules: RuleEngine,
        if_scorer: IsolationForestScorer,
        threshold: float = DEFAULT_THRESHOLD,
        metadata: dict | None = None,
    ):
        self._rules = rules
        self._if = if_scorer
        self.threshold = threshold
        self.metadata = metadata or {}

    @classmethod
    def fit(cls, df: pd.DataFrame, y: pd.Series) -> "CombinedScorer":
        """Fits both components. Pass training-window rows only."""
        rules = RuleEngine.fit(df, y)
        log.info("Learned rules: %s", rules.pca_rules)
        return cls(rules=rules, if_scorer=IsolationForestScorer().fit(df))

    # ── persistence ───────────────────────────────────────────────────────────

    def save(self, path: str = DEFAULT_MODEL_PATH) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        joblib.dump(
            {
                "rules": self._rules.to_dict(),
                "isolation_forest": self._if.to_dict(),
                "threshold": self.threshold,
                "metadata": self.metadata,
            },
            path,
        )
        log.info("Model bundle saved to: %s", path)

    @classmethod
    def load(cls, model_path: str | None = None) -> "CombinedScorer":
        path = model_path or DEFAULT_MODEL_PATH
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"No trained model at {path}. Run src/train_detector.py first."
            )
        bundle = joblib.load(path)
        log.info("Model bundle loaded from: %s", path)
        return cls(
            rules=RuleEngine.from_dict(bundle["rules"]),
            if_scorer=IsolationForestScorer.from_dict(bundle["isolation_forest"]),
            threshold=bundle["threshold"],
            metadata=bundle.get("metadata", {}),
        )

    # ── inference ─────────────────────────────────────────────────────────────

    def split_for(self, event_time: pd.Series) -> pd.Series:
        """
        Labels each event with the window it fell in when this model was
        trained: "fit", "tune" or "test". Only "test" events are out-of-sample.
        """
        tune_start = self.metadata.get("tune_start_time", float("inf"))
        test_start = self.metadata.get("test_start_time", float("inf"))
        split = pd.Series("fit", index=event_time.index)
        split[event_time >= tune_start] = "tune"
        split[event_time >= test_start] = "test"
        return split

    def score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Returns df with four new columns appended.
        The input df must contain V1–V28, Amount (raw currency units), and Time.
        """
        out = df.copy()
        out["rule_score"] = self._rules.score(df)
        out["if_score"] = self._if.score(df)
        out["combined_score"] = (
            RULE_WEIGHT * out["rule_score"] + IF_WEIGHT * out["if_score"]
        ).round(4)
        out["is_high_risk"] = out["combined_score"] >= self.threshold
        return out
