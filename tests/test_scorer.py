import pandas as pd
import pytest

from src.detection.isolation_forest import IsolationForestScorer
from src.detection.rule_engine import RuleEngine
from src.detection.scorer import IF_WEIGHT, RULE_WEIGHT, CombinedScorer
from src.train_detector import time_split, train


# ── CombinedScorer ────────────────────────────────────────────────────────────

def test_score_appends_columns_and_combines_with_weights(trained_scorer, transactions):
    out = trained_scorer.score(transactions)
    for col in ("rule_score", "if_score", "combined_score", "is_high_risk"):
        assert col in out.columns
    expected = (RULE_WEIGHT * out["rule_score"] + IF_WEIGHT * out["if_score"]).round(4)
    pd.testing.assert_series_equal(out["combined_score"], expected, check_names=False)
    assert out[["rule_score", "if_score", "combined_score"]].stack().between(0, 1).all()


def test_is_high_risk_respects_threshold(trained_scorer, transactions):
    out = trained_scorer.score(transactions)
    assert (out["is_high_risk"] == (out["combined_score"] >= trained_scorer.threshold)).all()


def test_score_does_not_mutate_input(trained_scorer, transactions):
    before = transactions.copy()
    trained_scorer.score(transactions)
    pd.testing.assert_frame_equal(transactions, before)


def test_save_load_round_trip(trained_scorer, transactions, tmp_path):
    path = tmp_path / "bundle.pkl"
    trained_scorer.save(str(path))
    loaded = CombinedScorer.load(str(path))
    assert loaded.threshold == trained_scorer.threshold
    assert loaded.metadata == trained_scorer.metadata
    pd.testing.assert_frame_equal(loaded.score(transactions), trained_scorer.score(transactions))


def test_load_missing_model_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        CombinedScorer.load(str(tmp_path / "nope.pkl"))


def test_split_for_labels_events_by_training_window():
    scorer = CombinedScorer(RuleEngine(), IsolationForestScorer(),
                            metadata={"tune_start_time": 100.0, "test_start_time": 200.0})
    split = scorer.split_for(pd.Series([0.0, 99.9, 100.0, 199.9, 200.0, 1e6]))
    assert split.tolist() == ["fit", "fit", "tune", "tune", "test", "test"]


def test_split_for_without_metadata_treats_everything_as_fit():
    scorer = CombinedScorer(RuleEngine(), IsolationForestScorer())
    assert scorer.split_for(pd.Series([0.0, 1e6])).tolist() == ["fit", "fit"]


def test_unfitted_isolation_forest_raises():
    with pytest.raises(RuntimeError):
        IsolationForestScorer().score(pd.DataFrame({"Time": [0.0]}))


# ── training protocol ─────────────────────────────────────────────────────────

def test_time_split_is_ordered_and_disjoint(transactions):
    fit, tune, test = time_split(transactions.sample(frac=1, random_state=0))
    assert len(fit) + len(tune) + len(test) == len(transactions)
    assert fit["Time"].max() <= tune["Time"].min()
    assert tune["Time"].max() <= test["Time"].min()
    assert len(fit) == pytest.approx(0.6 * len(transactions), abs=1)


def test_train_records_split_metadata_and_metrics(trained_scorer, transactions):
    meta = trained_scorer.metadata
    _, tune, test = time_split(transactions)
    assert meta["tune_start_time"] == tune["Time"].iloc[0]
    assert meta["test_start_time"] == test["Time"].iloc[0]
    assert set(meta["metrics"]) == {"fit", "tune", "test"}
    assert meta["metrics"]["test"]["rows"] == len(test)
    assert 0.0 < trained_scorer.threshold < 1.0


def test_train_separates_obvious_fraud(trained_scorer):
    assert trained_scorer.metadata["metrics"]["test"]["roc_auc"] > 0.9


def test_test_window_labels_cannot_influence_the_model(transactions):
    """Flipping every label in the test window must not change anything learned."""
    _, _, test = time_split(transactions)
    tampered = transactions.copy()
    tampered.loc[tampered["Time"] >= test["Time"].iloc[0], "Class"] ^= 1

    a, b = train(transactions), train(tampered)
    assert a._rules.to_dict() == b._rules.to_dict()
    assert a.threshold == b.threshold
    pd.testing.assert_series_equal(a.score(test)["if_score"], b.score(test)["if_score"])
