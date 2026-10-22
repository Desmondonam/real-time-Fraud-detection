import numpy as np
import pandas as pd
import pytest

from src.detection.rule_engine import RuleEngine

FEATURES = [f"V{i}" for i in range(1, 29)]
EVENING = 20 * 3600       # neutral hour: multiplier 1.0
NIGHT = 3 * 3600          # 00–05: multiplier 1.5
BUSINESS = 10 * 3600      # 09–17: multiplier 0.75


def row(**overrides) -> pd.DataFrame:
    base = {"Time": EVENING, "Amount": 0.0, **{c: 0.0 for c in FEATURES}}
    base.update(overrides)
    return pd.DataFrame([base])


@pytest.fixture
def engine() -> RuleEngine:
    return RuleEngine(
        pca_rules=[("V14", "<", -5.0, 1.5), ("V4", ">", 4.0, 0.5)],
        amount_mean=100.0,
        amount_std=50.0,
    )


# ── scoring ───────────────────────────────────────────────────────────────────

def test_clean_transaction_scores_zero(engine):
    assert engine.score(row(Amount=100.0)).iloc[0] == 0.0


def test_max_score_accounts_for_all_points_and_night_multiplier(engine):
    # amount (1.0 + 0.5) + rules (1.5 + 0.5), times the 1.5 night multiplier
    assert engine.max_score == pytest.approx((1.5 + 2.0) * 1.5)


@pytest.mark.parametrize("amount, points", [(100.0, 0.0), (201.0, 1.0), (251.0, 1.5), (-1.0, 1.0)])
def test_amount_rules_use_training_mean_and_std(engine, amount, points):
    # μ=100, σ=50 → 2σ boundary at 200 (and 0), 3σ at 250 (and -50)
    assert engine.score(row(Amount=amount)).iloc[0] == pytest.approx(points / engine.max_score)


def test_less_than_rule_fires_only_beyond_threshold(engine):
    assert engine.score(row(Amount=100.0, V14=-5.0)).iloc[0] == 0.0
    assert engine.score(row(Amount=100.0, V14=-5.1)).iloc[0] == pytest.approx(1.5 / engine.max_score)


def test_greater_than_rule_fires_only_beyond_threshold(engine):
    assert engine.score(row(Amount=100.0, V4=4.0)).iloc[0] == 0.0
    assert engine.score(row(Amount=100.0, V4=4.1)).iloc[0] == pytest.approx(0.5 / engine.max_score)


@pytest.mark.parametrize("time, multiplier", [(NIGHT, 1.5), (BUSINESS, 0.75), (EVENING, 1.0),
                                              (NIGHT + 86_400, 1.5)])
def test_time_of_day_multiplier(engine, time, multiplier):
    score = engine.score(row(Time=time, Amount=100.0, V14=-9.0)).iloc[0]
    assert score == pytest.approx(1.5 * multiplier / engine.max_score)


def test_everything_firing_at_night_scores_exactly_one(engine):
    df = row(Time=NIGHT, Amount=1_000.0, V14=-9.0, V4=9.0)
    assert engine.score(df).iloc[0] == pytest.approx(1.0)


def test_scores_are_bounded_and_keep_the_index(engine):
    rng = np.random.default_rng(1)
    df = pd.DataFrame(rng.normal(0, 10, (500, 28)), columns=FEATURES, index=range(100, 600))
    df["Amount"] = rng.normal(100, 500, 500)
    df["Time"] = rng.uniform(0, 172_800, 500)
    scores = engine.score(df)
    assert scores.between(0.0, 1.0).all()
    assert scores.index.equals(df.index)


def test_missing_columns_are_skipped(engine):
    assert engine.score(pd.DataFrame({"V14": [-9.0]})).iloc[0] == pytest.approx(1.5 / engine.max_score)


# ── fitting ───────────────────────────────────────────────────────────────────

def test_fit_learns_the_separating_features_and_directions(transactions):
    fitted = RuleEngine.fit(transactions.drop(columns="Class"), transactions["Class"])
    learned = {col: op for col, op, _, _ in fitted.pca_rules}
    assert {"V14", "V17", "V12", "V4"} <= set(learned)
    assert learned["V14"] == learned["V17"] == learned["V12"] == "<"
    assert learned["V4"] == ">"


def test_fit_sets_thresholds_in_the_legit_tail(transactions):
    fitted = RuleEngine.fit(transactions.drop(columns="Class"), transactions["Class"], tail_quantile=0.01)
    legit = transactions[transactions["Class"] == 0]
    for col, op, thresh, _ in fitted.pca_rules:
        share_beyond = (legit[col] < thresh).mean() if op == "<" else (legit[col] > thresh).mean()
        assert share_beyond == pytest.approx(0.01, abs=0.002)


def test_fit_uses_training_amount_stats(transactions):
    fitted = RuleEngine.fit(transactions.drop(columns="Class"), transactions["Class"])
    assert fitted.amount_mean == pytest.approx(transactions["Amount"].mean())
    assert fitted.amount_std == pytest.approx(transactions["Amount"].std())


def test_fit_without_fraud_rows_raises(transactions):
    legit = transactions[transactions["Class"] == 0]
    with pytest.raises(ValueError, match="no fraud"):
        RuleEngine.fit(legit.drop(columns="Class"), legit["Class"])


def test_dict_round_trip_preserves_scores(engine, transactions):
    restored = RuleEngine.from_dict(engine.to_dict())
    pd.testing.assert_series_equal(restored.score(transactions), engine.score(transactions))
