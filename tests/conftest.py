import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

FEATURES = [f"V{i}" for i in range(1, 29)]


def make_transactions(n_legit: int = 2_000, n_fraud: int = 40, seed: int = 0) -> pd.DataFrame:
    """
    Small synthetic dataset shaped like creditcard.csv: legit rows are standard
    normal, fraud rows are shifted strongly negative on V14/V17/V12 and positive
    on V4, with larger amounts. Rows are spread over ~48 h of Time.
    """
    rng = np.random.default_rng(seed)
    legit = pd.DataFrame(rng.normal(0, 1, (n_legit, 28)), columns=FEATURES)
    legit["Amount"] = rng.gamma(2.0, 40.0, n_legit)
    legit["Class"] = 0

    fraud = pd.DataFrame(rng.normal(0, 1, (n_fraud, 28)), columns=FEATURES)
    fraud[["V14", "V17", "V12"]] -= 8.0
    fraud["V4"] += 6.0
    fraud["Amount"] = rng.gamma(2.0, 400.0, n_fraud)
    fraud["Class"] = 1

    df = pd.concat([legit, fraud], ignore_index=True)
    df["Time"] = rng.uniform(0, 172_800, len(df))
    return df.sort_values("Time", ignore_index=True)[["Time", *FEATURES, "Amount", "Class"]]


@pytest.fixture(scope="session")
def transactions() -> pd.DataFrame:
    return make_transactions()


@pytest.fixture(scope="session")
def trained_scorer(transactions):
    from src.train_detector import train

    return train(transactions)
