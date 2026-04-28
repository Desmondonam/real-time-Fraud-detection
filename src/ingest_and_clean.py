"""
Ingest creditcard.csv into PostgreSQL.
Steps: schema validation → null handling → Amount normalization → load.
"""

import logging
import os
import sys

import pandas as pd
from dotenv import load_dotenv
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── expected schema ───────────────────────────────────────────────────────────

EXPECTED_COLUMNS = (
    ["Time"]
    + [f"V{i}" for i in range(1, 29)]
    + ["Amount", "Class"]
)

EXPECTED_DTYPES = {col: float for col in EXPECTED_COLUMNS}
EXPECTED_DTYPES["Class"] = int

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "creditcard.csv")
TABLE_NAME = "transactions"


# ── helpers ───────────────────────────────────────────────────────────────────

def get_engine():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "fraud_detection")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"
    return create_engine(url)


def validate_schema(df: pd.DataFrame) -> None:
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    extra = [c for c in df.columns if c not in EXPECTED_COLUMNS]

    if missing:
        raise ValueError(f"Missing columns: {missing}")
    if extra:
        log.warning("Unexpected columns (will be dropped): %s", extra)
        df.drop(columns=extra, inplace=True)

    # enforce column order
    df = df[EXPECTED_COLUMNS]

    wrong_types = []
    for col, expected in EXPECTED_DTYPES.items():
        try:
            df[col].astype(expected)
        except (ValueError, TypeError):
            wrong_types.append(col)

    if wrong_types:
        raise ValueError(f"Columns with incompatible types: {wrong_types}")

    log.info("Schema validation passed — %d columns, %d rows", len(df.columns), len(df))


def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]

    if cols_with_nulls.empty:
        log.info("No null values found")
        return df

    log.warning("Null values detected:\n%s", cols_with_nulls.to_string())

    feature_cols = [f"V{i}" for i in range(1, 29)] + ["Time"]
    for col in feature_cols:
        if df[col].isnull().any():
            median = df[col].median()
            df[col].fillna(median, inplace=True)
            log.info("Filled nulls in '%s' with median (%.4f)", col, median)

    # Amount: fill with median
    if df["Amount"].isnull().any():
        median = df["Amount"].median()
        df["Amount"].fillna(median, inplace=True)
        log.info("Filled nulls in 'Amount' with median (%.4f)", median)

    # Class must not be null — drop those rows
    class_nulls = df["Class"].isnull().sum()
    if class_nulls:
        log.warning("Dropping %d rows with null 'Class'", class_nulls)
        df.dropna(subset=["Class"], inplace=True)

    return df


def normalize_amount(df: pd.DataFrame) -> pd.DataFrame:
    scaler = StandardScaler()
    df["Amount"] = scaler.fit_transform(df[["Amount"]])
    log.info(
        "Amount normalized — mean=%.4f, std=%.4f (should be ~0 and ~1)",
        df["Amount"].mean(),
        df["Amount"].std(),
    )
    return df


def load_to_postgres(df: pd.DataFrame, engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))

    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="replace",
        index=False,
        chunksize=5_000,
        method="multi",
    )
    log.info("Loaded %d rows into table '%s'", len(df), TABLE_NAME)


def verify_load(engine) -> None:
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
        count = result.scalar()
        fraud = conn.execute(
            text(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE \"Class\" = 1")
        ).scalar()

    log.info(
        "Verification — total rows: %d | fraud rows: %d (%.3f%%)",
        count,
        fraud,
        100 * fraud / count,
    )


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("Loading CSV from %s", CSV_PATH)
    df = pd.read_csv(CSV_PATH)
    log.info("Raw shape: %s", df.shape)

    validate_schema(df)
    df = df[EXPECTED_COLUMNS]          # enforce column order after validation
    df = handle_nulls(df)
    df = normalize_amount(df)

    df["Class"] = df["Class"].astype(int)

    log.info("Connecting to PostgreSQL …")
    engine = get_engine()

    load_to_postgres(df, engine)
    verify_load(engine)
    log.info("Done.")


if __name__ == "__main__":
    sys.exit(main())
