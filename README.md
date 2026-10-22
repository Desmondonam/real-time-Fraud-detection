# Real-Time Fraud Detection System

An end-to-end fraud detection pipeline that ingests credit card transaction data into PostgreSQL, scores every transaction using a hybrid rule engine and Isolation Forest model, orchestrates hourly batch processing with Apache Airflow, and displays live results in a Grafana dashboard.

---

## Table of Contents

- [What It Does](#what-it-does)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Quick Start (Docker Compose)](#quick-start-docker-compose)
- [Running Scripts Locally](#running-scripts-locally)
- [How the System Works](#how-the-system-works)
  - [Stage 1 — Data Ingestion and Cleaning](#stage-1--data-ingestion-and-cleaning)
  - [Stage 2 — Detection Logic](#stage-2--detection-logic)
  - [Stage 3 — ETL Pipeline (Airflow)](#stage-3--etl-pipeline-airflow)
  - [Stage 4 — Dashboard (Grafana)](#stage-4--dashboard-grafana)
- [Model Performance](#model-performance)
- [Database Schema](#database-schema)
- [Script Reference](#script-reference)
- [Troubleshooting](#troubleshooting)

---

## What It Does

This system simulates a production fraud detection pipeline on the [Kaggle Credit Card Fraud Detection dataset](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud) (284,807 transactions, 0.17% fraud rate).

It:

1. **Ingests** the raw CSV into PostgreSQL, validating schema and handling nulls.
2. **Scores** every transaction using two complementary methods — a transparent rule engine and an unsupervised Isolation Forest — combined into a single risk score.
3. **Simulates streaming** via an Airflow DAG that processes the dataset in one-hour time windows on an hourly schedule, writing high-risk transactions to a `fraud_alerts` table.
4. **Visualises** live alert volume and fraud rate in Grafana, auto-refreshing every 30 seconds.

---

## Architecture

```
Raw CSV (144 MB)
      │
      │  DVC track + local remote
      ▼
┌─────────────────────────────┐
│  src/ingest_and_clean.py    │  Schema validation, null handling
│                             │
└────────────┬────────────────┘
             │  284,807 rows
             ▼
┌────────────────────────────────────────┐
│          PostgreSQL                    │
│                                        │
│  transactions      (284,807 rows)      │
│  fraud_alerts      (1,809+ rows)       │
│  fraud_alerts_enriched  (view)         │
│  hourly_transaction_summary  (view)    │
└───────┬──────────────────┬────────────┘
        │                  │
        │ reads            │ reads
        ▼                  ▼
┌───────────────┐   ┌──────────────────────────────┐
│  Airflow DAG  │   │        Grafana Dashboard       │
│  (hourly)     │   │   localhost:3001               │
│               │   │                                │
│  1h window    │   │  • Hourly alert volume         │
│  → score      │   │  • Live fraud rate             │
│  → alert      │   │  • Alerts by dataset hour      │
└──────┬────────┘   │  • High-risk transactions table│
       │            └──────────────────────────────┘
       ▼
┌──────────────────────────────────────┐
│  src/detection/  (CombinedScorer)    │
│                                      │
│  rule_engine.py                      │
│    Amount > 2σ / 3σ           +score │
│    Night hours (00-05)        ×1.5   │
│    Business hours (09-17)     ×0.75  │
│    6 PCA rules learned on     +score │
│      the training window             │
│                                      │
│  isolation_forest.py                 │
│    30 features (V1-V28,              │
│    Amount, time_hour)                │
│    contamination = "auto"            │
│                                      │
│  scorer.py  (CombinedScorer)         │
│    combined = 0.4×rule + 0.6×IF      │
│    flag if combined ≥ tuned thresh.  │
└──────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| Database | PostgreSQL 16 (Docker) |
| Data versioning | DVC 3.60 |
| ML / Scoring | scikit-learn (IsolationForest, MinMaxScaler) |
| Orchestration | Apache Airflow 2.10 |
| Dashboard | Grafana OSS 11.4 |
| Runtime | Docker Compose (Postgres, Airflow, Grafana, bootstrap job) |
| DB access | SQLAlchemy 2.0 + psycopg2 |
| Data wrangling | pandas 2.x |
| Model persistence | joblib |
| Config | python-dotenv |

---

## Project Structure

```
real-time-Fraud-detection/
│
├── docker-compose.yml           # Whole stack: Postgres, bootstrap, Airflow, Grafana
├── Dockerfile                   # App image (bootstrap job)
├── requirements.txt             # Pinned runtime dependencies
├── .env.example                 # Credentials template (copy to .env)
│
├── data/
│   ├── creditcard.csv.dvc       # DVC pointer — actual file managed by DVC
│   └── .gitignore
│
├── src/
│   ├── ingest_and_clean.py      # Stage 1: load CSV → PostgreSQL (COPY)
│   ├── train_detector.py        # Time-split training + held-out evaluation
│   ├── test_etl_run.py          # Run ETL logic without Airflow (dev)
│   └── detection/
│       ├── __init__.py
│       ├── rule_engine.py       # Rule-based scorer (rules learned on train window)
│       ├── isolation_forest.py  # IF training + inference
│       └── scorer.py            # CombinedScorer (rule + IF), model bundle I/O
│
├── airflow/
│   └── dags/
│       └── fraud_etl_dag.py     # Hourly Airflow DAG
│
├── docker/
│   ├── airflow/Dockerfile       # Airflow image + scikit-learn
│   └── postgres/01-create-databases.sql
│
├── grafana/
│   └── provisioning/
│       ├── datasources/
│       │   └── fraud_db.yaml          # Reads credentials from the container env
│       └── dashboards/
│           ├── dashboard.yaml
│           └── fraud_detection.json   # Auto-provisioned dashboard
│
├── scripts/
│   ├── schema.sql               # Tables + dashboard views (idempotent)
│   └── populate_alerts.py       # Backfills all 48 batch windows
│
├── models/                      # Model bundle when training locally (gitignored)
├── .dvc/                        # DVC configuration
└── README.md
```

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Docker Desktop / Docker Engine | Compose v2 | Runs the whole stack |
| DVC | 3.60+ | `pip install dvc` — to fetch `data/creditcard.csv` |
| Python | 3.12 | Only needed to run scripts or tests outside Docker |

---

## Quick Start (Docker Compose)

```bash
git clone https://github.com/Desmondonam/real-time-Fraud-detection.git
cd real-time-Fraud-detection

dvc pull                 # fetch data/creditcard.csv (see "DVC pull fails" below)
cp .env.example .env     # optional — set your own DB password
docker compose up -d --build
```

What happens:

1. **postgres** starts and creates the `fraud_detection` and `airflow_metadata` databases.
2. **bootstrap** (one-shot) applies `scripts/schema.sql`, loads the CSV with `COPY`, then trains the model with the time-based split and prints the held-out metrics. Both steps are skipped if their output already exists, so later `up`s are fast. Follow it with `docker compose logs -f bootstrap`.
3. **airflow** starts once bootstrap has finished, and **grafana** starts as soon as Postgres is healthy.

| Service | URL | Login |
|---|---|---|
| Grafana | [http://localhost:3001](http://localhost:3001) | admin / admin |
| Airflow | [http://localhost:8080](http://localhost:8080) | admin / admin |
| Postgres | `localhost:5433` | values from `.env` (default postgres / postgres) |

Postgres is published on **5433** so it does not clash with a local install on 5432 (override with `POSTGRES_HOST_PORT`).

Useful commands:

```bash
docker compose logs -f bootstrap        # ingest + training output
docker compose down                     # stop (data and model are kept in volumes)
docker compose down -v                  # stop and wipe Postgres, model and Grafana volumes
docker compose run --rm bootstrap python src/train_detector.py   # force a retrain
```

---

## Running Scripts Locally

Everything can also be run from the host against the compose Postgres (`DB_HOST=localhost`, `DB_PORT=5433` in `.env`):

```bash
pip install -r requirements.txt
python src/ingest_and_clean.py                          # schema + COPY load
python src/train_detector.py                            # or: --csv data/creditcard.csv
python scripts/populate_alerts.py                       # backfill all 48 windows
```

### Train the detection model

Sorts transactions by `Time`, fits the rule thresholds and Isolation Forest on the first 60%, tunes the decision threshold on the next 20%, and reports metrics on the final 20% (see [Model Performance](#model-performance)):

```
================================================================
  DETECTION EVALUATION  (threshold = 0.4908, tuned on 'tune')
================================================================
  window     rows  fraud  ROC-AUC  PR-AUC   prec recall     F1
  fit     170,884    360   0.9558  0.5640  0.371  0.689  0.482
  tune     56,961     57   0.9421  0.0728  0.136  0.386  0.201
  test     56,962     75   0.9605  0.1756  0.231  0.453  0.306
```

The model bundle (IF model, learned rules, tuned threshold, split cut-offs and metrics) is saved to `models/fraud_scorer.pkl`, and the metrics are also written to `models/metrics.json`. Inside Docker it lives in the `models` volume instead, so containers and host never mix scikit-learn versions.

---

## How the System Works

### Stage 1 — Data Ingestion and Cleaning

**Script:** `src/ingest_and_clean.py`

The raw `creditcard.csv` (284,807 rows × 31 columns) goes through three steps before it reaches PostgreSQL:

1. **Schema validation** — Checks that all 31 expected columns are present (`Time`, `V1`–`V28`, `Amount`, `Class`). Extra columns are dropped; missing columns raise an error.

2. **Null handling** — Any null values in `V1`–`V28` or `Amount` are filled with that column's median. Rows with a null `Class` are dropped entirely (they cannot be labelled).

3. **Bulk load** — Data is inserted into the `transactions` table in 5,000-row chunks via SQLAlchemy.

The `Time` column is left as-is (seconds since the first transaction, ranging 0–172,792, covering approximately 48 hours of data).

---

### Stage 2 — Detection Logic

**Package:** `src/detection/`

Every transaction receives three scores, then a combined flag.

#### Rule Engine (`rule_engine.py`)

A fast, interpretable scorer that applies threshold rules and returns a value in [0, 1]. Its parameters are **learned from the training window only** (`RuleEngine.fit`):

| Signal | Condition | Points |
|---|---|---|
| Amount spike | `|Amount − μ| > 2σ` (μ, σ from training window) | +1.0 |
| Extreme amount | `|Amount − μ| > 3σ` | +0.5 extra |
| PCA rule 1–6 | value beyond the 1% tail of *legitimate* training rows | +1.5, +1.0, +0.75, +0.5, +0.5, +0.25 |

The six PCA features are chosen by ranking V1–V28 on how far the fraud median sits from the legitimate median (in units of the legitimate IQR) on the training window. On the current split this picks V17, V14, V12, V10, V16 and V3, all in the "<" direction.

> Earlier versions hard-coded `V14 < −5`, `V17 < −5`, `V12 < −4`, … from published EDA on the *whole* dataset. Those thresholds had already seen the test labels, so they were replaced by the fitted rules above.

After summing points, a **time-of-day multiplier** is applied:

| Hour window | Multiplier | Rationale |
|---|---|---|
| 00:00 – 05:59 (night) | ×1.5 | Lower human oversight |
| 09:00 – 17:59 (business) | ×0.75 | Normal activity expected |
| All other hours | ×1.0 | Neutral |

The total is divided by the theoretical maximum (all points × 1.5) to produce a normalised score in [0, 1].

#### Isolation Forest (`isolation_forest.py`)

An unsupervised anomaly detection model trained on the training window (first 60% of transactions by time). Key parameters:

| Parameter | Value | Reason |
|---|---|---|
| `n_estimators` | 200 | More trees → more stable scores |
| `contamination` | `"auto"` | Not set from the fraud rate (that would leak labels); the offset it controls is cancelled by the [0, 1] rescaling anyway |
| `max_samples` | `'auto'` | Uses min(256, n_samples) sub-samples per tree |
| Features | V1–V28 + Amount + `time_hour` | 30 features total |

The `time_hour` feature is derived as `int((Time % 86400) / 3600)` — the hour of day (0–23), assuming the dataset starts at midnight.

The `Class` label is never shown to the model. The `decision_function()` output is inverted (more anomalous → higher raw score) and scaled to [0, 1] using `MinMaxScaler` fitted on the training window.

#### Combined Scorer (`scorer.py`)

```
combined_score = 0.4 × rule_score + 0.6 × if_score
```

The Isolation Forest receives higher weight because it captures multivariate anomaly patterns that single-feature threshold rules miss. The rule engine is kept as an interpretable fast-path: clear-cut violations (e.g. V14 < −5) are always reflected in the score even if the IF disagrees.

A transaction is flagged **high-risk** when `combined_score ≥ threshold`, where the threshold is the best-F1 cut-off on the *tune* window (currently 0.4908).

---

### Stage 3 — ETL Pipeline (Airflow)

**DAG:** `airflow/dags/fraud_etl_dag.py`  
**DAG ID:** `fraud_etl_pipeline`  
**Schedule:** `@hourly`

The DAG simulates a streaming workload by mapping each Airflow execution hour to a one-hour window of the dataset's `Time` column. Because the dataset covers ~48 hours, the mapping wraps around (mod 48) so the DAG can run indefinitely.

```
Airflow run hour N  →  dataset window [w×3600, (w+1)×3600)
where w = N mod 48
```

**Task graph:**

```
ensure_fraud_alerts_table
          │
          ▼
fetch_and_score_batch   ← determines window, loads V1–V28 + Amount + Time,
          │               runs CombinedScorer, returns high-risk rows as dict
          ▼
     write_alerts        ← inserts alert rows into fraud_alerts table
          │
          ▼
     log_summary         ← logs batch statistics to Airflow task logs
```

**`ensure_fraud_alerts_table`** — Creates the `fraud_alerts` table if it does not exist. Uses `CREATE TABLE IF NOT EXISTS` so it is fully idempotent.

**`fetch_and_score_batch`** — Loads all feature columns for the current window from the `transactions` table, runs `CombinedScorer.load()` to score each row, and returns high-risk rows (plus any `Class == 1` confirmed fraud rows, regardless of model score) as a serialisable dict for XCom.

**`write_alerts`** — Writes the alert payload to `fraud_alerts`, appending to existing data. Stores `rule_score`, `if_score`, `combined_score`, and `is_confirmed_fraud` alongside the raw `Time` and `Amount` values.

**`log_summary`** — Emits a structured summary log including the window bounds, total transactions processed, flagged count, and rows inserted.

**Retry policy:** 2 retries with a 5-minute delay — sufficient to recover from transient database connection failures.

---

### Stage 4 — Dashboard (Grafana)

**URL:** [http://localhost:3001](http://localhost:3001) — admin / admin  
**Refresh:** Every 30 seconds  
**Time range:** Last 48 hours (covers the full dataset window)

The dashboard and its datasource are fully provisioned from `grafana/provisioning/` — no manual setup required after `docker compose up`.

#### Panels

| Panel | Type | Description |
|---|---|---|
| **Total Alerts** | Stat | Count of all rows in `fraud_alerts` |
| **Confirmed Fraud Cases** | Stat | Rows where `is_confirmed_fraud = true` |
| **Fraud Confirmation Rate** | Stat | % of alerts that are confirmed fraud |
| **Avg Combined Score** | Stat | Mean combined_score across all alerts |
| **Dataset Fraud Rate** | Stat | Mean hourly fraud rate from `hourly_transaction_summary` |
| **Hourly Alert Volume** | Time series | Total alerts and confirmed fraud over time (`flagged_at` axis) |
| **Alert Volume by Dataset Hour** | Time series | Per-hour alert and confirmed fraud counts (hours 0–47) |
| **Recent High-Risk Transactions** | Table | Sortable, colour-coded by combined_score; shows all score components |
| **Live Fraud Rate by Dataset Hour** | Dual-axis line | Total transaction volume (left axis) vs fraud rate % (right axis) |

#### No merchant categories

The dataset is anonymised: apart from `Time` and `Amount`, every feature is a PCA component, and there is no merchant information at all. An earlier version bucketed the `Amount` z-score into made-up categories ("Electronics", "Luxury & High-Value", …) and charted them as if they were real. That panel has been removed rather than presenting synthetic labels as data.

---

## Model Performance

### Evaluation protocol

Transactions are sorted by `Time` and split into three consecutive windows. Nothing learned or tuned ever sees rows from a later window:

| Window | Share | Rows | Fraud | Used for |
|---|---|---|---|---|
| fit | first 60% | 170,884 | 360 | Isolation Forest, rule features + thresholds, Amount μ/σ |
| tune | next 20% | 56,961 | 57 | Choosing the decision threshold (best F1) |
| test | last 20% | 56,962 | 75 | Reported metrics, evaluated once |

### Held-out test results (last 20% of the timeline)

| Metric | Value |
|---|---|
| ROC-AUC | 0.9605 |
| PR-AUC (average precision) | 0.1756 |
| Precision at tuned threshold (0.4908) | 23.1% (34 / 147 alerts) |
| Recall | 45.3% (34 / 75 fraud cases) |
| F1 | 0.306 |
| False positives | 113 (0.20% of legitimate transactions) |
| False negatives | 41 |

### Reading these numbers

- **The ROC-AUC looks good, but PR-AUC tells you more here.** At a 0.13% fraud rate, ROC-AUC is dominated by the huge pool of easy negatives. PR-AUC of 0.18 means that, on average, fewer than one in five alerts is real fraud.
- **The tune window is harder than the test window** (tune F1 0.20 vs test 0.31). With only 57 and 75 fraud cases, these numbers carry wide error bars: a handful of cases moves recall by several points.
- **In-sample numbers are noticeably better** (fit-window F1 0.48). The previous version of this README reported in-sample figures (F1 0.475 from training and evaluating on all rows, with label-derived rules and contamination) as out-of-sample. They should not be compared with the table above.
- Hyperparameters such as the rule tail quantile (1%), score weights (0.4 / 0.6) and the number of trees were fixed up front, not searched against the test window.

## Database Schema

### `transactions`

| Column | Type | Description |
|---|---|---|
| Time | DOUBLE PRECISION | Seconds since first transaction (0–172,792) |
| V1 – V28 | DOUBLE PRECISION | PCA-transformed features (masked for privacy) |
| Amount | DOUBLE PRECISION | Transaction amount (raw currency units) |
| Class | INTEGER | Ground truth: 0 = legitimate, 1 = fraud |

### `fraud_alerts`

| Column | Type | Description |
|---|---|---|
| id | SERIAL | Primary key |
| batch_start_sec | DOUBLE PRECISION | Start of the dataset time window |
| batch_end_sec | DOUBLE PRECISION | End of the dataset time window |
| Time | DOUBLE PRECISION | Transaction time within the dataset |
| Amount | DOUBLE PRECISION | Transaction amount |
| rule_score | DOUBLE PRECISION | Rule engine score [0, 1] |
| if_score | DOUBLE PRECISION | Isolation Forest score [0, 1] |
| combined_score | DOUBLE PRECISION | Weighted combined score [0, 1] |
| is_confirmed_fraud | BOOLEAN | True if Class == 1 |
| flagged_at | TIMESTAMPTZ | Wall-clock time when the alert was written |

### Views

**`fraud_alerts_enriched`** — Adds `dataset_hour` to every `fraud_alerts` row. Used by the Alert Volume by Dataset Hour panel.

**`hourly_transaction_summary`** — Aggregates `transactions` by one-hour buckets: total count, confirmed fraud count, and fraud rate percentage. Used by the Live Fraud Rate panel.

---

## Script Reference

| Task | Command |
|---|---|
| Start everything | `docker compose up -d --build` |
| Stop everything | `docker compose down` |
| Ingest & clean | `python src/ingest_and_clean.py [--if-empty]` |
| Train model | `python src/train_detector.py [--csv data/creditcard.csv] [--if-missing]` |
| Test ETL (no Airflow) | `python src/test_etl_run.py` |
| Populate all windows | `python scripts/populate_alerts.py` |
| DVC pull data | `dvc pull` |

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'psycopg2'`** (host scripts)
Install the pinned requirements in the Python environment you are actually running:
```bash
pip install -r requirements.txt
```

**`cannot drop table fraud_alerts because other objects depend on it`**
The `fraud_alerts_enriched` view depends on the table. Drop with cascade:
```sql
DROP TABLE fraud_alerts CASCADE;
```
Then re-run `python src/ingest_and_clean.py --if-empty`, which re-applies `scripts/schema.sql`.

**Grafana shows "No data" on all panels**
- Confirm the `fraud_alerts` table has rows: `SELECT COUNT(*) FROM fraud_alerts;`
- If empty, run `python scripts/populate_alerts.py`
- Check the Grafana time range — set it to "Last 48 hours" or "Last 7 days"
- Confirm the datasource can connect: Connections → Data Sources → FraudDB → Test

**Airflow DAG not appearing**
- Check for parse errors: `docker compose exec airflow airflow dags list-import-errors`

**Port already in use**
- Grafana (3001), Airflow (8080) and Postgres (5433) are published on the host. Stop whatever holds the port, or change the left-hand side of the `ports:` mapping in `docker-compose.yml` (Postgres: set `POSTGRES_HOST_PORT` in `.env`).

**DVC pull fails on a new machine**
Configure the remote to point to wherever `dvc-storage` was placed:
```bash
dvc remote modify local_storage url /absolute/path/to/dvc-storage
dvc pull
```
