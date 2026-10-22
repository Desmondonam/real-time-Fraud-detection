-- Fraud detection schema. Idempotent: safe to run on every start-up.
-- Applied by src/ingest_and_clean.py (and therefore by the compose bootstrap).

-- Source of truth for the replayed stream and for (delayed) fraud labels.
-- txn_id is the row's position in creditcard.csv; the producer uses the same id.
CREATE TABLE IF NOT EXISTS transactions (
  txn_id   BIGINT PRIMARY KEY,
  "Time"   DOUBLE PRECISION NOT NULL,
  "V1"  DOUBLE PRECISION, "V2"  DOUBLE PRECISION, "V3"  DOUBLE PRECISION, "V4"  DOUBLE PRECISION,
  "V5"  DOUBLE PRECISION, "V6"  DOUBLE PRECISION, "V7"  DOUBLE PRECISION, "V8"  DOUBLE PRECISION,
  "V9"  DOUBLE PRECISION, "V10" DOUBLE PRECISION, "V11" DOUBLE PRECISION, "V12" DOUBLE PRECISION,
  "V13" DOUBLE PRECISION, "V14" DOUBLE PRECISION, "V15" DOUBLE PRECISION, "V16" DOUBLE PRECISION,
  "V17" DOUBLE PRECISION, "V18" DOUBLE PRECISION, "V19" DOUBLE PRECISION, "V20" DOUBLE PRECISION,
  "V21" DOUBLE PRECISION, "V22" DOUBLE PRECISION, "V23" DOUBLE PRECISION, "V24" DOUBLE PRECISION,
  "V25" DOUBLE PRECISION, "V26" DOUBLE PRECISION, "V27" DOUBLE PRECISION, "V28" DOUBLE PRECISION,
  "Amount" DOUBLE PRECISION,
  "Class"  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS transactions_time_idx ON transactions ("Time");

-- One row per event scored by the Kafka consumer (src/streaming/consumer.py).
CREATE TABLE IF NOT EXISTS scored_transactions (
  txn_id          BIGINT PRIMARY KEY,
  event_time_sec  DOUBLE PRECISION NOT NULL,   -- dataset "Time" of the event
  amount          DOUBLE PRECISION,
  rule_score      DOUBLE PRECISION,
  if_score        DOUBLE PRECISION,
  combined_score  DOUBLE PRECISION,
  is_high_risk    BOOLEAN NOT NULL,
  threshold       DOUBLE PRECISION,
  model_version   TEXT,
  split           TEXT,                        -- fit / tune / test window of that model
  produced_at     TIMESTAMPTZ,                 -- when the producer sent the event
  scored_at       TIMESTAMPTZ NOT NULL,
  latency_ms      DOUBLE PRECISION             -- scored_at - produced_at
);

CREATE INDEX IF NOT EXISTS scored_transactions_scored_at_idx ON scored_transactions (scored_at);

-- Held-out metrics of each promoted model (written by src/train_detector.py).
CREATE TABLE IF NOT EXISTS model_metrics (
  id               SERIAL PRIMARY KEY,
  model_version    TEXT NOT NULL,
  split            TEXT NOT NULL,
  n_rows           INTEGER,
  fraud_rows       INTEGER,
  roc_auc          DOUBLE PRECISION,
  pr_auc           DOUBLE PRECISION,
  alert_precision  DOUBLE PRECISION,
  recall           DOUBLE PRECISION,
  f1               DOUBLE PRECISION,
  threshold        DOUBLE PRECISION,
  recorded_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── Dashboard views for Grafana ──────────────────────────────────────────────

-- Alerts = high-risk scored events, joined with the label that a chargeback
-- feed would deliver later. The consumer never sees "Class".
CREATE OR REPLACE VIEW fraud_alerts AS
SELECT
  s.*,
  t."Class" = 1 AS is_confirmed_fraud
FROM scored_transactions s
LEFT JOIN transactions t USING (txn_id)
WHERE s.is_high_risk;

CREATE OR REPLACE VIEW hourly_transaction_summary AS
SELECT
  FLOOR(t."Time" / 3600)::int                                        AS hour_bucket,
  COUNT(*)                                                            AS total_transactions,
  SUM(CASE WHEN t."Class" = 1 THEN 1 ELSE 0 END)                    AS actual_fraud,
  ROUND(
    100.0 * SUM(CASE WHEN t."Class" = 1 THEN 1 ELSE 0 END)
    / NULLIF(COUNT(*), 0), 4
  )                                                                   AS fraud_rate_pct
FROM transactions t
GROUP BY 1
ORDER BY 1;
