-- Fraud detection schema. Idempotent: safe to run on every start-up.
-- Applied by src/ingest_and_clean.py (and therefore by the compose bootstrap).

CREATE TABLE IF NOT EXISTS transactions (
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

CREATE TABLE IF NOT EXISTS fraud_alerts (
  id                 SERIAL PRIMARY KEY,
  batch_start_sec    DOUBLE PRECISION NOT NULL,
  batch_end_sec      DOUBLE PRECISION NOT NULL,
  "Time"             DOUBLE PRECISION,
  "Amount"           DOUBLE PRECISION,
  rule_score         DOUBLE PRECISION,
  if_score           DOUBLE PRECISION,
  combined_score     DOUBLE PRECISION,
  is_confirmed_fraud BOOLEAN,
  flagged_at         TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ── Dashboard views for Grafana ──────────────────────────────────────────────

CREATE OR REPLACE VIEW fraud_alerts_enriched AS
SELECT
  fa.*,
  FLOOR(fa.batch_start_sec / 3600)::int AS dataset_hour
FROM fraud_alerts fa;

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
