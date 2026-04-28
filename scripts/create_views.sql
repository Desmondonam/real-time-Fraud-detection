-- Dashboard views for Grafana

CREATE OR REPLACE VIEW fraud_alerts_enriched AS
SELECT
  fa.*,
  CASE
    WHEN fa."Amount" < -1.0 THEN 'Grocery & Food'
    WHEN fa."Amount" < 0.0  THEN 'Retail & Shopping'
    WHEN fa."Amount" < 0.5  THEN 'Entertainment'
    WHEN fa."Amount" < 1.5  THEN 'Travel & Transport'
    WHEN fa."Amount" < 2.5  THEN 'Electronics'
    ELSE                         'Luxury & High-Value'
  END AS merchant_category,
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
