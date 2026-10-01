-- Example Athena SQL against the Glue Data Catalog tables created by
-- aws/bootstrap_aws.py. Run these via the Athena console, or
-- programmatically with aws/run_athena_query.py.

-- New S3 partitions need to be registered before they're queryable.
-- Either run a Glue Crawler on a schedule, or repair on demand:
MSCK REPAIR TABLE market_pipeline.stock_window_aggregates;
MSCK REPAIR TABLE market_pipeline.stock_enriched;

-- 1. Latest 5-minute price aggregates per symbol
SELECT symbol, window_start, window_end, mean_price, stddev_price, event_count
FROM market_pipeline.stock_window_aggregates
WHERE date = CAST(current_date AS varchar)
ORDER BY window_start DESC
LIMIT 50;

-- 2. Anomalies detected in the last hour, worst first
SELECT symbol, event_time, price, mean_price, stddev_price, zscore
FROM market_pipeline.stock_enriched
WHERE is_anomaly = true
  AND date = CAST(current_date AS varchar)
  AND hour = CAST(hour(current_timestamp) AS int)
ORDER BY abs(zscore) DESC;

-- 3. Anomaly rate by sector (cost-aware: scoped to one partition-day)
SELECT sector,
       count(*) AS total_events,
       sum(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomalies,
       round(100.0 * sum(CASE WHEN is_anomaly THEN 1 ELSE 0 END) / count(*), 2) AS anomaly_pct
FROM market_pipeline.stock_enriched
WHERE date = CAST(current_date AS varchar)
GROUP BY sector
ORDER BY anomaly_pct DESC;

-- 4. Volatility ranking (highest stddev windows) per symbol over the last day
SELECT symbol, window_start, stddev_price
FROM market_pipeline.stock_window_aggregates
WHERE date = CAST(current_date AS varchar)
ORDER BY stddev_price DESC
LIMIT 20;
