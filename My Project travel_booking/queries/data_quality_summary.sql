-- Databricks notebook source
DECLARE arrival_date DATE DEFAULT current_date();

CREATE TABLE IF NOT EXISTS travel_bookings.ops.dq_daily_summary (
  business_date DATE,
  dataset STRING,
  checks_passed INT,
  checks_failed INT,
  recorded_at TIMESTAMP
) USING DELTA;

MERGE INTO travel_bookings.ops.dq_daily_summary t
USING (
  SELECT
    COALESCE(TRY_CAST(arrival_date AS DATE), current_date()) AS business_date,
    dataset,
    SUM(CASE WHEN constraint_status = 'Success' THEN 1 ELSE 0 END) AS checks_passed,
    SUM(CASE WHEN constraint_status <> 'Success' THEN 1 ELSE 0 END) AS checks_failed,
    current_timestamp() AS recorded_at
  FROM travel_bookings.ops.dq_results
  WHERE business_date = COALESCE(TRY_CAST(arrival_date AS DATE), current_date())
  GROUP BY dataset
) s
ON t.business_date = s.business_date AND t.dataset = s.dataset
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;