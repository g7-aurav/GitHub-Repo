-- Databricks notebook source
DECLARE arrival_date DATE DEFAULT current_date();

INSERT INTO travel_bookings.ops.workflow2_run_log
SELECT
  CONCAT('wf2-', STRING(current_timestamp())) AS run_id,
  COALESCE(TRY_CAST(arrival_date AS DATE), current_date()) AS arrival_date,
  'SUCCESS' AS status,
  'Completed SQL workflow' AS message,
  current_timestamp() AS recorded_at;