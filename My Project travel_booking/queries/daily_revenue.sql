-- Databricks notebook source
CREATE WIDGET TEXT arrival_date DEFAULT '';

CREATE TABLE IF NOT EXISTS travel_bookings.analytics.daily_revenue_by_type (
  business_date DATE,
  booking_type STRING,
  total_amount DOUBLE,
  total_quantity BIGINT
) USING DELTA
TBLPROPERTIES (
  delta.autoOptimize.optimizeWrite = true,    -- Auto-optimize writes
  delta.autoOptimize.autoCompact = true       -- Auto-compact small files
);
DELETE FROM travel_bookings.analytics.daily_revenue_by_type
WHERE business_date = COALESCE(TRY_CAST(:arrival_date AS DATE), current_date());
INSERT INTO travel_bookings.analytics.daily_revenue_by_type (business_date, booking_type, total_amount, total_quantity)
SELECT
  COALESCE(TRY_CAST(:arrival_date AS DATE), current_date()) AS business_date,
  booking_type,
  CAST(SUM(CAST(amount AS DOUBLE) - CAST(discount AS DOUBLE)) AS DOUBLE) AS total_amount,
  CAST(SUM(CAST(quantity AS BIGINT)) AS BIGINT) AS total_quantity
FROM travel_bookings.bronze.booking_inc
WHERE business_date = COALESCE(TRY_CAST(:arrival_date AS DATE), current_date())
GROUP BY booking_type;