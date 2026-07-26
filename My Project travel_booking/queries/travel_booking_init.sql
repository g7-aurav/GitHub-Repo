-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS travel_bookings.analytics;
CREATE SCHEMA IF NOT EXISTS travel_bookings.ops;
CREATE TABLE IF NOT EXISTS travel_bookings.ops.workflow2_run_log (
   run_id STRING,
   arrival_date DATE,
   status STRING,
   message STRING,
   recorded_at TIMESTAMP
) USING DELTA;
