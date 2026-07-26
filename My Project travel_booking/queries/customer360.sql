-- Databricks notebook source
CREATE OR REPLACE VIEW travel_bookings.analytics.customer_360 AS
SELECT

  d.customer_sk,                   
  d.customer_id,                    
  d.customer_name,                  
  d.customer_address,               
  d.email,                          

  f.booking_type,                   
  

  SUM(f.total_amount_sum) AS lifetime_amount,    
  SUM(f.total_quantity_sum) AS lifetime_quantity 

FROM travel_bookings.default.customer_dim d
JOIN travel_bookings.default.booking_fact f
  ON f.customer_sk <=> d.customer_sk  
WHERE d.is_current = true              
GROUP BY d.customer_sk, d.customer_id, d.customer_name, d.customer_address, d.email, f.booking_type;