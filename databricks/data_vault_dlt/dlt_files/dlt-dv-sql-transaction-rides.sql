-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Link & Satellite [Rides]
-- MAGIC
-- MAGIC
-- MAGIC ### Link
-- MAGIC * lnk_rides
-- MAGIC
-- MAGIC ### Satellite
-- MAGIC * sat_mongodb_rides
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,lnk_rides
CREATE OR REFRESH STREAMING LIVE TABLE lnk_rides
(
  lnk_rides_id STRING NOT NULL,  
  hk_cpf STRING NOT NULL, 
  hk_vehicle_id STRING NOT NULL,
  hk_txn_id STRING NOT NULL,
  -- hk_subscription_id STRING,
  id STRING NOT NULL,
  cpf STRING NOT NULL,
  vehicle_id LONG NOT NULL,
  txn_id STRING NOT NULL,
  -- subscription_id LONG NOT NULL,
  load_ts TIMESTAMP  NOT NULL,
  source STRING NOT NULL
)
AS SELECT
  sha1(CONCAT(users.hk_cpf, vehicles.hk_vehicle_id, payments.hk_txn_id)) AS lnk_rides_id,
  users.hk_cpf,
  vehicles.hk_vehicle_id,
  payments.hk_txn_id,
  -- subscriptions.hk_subscription_id,
  rides.id,
  rides.cpf,
  rides.vehicle_id,
  rides.txn_id,
  current_timestamp AS load_ts,
  "mongodb" AS source
FROM STREAM(live.raw_vw_py_mongodb_rides) AS rides
INNER JOIN STREAM(live.hub_users) AS users
ON rides.cpf = users.cpf
INNER JOIN STREAM(live.hub_vehicles) AS vehicles
ON rides.vehicle_id = vehicles.id
INNER JOIN STREAM(live.hub_payments) AS payments
ON rides.txn_id = payments.txn_id
-- INNER JOIN STREAM(live.hub_subscriptions) AS subscriptions
-- ON rides.subscription_id = subscriptions.id

-- COMMAND ----------

-- DBTITLE 1,sat_mongodb_rides
CREATE OR REFRESH STREAMING LIVE TABLE sat_mongodb_rides
(
  lnk_rides_id STRING NOT NULL,
  id STRING,
  user_id LONG,
  vehicle_id LONG,
  txn_id STRING,
  subscription_id LONG,
  product_id STRING,
  loc_source STRING,
  loc_destination STRING,
  distance_miles DOUBLE,
  distance_km DOUBLE,
  name STRING,
  cab_type STRING,
  price_usd DOUBLE,
  price_brl DOUBLE,
  dynamic_fare DOUBLE,
  time_stamp LONG,  
  load_ts TIMESTAMP,
  source STRING

  CONSTRAINT valid_lnk_rides_id EXPECT (lnk_rides_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  lnk_rides.lnk_rides_id,
  rw_rides.id,
  rw_rides.user_id,
  rw_rides.vehicle_id,
  rw_rides.txn_id,
  rw_rides.subscription_id,
  rw_rides.product_id,
  rw_rides.loc_source,
  rw_rides.loc_destination,
  rw_rides.distance_miles,
  rw_rides.distance_km,
  rw_rides.name,
  rw_rides.cab_type,
  rw_rides.price_usd,
  rw_rides.price_brl,
  rw_rides.dynamic_fare,
  rw_rides.time_stamp,  
  current_timestamp() as load_ts,
  "mongodb" as source  
FROM STREAM(live.raw_vw_py_mongodb_rides) AS rw_rides
INNER JOIN STREAM(live.lnk_rides_str) AS lnk_rides
ON rw_rides.id = lnk_rides.id
  AND rw_rides.vehicle_id = lnk_rides.vehicle_id
  AND rw_rides.cpf = lnk_rides.cpf
  AND rw_rides.txn_id = lnk_rides.txn_id