-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Hubs
-- MAGIC
-- MAGIC * hub_users
-- MAGIC * hub_vehicles
-- MAGIC * hub_payments
-- MAGIC * hub_subscriptions

-- COMMAND ----------

-- DBTITLE 1,hub_users
CREATE OR REFRESH STREAMING LIVE TABLE hub_users
(
  hk_cpf STRING NOT NULL,
  cpf STRING NOT NULL,
  load_ts TIMESTAMP,
  source STRING
  
  CONSTRAINT valid_hk_cpf EXPECT (hk_cpf IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_cpf EXPECT (cpf IS NOT NULL) ON VIOLATION DROP ROW
)
AS 
SELECT DISTINCT
  hk_cpf,
  cpf,
  load_ts,
  "mssql" AS source
FROM STREAM(live.raw_vw_py_mssql_user)
UNION ALL
SELECT DISTINCT
  hk_cpf,
  cpf,
  load_ts,
  "mongodb" AS source
FROM STREAM(live.raw_vw_py_mongodb_user)

-- COMMAND ----------

-- DBTITLE 1,hub_vehicles
CREATE OR REFRESH STREAMING LIVE TABLE hub_vehicles
(
  hk_vehicle_id STRING NOT NULL,
  id LONG NOT NULL,
  load_ts TIMESTAMP,
  source STRING
  CONSTRAINT valid_hk_vehicle_id EXPECT (hk_vehicle_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
)
AS 
SELECT DISTINCT
  hk_vehicle_id,
  id,
  load_ts,
  "postgres" AS source
FROM STREAM(live.raw_vw_py_postgres_vehicle)

-- COMMAND ----------

-- DBTITLE 1,hub_payments
CREATE OR REFRESH STREAMING LIVE TABLE hub_payments
(
  hk_txn_id STRING NOT NULL,
  txn_id STRING NOT NULL,
  load_ts TIMESTAMP,
  source STRING
  CONSTRAINT valid_hk_txn_id EXPECT (hk_txn_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_txn_id EXPECT (txn_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS 
SELECT DISTINCT
  hk_txn_id,
  txn_id,
  load_ts,
  "postgres" AS source
FROM STREAM(live.raw_vw_py_postgres_payments)

-- COMMAND ----------

-- DBTITLE 1,hub_subscriptions
CREATE OR REFRESH STREAMING LIVE TABLE hub_subscriptions
(
  hk_subscription_id STRING NOT NULL,
  id LONG NOT NULL,
  load_ts TIMESTAMP,
  source STRING
  CONSTRAINT valid_hk_subscription_id EXPECT (hk_subscription_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW
)
AS 
SELECT DISTINCT
  hk_subscription_id,
  id,
  load_ts,
  "postgres" AS source
FROM STREAM(live.raw_vw_py_postgres_subscription)