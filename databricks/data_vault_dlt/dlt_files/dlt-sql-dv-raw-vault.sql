-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Raw Vault [SQL]
-- MAGIC
-- MAGIC * raw_vw_mssql_user
-- MAGIC * raw_vw_mongodb_user
-- MAGIC * raw_vw_postgres_vehicle
-- MAGIC * raw_vw_postgres_payments
-- MAGIC * raw_vw_postgres_subscription
-- MAGIC * raw_vw_mongodb_rides

-- COMMAND ----------

-- DBTITLE 1,raw_vw_mssql_user
CREATE STREAMING LIVE VIEW raw_vw_mssql_user
AS  
SELECT
  sha1(UPPER(TRIM(cpf))) as hk_cpf,
  current_timestamp() as load_ts,
  "mssql" as source,
  cpf,
  user_id,
  uuid,
  first_name,
  last_name,
  date_birth,
  city,
  country, 
  company_name
FROM STREAM(LIVE.bronze_mssql_users)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_mongodb_user
CREATE STREAMING LIVE VIEW raw_vw_mongodb_user
AS  
SELECT
  sha1(UPPER(TRIM(cpf))) as hk_cpf,
  current_timestamp() as load_ts,
  "mongodb" as source,
  cpf,
  user_id,
  first_name,
  last_name,
  date_of_birth,
  gender,
  social_insurance_number,
  email
FROM STREAM(LIVE.bronze_mongodb_users)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_postgres_vehicle
CREATE STREAMING LIVE VIEW raw_vw_postgres_vehicle
AS  
SELECT
  sha1(UPPER(TRIM(id))) as hk_vehicle_id,
  current_timestamp() as load_ts,
  "postgres" as source,
  id,
  name,
  seats,
  km_driven,
  year
FROM STREAM(LIVE.bronze_postgres_vehicle)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_postgres_payments
CREATE STREAMING LIVE VIEW raw_vw_postgres_payments
AS  
SELECT
  sha1(UPPER(TRIM(txn_id))) as hk_txn_id,
  current_timestamp() as load_ts,
  "postgres" as source,
  txn_id,
  user_id,
  city,
  country,
  credit_card_type,
  currency,
  currency_mode,
  subscription_id,
  price,
  dt_current_timestamp,
  time
FROM STREAM(LIVE.bronze_postgres_payments)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_postgres_subscription
CREATE STREAMING LIVE VIEW raw_vw_postgres_subscription
AS  
SELECT
  sha1(UPPER(TRIM(id))) as hk_subscription_id,
  current_timestamp() as load_ts,
  "postgres" as source,
  id,
  user_id,
  uid,
  payment_method,
  payment_term,
  status,
  dt_current_timestamp
FROM STREAM(LIVE.bronze_postgres_subscription)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_mongodb_rides
CREATE STREAMING LIVE VIEW raw_vw_mongodb_rides
AS  
SELECT
  current_timestamp() as load_ts,
  "mongodb" as source,
  id,
  product_id,
  user_id,
  vehicle_id,
  txn_id,
  subscription_id,
  cpf,
  name,
  source AS src_location,
  destination AS dst_location,
  distance,
  surge_multiplier,
  price,
  cab_type,
  time_stamp,  
  dt_current_timestamp  
FROM STREAM(LIVE.bronze_mongodb_rides)