-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Raw vault<VIEWS>
-- MAGIC * raw_vw_mssql_user
-- MAGIC * raw_vw_mongodb_users
-- MAGIC * raw_vw_postgres_vehicle
-- MAGIC * raw_vw_postgres_payments
-- MAGIC * raw_vw_postgres_subscription
-- MAGIC * raw_vw_mongodb_ride

-- COMMAND ----------

-- DBTITLE 1,raw_vw_mssql_user
CREATE STREAMING LIVE VIEW raw_vw_mssql_user
AS
SELECT
  sha1(UPPER(TRIM(cpf))) AS hk_cpf,
  current_timestamp() AS load_ts,
  "mssql" AS source,
  cpf,
  user_id,
  uuid,
  first_name,
  last_name,
  date_birth,
  city,
  country,
  company_name
FROM STREAM(Live.bronze_mssql_users)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_mongodb_users
CREATE STREAMING LIVE VIEW raw_vw_mongodb_users
AS
SELECT
  sha1(UPPER(TRIM(cpf))) AS hk_cpf,
  current_timestamp() AS load_ts,
  "mongodb" AS source,
  cpf,
  user_id,
  first_name,
  last_name,
  date_of_birth,
  gender,
  social_insurance_number,
  email
FROM STREAM(Live.bronze_mongodb_users)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_postgres_vehicle
CREATE STREAMING LIVE VIEW raw_vw_postgres_vehicle
AS
SELECT
  sha1(UPPER(TRIM(cpf))) AS hk_cpf,
  current_timestamp() AS load_ts,
  "postgres" AS source,
  id,
  name,
  seats,
  km_driven,
  year
FROM STREAM(Live.bronze_postgres_vehicle)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_postgres_payments
CREATE STREAMING LIVE VIEW raw_vw_postgres_payments
AS
SELECT
  sha1(UPPER(TRIM(cpf))) AS hk_cpf,
  current_timestamp() AS load_ts,
  "postgres" AS source,
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
FROM STREAM(Live.bronze_postgres_payments)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_postgres_subscription
CREATE STREAMING LIVE VIEW raw_vw_postgres_subscription
AS
SELECT
  sha1(UPPER(TRIM(cpf))) AS hk_cpf,
  current_timestamp() AS load_ts,
  "postgres" AS source,
  txn_id,
  user_id,
  uid,
  payment_method,
  payment_term,
  status,
  dt_current_timestamp
FROM STREAM(Live.bronze_postgres_subscription)

-- COMMAND ----------

-- DBTITLE 1,raw_vw_mongodb_ride
CREATE STREAMING LIVE VIEW raw_vw_mongodb_ride
AS
SELECT
  sha1(UPPER(TRIM(cpf))) AS hk_cpf,
  current_timestamp() AS load_ts,
  "mongodb" AS source,
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
FROM STREAM(Live.bronze_mongodb_ride)
