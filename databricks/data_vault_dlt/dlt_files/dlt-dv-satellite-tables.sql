-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Satellites
-- MAGIC
-- MAGIC * sat_mssql_user
-- MAGIC * sat_mongodb_user
-- MAGIC * sat_postgres_vehicle
-- MAGIC * sat_postgres_payment
-- MAGIC * sat_postgres_subscription

-- COMMAND ----------

-- DBTITLE 1,sat_mssql_user
CREATE OR REFRESH STREAMING LIVE TABLE sat_mssql_user
(
  hk_cpf STRING NOT NULL,
  user_id LONG,
  uuid STRING,
  name STRING,
  date_birth DATE,
  phone_number STRING,
  company_name STRING,
  job STRING,
  city STRING,
  country STRING,  
  load_ts TIMESTAMP,
  source STRING

  CONSTRAINT valid_hk_cpf EXPECT (hk_cpf IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  hk_cpf,
  user_id,
  uuid,
  name,
  date_birth,
  phone_number,
  company_name,
  job,
  city,
  country, 
  load_ts,
  source
FROM STREAM(live.raw_vw_py_mssql_user)

-- COMMAND ----------

-- DBTITLE 1,sat_mongodb_user
CREATE OR REFRESH STREAMING LIVE TABLE sat_mongodb_user
(
  hk_cpf STRING NOT NULL,
  user_id LONG,
  uid STRING,
  name STRING,
  date_birth DATE,
  email STRING,
  title STRING,
  key_skill STRING,
  city STRING,
  country STRING,
  state STRING,
  zip_code STRING,
  phone_number STRING,
  social_insurance_number STRING,
  cc_number STRING,
  payment_method STRING,
  load_ts TIMESTAMP,
  source STRING

  CONSTRAINT valid_hk_cpf EXPECT (hk_cpf IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  hk_cpf,
  user_id,
  uid,
  name,
  date_birth,
  email,
  title,
  key_skill,
  city,
  country,
  state,
  zip_code,
  phone_number,
  social_insurance_number,
  cc_number,
  payment_method,
  load_ts,
  source
FROM STREAM(live.raw_vw_py_mongodb_user)

-- COMMAND ----------

-- DBTITLE 1,sat_postgres_vehicle
CREATE OR REFRESH STREAMING LIVE TABLE sat_postgres_vehicle
(
  hk_vehicle_id STRING NOT NULL,
  id LONG,
  name STRING,
  engine STRING,
  max_power STRING,
  torque STRING,
  mileage STRING,
  fuel STRING,
  km_driven LONG,
  transmission STRING,
  seats DOUBLE,
  seller_type STRING,
  year LONG,
  drive_type_desc STRING,
  car_classification STRING,
  load_ts TIMESTAMP,
  source STRING
  
  CONSTRAINT valid_hk_vehicle_id EXPECT (hk_vehicle_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  hk_vehicle_id,
  id,
  name,
  engine,
  max_power,
  torque,
  mileage,
  fuel,
  km_driven,
  transmission,
  seats,
  seller_type,
  year,
  drive_type_desc,
  car_classification,
  load_ts,
  source
FROM STREAM(live.raw_vw_py_postgres_vehicle)

-- COMMAND ----------

-- DBTITLE 1,sat_postgres_payment
CREATE OR REFRESH STREAMING LIVE TABLE sat_postgres_payment
(
  hk_txn_id STRING NOT NULL,
  txn_id STRING,
  txn_time STRING,
  user_id LONG,
  subscription_id LONG,
  gender STRING,
  job_title STRING,
  credit_card_type STRING,
  currency STRING,
  currency_mode STRING,
  language STRING,
  price FLOAT,
  city STRING,
  country STRING,
  load_ts TIMESTAMP,
  source STRING

  CONSTRAINT valid_hk_txn_id EXPECT (hk_txn_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  hk_txn_id,
  txn_id,
  txn_time,
  user_id,
  subscription_id,
  gender,
  job_title,
  credit_card_type,
  currency,
  currency_mode,
  language,
  price,
  city,
  country,
  load_ts,
  source
FROM STREAM(live.raw_vw_py_postgres_payments)

-- COMMAND ----------

-- DBTITLE 1,sat_postgres_subscription
CREATE OR REFRESH STREAMING LIVE TABLE sat_postgres_subscription
(
  hk_subscription_id STRING NOT NULL,
  id LONG,
  uid STRING,
  user_id LONG,
  method STRING,
  term STRING,
  plan STRING,
  status STRING,
  frequency STRING,
  importance STRING,
  price DOUBLE,
  load_ts TIMESTAMP,
  source STRING

  CONSTRAINT valid_hk_subscription_id EXPECT (hk_subscription_id IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT
  hk_subscription_id,
  id,
  uid,
  user_id,
  method,
  term,
  plan,
  status,
  frequency,
  importance,
  price,
  load_ts,
  source
FROM STREAM(live.raw_vw_py_postgres_subscription)  