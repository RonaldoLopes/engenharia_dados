-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Landing Zone
-- MAGIC * com.owshq.data/mssql/users
-- MAGIC * com.owshq.data/mongodb/users
-- MAGIC * com.owshq.data/postgres/vehicle
-- MAGIC * com.owshq.data/postgres/payments
-- MAGIC * com.owshq.data/postgres/subscription
-- MAGIC * com.owshq.data/mongodb/ride
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,bronze_mssql_users
CREATE OR REFRESH STREAMING TABLE bronze_mssql_users
AS SELECT *, _metadata.file_path AS source_file_path FROM STREAM read_files(
  'dbfs:/mnt/owshq-stg-files/com.owshq.data/mssql/users/',
  format => 'json'
)

-- COMMAND ----------

-- DBTITLE 1,bronze_mongodb_users
CREATE OR REFRESH STREAMING TABLE bronze_mongodb_users
AS SELECT *, _metadata.file_path AS source_file_path FROM STREAM read_files(
  'dbfs:/mnt/owshq-stg-files/com.owshq.data/mongodb/users/',
  format => 'json'
)

-- COMMAND ----------

-- DBTITLE 1,bronze_postgres_vehicle
CREATE OR REFRESH STREAMING TABLE bronze_postgres_vehicle
AS SELECT *, _metadata.file_path AS source_file_path FROM STREAM read_files(
  'dbfs:/mnt/owshq-stg-files/com.owshq.data/postgres/vehicle/',
  format => 'json'
)

-- COMMAND ----------

-- DBTITLE 1,bronze_postgres_payments
CREATE OR REFRESH STREAMING TABLE bronze_postgres_payments
AS SELECT *, _metadata.file_path AS source_file_path FROM STREAM read_files(
  'dbfs:/mnt/owshq-stg-files/com.owshq.data/postgres/payments/',
  format => 'json'
)

-- COMMAND ----------

-- DBTITLE 1,bronze_postgres_subscription
CREATE OR REFRESH STREAMING TABLE bronze_postgres_subscription
AS SELECT *, _metadata.file_path AS source_file_path FROM STREAM read_files(
  'dbfs:/mnt/owshq-stg-files/com.owshq.data/postgres/subscription/',
  format => 'json'
)

-- COMMAND ----------

-- DBTITLE 1,bronze_mongodb_rides
CREATE OR REFRESH STREAMING TABLE bronze_mongodb_rides
AS SELECT *, _metadata.file_path AS source_file_path FROM STREAM read_files(
  'dbfs:/mnt/owshq-stg-files/com.owshq.data/mongodb/rides/',
  format => 'json'
)
